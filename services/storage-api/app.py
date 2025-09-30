from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from db import init_db, conn_cursor
import csv, os

# Aplicación FastAPI
app = FastAPI(title="storage-api")

# Esquema del body para /result
class ResultIn(BaseModel):
    run_id: str
    question_id: int
    is_cache_hit: bool
    llm_answer: str | None = None
    score: float | None = None
    latency_ms: int | None = None

@app.on_event("startup")
def startup():
    # Crea tablas/vistas si no existen
    init_db()

@app.post("/seed")
def seed(limit: int = 10000):
    """
    Carga registros desde /data/yahoo.csv con tolerancia a distintos formatos:
      - Con encabezado: admite nombres como:
          * class_index -> class
          * question_title -> title
          * question_content -> question
          * best_answer (o 'answer')
          * id (opcional). Si no viene, se genera.
      - Sin encabezado: se asume orden [class, title, question, best_answer]
    Inserta hasta 'limit' filas. Ignora duplicados por question_id.
    """
    path = "/data/yahoo.csv"
    if not os.path.exists(path):
        raise HTTPException(400, "No encuentro /data/yahoo.csv dentro del contenedor. ¿Montaste ./data:/data?")

    inserted = 0
    next_id = 1

    with conn_cursor() as (c, cur), open(path, "r", encoding="utf-8", newline="") as f:
        # Detectar si tiene encabezado
        sample = f.read(4096)
        f.seek(0)
        try:
            has_header = csv.Sniffer().has_header(sample)
        except Exception:
            has_header = True  # por si falla el detector

        if has_header:
            rd = csv.DictReader(f)
            # normalizar nombres a minúsculas
            rd.fieldnames = [name.strip().lower() for name in rd.fieldnames]

            # mapear nombres conocidos a los que usa la BD
            rename_map = {
                "class_index": "class",
                "question_title": "title",
                "question_content": "question",
            }
            rd.fieldnames = [rename_map.get(col, col) for col in rd.fieldnames]
            cols = set(rd.fieldnames)

            # aceptar best_answer o answer
            has_best = "best_answer" in cols
            has_ans = "answer" in cols
            if not {"class", "title", "question"}.issubset(cols) or not (has_best or has_ans):
                raise HTTPException(
                    400,
                    f"Columnas esperadas: class,title,question,(best_answer|answer). Encontré: {rd.fieldnames}"
                )

            for row in rd:
                # question_id: usar 'id' si viene, si no generar
                if "id" in row and row["id"] and row["id"].strip().isdigit():
                    qid = int(row["id"])
                else:
                    qid = next_id
                    next_id += 1

                best = row.get("best_answer") if has_best else row.get("answer")
                try:
                    cur.execute(
                        """
                        INSERT INTO qa_yahoo(question_id,class_id,title,question,best_answer)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (question_id) DO NOTHING
                        """,
                        (qid, int(row["class"]), row["title"], row["question"], best),
                    )
                    inserted += 1
                    if inserted >= limit:
                        break
                except Exception:
                    # si una fila viene mala, la saltamos
                    c.rollback()
                    continue
        else:
            # Sin encabezado: leer por posición [class, title, question, best_answer]
            rr = csv.reader(f)
            for row in rr:
                if not row or len(row) < 4:
                    continue
                klass, title, question, best = row[0], row[1], row[2], row[3]
                try:
                    cur.execute(
                        """
                        INSERT INTO qa_yahoo(question_id,class_id,title,question,best_answer)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (question_id) DO NOTHING
                        """,
                        (next_id, int(klass), title, question, best),
                    )
                    inserted += 1
                    next_id += 1
                    if inserted >= limit:
                        break
                except Exception:
                    c.rollback()
                    continue

    return {"inserted": inserted}

@app.get("/questions/random")
def random_question():
    """Devuelve 1 pregunta aleatoria (método portable)."""
    with conn_cursor() as (c, cur):
        cur.execute("""
            SELECT question_id, title, question, best_answer
            FROM qa_yahoo
            ORDER BY random()
            LIMIT 1
        """)
        r = cur.fetchone()
        if not r:
            raise HTTPException(404, "No hay datos. Ejecuta /seed primero.")
        return {"question_id": r[0], "title": r[1], "question": r[2], "best_answer": r[3]}


@app.post("/result")
def record_result(body: ResultIn):
    """Guarda resultado de una petición (hit/miss, respuesta LLM, score, latencia)."""
    with conn_cursor() as (c, cur):
        cur.execute(
            """
            INSERT INTO qa_results(run_id,question_id,is_cache_hit,llm_answer,score,latency_ms)
            VALUES(%s,%s,%s,%s,%s,%s)
            """,
            (body.run_id, body.question_id, body.is_cache_hit, body.llm_answer, body.score, body.latency_ms),
        )
    return {"ok": True}

@app.get("/metrics")
def metrics(run_id: str):
    """Métricas simples por corrida: hit_rate y latencia promedio."""
    with conn_cursor() as (c, cur):
        cur.execute(
            """
            SELECT
              AVG(CASE WHEN is_cache_hit THEN 1 ELSE 0 END)::float AS hit_rate,
              AVG(latency_ms)::float AS avg_latency
            FROM qa_results WHERE run_id=%s
            """,
            (run_id,),
        )
        row = cur.fetchone()
    return {"run_id": run_id, "hit_rate": row[0], "avg_latency": row[1]}
