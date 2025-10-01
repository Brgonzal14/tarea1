from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from db import init_db, conn_cursor
import csv, os, logging

# ----------------- App & logging -----------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("storage-api")

app = FastAPI(title="storage-api")

# ----------------- Schemas -----------------
class ResultIn(BaseModel):
    run_id: str
    question_id: int
    is_cache_hit: bool
    llm_answer: str | None = None
    score: float | None = None
    latency_ms: int | None = None

# (opcional) metadatos para la corrida
class RunMeta(BaseModel):
    run_id: str
    traffic_dist: str | None = None
    cache_policy: str | None = None
    cache_size_mb: int | None = None
    ttl_seconds: int | None = None
    notes: str | None = None

# ----------------- Startup -----------------
@app.on_event("startup")
def startup():
    init_db()
    log.info("storage-api iniciado, tablas verificadas/creadas.")

# ----------------- Health -----------------
@app.get("/health")
def health():
    return {"ok": True}

# ----------------- Runs (opcional, recomendado) -----------------
@app.post("/runs/upsert")
def runs_upsert(meta: RunMeta):
    """
    Guarda/actualiza metadatos de una corrida (para el informe).
    Si ya existe, solo actualiza campos no nulos.
    """
    with conn_cursor() as (c, cur):
        cur.execute("""
            INSERT INTO qa_runs(run_id, traffic_dist, cache_policy, cache_size_mb, ttl_seconds, notes)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id) DO UPDATE SET
              traffic_dist = COALESCE(EXCLUDED.traffic_dist, qa_runs.traffic_dist),
              cache_policy = COALESCE(EXCLUDED.cache_policy, qa_runs.cache_policy),
              cache_size_mb = COALESCE(EXCLUDED.cache_size_mb, qa_runs.cache_size_mb),
              ttl_seconds  = COALESCE(EXCLUDED.ttl_seconds,  qa_runs.ttl_seconds),
              notes        = COALESCE(EXCLUDED.notes,        qa_runs.notes)
        """, (meta.run_id, meta.traffic_dist, meta.cache_policy, meta.cache_size_mb, meta.ttl_seconds, meta.notes))
    return {"ok": True}

# ----------------- Seed -----------------
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
      - Sin encabezado: [class, title, question, best_answer]
    SALTA filas con 'question' vacía.
    """
    path = "/data/yahoo.csv"
    if not os.path.exists(path):
        raise HTTPException(400, "No encuentro /data/yahoo.csv dentro del contenedor. ¿Montaste ./data:/data?")

    inserted = 0
    next_id = 1

    def _clean(s):
        return (s or "").strip()

    with conn_cursor() as (c, cur), open(path, "r", encoding="utf-8", newline="") as f:
        # Detectar encabezado
        sample = f.read(4096)
        f.seek(0)
        try:
            has_header = csv.Sniffer().has_header(sample)
        except Exception:
            has_header = True

        if has_header:
            rd = csv.DictReader(f)
            rd.fieldnames = [name.strip().lower() for name in rd.fieldnames]
            rename_map = {"class_index": "class", "question_title": "title", "question_content": "question"}
            rd.fieldnames = [rename_map.get(col, col) for col in rd.fieldnames]
            cols = set(rd.fieldnames)

            has_best = "best_answer" in cols
            has_ans = "answer" in cols
            if not {"class", "title", "question"}.issubset(cols) or not (has_best or has_ans):
                raise HTTPException(
                    400,
                    f"Columnas esperadas: class,title,question,(best_answer|answer). Encontré: {rd.fieldnames}"
                )

            for row in rd:
                # Normaliza y filtra vacías
                klass = _clean(row.get("class"))
                title = _clean(row.get("title"))
                question = _clean(row.get("question"))
                if not question:  # SALTAR VACÍAS
                    continue
                best = _clean(row.get("best_answer") if has_best else row.get("answer"))

                # question_id
                id_raw = _clean(row.get("id")) if "id" in row else ""
                if id_raw.isdigit():
                    qid = int(id_raw)
                else:
                    qid = next_id
                    next_id += 1

                try:
                    cur.execute(
                        """
                        INSERT INTO qa_yahoo(question_id,class_id,title,question,best_answer)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (question_id) DO NOTHING
                        """,
                        (qid, int(klass) if klass.isdigit() else None, title, question, best),
                    )
                    inserted += 1
                    if inserted >= limit:
                        break
                except Exception as e:
                    log.warning("Fila saltada en seed por error: %s", e)
                    c.rollback()
                    continue
        else:
            rr = csv.reader(f)
            for row in rr:
                if not row or len(row) < 4:
                    continue
                klass, title, question, best = _clean(row[0]), _clean(row[1]), _clean(row[2]), _clean(row[3])
                if not question:  # SALTAR VACÍAS
                    continue
                try:
                    cur.execute(
                        """
                        INSERT INTO qa_yahoo(question_id,class_id,title,question,best_answer)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (question_id) DO NOTHING
                        """,
                        (next_id, int(klass) if klass.isdigit() else None, title, question, best),
                    )
                    inserted += 1
                    next_id += 1
                    if inserted >= limit:
                        break
                except Exception as e:
                    log.warning("Fila saltada (sin encabezado) por error: %s", e)
                    c.rollback()
                    continue

    return {"inserted": inserted}

# ----------------- Limpieza de vacías (opcional) -----------------
@app.post("/cleanup_empty")
def cleanup_empty():
    """
    Elimina preguntas con 'question' NULL o vacía y sus resultados asociados.
    - Paso 1: recolecta IDs 'malas'
    - Paso 2: borra qa_results de esas IDs
    - Paso 3: borra qa_yahoo de esas IDs
    """
    try:
        with conn_cursor() as (c, cur):
            # 1) IDs de preguntas vacías
            cur.execute("""
                SELECT question_id
                FROM qa_yahoo
                WHERE question IS NULL OR length(trim(question)) = 0
            """)
            bad_ids = [r[0] for r in cur.fetchall()]

            if not bad_ids:
                return {"deleted_results": 0, "deleted_questions": 0, "bad_ids": []}

            # 2) Borra resultados que referencian esas preguntas
            cur.execute(
                "DELETE FROM qa_results WHERE question_id = ANY(%s)",
                (bad_ids,)
            )
            deleted_results = cur.rowcount if cur.rowcount is not None else 0

            # 3) Borra las preguntas
            cur.execute(
                "DELETE FROM qa_yahoo WHERE question_id = ANY(%s)",
                (bad_ids,)
            )
            deleted_questions = cur.rowcount if cur.rowcount is not None else 0

            return {
                "deleted_results": deleted_results,
                "deleted_questions": deleted_questions,
                "bad_ids": bad_ids[:50]  # muestra solo algunas por si acaso
            }
    except Exception as e:
        log.exception("cleanup_empty failed")
        raise HTTPException(status_code=500, detail=f"cleanup_empty error: {e}")


# ----------------- Random question -----------------
@app.get("/questions/random")
def random_question():
    """Devuelve 1 pregunta aleatoria, garantizando que 'question' no esté vacía."""
    with conn_cursor() as (c, cur):
        cur.execute("""
            SELECT question_id, title, question, best_answer
            FROM qa_yahoo
            WHERE question IS NOT NULL AND length(trim(question)) > 0
            ORDER BY random()
            LIMIT 1
        """)
        r = cur.fetchone()
        if not r:
            raise HTTPException(404, "No hay datos (con question no vacía). Ejecuta /seed primero.")
        return {"question_id": r[0], "title": r[1], "question": r[2], "best_answer": r[3]}

# ----------------- Record result -----------------
@app.post("/result")
def record_result(body: ResultIn):
    """
    Inserta un resultado y, si la corrida (run_id) no existe en qa_runs,
    la crea antes para no romper la FK.
    También tolera None cuando hay HIT (llm_answer/score).
    """
    try:
        # Valores seguros si fue HIT (evita NULLs en columnas NOT NULL)
        llm_answer = "" if body.is_cache_hit else (body.llm_answer or "")
        score = 0.0 if body.is_cache_hit else (float(body.score) if body.score is not None else 0.0)
        latency_ms = int(body.latency_ms or 0)

        with conn_cursor() as (c, cur):
            # 1) Asegura que exista la corrida (UPsert en qa_runs)
            cur.execute("""
                INSERT INTO qa_runs(run_id) VALUES (%s)
                ON CONFLICT (run_id) DO NOTHING
            """, (body.run_id,))

            # 2) Inserta el resultado
            cur.execute("""
                INSERT INTO qa_results(run_id,question_id,is_cache_hit,llm_answer,score,latency_ms)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (body.run_id, body.question_id, body.is_cache_hit, llm_answer, score, latency_ms))

        return {"ok": True}

    except Exception as e:
        log.exception("Error insertando en qa_results")
        raise HTTPException(status_code=500, detail=f"DB insert error: {e}")

# ----------------- Metrics -----------------
@app.get("/metrics")
def metrics(run_id: str):
    """Métricas simples por corrida: hit_rate y latencia promedio."""
    with conn_cursor() as (c, cur):
        cur.execute("""
            SELECT
              AVG(CASE WHEN is_cache_hit THEN 1 ELSE 0 END)::float AS hit_rate,
              AVG(latency_ms)::float AS avg_latency
            FROM qa_results
            WHERE run_id=%s
        """, (run_id,))
        row = cur.fetchone()
    return {"run_id": run_id, "hit_rate": row[0], "avg_latency": row[1]}
