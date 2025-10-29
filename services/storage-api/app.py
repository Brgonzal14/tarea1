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
    is_cache_hit: bool # Esto podría volverse menos relevante en Tarea 2, pero lo mantenemos por ahora
    llm_answer: str | None = None
    score: float | None = None
    latency_ms: int | None = None # La latencia medida aquí será diferente en Tarea 2

# Metadatos para la corrida (igual que antes)
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
    Carga registros desde /data/yahoo.csv con tolerancia a distintos formatos.
    SALTA filas con 'question' vacía. (Sin cambios respecto a Tarea 1)
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
            has_header = True # Asumir header si falla Sniffer

        if has_header:
            rd = csv.DictReader(f)
            # Normalizar nombres de columnas
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
                if not question: continue # SALTAR VACÍAS
                best = _clean(row.get("best_answer") if has_best else row.get("answer"))

                # question_id
                id_raw = _clean(row.get("id")) if "id" in row else ""
                qid = int(id_raw) if id_raw.isdigit() else next_id
                if not id_raw.isdigit(): next_id += 1

                try:
                    cur.execute(
                        """
                        INSERT INTO qa_yahoo(question_id,class_id,title,question,best_answer)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (question_id) DO NOTHING
                        """,
                        (qid, int(klass) if klass.isdigit() else None, title, question, best),
                    )
                    if cur.rowcount > 0: inserted += 1
                    if inserted >= limit: break
                except Exception as e:
                    log.warning("Fila saltada en seed por error: %s - Data: %s", e, row)
                    c.rollback()
                    continue
        else: # Sin encabezado
            rr = csv.reader(f)
            for row in rr:
                if len(row) < 4: continue
                klass, title, question, best = map(_clean, row[:4])
                if not question: continue # SALTAR VACÍAS
                try:
                    cur.execute(
                        """
                        INSERT INTO qa_yahoo(question_id,class_id,title,question,best_answer)
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (question_id) DO NOTHING
                        """,
                        (next_id, int(klass) if klass.isdigit() else None, title, question, best),
                    )
                    if cur.rowcount > 0: inserted += 1
                    next_id += 1
                    if inserted >= limit: break
                except Exception as e:
                    log.warning("Fila saltada (sin encabezado) por error: %s - Data: %s", e, row)
                    c.rollback()
                    continue

    return {"inserted": inserted}

# ----------------- Limpieza de vacías (opcional) -----------------
@app.post("/cleanup_empty")
def cleanup_empty():
    """
    Elimina preguntas con 'question' NULL o vacía y sus resultados asociados.
    (Sin cambios respecto a Tarea 1)
    """
    try:
        with conn_cursor() as (c, cur):
            # 1) IDs de preguntas vacías
            cur.execute("SELECT question_id FROM qa_yahoo WHERE question IS NULL OR length(trim(question)) = 0")
            bad_ids = [r[0] for r in cur.fetchall()]

            if not bad_ids:
                return {"deleted_results": 0, "deleted_questions": 0, "bad_ids": []}

            # 2) Borra resultados asociados
            cur.execute("DELETE FROM qa_results WHERE question_id = ANY(%s)", (bad_ids,))
            deleted_results = cur.rowcount if cur.rowcount is not None else 0

            # 3) Borra las preguntas vacías
            cur.execute("DELETE FROM qa_yahoo WHERE question_id = ANY(%s)", (bad_ids,))
            deleted_questions = cur.rowcount if cur.rowcount is not None else 0

            return {
                "deleted_results": deleted_results,
                "deleted_questions": deleted_questions,
                "bad_ids": bad_ids[:50] # Muestra solo algunas
            }
    except Exception as e:
        log.exception("cleanup_empty failed")
        raise HTTPException(status_code=500, detail=f"cleanup_empty error: {e}")

# --- NUEVO ENDPOINT PARA TAREA 2 ---
@app.get("/results/exists/{question_id}")
def check_result_exists(question_id: int):
    """
    Verifica si ya existe algún resultado para una question_id dada en la tabla qa_results.
    Devuelve 200 OK si existe al menos uno, 404 Not Found si no existe.
    Usado por traffic-gen en Tarea 2 para decidir si publicar en Kafka.
    """
    try:
        with conn_cursor() as (c, cur):
            cur.execute(
                "SELECT 1 FROM qa_results WHERE question_id = %s LIMIT 1",
                (question_id,)
            )
            exists = cur.fetchone() is not None # fetchone() devuelve None si no hay filas

        if exists:
            # Si encontramos un resultado, devolvemos OK (código 200 por defecto)
            return {"exists": True, "question_id": question_id}
        else:
            # Si no hay resultados para esa ID, devolvemos 404
            raise HTTPException(status_code=404, detail="No results found for this question_id")

    except HTTPException:
        raise # Re-lanzar el 404
    except Exception as e:
        log.exception(f"Error al verificar existencia de resultados para question_id {question_id}")
        # Error genérico del servidor si algo más falla (ej: conexión DB)
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")

# --- FIN NUEVO ENDPOINT ---


# ----------------- Random question -----------------
@app.get("/questions/random")
def random_question():
    """
    Devuelve 1 pregunta aleatoria, garantizando que 'question' no esté vacía.
    (Sin cambios respecto a Tarea 1)
    """
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
            # Este error es más probable si la BD está vacía o si cleanup_empty borró todo
            raise HTTPException(404, "No hay datos de preguntas válidas. Ejecuta /seed primero y asegúrate que el CSV tenga preguntas no vacías.")
        return {"question_id": r[0], "title": r[1], "question": r[2], "best_answer": r[3]}

# ----------------- Record result (MODIFICADO LIGERAMENTE PARA TAREA 2) -----------------
# Este endpoint ahora será llamado por un consumidor Kafka (o Flink indirectamente)
# en lugar de directamente por traffic-gen.
# Podríamos necesitar ajustar el schema `ResultIn` si la información llega diferente desde Kafka/Flink.
@app.post("/result")
def record_result(body: ResultIn):
    """
    Inserta un resultado validado (presumiblemente proveniente de Kafka/Flink).
    Asegura que la corrida (run_id) exista.
    En Tarea 2, is_cache_hit podría ser siempre False o no aplicar.
    """
    try:
        # Ya no asumimos que score/llm_answer pueden ser None por cache hit.
        # Asumimos que si llega aquí, es un resultado completo procesado.
        llm_answer = body.llm_answer or ""
        score = float(body.score) if body.score is not None else 0.0
        # La latencia reportada aquí sería la latencia TOTAL del pipeline asíncrono,
        # lo cual es difícil de medir directamente en este punto. Podríamos omitirla
        # o calcularla de otra forma (ej: Flink podría calcularla). Por ahora, usamos lo que venga.
        latency_ms = int(body.latency_ms or 0)
        # En Tarea 2, el concepto de 'cache_hit' medido por traffic-gen ya no aplica directamente
        # al resultado final guardado aquí. Podríamos poner siempre False o eliminar la columna.
        # Por simplicidad, usamos el valor que venga, pero sabiendo que no significa lo mismo.
        is_cache_hit = body.is_cache_hit

        with conn_cursor() as (c, cur):
            # 1) Asegura que exista la corrida (UPsert en qa_runs)
            cur.execute("INSERT INTO qa_runs(run_id) VALUES (%s) ON CONFLICT (run_id) DO NOTHING", (body.run_id,))

            # 2) Inserta el resultado
            #    NOTA: Podríamos querer usar ON CONFLICT DO UPDATE aquí si Flink puede reenviar
            #    la misma question_id con un mejor score después.
            cur.execute("""
                INSERT INTO qa_results(run_id, question_id, is_cache_hit, llm_answer, score, latency_ms)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, question_id) DO UPDATE SET -- Ejemplo: Actualizar si ya existe para esta corrida
                  llm_answer = EXCLUDED.llm_answer,
                  score = EXCLUDED.score,
                  latency_ms = EXCLUDED.latency_ms, -- O quizás promediar/sumar latencias?
                  created_at = NOW() -- Actualizar timestamp
            """, (body.run_id, body.question_id, is_cache_hit, llm_answer, score, latency_ms))

        return {"ok": True}

    except Exception as e:
        log.exception(f"Error insertando/actualizando resultado para run {body.run_id}, qid {body.question_id}")
        raise HTTPException(status_code=500, detail=f"DB insert/update error: {e}")

# ----------------- Metrics (SIN CAMBIOS, pero su significado cambia) -----------------
# Este endpoint sigue funcionando, pero las métricas (hit_rate, avg_latency)
# reflejarán lo que se guardó en la tabla qa_results, que en Tarea 2
# ya no representa directamente la latencia/hit medida por traffic-gen en T1.
@app.get("/metrics")
def metrics(run_id: str):
    """Métricas simples por corrida: hit_rate y latencia promedio."""
    with conn_cursor() as (c, cur):
        # La query es la misma, pero la interpretación de los datos cambia en Tarea 2
        cur.execute("""
            SELECT
              AVG(CASE WHEN is_cache_hit THEN 1 ELSE 0 END)::float AS hit_rate, -- Este hit_rate ya no es tan significativo
              AVG(latency_ms)::float AS avg_latency -- Esta latencia ahora podría ser la total del pipeline
            FROM qa_results
            WHERE run_id=%s
        """, (run_id,))
        row = cur.fetchone()
        if not row or row[0] is None: # Si no hay datos para la corrida
             raise HTTPException(status_code=404, detail=f"No results found for run_id {run_id}")
    return {"run_id": run_id, "hit_rate": row[0], "avg_latency": row[1]}
