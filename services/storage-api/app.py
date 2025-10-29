from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from db import init_db, conn_cursor
import csv, os, logging
import threading # --- NUEVO (T2)
import time      # --- NUEVO (T2)
import json      # --- NUEVO (T2)
from kafka import KafkaConsumer # --- NUEVO (T2)
from kafka.errors import KafkaError # --- NUEVO (T2)

# ----------------- App & logging -----------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("storage-api")

app = FastAPI(title="storage-api")

# --- NUEVO (T2): Configuración del Consumidor Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_VALIDADOS = os.getenv('KAFKA_TOPIC_VALIDADOS', 'resultados_validados')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP_STORAGE', 'storage_consumer_group')
# Usamos el RUN_ID del entorno para asociar los resultados guardados
DEFAULT_RUN_ID = os.getenv('RUN_ID', 'default_run_t2')
log.info(f"Consumidor Kafka se iniciará para RUN_ID: {DEFAULT_RUN_ID}")
log.info(f"Escuchando en Tópico Kafka: {KAFKA_TOPIC_VALIDADOS}")
# --- FIN NUEVO (T2) ---


# ----------------- Schemas -----------------
class ResultIn(BaseModel):
    run_id: str
    question_id: int
    is_cache_hit: bool
    llm_answer: str | None = None
    score: float | None = None
    latency_ms: int | None = None

class RunMeta(BaseModel):
    run_id: str
    traffic_dist: str | None = None
    cache_policy: str | None = None
    cache_size_mb: int | None = None
    ttl_seconds: int | None = None
    notes: str | None = None

# --- NUEVO (T2): Lógica de BD refactorizada ---
def _insert_or_update_result(run_id: str, qid: int, llm_answer: str, score: float, latency_ms: int = 0, is_cache_hit: bool = False):
    """
    Función interna para insertar o actualizar un resultado en la BD.
    Usada tanto por el endpoint HTTP /result como por el consumidor Kafka.
    """
    try:
        with conn_cursor() as (c, cur):
            # 1) Asegura que exista la corrida
            cur.execute("INSERT INTO qa_runs(run_id) VALUES (%s) ON CONFLICT (run_id) DO NOTHING", (run_id,))

            # 2) Inserta o actualiza el resultado
            # (La lógica ON CONFLICT ya estaba en el app.py subido por el usuario)
            cur.execute("""
                INSERT INTO qa_results(run_id, question_id, is_cache_hit, llm_answer, score, latency_ms, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (question_id, run_id) DO UPDATE SET
                  llm_answer = EXCLUDED.llm_answer,
                  score = EXCLUDED.score,
                  latency_ms = EXCLUDED.latency_ms,
                  is_cache_hit = EXCLUDED.is_cache_hit,
                  created_at = NOW()
            """, (run_id, qid, is_cache_hit, llm_answer, score, latency_ms))

        log.info(f"Resultado guardado/actualizado para Run: {run_id}, QID: {qid}, Score: {score:.4f}")

    except Exception as e:
        log.exception(f"Error en _insert_or_update_result para Run: {run_id}, QID: {qid}: {e}")
        # No relanzamos la excepción para no matar al consumidor, solo logueamos.

# --- FIN NUEVO (T2) ---

# --- NUEVO (T2): Lógica del Consumidor Kafka ---
def run_kafka_consumer():
    """Ejecuta el consumidor Kafka en un bucle infinito (para ser usado en un thread)."""
    log.info("Iniciando hilo consumidor Kafka...")
    consumer = None

    # Bucle de reintento para la conexión inicial
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_VALIDADOS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_CONSUMER_GROUP,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                consumer_timeout_ms=10000 # Timeout de 10s para re-chequear si sigue vivo
            )
            log.info("Consumidor Kafka conectado exitosamente.")
        except KafkaError as e:
            log.error(f"Error al conectar consumidor Kafka: {e}. Reintentando en 10 segundos...")
            time.sleep(10)

    # Bucle principal de consumo
    try:
        for message in consumer:
            log.info(f"Mensaje recibido de Flink (Tópico: {message.topic}, Offset: {message.offset})")
            data = message.value

            # Formato esperado de Flink: {"question_id": ..., "llm_answer": ..., "score": ...}
            if not isinstance(data, dict) or 'question_id' not in data or 'score' not in data:
                log.warning(f"Mensaje de Kafka inválido o incompleto (saltando): {data}")
                continue

            try:
                qid = int(data['question_id'])
                llm_answer = str(data.get('llm_answer', ''))
                score = float(data['score'])

                # Usar la función refactorizada para guardar en la BD
                # Usamos el RUN_ID del entorno. Latency y cache_hit no son relevantes desde Flink.
                _insert_or_update_result(
                    run_id=DEFAULT_RUN_ID,
                    qid=qid,
                    llm_answer=llm_answer,
                    score=score,
                    latency_ms=0, # Latencia no es medida aquí
                    is_cache_hit=False # Cache hit no aplica en este punto
                )

            except (ValueError, TypeError) as e:
                log.error(f"Error al parsear datos del mensaje Kafka: {e}. Mensaje: {data}")
            except Exception as e:
                 log.error(f"Error inesperado al procesar mensaje de Kafka QID {data.get('question_id')}: {e}")

    except KeyboardInterrupt:
        log.info("Cerrando consumidor Kafka (KeyboardInterrupt)")
    except Exception as e:
         log.exception(f"Error crítico en el bucle del consumidor Kafka: {e}")
    finally:
        if consumer:
            consumer.close()
            log.info("Consumidor Kafka cerrado.")

# --- FIN NUEVO (T2) ---

# ----------------- Startup -----------------
@app.on_event("startup")
def startup():
    init_db()
    log.info("storage-api iniciado, tablas verificadas/creadas.")

    # --- MODIFICADO (T2) ---
    log.info("Iniciando consumidor Kafka en un hilo separado...")
    consumer_thread = threading.Thread(target=run_kafka_consumer, daemon=True)
    consumer_thread.start()
    # --- FIN MODIFICACIÓN (T2) ---

# ----------------- Health -----------------
@app.get("/health")
def health():
    return {"ok": True}

# ----------------- Runs (opcional, recomendado) -----------------
@app.post("/runs/upsert")
def runs_upsert(meta: RunMeta):
    # (Sin cambios)
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
    # (Sin cambios, usando el app.py subido como base)
    path = "/data/yahoo.csv"
    if not os.path.exists(path):
        raise HTTPException(400, "No encuentro /data/yahoo.csv dentro del contenedor. ¿Montaste ./data:/data?")

    inserted = 0
    next_id = 1
    def _clean(s): return (s or "").strip()

    with conn_cursor() as (c, cur), open(path, "r", encoding="utf-8", newline="") as f:
        sample = f.read(4096); f.seek(0)
        try: has_header = csv.Sniffer().has_header(sample)
        except Exception: has_header = True

        if has_header:
            rd = csv.DictReader(f)
            rd.fieldnames = [name.strip().lower() for name in rd.fieldnames]
            rename_map = {"class_index": "class", "question_title": "title", "question_content": "question"}
            rd.fieldnames = [rename_map.get(col, col) for col in rd.fieldnames]
            cols = set(rd.fieldnames)
            has_best = "best_answer" in cols
            has_ans = "answer" in cols
            if not {"class", "title", "question"}.issubset(cols) or not (has_best or has_ans):
                raise HTTPException(400, f"Columnas esperadas: class,title,question,(best_answer|answer). Encontré: {rd.fieldnames}")

            for row in rd:
                klass, title, question = _clean(row.get("class")), _clean(row.get("title")), _clean(row.get("question"))
                if not question: continue
                best = _clean(row.get("best_answer") if has_best else row.get("answer"))
                id_raw = _clean(row.get("id")) if "id" in row else ""
                qid = int(id_raw) if id_raw.isdigit() else next_id
                if not id_raw.isdigit(): next_id += 1
                try:
                    cur.execute(
                        "INSERT INTO qa_yahoo(question_id,class_id,title,question,best_answer) VALUES(%s,%s,%s,%s,%s) ON CONFLICT (question_id) DO NOTHING",
                        (qid, int(klass) if klass.isdigit() else None, title, question, best),
                    )
                    if cur.rowcount > 0: inserted += 1
                    if inserted >= limit: break
                except Exception as e: log.warning(f"Fila saltada en seed por error: {e} - Data: {row}"); c.rollback()
        else:
            rr = csv.reader(f)
            for row in rr:
                if len(row) < 4: continue
                klass, title, question, best = map(_clean, row[:4])
                if not question: continue
                try:
                    cur.execute(
                        "INSERT INTO qa_yahoo(question_id,class_id,title,question,best_answer) VALUES(%s,%s,%s,%s,%s) ON CONFLICT (question_id) DO NOTHING",
                        (next_id, int(klass) if klass.isdigit() else None, title, question, best),
                    )
                    if cur.rowcount > 0: inserted += 1
                    next_id += 1
                    if inserted >= limit: break
                except Exception as e: log.warning(f"Fila saltada (sin encabezado) por error: {e} - Data: {row}"); c.rollback()
    return {"inserted": inserted}

# ----------------- Limpieza de vacías (opcional) -----------------
@app.post("/cleanup_empty")
def cleanup_empty():
    # (Sin cambios)
    try:
        with conn_cursor() as (c, cur):
            cur.execute("SELECT question_id FROM qa_yahoo WHERE question IS NULL OR length(trim(question)) = 0")
            bad_ids = [r[0] for r in cur.fetchall()]
            if not bad_ids: return {"deleted_results": 0, "deleted_questions": 0, "bad_ids": []}
            cur.execute("DELETE FROM qa_results WHERE question_id = ANY(%s)", (bad_ids,)); deleted_results = cur.rowcount or 0
            cur.execute("DELETE FROM qa_yahoo WHERE question_id = ANY(%s)", (bad_ids,)); deleted_questions = cur.rowcount or 0
            return {"deleted_results": deleted_results, "deleted_questions": deleted_questions, "bad_ids": bad_ids[:50]}
    except Exception as e:
        log.exception("cleanup_empty failed"); raise HTTPException(status_code=500, detail=f"cleanup_empty error: {e}")

# --- Endpoint de Verificación (T2) ---
@app.get("/results/exists/{question_id}")
def check_result_exists(question_id: int):
    # (Sin cambios, ya estaba en el app.py subido)
    try:
        with conn_cursor() as (c, cur):
            cur.execute("SELECT 1 FROM qa_results WHERE question_id = %s LIMIT 1", (question_id,))
            exists = cur.fetchone() is not None
        if exists:
            return {"exists": True, "question_id": question_id}
        else:
            raise HTTPException(status_code=404, detail="No results found for this question_id")
    except HTTPException: raise
    except Exception as e:
        log.exception(f"Error al verificar existencia de resultados para question_id {question_id}")
        raise HTTPException(status_code=500, detail=f"Database query error: {e}")

# ----------------- Random question -----------------
@app.get("/questions/random")
def random_question():
    # (Sin cambios)
    with conn_cursor() as (c, cur):
        cur.execute("""
            SELECT question_id, title, question, best_answer
            FROM qa_yahoo WHERE question IS NOT NULL AND length(trim(question)) > 0
            ORDER BY random() LIMIT 1
        """)
        r = cur.fetchone()
        if not r:
            raise HTTPException(404, "No hay datos de preguntas válidas. Ejecuta /seed primero.")
        return {"question_id": r[0], "title": r[1], "question": r[2], "best_answer": r[3]}

# ----------------- Record result (MODIFICADO T2) -----------------
# Este endpoint HTTP ahora es solo una forma de insertar/actualizar manualmente si es necesario.
# La lógica principal de inserción ahora es asíncrona a través del consumidor Kafka.
@app.post("/result")
def record_result(body: ResultIn):
    """
    Inserta o actualiza un resultado manualmente.
    La inserción principal ahora ocurre a través del consumidor Kafka.
    """
    try:
        # Llama a la función refactorizada
        _insert_or_update_result(
            run_id=body.run_id,
            qid=body.question_id,
            llm_answer=body.llm_answer or "",
            score=float(body.score) if body.score is not None else 0.0,
            latency_ms=int(body.latency_ms or 0),
            is_cache_hit=body.is_cache_hit
        )
        return {"ok": True}

    except Exception as e:
        log.exception(f"Error en endpoint /result para run {body.run_id}, qid {body.question_id}")
        raise HTTPException(status_code=500, detail=f"DB insert/update error: {e}")

# ----------------- Metrics -----------------
@app.get("/metrics")
def metrics(run_id: str):
    # (Sin cambios, pero la interpretación de las métricas cambiará)
    with conn_cursor() as (c, cur):
        cur.execute("""
            SELECT
              AVG(CASE WHEN is_cache_hit THEN 1 ELSE 0 END)::float AS hit_rate,
              AVG(latency_ms)::float AS avg_latency
            FROM qa_results
            WHERE run_id=%s
        """, (run_id,))
        row = cur.fetchone()
        if not row or row[0] is None:
             # Devolver 404 si no hay datos para esa corrida
             raise HTTPException(status_code=404, detail=f"No results found for run_id {run_id}")
    return {"run_id": run_id, "hit_rate": row[0], "avg_latency": row[1]}

