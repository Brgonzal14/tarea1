-- Tabla con las preguntas del dataset Yahoo! Answers.
CREATE TABLE IF NOT EXISTS qa_yahoo (
  question_id BIGINT PRIMARY KEY,   -- id único de la pregunta
  class_id INT,                     -- categoría/clase de Yahoo
  title TEXT,                       -- título de la pregunta
  question TEXT,                    -- cuerpo de la pregunta
  best_answer TEXT                  -- "mejor respuesta" humana
);

-- Tabla para registrar metadatos de cada corrida/experimento 
CREATE TABLE IF NOT EXISTS qa_runs (
  run_id TEXT PRIMARY KEY,          -- nombre/ID de la corrida (ej: "dev1")
  started_at TIMESTAMP DEFAULT NOW(),
  traffic_dist TEXT,                -- distribución usada (poisson, bursty)
  cache_policy TEXT,                -- política de caché (LRU, LFU)
  cache_size_mb INT,                -- tamaño del caché (experimento)
  ttl_seconds INT,                  -- TTL configurado
  notes TEXT
);

-- Resultados de cada petición procesada durante una corrida.
CREATE TABLE IF NOT EXISTS qa_results (
  id BIGSERIAL PRIMARY KEY,
  run_id TEXT REFERENCES qa_runs(run_id),      -- a qué corrida pertenece
  question_id BIGINT REFERENCES qa_yahoo(question_id),
  is_cache_hit BOOLEAN,                        -- fue hit en caché o no
  llm_answer TEXT,                             -- respuesta generada (si hubo miss)
  score REAL,                                  -- puntaje de calidad
  latency_ms INT,                              -- latencia medida (ms)
  created_at TIMESTAMP DEFAULT NOW()
);

--- DESPUÉS
CREATE OR REPLACE VIEW qa_counters AS
SELECT question_id,
       COUNT(*) FILTER (WHERE is_cache_hit) AS hits,
       COUNT(*) FILTER (WHERE NOT is_cache_hit) AS misses
FROM qa_results
GROUP BY question_id;

