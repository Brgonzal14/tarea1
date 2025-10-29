import os
import json
import logging
import requests # Para llamar al scorer

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, FilterFunction, ProcessFunction
from pyflink.common.serialization import SimpleStringSchema # Para Kafka si no usamos JSON Schema

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger("flink_job")

# --- Configuración (Leer de variables de entorno) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_TOPIC_RESP_OK = os.getenv('KAFKA_TOPIC_RESP_OK', 'respuestas_llm_ok')
KAFKA_TOPIC_VALIDADOS = os.getenv('KAFKA_TOPIC_VALIDADOS', 'resultados_validados')
KAFKA_TOPIC_PREGUNTAS = os.getenv('KAFKA_TOPIC_PREGUNTAS', 'preguntas_nuevas') # Para re-encolar
KAFKA_CONSUMER_GROUP_FLINK = os.getenv('KAFKA_CONSUMER_GROUP_FLINK', 'flink_scorer_group')

URL_SCORER = os.getenv('URL_SCORER', 'http://scorer:8004/score')
SCORE_THRESHOLD = float(os.getenv('SCORE_THRESHOLD', '0.1')) # Umbral de calidad (ej: 0.1)
MAX_FLINK_RETRIES = int(os.getenv('MAX_FLINK_RETRIES', 1)) # Máximos reintentos INICIADOS por Flink

log.info(f"Iniciando Job Flink: Score Threshold={SCORE_THRESHOLD}, Max Flink Retries={MAX_FLINK_RETRIES}")
log.info(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
log.info(f"Consumiendo de: {KAFKA_TOPIC_RESP_OK}")
log.info(f"Publicando Validados en: {KAFKA_TOPIC_VALIDADOS}")
log.info(f"Republicando Preguntas en: {KAFKA_TOPIC_PREGUNTAS}")
log.info(f"URL Scorer: {URL_SCORER}")

# --- Definición del Job PyFlink ---

# 1. Crear el entorno de ejecución
env = StreamExecutionEnvironment.get_execution_environment()
# Configurar para modo Streaming (es el default, pero explícito es mejor)
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
# Añadir dependencias JAR para el conector Kafka (¡MUY IMPORTANTE!)
# Asegúrate de que la versión del conector coincida con tu versión de Flink (1.17) y Kafka
kafka_jar_path = "file:///opt/flink/lib/flink-sql-connector-kafka-1.17.1.jar" # Asume que está en la imagen Flink
env.add_jars(kafka_jar_path)


# 2. Definir el Tipo de Fila (Row Type) para los mensajes Kafka
# Asumiendo que `respuestas_llm_ok` tiene: question_id, question_text, llm_answer, best_answer
source_type_info = Types.ROW_NAMED(
    ["question_id", "question_text", "llm_answer", "best_answer"],
    [Types.INT(), Types.STRING(), Types.STRING(), Types.STRING()]
)

# 3. Crear el Consumidor Kafka (Source)
kafka_consumer = FlinkKafkaConsumer(
    topics=KAFKA_TOPIC_RESP_OK,
    deserialization_schema=JsonRowDeserializationSchema.builder().type_info(source_type_info).build(),
    properties={
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': KAFKA_CONSUMER_GROUP_FLINK,
        'auto.offset.reset': 'earliest' # Empezar desde el principio si es un grupo nuevo
    }
)

# 4. Leer el Stream desde Kafka
source_stream = env.add_source(kafka_consumer).name("KafkaSource_RespOK")

# --- Lógica de Procesamiento ---

# 5. Definir una función Map para llamar al Scorer
class ScorerMapFunction(MapFunction):
    def map(self, row):
        # row es un objeto Row de Flink
        qid = row.question_id
        llm_answer = row.llm_answer
        best_answer = row.best_answer
        score = 0.0 # Default score
        error = None

        log.info(f"Calculando score para QID: {qid}")
        try:
            response = requests.post(URL_SCORER, json={"gold_answer": best_answer, "llm_answer": llm_answer}, timeout=10)
            response.raise_for_status() # Lanza excepción si hay error HTTP
            score = response.json().get('score', 0.0)
            log.info(f"Score obtenido para QID {qid}: {score:.4f}")
        except requests.exceptions.RequestException as e:
            log.error(f"Error al llamar al scorer para QID {qid}: {e}")
            error = str(e) # Guardar el error
            # Podríamos implementar reintentos aquí también, o simplemente marcar como fallido

        # Devolver un nuevo Row con el score añadido (y el error si hubo)
        # Necesitamos incluir los campos originales + score + error (opcional) + flink_retry_count
        return Types.ROW(
            question_id=qid,
            question_text=row.question_text,
            llm_answer=llm_answer,
            best_answer=best_answer, # Podríamos quitarlo si ya no se necesita
            score=float(score),
            error=error,
            flink_retry_count=int(row.flink_retry_count if hasattr(row, 'flink_retry_count') else 0) # Inicializar contador Flink
        )

# 6. Aplicar la función Map para obtener el score
scored_stream = source_stream.map(
    ScorerMapFunction(),
    output_type=Types.ROW_NAMED(
        ["question_id", "question_text", "llm_answer", "best_answer", "score", "error", "flink_retry_count"],
        [Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.FLOAT(), Types.STRING(), Types.INT()]
    )
).name("ScoreCalculator")


# 7. Filtrar/Dividir el stream basado en el score y errores

# Stream para resultados VÁLIDOS (score >= threshold y sin error)
valid_stream = scored_stream.filter(
    lambda row: row.error is None and row.score >= SCORE_THRESHOLD
).name("FilterValidScore")

# Stream para resultados a RE-ENCOLAR (score < threshold, sin error, y reintentos Flink < MAX)
requeue_stream = scored_stream.filter(
    lambda row: row.error is None and row.score < SCORE_THRESHOLD and row.flink_retry_count < MAX_FLINK_RETRIES
).name("FilterRequeueScore")

# Stream para resultados con ERRORES del scorer o que superaron MAX_FLINK_RETRIES
error_or_max_retry_stream = scored_stream.filter(
    lambda row: row.error is not None or (row.score < SCORE_THRESHOLD and row.flink_retry_count >= MAX_FLINK_RETRIES)
).name("FilterErrorOrMaxRetry")


# --- Sinks (Destinos Kafka) ---

# 8. Sink para resultados VALIDADOS -> `resultados_validados`
#    Necesita un schema JSON que coincida con lo que espera `storage-api` (o lo que queramos guardar)
#    Por ejemplo: question_id, llm_answer, score
validated_type_info = Types.ROW_NAMED(
    ["question_id", "llm_answer", "score"], # Ajusta campos según necesites
    [Types.INT(), Types.STRING(), Types.FLOAT()]
)

kafka_producer_validated = FlinkKafkaProducer(
    topic=KAFKA_TOPIC_VALIDADOS,
    serialization_schema=JsonRowSerializationSchema.builder().with_type_info(validated_type_info).build(),
    producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
)
# Seleccionar y proyectar solo los campos necesarios antes de enviar al sink
valid_stream.map(lambda row: Types.ROW(question_id=row.question_id, llm_answer=row.llm_answer, score=row.score),
                 output_type=validated_type_info)\
            .add_sink(kafka_producer_validated).name("KafkaSink_Validated")


# 9. Sink para RE-ENCOLAR -> `preguntas_nuevas`
#    Necesita el formato esperado por `llm_worker`: question_id, question_text, best_answer, retry_count
requeue_type_info = Types.ROW_NAMED(
    ["question_id", "question_text", "best_answer", "retry_count", "flink_retry_count"], # Añadimos flink_retry_count
    [Types.INT(), Types.STRING(), Types.STRING(), Types.INT(), Types.INT()]
)

kafka_producer_requeue = FlinkKafkaProducer(
    topic=KAFKA_TOPIC_PREGUNTAS, # De vuelta al inicio del ciclo
    serialization_schema=JsonRowSerializationSchema.builder().with_type_info(requeue_type_info).build(),
    producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"}
)
# Incrementar el contador de reintentos de Flink antes de re-encolar
requeue_stream.map(lambda row: Types.ROW(question_id=row.question_id,
                                          question_text=row.question_text,
                                          best_answer=row.best_answer,
                                          # Se mantiene el retry_count original del LLM Worker
                                          retry_count=int(row.retry_count if hasattr(row, 'retry_count') else 0),
                                          flink_retry_count=row.flink_retry_count + 1), # Incrementamos Flink retry
                   output_type=requeue_type_info)\
              .add_sink(kafka_producer_requeue).name("KafkaSink_Requeue")


# Por ahora, solo se imprimime un log para estos casos
class LogErrorsProcessFunction(ProcessFunction):
    def process_element(self, row, ctx):
        log.error(f"Pregunta QID {row.question_id} descartada. Error: {row.error}, Score: {row.score}, Flink Retries: {row.flink_retry_count}")
        # Aquí se envia a un tópico Kafka de errores

error_or_max_retry_stream.process(LogErrorsProcessFunction()).name("LogErrorOrMaxRetry")


# Ejecutar el Job
log.info("Definición del Job completada. Ejecutando...")
env.execute("ScoreCalculatorJob_KafkaToKafka")