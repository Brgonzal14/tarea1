#!/usr/bin/env bash
set -euo pipefail

# Permite sobreescribir el broker desde el entorno (útil si lo corres desde tu host)
BROKER="${KAFKA_BROKER:-kafka:9092}"
PARTITIONS="${PARTITIONS:-1}"
REPLICATION="${REPLICATION:-1}"

TOPICS=(
  "preguntas_nuevas"
  "respuestas_llm_ok"
  "respuestas_llm_fallidas_reintentar"
  "resultados_validados"
)

echo "Esperando a Kafka en ${BROKER}..."
# Espera a que Kafka responda de verdad (no solo que abra el puerto)
until kafka-topics --bootstrap-server "${BROKER}" --list >/dev/null 2>&1; do
  sleep 2
done
echo "Kafka está listo."

for topic in "${TOPICS[@]}"; do
  echo "Creando tópico: ${topic}"
  kafka-topics --create --if-not-exists \
    --bootstrap-server "${BROKER}" \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}"
done

echo "Creación de tópicos completada."
