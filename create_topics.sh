#!/bin/bash

# Espera a que Kafka esté listo aceptando conexiones en el puerto 9092
# El comando nc (netcat) intenta conectarse. El bucle espera hasta que tenga éxito.
echo "Esperando a que Kafka esté disponible..."
while ! nc -z kafka 9092; do
  sleep 1
done
echo "Kafka está listo."

# Lista de tópicos a crear
topics=(
  "preguntas_nuevas"
  "respuestas_llm_ok"
  "respuestas_llm_fallidas_reintentar"
  "resultados_validados"
)

# Crear cada tópico
for topic in "${topics[@]}"
do
  echo "Creando tópico: $topic"
  kafka-topics --create --if-not-exists \
    --bootstrap-server kafka:9092 \
    --partitions 1 \
    --replication-factor 1 \
    --topic "$topic"
done

echo "Creación de tópicos completada."
