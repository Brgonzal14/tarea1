# Imagen base Python
FROM python:3.11-slim

# Directorio de trabajo
WORKDIR /opt/flink/job

# Instalar dependencias necesarias para construir/ejecutar el job localmente si fuera necesario
# y para que Flink las pueda usar al subir el job.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script del job
COPY job.py .

# Nota: No hay CMD aquí. Esta imagen no se ejecuta como un servicio continuo.
# Se usa para construir y potencialmente enviar el job a Flink.
