# Imagen base con Python
FROM python:3.11-slim

# Carpeta de trabajo
WORKDIR /app

# Copiar e instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script del consumidor
COPY consumer.py .

# Comando para iniciar el consumidor
# Usamos 'python -u' para logs sin buffer
CMD ["python", "-u", "consumer.py"]
