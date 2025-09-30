# Imagen base con Python 
FROM python:3.11-slim

# Carpeta de trabajo dentro del contenedor
WORKDIR /app

# 1) Se copian las dependencias y se instalan
COPY requirements.txt ./
RUN pip install -r requirements.txt

# 2) Se copia el código y el SQL del esquema
COPY models.sql db.py app.py ./

# 3) Puerto donde correrá la API
EXPOSE 8001

# 4) Comando para iniciar el servidor FastAPI
CMD ["uvicorn","app:app","--host","0.0.0.0","--port","8001"]
