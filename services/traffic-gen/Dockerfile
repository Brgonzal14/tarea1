FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY run.py .
# No expone puertos; es un job que se ejecuta y termina
CMD ["python", "run.py"]
