# services/flink_job/Dockerfile
FROM python:3.10-slim

WORKDIR /opt/flink/job

# instala primero deps para cachear
COPY requirements.txt .
# preferimos wheels binarios para evitar compilaciones
RUN pip install --no-cache-dir --prefer-binary -r requirements.txt

# copia tu job
COPY job.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "-u", "job.py"]
