#!/usr/bin/env bash
set -euo pipefail

echo "[submitter] /job:"
ls -l /job

# (opcional) instala deps si existen
if [ -f /job/requirements.txt ]; then
  pip3 install --no-cache-dir -r /job/requirements.txt || true
fi

echo "[submitter] Enviando job.py a Flink…"
# IMPORTANTE: sin http:// — la CLI espera host:puerto
flink run -d -m jobmanager:8081 -py /job/job.py -pyfs /job
echo "[submitter] Job enviado."
