import os, random, time
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="responder-llm")

# Configuración de modo y latencia desde .env
LLM_MODE = os.getenv("LLM_MODE", "STUB")
MIN_LAT_MS = int(os.getenv("LLM_MIN_LAT_MS", "200"))
MAX_LAT_MS = int(os.getenv("LLM_MAX_LAT_MS", "800"))

class QuestionIn(BaseModel):
    question: str
    gold_answer: str | None = None

@app.get("/health")
def health():
    return {"ok": True, "mode": LLM_MODE}

@app.post("/answer")
def answer(body: QuestionIn):
    """
    Si está en modo STUB, simula una latencia y devuelve una respuesta dummy.
    Más adelante se podría conectar a Gemini u Ollama.
    """
    if LLM_MODE == "STUB":
        # Simular latencia
        delay = random.randint(MIN_LAT_MS, MAX_LAT_MS) / 1000
        time.sleep(delay)
        return {
            "answer": f"(respuesta simulada) A la pregunta: {body.question[:50]}...",
            "latency_ms": int(delay * 1000)
        }

    # Si se configurara otro modo, se podría extender acá
    return {"error": f"Modo {LLM_MODE} no implementado"}
