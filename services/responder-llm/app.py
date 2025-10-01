import os, time, random
import httpx # type: ignore
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="responder-llm")

LLM_MODE = os.getenv("LLM_MODE", "STUB").upper()

# --- Gemini config
GEMINI_API_KEY      = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL        = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")
GEMINI_API_VERSION  = os.getenv("GEMINI_API_VERSION", "v1")  # <-- v1 o v1beta
GEMINI_BASE         = "https://generativelanguage.googleapis.com"

# STUB (por si lo necesitas para pruebas locales)
LLM_MIN_LAT_MS = int(os.getenv("LLM_MIN_LAT_MS", "200"))
LLM_MAX_LAT_MS = int(os.getenv("LLM_MAX_LAT_MS", "800"))

class AskIn(BaseModel):
    question: str

@app.get("/health")
def health():
    return {
        "ok": True,
        "mode": LLM_MODE,
        "gemini_key_set": bool(GEMINI_API_KEY),
        "gemini_model": GEMINI_MODEL if LLM_MODE == "GEMINI" else None,
        "gemini_api_version": GEMINI_API_VERSION if LLM_MODE == "GEMINI" else None,
    }

@app.get("/models")
async def list_models():
    """Devuelve lo que TU clave ve en la API (clave y versión actuales)."""
    if not GEMINI_API_KEY:
        raise HTTPException(500, "Falta GEMINI_API_KEY")
    url = f"{GEMINI_BASE}/{GEMINI_API_VERSION}/models?key={GEMINI_API_KEY}"
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url)
    return {"status": r.status_code, "body": r.json() if r.headers.get("content-type","").startswith("application/json") else r.text}

def stub_answer(q: str) -> str:
    import random, time
    time.sleep(random.randint(LLM_MIN_LAT_MS, LLM_MAX_LAT_MS) / 1000.0)
    return f"(respuesta simulada) A la pregunta: {q[:120]}..."

async def call_gemini(question: str) -> str:
    if not GEMINI_API_KEY:
        raise HTTPException(500, "Falta GEMINI_API_KEY")
    url = f"{GEMINI_BASE}/{GEMINI_API_VERSION}/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
    payload = {"contents": [{"parts": [{"text": f"Responde en español, breve y claro:\n\n{question}"}]}]}
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            r = await client.post(url, json=payload)
        if r.status_code != 200:
            raise HTTPException(502, f"Gemini HTTP {r.status_code}: {r.text[:500]}")
        data = r.json()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, f"Gemini request error: {e}")

    try:
        cands = data.get("candidates") or []
        parts = (cands[0].get("content", {}).get("parts") or [])
        text = (parts[0].get("text") or "").strip()
        return text or "(Gemini devolvió respuesta vacía)"
    except Exception as e:
        raise HTTPException(502, f"Gemini parse error: {e}")

@app.post("/answer")
async def answer(body: AskIn):
    q = (body.question or "").strip()
    if not q:
        raise HTTPException(400, "question vacío")
    if LLM_MODE == "GEMINI":
        return {"answer": await call_gemini(q)}
    if LLM_MODE == "STUB":
        return {"answer": stub_answer(q)}
    raise HTTPException(501, f"Modo {LLM_MODE} no implementado")
