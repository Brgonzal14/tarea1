from fastapi import FastAPI
from pydantic import BaseModel
import difflib, re

app = FastAPI(title="scorer")

class ScoreIn(BaseModel):
    gold_answer: str
    llm_answer: str

def normalize(txt: str) -> str:
    txt = txt.lower().strip()
    txt = re.sub(r"\s+", " ", txt)          # colapsa espacios
    txt = re.sub(r"[^\w\sáéíóúüñ]", "", txt) # saca signos
    return txt

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/score")
def score(body: ScoreIn):
    gold = normalize(body.gold_answer)
    ans  = normalize(body.llm_answer)
    sim = difflib.SequenceMatcher(None, gold, ans).ratio()  # 0..1
    return {"score": sim}
