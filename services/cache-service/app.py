import os, json, time
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis

# ---------- Config ----------
REDIS_HOST = os.getenv("REDIS_HOST", "redis")  # nombre del servicio en docker-compose
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "21600"))  # default 6h

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI(title="cache-service")

# contadores (simples) en memoria del proceso
STATS = {"gets": 0, "hits": 0, "misses": 0, "puts": 0, "deletes": 0}

class CachePut(BaseModel):
    value: dict | list | str | int | float | bool | None = None
    ttl_seconds: int | None = None  # si lo mandas, sobreescribe el TTL por defecto

# ---------- Health ----------
@app.get("/health")
def health():
    try:
        r.ping()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(500, f"Redis no disponible: {e}")

# ---------- GET ----------
@app.get("/cache/{key}")
def get_key(key: str):
    """
    Lee una clave. Si no existe -> miss, si existe -> hit.
    Guardamos valores como JSON string si no son texto simple.
    """
    STATS["gets"] += 1
    raw = r.get(key)
    if raw is None:
        STATS["misses"] += 1
        raise HTTPException(404, "MISS")
    STATS["hits"] += 1

    # intentar decodificar JSON si corresponde
    try:
        return {"key": key, "value": json.loads(raw)}
    except Exception:
        return {"key": key, "value": raw}

# ---------- PUT ----------
@app.put("/cache/{key}")
def put_key(key: str, body: CachePut):
    """
    Guarda una clave con TTL. Acepta valores JSON o escalares.
    """
    ttl = body.ttl_seconds if body.ttl_seconds is not None else CACHE_TTL
    to_store = body.value
    # convertimos a string (JSON si no es texto)
    if isinstance(to_store, (dict, list)):
        payload = json.dumps(to_store, ensure_ascii=False)
    else:
        payload = str(to_store) if to_store is not None else ""

    ok = r.set(name=key, value=payload, ex=ttl)
    if not ok:
        raise HTTPException(500, "No se pudo escribir en Redis")
    STATS["puts"] += 1
    return {"ok": True, "key": key, "ttl_seconds": ttl}

# ---------- DELETE ----------
@app.delete("/cache/{key}")
def delete_key(key: str):
    removed = r.delete(key)
    STATS["deletes"] += 1
    return {"ok": True, "removed": int(removed)}

# ---------- Stats ----------
@app.get("/stats")
def stats():
    rate = (STATS["hits"] / STATS["gets"]) if STATS["gets"] else 0.0
    info = {"gets": STATS["gets"], "hits": STATS["hits"], "misses": STATS["misses"],
            "puts": STATS["puts"], "deletes": STATS["deletes"], "hit_rate": rate}
    try:
        info["keys"] = int(r.dbsize())
        info["used_memory_human"] = r.info().get("used_memory_human")
        info["maxmemory_human"] = r.config_get("maxmemory").get("maxmemory")
    except Exception:
        pass
    return info
