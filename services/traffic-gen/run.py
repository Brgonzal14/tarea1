import os, asyncio, time, random
import httpx # type: ignore

# ---------- Config ----------
RUN_ID = os.getenv("RUN_ID", "dev1") + f"-{int(time.time())}"
BASE_RATE_RPS = float(os.getenv("BASE_RATE_RPS", "10"))
DURATION_SECONDS = int(os.getenv("DURATION_SECONDS", "20"))
TRAFFIC_DIST = os.getenv("TRAFFIC_DIST", "poisson").lower()  # poisson | bursty
CACHE_TTL = int(os.getenv("CACHE_TTL_SECONDS", "21600"))
TOTAL_REQUESTS = int(os.getenv("TOTAL_REQUESTS", "0"))  # si >0, corre por cantidad en vez de tiempo

URL_STORAGE = os.getenv("URL_STORAGE", "http://storage-api:8001")
URL_CACHE   = os.getenv("URL_CACHE",   "http://cache-service:8002")
URL_LLM     = os.getenv("URL_LLM",     "http://responder-llm:8003")
URL_SCORER  = os.getenv("URL_SCORER",  "http://scorer:8004")

# Ajustes pensados para LLM real (Gemini)
LLM_TIMEOUT_S   = int(os.getenv("LLM_TIMEOUT_S", "45"))      # lectura de respuesta
LLM_RETRIES     = int(os.getenv("LLM_RETRIES", "2"))         # reintentos ante 429/5xx/timeout
LLM_BACKOFF_S   = float(os.getenv("LLM_BACKOFF_S", "0.6"))   # backoff exponencial base
MAX_INFLIGHT    = int(os.getenv("MAX_INFLIGHT", "20"))       # límite de peticiones simultáneas

# Semáforo global para no saturar servicios (especialmente el LLM)
_sem = asyncio.Semaphore(MAX_INFLIGHT)

print(f"[traffic-gen] RUN_ID={RUN_ID} dist={TRAFFIC_DIST} base_rps={BASE_RATE_RPS} "
      f"dur={DURATION_SECONDS}s total_requests={TOTAL_REQUESTS}")

def interarrival(rate_rps: float) -> float:
    rate_rps = max(rate_rps, 0.001)
    return random.expovariate(rate_rps)

async def _post_with_retries(client: httpx.AsyncClient, url: str, json: dict, timeout: float):
    """
    POST con reintentos y backoff (pensado para LLM/score).
    Reintenta en 429/5xx o timeout de red.
    """
    delay = LLM_BACKOFF_S
    last_exc = None
    last_resp = None
    for attempt in range(LLM_RETRIES + 1):
        try:
            resp = await client.post(url, json=json, timeout=timeout)
            # Reintentar sólo si es 429 o 5xx
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                last_resp = resp
                if attempt < LLM_RETRIES:
                    await asyncio.sleep(delay)
                    delay *= 2
                    continue
            return resp
        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.NetworkError) as e:
            last_exc = e
            if attempt < LLM_RETRIES:
                await asyncio.sleep(delay)
                delay *= 2
                continue
            # sin más reintentos:
            raise
    # Si salimos por aquí, devolvemos lo último que tengamos o re-levantamos
    if last_resp is not None:
        return last_resp
    if last_exc:
        raise last_exc

async def warmup(client: httpx.AsyncClient):
    print("[warmup] verificando servicios...")
    checks = [
        ("cache-health",   "GET",  f"{URL_CACHE}/health",   None, 10),
        ("storage-random", "GET",  f"{URL_STORAGE}/questions/random", None, 10),
        ("llm-health",     "GET",  f"{URL_LLM}/health",     None, 10),
        ("scorer-health",  "GET",  f"{URL_SCORER}/health",  None, 10),
    ]
    for name, method, url, body, to in checks:
        try:
            if method == "GET":
                r = await client.get(url, timeout=to)
            else:
                r = await client.post(url, json=body, timeout=to)
            # Para el LLM mostramos el modo /GEMINI
            if name == "llm-health":
                try:
                    mode = r.json().get("mode")
                    print(f"[warmup] {name} {r.status_code} mode={mode}")
                except Exception:
                    print(f"[warmup] {name} {r.status_code}")
            else:
                print(f"[warmup] {name} {r.status_code}")
        except Exception as e:
            print(f"[warmup][ERROR] {name}: {e}")

async def one_request(client: httpx.AsyncClient, idx: int):
    async with _sem:
        t0 = time.perf_counter()
        is_hit = False
        llm_answer = None
        score = None

        # 1) Pregunta al storage
        try:
            r_q = await client.get(f"{URL_STORAGE}/questions/random", timeout=10)
            if r_q.status_code != 200:
                print(f"[req {idx}] /questions/random status={r_q.status_code}")
                return
            q = r_q.json()
            qid = q["question_id"]
            key = f"q:{qid}"
        except Exception as e:
            print(f"[req {idx}][ERROR] /questions/random:", e)
            return

        # 2) Intento de cache GET
        try:
            r_cache = await client.get(f"{URL_CACHE}/cache/{key}", timeout=5)
            is_hit = (r_cache.status_code == 200)
            print(f"[req {idx}] cache GET {r_cache.status_code} hit={is_hit}")
        except Exception as e:
            is_hit = False
            print(f"[req {idx}][WARN] cache GET:", e)

        # 3) Miss -> LLM + scorer + cache PUT
        if not is_hit:
            try:
                r_llm = await _post_with_retries(
                    client,
                    f"{URL_LLM}/answer",
                    json={"question": q["question"]},
                    timeout=LLM_TIMEOUT_S
                )
                print(f"[req {idx}] llm /answer {r_llm.status_code}")
                if r_llm.status_code != 200:
                    # si el LLM falla, no seguimos con scorer/PUT
                    try:
                        body_txt = r_llm.text[:200]
                    except Exception:
                        body_txt = "<no-body>"
                    print(f"[req {idx}][ERROR] LLM body: {body_txt}")
                    return
                llm_answer = r_llm.json().get("answer", "")
            except Exception as e:
                print(f"[req {idx}][ERROR] LLM /answer:", e)
                return

            try:
                r_sc = await _post_with_retries(
                    client,
                    f"{URL_SCORER}/score",
                    json={"gold_answer": q["best_answer"], "llm_answer": llm_answer},
                    timeout=15
                )
                score = r_sc.json().get("score", 0.0)
                print(f"[req {idx}] scorer /score {r_sc.status_code} score={score}")
            except Exception as e:
                print(f"[req {idx}][ERROR] scorer /score:", e)
                return

            try:
                r_put = await client.put(
                    f"{URL_CACHE}/cache/{key}",
                    json={"value": {"answer": llm_answer, "score": score}, "ttl_seconds": CACHE_TTL},
                    timeout=5
                )
                print(f"[req {idx}] cache PUT {r_put.status_code}")
            except Exception as e:
                print(f"[req {idx}][WARN] cache PUT:", e)

        latency_ms = int((time.perf_counter() - t0) * 1000)

        # 4) Registrar resultado
        try:
            r_res = await client.post(
                f"{URL_STORAGE}/result",
                json={
                    "run_id": RUN_ID,
                    "question_id": qid,
                    "is_cache_hit": is_hit,
                    "llm_answer": None if is_hit else llm_answer,
                    "score": None if is_hit else score,
                    "latency_ms": latency_ms
                },
                timeout=10
            )
            print(f"[req {idx}] result POST {r_res.status_code} latency_ms={latency_ms}")
        except Exception as e:
            print(f"[req {idx}][WARN] POST /result:", e)

async def loop_time(client: httpx.AsyncClient):
    print(f"[loop] time-based for {DURATION_SECONDS}s")
    end = time.monotonic() + DURATION_SECONDS
    tasks = []
    i = 0
    if TRAFFIC_DIST == "bursty":
        high = BASE_RATE_RPS * 3
        low  = max(BASE_RATE_RPS * 0.2, 0.1)
        current = high
        next_swap = time.monotonic() + 5.0

    while time.monotonic() < end:
        i += 1
        tasks.append(asyncio.create_task(one_request(client, i)))
        if TRAFFIC_DIST == "poisson":
            await asyncio.sleep(interarrival(BASE_RATE_RPS))
        else:
            await asyncio.sleep(interarrival(current))
            if time.monotonic() >= next_swap:
                current = high if current == low else low
                next_swap = time.monotonic() + 5.0

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    print("[loop] done (time).")

async def loop_count(client: httpx.AsyncClient, n: int):
    print(f"[loop] starting {n} requests…")
    for i in range(1, max(n, 1) + 1):
        await one_request(client, i)
    print("[loop] done (count).")

async def main():
    # timeouts por defecto del cliente (se pueden sobreescribir por llamada)
    timeout = httpx.Timeout(connect=5.0, read=LLM_TIMEOUT_S, write=10.0, pool=LLM_TIMEOUT_S)
    async with httpx.AsyncClient(timeout=timeout) as client:
        await warmup(client)
        if TOTAL_REQUESTS > 0:
            await loop_count(client, TOTAL_REQUESTS)
        else:
            await loop_time(client)

        try:
            r = await client.get(f"{URL_STORAGE}/metrics", params={"run_id": RUN_ID}, timeout=10)
            print("[metrics]", r.json())
        except Exception as e:
            print("[metrics][WARN]", e)

if __name__ == "__main__":
    asyncio.run(main())
