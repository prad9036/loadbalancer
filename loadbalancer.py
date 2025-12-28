import os
import time
import threading
import requests
import redis
import lmdb
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from dotenv import load_dotenv
from urllib.parse import urlparse

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

ADMIN_KEY = os.getenv("LB_ADMIN_KEY")
REDIS_URL = os.getenv("REDIS_URL")
TG_REDIRECT = "https://t.me/ppsl24_bot"

MAX_REQUESTS_PER_IP = int(os.getenv("LB_MAX_REQUESTS_PER_IP", "10"))
TTL_SECONDS = int(os.getenv("LB_TTL_SECONDS", "18000"))
POLL_INTERVAL = int(os.getenv("LB_POLL_INTERVAL", "10"))
REDIRECT_CODE = int(os.getenv("LB_REDIRECT_CODE", "302"))

REFERER_WHITELIST = {
    d.strip().lower()
    for d in os.getenv("LB_REFERER_WHITELIST", "").split(",")
    if d.strip()
}

DEBUG = os.getenv("LB_DEBUG", "0") == "1"

def dbg(*a):
    if DEBUG:
        print("[DEBUG]", *a)

# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI()

# ============================================================
# REDIS (SPECIAL HASHES)
# ============================================================

r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
SPECIAL_CACHE = {"set": set()}

def load_special_hashes():
    SPECIAL_CACHE["set"] = set(r.smembers("special_hashes"))
    dbg("Loaded special hashes:", len(SPECIAL_CACHE["set"]))

load_special_hashes()

def refresh_specials_loop():
    while True:
        load_special_hashes()
        time.sleep(60)

threading.Thread(target=refresh_specials_loop, daemon=True).start()

def is_special(h):
    return h in SPECIAL_CACHE["set"]

# ============================================================
# LMDB FOR CDNS
# ============================================================

LMDB_PATH = "cdn.lmdb"
LMDB_MAP_SIZE = 512 * 1024 * 1024  # 512MB
_env = lmdb.open(LMDB_PATH, map_size=LMDB_MAP_SIZE, max_dbs=1, lock=True, sync=False, readahead=False)
_lmdb_lock = threading.Lock()

def set_cdn(url, data):
    data["_ts"] = int(time.time())
    with _lmdb_lock:
        with _env.begin(write=True) as txn:
            txn.put(url.encode(), json.dumps(data).encode())

def get_cdn(url):
    with _env.begin() as txn:
        v = txn.get(url.encode())
        return json.loads(v) if v else None

def list_cdns():
    cdns = {}
    with _env.begin() as txn:
        for k, v in txn.cursor():
            cdns[k.decode()] = json.loads(v)
    return cdns

# ============================================================
# INIT CDNS FROM ENV
# ============================================================

ENV_CDNS = os.getenv("LB_CDN_URLS", "")
if ENV_CDNS:
    for url in ENV_CDNS.split(","):
        url = url.strip().rstrip("/")
        if url.startswith("http") and not get_cdn(url):
            set_cdn(url, {"load": 99999, "last_ok": 0})

# ============================================================
# UTILS
# ============================================================

BEST_CDN = {"url": None, "ts": 0}
BEST_CDN_TTL = 10
LOCAL_RL = {}

def get_best_cdn():
    now = time.time()
    if BEST_CDN["url"] and now - BEST_CDN["ts"] < BEST_CDN_TTL:
        return BEST_CDN["url"]

    cdns = list_cdns()
    online = [(url, meta) for url, meta in cdns.items() if meta.get("last_ok") == 1]
    if not online:
        return None

    best = min(online, key=lambda x: x[1].get("load", 99999))
    BEST_CDN["url"] = best[0]
    BEST_CDN["ts"] = now
    return best[0]

def record_ip(ip, h):
    now = time.time()
    key = f"{ip}:{h}"
    hits = LOCAL_RL.get(key, [])
    hits = [t for t in hits if now - t < TTL_SECONDS]
    hits.append(now)
    LOCAL_RL[key] = hits
    return len(hits)

def referer_blocked(request: Request):
    ref = request.headers.get("referer")
    if not ref:
        return False
    host = (urlparse(ref).hostname or "").lower()
    return not any(host.endswith(w) for w in REFERER_WHITELIST)

def require_admin(request: Request):
    if request.headers.get("x-admin-key") != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

# ============================================================
# POLLER (LEADER ONLY)
# ============================================================

IS_LEADER = os.getenv("KOYEB_INSTANCE_ID", "").endswith("0")

def poller():
    while True:
        best_url = None
        best_load = None
        now = int(time.time())
        for url, meta in list_cdns().items():
            try:
                resp = requests.get(f"{url}/status", timeout=4)
                js = resp.json()
                loads = js.get("loads", {})
                total = sum(loads.values()) if isinstance(loads, dict) else 99999

                set_cdn(url, {"load": total, "last_ok": 1, "updated_at": now})

                if best_load is None or total < best_load:
                    best_load = total
                    best_url = url

            except Exception:
                set_cdn(url, {"load": 99999, "last_ok": 0, "updated_at": now})

        if best_url:
            BEST_CDN["url"] = best_url
            BEST_CDN["ts"] = time.time()

        time.sleep(POLL_INTERVAL)

if IS_LEADER:
    threading.Thread(target=poller, daemon=True).start()

# ============================================================
# ROUTES
# ============================================================

@app.get("/health")
async def health():
    return "ok"

@app.post("/add_cdn")
async def add_cdn(request: Request):
    require_admin(request)
    data = await request.json()
    urls = data.get("urls", [])
    added = []
    for u in urls:
        if u.startswith("http"):
            u = u.rstrip("/")
            if not get_cdn(u):
                set_cdn(u, {"load": 99999, "last_ok": 0})
                added.append(u)
    return {"added": added}

@app.post("/add_special")
async def add_special(request: Request):
    require_admin(request)
    data = await request.json()
    hashes = data.get("hashes", [])
    for h in hashes:
        r.sadd("special_hashes", h)
    load_special_hashes()
    return {"added": hashes}

@app.get("/dl/{hash}/{filename:path}")
async def dl(hash: str, filename: str, request: Request):
    if referer_blocked(request) or is_special(hash):
        return RedirectResponse(TG_REDIRECT, status_code=302)
    if record_ip(request.client.host, hash) > MAX_REQUESTS_PER_IP:
        return JSONResponse({"error": "IP limit exceeded"}, status_code=429)
    cdn = get_best_cdn()
    if not cdn:
        return JSONResponse({"error": "No CDN online"}, status_code=503)
    return RedirectResponse(f"{cdn}/dl/{hash}/{filename}", status_code=REDIRECT_CODE)

@app.get("/watch/{hash}")
async def watch(hash: str, request: Request):
    if referer_blocked(request) or is_special(hash):
        return RedirectResponse(TG_REDIRECT, status_code=302)
    if record_ip(request.client.host, hash) > MAX_REQUESTS_PER_IP:
        return JSONResponse({"error": "IP limit exceeded"}, status_code=429)
    cdn = get_best_cdn()
    if not cdn:
        return JSONResponse({"error": "No CDN online"}, status_code=503)
    return RedirectResponse(f"{cdn}/watch/{hash}", status_code=REDIRECT_CODE)

@app.get("/stats")
async def stats(request: Request):
    require_admin(request)
    cdns = [{"url": url, **meta} for url, meta in list_cdns().items()]
    return {
        "cdns": cdns,
        "best_cdn": BEST_CDN["url"],
        "special_hashes": list(SPECIAL_CACHE["set"])
    }
