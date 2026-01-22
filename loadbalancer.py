import os
import time
import threading
import json
import random
import math
import requests
import redis
import lmdb
<<<<<<< HEAD
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from dotenv import load_dotenv
=======
from concurrent.futures import ThreadPoolExecutor, as_completed
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import (
    RedirectResponse,
    JSONResponse,
    StreamingResponse,
)
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

ADMIN_KEY = os.getenv("LB_ADMIN_KEY", "")
REDIS_URL = os.getenv("REDIS_URL")
TG_REDIRECT = "https://t.me/ppsl24_bot"

MAX_REQUESTS_PER_IP = int(os.getenv("LB_MAX_REQUESTS_PER_IP", "10"))
TTL_SECONDS = int(os.getenv("LB_TTL_SECONDS", "18000"))
POLL_INTERVAL = int(os.getenv("LB_POLL_INTERVAL", "10"))
REDIRECT_CODE = int(os.getenv("LB_REDIRECT_CODE", "302"))

FAIL_THRESHOLD = math.ceil((5 * 60) / POLL_INTERVAL)

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

r = redis.Redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None
SPECIAL_CACHE = {"set": set()}

def load_special_hashes():
<<<<<<< HEAD
    try:
        SPECIAL_CACHE["set"] = set(r.smembers("special_hashes"))
        dbg("Loaded special hashes:", len(SPECIAL_CACHE["set"]))
=======
    if not r:
        SPECIAL_CACHE["set"] = set()
        return
    try:
        SPECIAL_CACHE["set"] = set(r.smembers("special_hashes"))
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
    except Exception as e:
        dbg("Redis error:", e)

load_special_hashes()

def refresh_specials_loop():
    while True:
        load_special_hashes()
        time.sleep(60)

threading.Thread(target=refresh_specials_loop, daemon=True).start()

def is_special(h: str) -> bool:
    return h in SPECIAL_CACHE["set"]

# ============================================================
# LMDB FOR CDNS
# ============================================================

LMDB_PATH = "cdn.lmdb"
<<<<<<< HEAD
LMDB_MAP_SIZE = 512 * 1024 * 1024  # 512MB
=======
LMDB_MAP_SIZE = 512 * 1024 * 1024

>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
_env = lmdb.open(
    LMDB_PATH,
    map_size=LMDB_MAP_SIZE,
    max_dbs=1,
    lock=True,
    sync=False,
<<<<<<< HEAD
    readahead=False
)
=======
    readahead=False,
)

>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
_lmdb_lock = threading.Lock()

def set_cdn(url: str, data: dict):
    data["_ts"] = int(time.time())
    with _lmdb_lock:
        with _env.begin(write=True) as txn:
            txn.put(url.encode(), json.dumps(data).encode())

def delete_cdn(url: str):
    with _lmdb_lock:
        with _env.begin(write=True) as txn:
            txn.delete(url.encode())

def get_cdn(url: str):
    with _env.begin() as txn:
        v = txn.get(url.encode())
        return json.loads(v) if v else None

def list_cdns() -> dict:
    out = {}
    with _env.begin() as txn:
        for k, v in txn.cursor():
            out[k.decode()] = json.loads(v)
    return out

# ============================================================
# TRUSTED HOSTS
# ============================================================

TRUSTED_HOSTS = {"localhost", "127.0.0.1", "::1"}
_TRUST_LOCK = threading.Lock()

def rebuild_trusted_hosts():
    with _TRUST_LOCK:
        TRUSTED_HOSTS.clear()
        TRUSTED_HOSTS.update({"localhost", "127.0.0.1", "::1"})
        for cdn in list_cdns().keys():
            try:
                h = urlparse(cdn).hostname
                if h:
                    TRUSTED_HOSTS.add(h.lower())
            except Exception:
                pass

# ============================================================
# INIT CDNS FROM ENV
# ============================================================

ENV_CDNS = os.getenv("LB_CDN_URLS", "")
if ENV_CDNS:
    for u in ENV_CDNS.split(","):
        u = u.strip().rstrip("/")
        if u.startswith("http") and not get_cdn(u):
            set_cdn(u, {
                "load": 99999,
                "last_ok": 0,
                "fail_count": 0,
            })

rebuild_trusted_hosts()

# ============================================================
# UTILITIES
# ============================================================

BEST_CDN = {"url": None, "ts": 0}
BEST_CDN_TTL = 10  # seconds
LOCAL_RL = {}

def get_best_cdn():
    now = time.time()
    if BEST_CDN["url"] and now - BEST_CDN["ts"] < BEST_CDN_TTL:
        return BEST_CDN["url"]

    cdns = list_cdns()
    online = [(u, m) for u, m in cdns.items() if m.get("last_ok") == 1]
    if not online:
        return None

<<<<<<< HEAD
    min_load = min(meta.get("load", 99999) for _, meta in online)

    # tolerance allows "almost same load" to share traffic
    TOLERANCE = 1
    candidates = [
        url for url, meta in online
        if abs(meta.get("load", 99999) - min_load) <= TOLERANCE
    ]

    chosen = random.choice(candidates)
    BEST_CDN["url"] = chosen
    BEST_CDN["ts"] = now
    return chosen
=======
    min_load = min(m.get("load", 99999) for _, m in online)
    candidates = [u for u, m in online if abs(m.get("load", 99999) - min_load) <= 1]
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)

    chosen = random.choice(candidates)
    BEST_CDN.update(url=chosen, ts=now)
    return chosen

def record_ip(ip: str, h: str) -> int:
    now = time.time()
    key = f"{ip}:{h}"
    hits = [t for t in LOCAL_RL.get(key, []) if now - t < TTL_SECONDS]
    hits.append(now)
    LOCAL_RL[key] = hits
    return len(hits)

def referer_blocked(request: Request) -> bool:
    ip = request.client.host
    if ip in ("127.0.0.1", "::1"):
        return False

    ref = request.headers.get("referer")
    if not ref:
        return False

    host = (urlparse(ref).hostname or "").lower()
<<<<<<< HEAD
=======
    if host in TRUSTED_HOSTS:
        return False

>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
    return not any(
        host == w or host.endswith("." + w)
        for w in REFERER_WHITELIST
    )

def require_admin(request: Request):
    if request.headers.get("x-admin-key") != ADMIN_KEY:
        raise HTTPException(status_code=401, detail="unauthorized")

# ============================================================
<<<<<<< HEAD
# PARALLEL POLLER (LEADER ONLY)
=======
# CDN POLLER WITH PURGE
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
# ============================================================

IS_LEADER = os.getenv("KOYEB_INSTANCE_ID", "").endswith("0")

<<<<<<< HEAD
def check_cdn(url):
    now = int(time.time())
    try:
        resp = requests.get(f"{url}/status", timeout=(2, 3))
        js = resp.json()
        loads = js.get("loads", {})
        total = sum(loads.values()) if isinstance(loads, dict) else 99999

=======
def check_cdn(url: str):
    now = int(time.time())
    try:
        r = requests.get(f"{url}/status", timeout=(2, 3))
        js = r.json()
        loads = js.get("loads", {})
        total = sum(loads.values()) if isinstance(loads, dict) else 99999
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
        return {"url": url, "ok": True, "load": total, "ts": now}
    except Exception:
        return {"url": url, "ok": False, "load": 99999, "ts": now}

def poller():
<<<<<<< HEAD
    executor = ThreadPoolExecutor(max_workers=16)

    while True:
        cdns = list_cdns()
        futures = [executor.submit(check_cdn, url) for url in cdns.keys()]

        best_load = None
        best_urls = []

        for fut in as_completed(futures):
            res = fut.result()
            url = res["url"]
=======
    pool = ThreadPoolExecutor(max_workers=16)

    while True:
        cdns = list_cdns()
        futures = {pool.submit(check_cdn, u): u for u in cdns}

        for f in as_completed(futures):
            res = f.result()
            url = res["url"]
            prev = cdns.get(url, {})
            fail_count = prev.get("fail_count", 0)
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)

            if res["ok"]:
                set_cdn(url, {
                    "load": res["load"],
                    "last_ok": 1,
<<<<<<< HEAD
                    "updated_at": res["ts"]
                })

                if best_load is None or res["load"] < best_load:
                    best_load = res["load"]
                    best_urls = [url]
                elif res["load"] == best_load:
                    best_urls.append(url)
            else:
                set_cdn(url, {
                    "load": 99999,
                    "last_ok": 0,
                    "updated_at": res["ts"]
                })

        if best_urls:
            BEST_CDN["url"] = random.choice(best_urls)
            BEST_CDN["ts"] = time.time()
=======
                    "fail_count": 0,
                    "updated_at": res["ts"],
                })
            else:
                fail_count += 1
                if fail_count >= FAIL_THRESHOLD:
                    delete_cdn(url)
                    dbg("Purged dead CDN:", url)
                    continue

                set_cdn(url, {
                    "load": 99999,
                    "last_ok": 0,
                    "fail_count": fail_count,
                    "updated_at": res["ts"],
                })
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)

        rebuild_trusted_hosts()
        time.sleep(POLL_INTERVAL)

if IS_LEADER:
    threading.Thread(target=poller, daemon=True).start()

# ============================================================
# STREAMING
# ============================================================

async def stream_upstream(url: str, headers: dict):
    async with httpx.AsyncClient(timeout=None) as c:
        async with c.stream("GET", url, headers=headers) as r:
            async for chunk in r.aiter_bytes():
                yield chunk

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
    added = []
<<<<<<< HEAD
    for u in data.get("urls", []):
        u = u.rstrip("/")
        if u.startswith("http") and not get_cdn(u):
            set_cdn(u, {"load": 99999, "last_ok": 0})
            added.append(u)
=======

    for u in data.get("urls", []):
        u = u.rstrip("/")
        if u.startswith("http"):
            set_cdn(u, {
                "load": 99999,
                "last_ok": 0,
                "fail_count": 0,
            })
            added.append(u)

    rebuild_trusted_hosts()
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
    return {"added": added}

@app.post("/add_special")
async def add_special(request: Request):
    require_admin(request)
    data = await request.json()
<<<<<<< HEAD
    for h in data.get("hashes", []):
        r.sadd("special_hashes", h)
=======
    if r:
        for h in data.get("hashes", []):
            r.sadd("special_hashes", h)
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
    load_special_hashes()
    return {"added": data.get("hashes", [])}

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

@app.get("/watch/{hash}/{filename:path}")
async def watch(hash: str, filename: str, request: Request):
    if referer_blocked(request) or is_special(hash):
        return RedirectResponse(TG_REDIRECT, status_code=302)

    if record_ip(request.client.host, hash) > MAX_REQUESTS_PER_IP:
        return JSONResponse({"error": "IP limit exceeded"}, status_code=429)

    cdn = get_best_cdn()
    if not cdn:
        return JSONResponse({"error": "No CDN online"}, status_code=503)

<<<<<<< HEAD
    return RedirectResponse(f"{cdn}/watch/{hash}/{filename}", status_code=REDIRECT_CODE)
=======
    headers = dict(request.headers)
    headers.pop("host", None)

    return StreamingResponse(
        stream_upstream(f"{cdn}/watch/{hash}/{filename}", headers),
        media_type=None,
    )
>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)

@app.get("/stats")
async def stats(request: Request):
    require_admin(request)
<<<<<<< HEAD
    cdns = [{"url": u, **m} for u, m in list_cdns().items()]
=======

    cdns = []
    for url, meta in list_cdns().items():
        fc = meta.get("fail_count", 0)
        m = dict(meta)
        m["fail_count"] = f"{fc}/{FAIL_THRESHOLD}"
        cdns.append({"url": url, **m})

>>>>>>> 4d67143 (Add CDN health checking with automatic purge and streaming support)
    return {
        "cdns": cdns,
        "trusted_hosts": sorted(TRUSTED_HOSTS),
        "best_cdn": BEST_CDN["url"],
        "special_hashes": list(SPECIAL_CACHE["set"]),
    }
