import os
import threading
import time
import requests
from flask import Flask, redirect, request, jsonify
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# ============================================================
# SPECIAL HASH REDIRECT (FROM ENV + RUNTIME ADD)
# ============================================================

ENV_SPECIAL = os.getenv("LB_SPECIAL_HASHES", "")
SPECIAL_HASHES = set([h.strip() for h in ENV_SPECIAL.split(",") if h.strip()])

TG_REDIRECT = "https://t.me/ppsl24_bot"

# ============================================================
# ENV CONFIGURATION
# ============================================================

MAX_REQUESTS_PER_IP = int(os.getenv("LB_MAX_REQUESTS_PER_IP", "10"))
TTL_SECONDS = int(os.getenv("LB_TTL_SECONDS", str(5 * 3600)))
POLL_INTERVAL = int(os.getenv("LB_POLL_INTERVAL", "10"))
ENV_CDNS = os.getenv("LB_CDN_URLS", "")
DEBUG = os.getenv("LB_DEBUG", "0") == "1"

def dbg(*a):
    if DEBUG:
        print("[DEBUG]", *a)

REDIRECT_CODE = int(os.getenv("LB_REDIRECT_CODE", "302"))

# ============================================================
# DATA STRUCTURES
# ============================================================

CDN_SERVERS = {}
USAGE = {}
IP_USAGE = {}

# ============================================================
# INITIALIZE CDN SERVERS FROM ENV
# ============================================================

if ENV_CDNS.strip():
    for url in ENV_CDNS.split(","):
        url = url.strip().rstrip("/")
        if url.startswith("http://") or url.startswith("https://"):
            CDN_SERVERS[url] = {
                "status": {},
                "load": 99999,
                "last_ok": False
            }

# ============================================================
# TTL CLEANUP LOOP
# ============================================================

def cleanup_loop():
    while True:
        now = time.time()
        empty_ips = []

        for ip, hashes in list(IP_USAGE.items()):
            empty_hashes = []

            for h, ts_list in hashes.items():
                new_ts = [ts for ts in ts_list if now - ts <= TTL_SECONDS]
                if new_ts:
                    IP_USAGE[ip][h] = new_ts
                else:
                    empty_hashes.append(h)

            for h in empty_hashes:
                del IP_USAGE[ip][h]

            if not IP_USAGE[ip]:
                empty_ips.append(ip)

        for ip in empty_ips:
            del IP_USAGE[ip]

        time.sleep(60)

threading.Thread(target=cleanup_loop, daemon=True).start()

# ============================================================
# CDN POLLER
# ============================================================

def poller():
    while True:
        for base_url in list(CDN_SERVERS.keys()):
            status_url = f"{base_url}/status"

            try:
                resp = requests.get(status_url, timeout=4)
                status_json = resp.json()

                loads = status_json.get("loads", {})
                if isinstance(loads, dict):
                    total_load = sum(loads.values())
                else:
                    total_load = 99999

                CDN_SERVERS[base_url]["status"] = status_json
                CDN_SERVERS[base_url]["load"] = total_load
                CDN_SERVERS[base_url]["last_ok"] = True
                dbg(f"OK {base_url}: load={total_load}")

            except Exception as e:
                CDN_SERVERS[base_url]["last_ok"] = False
                CDN_SERVERS[base_url]["status"] = {}
                CDN_SERVERS[base_url]["load"] = 99999
                dbg(f"DOWN {base_url}: {e}")

        time.sleep(POLL_INTERVAL)

threading.Thread(target=poller, daemon=True).start()

# ============================================================
# ADD CDN
# ============================================================

@app.route("/add_cdn", methods=["POST"])
def add_cdn():
    data = request.json or {}
    urls = []

    if "url" in data:
        urls.append(data["url"])

    if "urls" in data:
        if not isinstance(data["urls"], list):
            return jsonify({"error": "urls must be a list"}), 400
        urls.extend(data["urls"])

    if not urls:
        return jsonify({"error": "no URLs provided"}), 400

    added = []

    for url in urls:
        url = url.strip().rstrip("/")
        if url.startswith("http://") or url.startswith("https://"):
            CDN_SERVERS[url] = {
                "status": {},
                "load": 99999,
                "last_ok": False
            }
            added.append(url)

    return jsonify({"added": added, "total_instances": len(CDN_SERVERS)})

# ============================================================
# SPECIAL HASH ENDPOINTS
# ============================================================

@app.route("/add_special", methods=["POST"])
def add_special():
    data = request.json or {}
    incoming = data.get("hashes")

    if not incoming or not isinstance(incoming, list):
        return jsonify({"error": "Provide list under 'hashes'"}), 400

    added = []
    for h in incoming:
        if isinstance(h, str) and h.strip():
            h = h.strip()
            SPECIAL_HASHES.add(h)
            added.append(h)

    return jsonify({
        "added": added,
        "total_special": len(SPECIAL_HASHES)
    })

@app.route("/special")
def special_list():
    return jsonify({
        "special_hashes": sorted(list(SPECIAL_HASHES))
    })

# ============================================================
# LOAD SELECTION
# ============================================================

def choose_cdn():
    best_url = None
    best_load = None

    for url, info in CDN_SERVERS.items():
        if not info.get("last_ok"):
            continue

        load = info.get("load", 99999)
        if best_load is None or load < best_load:
            best_load = load
            best_url = url

    return best_url

# ============================================================
# RECORD IP USAGE
# ============================================================

def record_ip_usage(ip, h):
    now = time.time()

    if ip not in IP_USAGE:
        IP_USAGE[ip] = {}

    if h not in IP_USAGE[ip]:
        IP_USAGE[ip][h] = []

    timestamps = [ts for ts in IP_USAGE[ip][h] if now - ts <= TTL_SECONDS]
    timestamps.append(now)
    IP_USAGE[ip][h] = timestamps

    return len(timestamps)

# ============================================================
# DOWNLOAD ROUTE
# ============================================================

@app.route("/dl/<hash>/<path:filename>")
def route_dl(hash, filename):

    # SPECIAL HASH REDIRECT
    if hash in SPECIAL_HASHES:
        return redirect(TG_REDIRECT, code=302)

    USAGE[hash] = USAGE.get(hash, 0) + 1

    client_ip = request.headers.get(
        "X-Forwarded-For",
        request.remote_addr
    ).split(",")[0].strip()

    count = record_ip_usage(client_ip, hash)
    if MAX_REQUESTS_PER_IP > 0 and count > MAX_REQUESTS_PER_IP:
        return jsonify({
            "error": "IP limit exceeded",
            "allowed": MAX_REQUESTS_PER_IP,
            "your_requests": count,
            "window_seconds": TTL_SECONDS,
            "hash": hash,
            "ip": client_ip
        }), 429

    cdn = choose_cdn()
    if not cdn:
        return "No CDN instance online", 503

    final_url = f"{cdn}/dl/{hash}/{filename}"
    return redirect(final_url, code=REDIRECT_CODE)

# ============================================================
# WATCH ROUTE
# ============================================================

@app.route("/watch/<hash>")
def route_watch(hash):

    # SPECIAL HASH REDIRECT
    if hash in SPECIAL_HASHES:
        return redirect(TG_REDIRECT, code=302)

    USAGE[hash] = USAGE.get(hash, 0) + 1

    client_ip = request.headers.get(
        "X-Forwarded-For",
        request.remote_addr
    ).split(",")[0].strip()

    count = record_ip_usage(client_ip, hash)
    if count > MAX_REQUESTS_PER_IP:
        return jsonify({
            "error": "IP limit exceeded",
            "allowed": MAX_REQUESTS_PER_IP,
            "your_requests": count,
            "window_seconds": TTL_SECONDS,
            "hash": hash,
            "ip": client_ip
        }), 429

    cdn = choose_cdn()
    if not cdn:
        return "No CDN instance online", 503

    final_url = f"{cdn}/watch/{hash}"
    return redirect(final_url, code=REDIRECT_CODE)

# ============================================================
# STATS
# ============================================================

@app.route("/stats")
def stats():
    return jsonify({
        "servers": CDN_SERVERS,
        "usage": USAGE,
        "ip_usage": IP_USAGE,
        "special_hashes": sorted(list(SPECIAL_HASHES))
    })

# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    host = os.getenv("LB_HOST", "0.0.0.0")
    port = int(os.getenv("LB_PORT", "8080"))
    print(f"Load balancer running on {host}:{port}")
    app.run(host=host, port=port)
