import threading
import time
import requests
from flask import Flask, redirect, request, jsonify

app = Flask(__name__)

# ============================================================
# CDN registry:
# {
#   "https://abc.trycloudflare.com": {
#        "status": {...},
#        "load": 3,
#        "last_ok": True
#   }
# }
# ============================================================

CDN_SERVERS = {}

# Track usage per hash
USAGE = {}

# Per-IP tracking
IP_USAGE = {}       # { ip: { hash: count } }
MAX_REQUESTS_PER_IP = 10

POLL_INTERVAL = 10     # seconds


# ============================================================
# Background POLLER - fetch CDN /status
# ============================================================

def poller():
    while True:
        for base_url in list(CDN_SERVERS.keys()):
            status_url = f"{base_url}/status"

            try:
                resp = requests.get(status_url, timeout=4)
                status_json = resp.json()

                # Extract load
                loads = status_json.get("loads", {})
                if isinstance(loads, dict):
                    total_load = sum(loads.values())
                else:
                    total_load = 99999

                # Update info
                CDN_SERVERS[base_url]["status"] = status_json
                CDN_SERVERS[base_url]["load"] = total_load
                CDN_SERVERS[base_url]["last_ok"] = True

            except Exception:
                # Mark server DOWN
                CDN_SERVERS[base_url]["last_ok"] = False
                CDN_SERVERS[base_url]["status"] = {}
                CDN_SERVERS[base_url]["load"] = 99999

        time.sleep(POLL_INTERVAL)


threading.Thread(target=poller, daemon=True).start()


# ============================================================
# API: add one or more CDN servers dynamically
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
        return jsonify({"error": "No URL provided"}), 400

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

    return jsonify({
        "added": added,
        "total_instances": len(CDN_SERVERS)
    })


# ============================================================
# Load selection logic
# ============================================================

def choose_cdn():
    best_server = None
    best_load = None

    for url, info in CDN_SERVERS.items():
        if not info.get("last_ok"):
            continue

        load = info.get("load", 99999)

        if best_load is None or load < best_load:
            best_load = load
            best_server = url

    return best_server


# ============================================================
# DL ROUTE: /dl/<hash>/<filename>
# ============================================================

@app.route("/dl/<hash>/<path:filename>")
def route_dl(hash, filename):

    # Count global usage
    USAGE[hash] = USAGE.get(hash, 0) + 1

    # Determine client IP
    client_ip = request.headers.get("X-Forwarded-For",
                    request.remote_addr).split(",")[0].strip()

    # Per-IP usage
    if client_ip not in IP_USAGE:
        IP_USAGE[client_ip] = {}

    IP_USAGE[client_ip][hash] = IP_USAGE[client_ip].get(hash, 0) + 1

    if IP_USAGE[client_ip][hash] > MAX_REQUESTS_PER_IP:
        return jsonify({
            "error": "IP limit exceeded",
            "allowed": MAX_REQUESTS_PER_IP,
            "your_requests": IP_USAGE[client_ip][hash],
            "hash": hash,
            "ip": client_ip
        }), 429

    # Pick least-loaded CDN
    server = choose_cdn()
    if not server:
        return "No CDN instance online", 503

    final_url = f"{server}/dl/{hash}/{filename}"
    return redirect(final_url, code=301)


# ============================================================
# WATCH ROUTE: /watch/<hash>
# ============================================================

@app.route("/watch/<hash>")
def route_watch(hash):

    # Count global usage
    USAGE[hash] = USAGE.get(hash, 0) + 1

    # Determine client IP
    client_ip = request.headers.get("X-Forwarded-For",
                    request.remote_addr).split(",")[0].strip()

    # Per-IP usage
    if client_ip not in IP_USAGE:
        IP_USAGE[client_ip] = {}

    IP_USAGE[client_ip][hash] = IP_USAGE[client_ip].get(hash, 0) + 1

    if IP_USAGE[client_ip][hash] > MAX_REQUESTS_PER_IP:
        return jsonify({
            "error": "IP limit exceeded",
            "allowed": MAX_REQUESTS_PER_IP,
            "your_requests": IP_USAGE[client_ip][hash],
            "hash": hash,
            "ip": client_ip
        }), 429

    # Pick least-loaded CDN
    server = choose_cdn()
    if not server:
        return "No CDN instance online", 503

    final_url = f"{server}/watch/{hash}"
    return redirect(final_url, code=301)


# ============================================================
# Stats
# ============================================================

@app.route("/stats")
def stats():
    return jsonify({
        "servers": CDN_SERVERS,
        "usage": USAGE,
        "ip_usage": IP_USAGE
    })


# ============================================================
# Run
# ============================================================

if __name__ == "__main__":
    print("Load balancer running on 0.0.0.0:8080")
    app.run(host="0.0.0.0", port=8080)
