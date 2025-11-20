import threading
import time
import requests
from flask import Flask, redirect, request, jsonify

app = Flask(__name__)

# CDN server registry
# Example:
# {
#   "https://xxxx.trycloudflare.com": {
#        "status": {...}, 
#        "last_ok": True
#   }
# }
CDN_SERVERS = {}

# Global usage tracker for each hash
USAGE = {}

# Per-IP per-hash usage limiter
IP_USAGE = {}   # { ip: { hash: count } }

# Set the maximum allowed requests for 1 IP on one file hash
MAX_REQUESTS_PER_IP = 10   # adjust if needed

# Polling interval for /status
POLL_INTERVAL = 10


# ============================================================
# Background poller thread: checks all CDN server /status
# ============================================================
def poller():
    while True:
        for base_url in list(CDN_SERVERS.keys()):
            status_url = f"{base_url.rstrip('/')}/status"
            try:
                resp = requests.get(status_url, timeout=4)
                CDN_SERVERS[base_url]["status"] = resp.json()
                CDN_SERVERS[base_url]["last_ok"] = True
            except:
                CDN_SERVERS[base_url]["last_ok"] = False
        time.sleep(POLL_INTERVAL)


threading.Thread(target=poller, daemon=True).start()


# ============================================================
# API: Add new CDN instance dynamically
# ============================================================
@app.route('/add_cdn', methods=['POST'])
def add_cdn():
    global CDN_SERVERS

    data = request.json or {}
    urls = []

    # Support: { "url": "one" }
    if "url" in data:
        urls.append(data["url"])

    # Support: { "urls": ["a","b"] }
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
            # store default object
            CDN_SERVERS[url] = {
                "status": "unknown",
                "load": None,
                "connected_bots": None,
                "uptime": None,
                "version": None
            }

            added.append(url)

    return jsonify({
        "added": added,
        "total_instances": len(CDN_SERVERS)
    })


# ============================================================
# Load selection logic — picks least-loaded CDN
# ============================================================
def choose_cdn():
    best_server = None
    best_load = None

    for base_url, info in CDN_SERVERS.items():
        if not info.get("last_ok"):
            continue

        status = info.get("status", {})
        loads = status.get("loads", {})

        if not loads:
            continue

        total_load = sum(loads.values())

        if best_load is None or total_load < best_load:
            best_load = total_load
            best_server = base_url

    return best_server


# ============================================================
# Main DL endpoint with per-IP limit + usage tracking
# ============================================================
@app.route("/dl/<hash>/<path:filename>")
def route_dl(hash, filename):

    # Track total usage per hash
    USAGE[hash] = USAGE.get(hash, 0) + 1

    # Determine client IP
    client_ip = request.headers.get("X-Forwarded-For",
                    request.remote_addr).split(",")[0].strip()

    # Track per-IP usage
    if client_ip not in IP_USAGE:
        IP_USAGE[client_ip] = {}

    IP_USAGE[client_ip][hash] = IP_USAGE[client_ip].get(hash, 0) + 1

    # Enforce per-IP-per-file limit
    if IP_USAGE[client_ip][hash] > MAX_REQUESTS_PER_IP:
        return jsonify({
            "error": "IP limit exceeded",
            "allowed": MAX_REQUESTS_PER_IP,
            "your_requests": IP_USAGE[client_ip][hash],
            "hash": hash,
            "ip": client_ip
        }), 429

    # Pick best CDN
    server = choose_cdn()
    if not server:
        return "No CDN servers online", 503

    # Final redirect target (permanent)
    final_url = f"{server}/dl/{hash}/{filename}"
    return redirect(final_url, code=301)


@app.route("/watch/<hash_value>", methods=["GET"])
def watch(hash_value):
    global CDN_INSTANCES, LIVENESS, USAGE, IP_USAGE

    # --- pick CDN ---
    target = pick_best_cdn()
    if not target:
        return jsonify({"error": "No CDN instances available"}), 503

    # --- usage tracking ---
    USAGE[hash_value] = USAGE.get(hash_value, 0) + 1

    ip = request.remote_addr
    if ip not in IP_USAGE:
        IP_USAGE[ip] = {}

    IP_USAGE[ip][hash_value] = IP_USAGE[ip].get(hash_value, 0) + 1
    if IP_USAGE[ip][hash_value] > MAX_REQUESTS_PER_IP:
        return jsonify({
            "error": "Rate limit exceeded",
            "ip": ip,
            "hash": hash_value,
            "limit": MAX_REQUESTS_PER_IP
        }), 429

    # --- STRICT redirect path ---
    #   final → https://<instance>/watch/<hash>
    final_url = f"{target}/watch/{hash_value}"

    print("WATCH REDIRECT:", final_url)   # debug print (remove later)

    return redirect(final_url, code=301)


# ============================================================
# Stats endpoint for debugging/monitoring
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
