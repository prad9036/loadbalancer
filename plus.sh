#!/bin/bash

# Clone repo
git clone https://github.com/prad9036/filestream-deekshit
cd filestream-deekshit

# Checkout the most recent commit (by commit date)
git fetch --all
latest_commit=$(git rev-list --all --max-count=1 --date-order)
git checkout "$latest_commit"

# Install environment + start WebStreamer
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

nohup python -m WebStreamer > ws.log 2>&1 &
cd /workspace

# Start cloudflared (make sure log stays in /workspace)
curl -fsSL https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o cloudflared
chmod +x cloudflared
nohup ./cloudflared tunnel --url http://localhost:8082 > /workspace/8082.log 2>&1 &

# Wait for gunicorn on 8080
while ! nc -z localhost 8080; do
    sleep 1
done

# Wait until cloudflared generates a URL
while ! grep -Eo "https://[a-zA-Z0-9.-]+\.trycloudflare\.com" /workspace/8082.log >/dev/null 2>&1; do
    sleep 1
done

# Extract latest URL
cdn_url=$(grep -Eo "https://[a-zA-Z0-9.-]+\.trycloudflare\.com" /workspace/8082.log | tail -n 1)

# Add CDN to loadbalancer
curl "http://localhost:8080/add_cdn" \
    -H 'content-type: application/json' \
    -d "{\"url\":\"$cdn_url\"}"
