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

# Number of Cloudflare tunnels to run (set via environment variable)
NUM_TUNNELS=${NUM_TUNNELS:-2}  # default to 2 if not set
PORT=${PORT:-8082}             # default port 8082

# Download cloudflared once
wget -qO cloudflared https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64
chmod +x cloudflared

for i in $(seq 1 "$NUM_TUNNELS"); do
(
    # Run the tunnel in the background and log output
    ./cloudflared tunnel --url http://localhost:$PORT >/workspace/cf$i.log 2>&1 &

    # Loop to extract the URL and add it to the load balancer
    while true; do
        url=$(grep -Eo "https://[A-Za-z0-9.-]+\.trycloudflare\.com" /workspace/cf$i.log | tail -n1)
        if [ -n "$url" ]; then
            curl -s -H "content-type: application/json" -d "{\"url\":\"$url\"}" "http://localhost:8080/add_cdn"
            sleep 300
        else
            sleep 1
        fi
    done
) &
done

# Wait for gunicorn on 8080
while ! nc -z localhost 8080; do
    sleep 1
done
