#!/bin/bash

# Clone repo
git clone https://github.com/prad9036/filestream-deekshit
cd filestream-deekshit

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
    ./cloudflared tunnel --url http://localhost:$PORT >/workspace/cf$i.log 2>&1 &

    # Wait for cloudflared URL
    while true; do
        url=$(grep -Eo "https://[A-Za-z0-9.-]+\.trycloudflare\.com" /workspace/cf$i.log | tail -n1)
        [ -n "$url" ] && break
        sleep 1
    done

    # Retry add_cdn until HTTP 200
    while true; do
        status=$(curl -s -o /dev/null -w "%{http_code}" \
            -X POST \
            -H "Content-Type: application/json" \
            -H "X-Admin-key: $LB_ADMIN_KEY" \
            -d "{\"urls\":[\"$url\"]}" \
            "http://localhost:8080/add_cdn")

        echo "Tunnel $i response: $status"

        if [ "$status" -eq 200 ]; then
            echo "Tunnel $i registered successfully: $url"
            break
        fi

        sleep 2
    done
) &
done


# Wait for gunicorn on 8080
while ! nc -z localhost 8080; do
    sleep 1
done
