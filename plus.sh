#!/bin/bash

# Start cloudflared
nohup ./cloudflared tunnel --url http://localhost:8082 >8082.log 2>&1 &

# Start WebStreamer
nohup python -m WebStreamer >ws.log 2>&1 &

# Wait for Gunicorn on 8080
while ! nc -z localhost 8080; do
    sleep 1
done

# Wait for a Cloudflare URL
while ! grep -Eo "https://[a-zA-Z0-9.-]+\.trycloudflare\.com" 8082.log >/dev/null; do
    sleep 1
done

# Add CDN
curl "http://localhost:8080/add_cdn" \
    -H 'content-type: application/json' \
    -d "{\"url\":\"$(grep -Eo 'https://[a-zA-Z0-9.-]+\.trycloudflare\.com' 8082.log | tail -n 1)\"}"
