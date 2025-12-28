# Load Balancer with CDN Selection and Special Hash Protection

A Python-based load balancer for serving files via multiple CDNs, with IP rate limiting, referer checks, and special hash protection. Built using **FastAPI** / **Flask**, **LMDB** for CDN metadata storage, and **Redis** for persisting special hashes.

---

## Features

- Dynamic CDN selection based on load.
- IP-based rate limiting per file/hash.
- Referer whitelist to restrict unauthorized links.
- Special hash support to redirect certain requests to a Telegram bot.
- Persistent CDN storage using **LMDB**.
- Special hashes persisted in **Redis**.
- Admin APIs for managing CDNs and special hashes.
- Leader election for polling CDN status in multi-instance setups.

---

## Requirements

- Python 3.12+
- Redis server
- LMDB (Python package `lmdb`)
- Flask (or FastAPI)
- Requests
- Gunicorn (optional for production)
- dotenv for environment variable management

Install Python dependencies:

```bash
pip install flask redis lmdb requests python-dotenv gunicorn
```

---

## Environment Variables

Create a `.env` file in the root directory with the following variables:

```ini
LB_ADMIN_KEY=supersecretkey
REDIS_URL=redis://localhost:6379/0
LB_MAX_REQUESTS_PER_IP=10
LB_TTL_SECONDS=18000
LB_POLL_INTERVAL=10
LB_REDIRECT_CODE=302
LB_REFERER_WHITELIST=example.com,mywebsite.com
LB_CDN_URLS=https://cdn1.example.com,https://cdn2.example.com
KOYEB_INSTANCE_ID=local-0  # for leader election
LB_DEBUG=1
```

- `LB_ADMIN_KEY`: Key to access admin APIs.
- `REDIS_URL`: Redis connection URL for special hashes.
- `LB_MAX_REQUESTS_PER_IP`: Max requests per IP per hash.
- `LB_TTL_SECONDS`: Time window for rate limiting.
- `LB_POLL_INTERVAL`: Interval (in seconds) for polling CDN load.
- `LB_REDIRECT_CODE`: HTTP redirect code (302/307).
- `LB_REFERER_WHITELIST`: Comma-separated domains allowed as referers.
- `LB_CDN_URLS`: Initial list of CDN URLs.
- `KOYEB_INSTANCE_ID`: Unique instance ID to elect leader.
- `LB_DEBUG`: Enable debug logging (`1` for on).

---

## Running Locally

Run the app using **Flask** directly:

```bash
python loadbalancer.py
```

Or using **Gunicorn** with 2 workers:

```bash
gunicorn -w 2 -b 0.0.0.0:8000 loadbalancer:app
```

---

## API Endpoints

### Health Check

```http
GET /health
```
Returns `ok` if the service is running.

### Add CDN (Admin)

```http
POST /add_cdn
Headers: X-Admin-Key: <ADMIN_KEY>
Body: { "urls": ["https://cdn3.example.com"] }
```
Adds new CDN URLs to LMDB.

### Add Special Hashes (Admin)

```http
POST /add_special
Headers: X-Admin-Key: <ADMIN_KEY>
Body: { "hashes": ["hash1", "hash2"] }
```
Stores special hashes in Redis and updates the cache.

### Download File

```http
GET /dl/<hash>/<filename>
```
Redirects to the best CDN. If the hash is special or referer is blocked, redirects to Telegram bot.

### Watch File

```http
GET /watch/<hash>
```
Similar to `/dl/`, redirects to streaming path.

### Stats (Admin)

```http
GET /stats
Headers: X-Admin-Key: <ADMIN_KEY>
```
Returns JSON with all CDN metadata, best CDN, and special hashes.

---

## Notes

- **CDNs**: Stored in LMDB (`cdn.lmdb`), including load, last OK status, and timestamps.
- **Special hashes**: Stored in Redis for persistence across restarts.
- **Leader election**: Only the instance with `KOYEB_INSTANCE_ID` ending in `0` runs the poller thread.
- **Rate limiting**: In-memory, configurable per IP/hash combination.
- **Referer whitelist**: Only requests coming from allowed domains are served directly.

---

## License

MIT License

---

## Example Usage

```bash
# Get stats
curl -H "X-Admin-Key: supersecretkey" http://localhost:8000/stats

# Add a new CDN
curl -X POST -H "X-Admin-Key: supersecretkey" -H "Content-Type: application/json" \
    -d '{"urls": ["https://cdn3.example.com"]}' \
    http://localhost:8000/add_cdn

# Download a file
curl http://localhost:8000/dl/hash123/file.mp4
```
