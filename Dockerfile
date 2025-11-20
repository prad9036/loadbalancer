FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Koyeb expects the app to listen on $PORT
ENV PORT=8080

CMD ["python", "loadbalancer.py"]
