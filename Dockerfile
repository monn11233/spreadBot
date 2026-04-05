FROM python:3.12-slim

# Install system deps (needed for some Python packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Persistent data lives here — mount a volume in production
RUN mkdir -p /data

# Render / Railway inject PORT env var; fall back to 8080
ENV PORT=8080
ENV DASHBOARD_HOST=0.0.0.0
ENV DB_PATH=/data/spreadbot.db
ENV BOT_MODE=paper

EXPOSE $PORT

CMD ["python", "main.py", "--mode", "paper"]
