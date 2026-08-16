FROM python:3.11-slim

WORKDIR /app

# Install system dependencies if needed (e.g. curl for healthcheck or build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create directory for SQLite database
RUN mkdir -p /app/data

EXPOSE 8005

CMD ["python", "main.py"]
