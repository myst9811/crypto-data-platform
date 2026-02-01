FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better caching)
COPY requirements/base.txt requirements/base.txt
COPY requirements/ingestion.txt requirements/ingestion.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements/ingestion.txt

# Copy source code and config
COPY src/ src/
COPY config/ config/

# Set Python path
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command (will be overridden per producer)
CMD ["python", "-m", "src.ingestion.binance_producer"]
