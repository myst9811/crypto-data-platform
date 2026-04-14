FROM python:3.12-slim

# Install Java 17 (needed for PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV PYTORCH_ENABLE_MPS_FALLBACK=1

WORKDIR /app

# Install Python dependencies
COPY requirements.txt requirements/ ./
COPY requirements/ requirements/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY config/ config/
COPY src/ src/
COPY ml/ ml/

# Create data directories
RUN mkdir -p data/bronze data/silver data/gold data/checkpoints data/features \
    logs ml/artifacts mlruns

EXPOSE 8000 8501
