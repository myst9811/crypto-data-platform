FROM bitnami/spark:3.5

USER root

# Install netcat for health checks
RUN apt-get update && apt-get install -y netcat-openbsd curl && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements/base.txt /tmp/base.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /tmp/base.txt

# Copy application code
COPY src/ /opt/spark-apps/
COPY config/ /opt/spark-apps/config/

# Copy submission script
COPY docker/spark-app/submit-job.sh /opt/spark-apps/submit-job.sh
RUN chmod +x /opt/spark-apps/submit-job.sh

# Create data directories
RUN mkdir -p /opt/spark-data/checkpoints /opt/spark-data/delta

USER 1001

WORKDIR /opt/spark-apps

CMD ["/bin/bash", "/opt/spark-apps/submit-job.sh"]
