#!/bin/bash
set -e

echo "=== Spark Streaming Job Submission ==="
echo "Waiting for Spark master to be ready..."

# Wait for Spark master
for i in {1..30}; do
    if curl -s http://spark-master:8080 > /dev/null 2>&1; then
        echo "Spark master is ready!"
        break
    fi
    echo "Waiting for Spark master ($i/30)..."
    sleep 5
done

# Wait for Kafka
echo "Waiting for Kafka to be ready..."
for i in {1..30}; do
    if nc -z kafka 29092 2>/dev/null; then
        echo "Kafka is ready!"
        break
    fi
    echo "Waiting for Kafka ($i/30)..."
    sleep 5
done

# Additional wait for stability
echo "Waiting 10 seconds for services to stabilize..."
sleep 10

echo "Submitting Spark streaming job..."

/opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --name "crypto-streaming-pipeline" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=1g \
    --conf spark.streaming.kafka.maxRatePerPartition=100 \
    --conf spark.sql.streaming.checkpointLocation=/opt/spark-data/checkpoints \
    /opt/spark-apps/processing/spark_streaming.py \
    --config /opt/spark-apps/config/spark_config.yaml

echo "Spark job submitted successfully!"
