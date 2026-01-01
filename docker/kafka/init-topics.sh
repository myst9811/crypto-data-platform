#!/bin/bash

echo "Waiting for Kafka to be ready..."
sleep 10

echo "Creating Kafka topics..."

# Create raw-trades topic
kafka-topics --create \
  --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic raw-trades \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config compression.type=snappy

echo "Created topic: raw-trades"

# Create raw-orderbook topic
kafka-topics --create \
  --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic raw-orderbook \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config compression.type=snappy

echo "Created topic: raw-orderbook"

# Create raw-ticker topic
kafka-topics --create \
  --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic raw-ticker \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config compression.type=snappy

echo "Created topic: raw-ticker"

# List all topics
echo ""
echo "Available topics:"
kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "Kafka topics initialized successfully!"
