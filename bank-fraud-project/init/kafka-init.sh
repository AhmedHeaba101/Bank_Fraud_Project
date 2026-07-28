#!/bin/bash
set -e

echo "[Kafka Init] Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do
  echo "   Kafka not ready yet, retrying..."
  sleep 3
done

echo "[Kafka Init] Creating topic 'bank-transactions'..."
kafka-topics --create \
  --topic bank-transactions \
  --bootstrap-server kafka:29092 \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "[Kafka Init] Done."
