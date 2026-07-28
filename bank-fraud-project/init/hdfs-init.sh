#!/bin/bash
set -e

echo "[HDFS Init] Waiting for HDFS to be ready..."
until hdfs dfs -fs hdfs://namenode:8020 -ls / > /dev/null 2>&1; do
  echo "   HDFS not ready yet, retrying..."
  sleep 3
done

echo "[HDFS Init] Creating directories..."
hdfs dfs -fs hdfs://namenode:8020 -mkdir -p /data/transactions
hdfs dfs -fs hdfs://namenode:8020 -mkdir -p /checkpoints/transactions
hdfs dfs -fs hdfs://namenode:8020 -chmod -R 777 /data
hdfs dfs -fs hdfs://namenode:8020 -chmod -R 777 /checkpoints

echo "[HDFS Init] Done."
