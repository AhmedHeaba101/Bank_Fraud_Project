#!/bin/bash
set -e

echo "[Hive Init] Waiting for Hive server to be ready..."
until beeline -u "jdbc:hive2://hive-server:10000" -e "SHOW DATABASES;" > /dev/null 2>&1; do
  echo "   Hive not ready yet, retrying..."
  sleep 5
done

echo "[Hive Init] Creating table 'transactions'..."
beeline -u "jdbc:hive2://hive-server:10000" -f /scripts/create_table.hql

echo "[Hive Init] Done."
