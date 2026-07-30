
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

spark = SparkSession.builder.appName("ReportGenerator").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

DATA_PATH = "hdfs://namenode:8020/data/transactions/"
OUTPUT_DIR = "/opt/reports"

os.makedirs(OUTPUT_DIR, exist_ok=True)

df = spark.read.parquet(DATA_PATH)
df.createOrReplaceTempView("transactions")

print(f"Total rows read: {df.count()}")


def save_json(filename, rows):
    path = os.path.join(OUTPUT_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)
    print(f"Saved {filename} ({len(rows)} rows)")


fraud_by_location = (
    df.filter(col("is_fraud") == True)
    .groupBy("location")
    .agg(count("*").alias("fraud_count"))
    .orderBy(col("fraud_count").desc())
)
save_json("fraud_by_location.json", [row.asDict() for row in fraud_by_location.collect()])


top_fraud_accounts = (
    df.filter(col("is_fraud") == True)
    .groupBy("account_id")
    .agg(count("*").alias("fraud_count"), spark_sum("amount").alias("total_amount"))
    .orderBy(col("fraud_count").desc())
    .limit(10)
)
save_json("top_fraud_accounts.json", [row.asDict() for row in top_fraud_accounts.collect()])


fraud_vs_normal = df.groupBy("is_fraud").agg(count("*").alias("total"))
save_json("fraud_vs_normal.json", [row.asDict() for row in fraud_vs_normal.collect()])


all_tx = df.select(
    "transaction_id", "account_id", "amount", "transaction_type",
    "location", "timestamp", "is_fraud"
)
save_json("all_transactions.json", [row.asDict() for row in all_tx.collect()])

print("All reports generated successfully.")
spark.stop()
