"""
Fraud Detection Streaming Job
------------------------------
بيقرأ المعاملات من Kafka topic (bank-transactions)، يطبّق قاعدة بسيطة
لكشف الفراود، ويخزن النتيجة في HDFS كـ Parquet جاهزة إن Hive يقرأها.

Run:
    spark-submit --master spark://spark-master:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
      fraud_detection_streaming.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("BankFraudDetection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
    .add("transaction_id", StringType())
    .add("account_id", StringType())
    .add("amount", DoubleType())
    .add("transaction_type", StringType())
    .add("location", StringType())
    .add("timestamp", StringType())
)

# قراءة الستريم من Kafka
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "bank-transactions")
    .option("startingOffsets", "latest")
    .load()
)

transactions = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# قاعدة بسيطة لكشف الفراود: أي معاملة أكبر من 5000 تعتبر مشبوهة
flagged = transactions.withColumn(
    "is_fraud", when(col("amount") > 5000, True).otherwise(False)
)

# تخزين النتيجة في HDFS كـ Parquet (Hive هيقرأ من هنا)
query = (
    flagged.writeStream.format("parquet")
    .option("path", "hdfs://namenode:8020/data/transactions/")
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/transactions/")
    .outputMode("append")
    .start()
)

query.awaitTermination()
