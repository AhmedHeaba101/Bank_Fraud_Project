
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


flagged = transactions.withColumn(
    "is_fraud", when(col("amount") > 5000, True).otherwise(False)
)


query = (
    flagged.writeStream.format("parquet")
    .option("path", "hdfs://namenode:8020/data/transactions/")
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/transactions/")
    .outputMode("append")
    .start()
)

query.awaitTermination()
