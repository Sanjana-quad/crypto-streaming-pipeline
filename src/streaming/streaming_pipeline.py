from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, DoubleType

from src.utils.config_loader import load_config
from src.utils.logger import setup_logger
from pyspark.sql.functions import window, avg, to_timestamp

logger = setup_logger()


def get_spark():
    return (
        SparkSession.builder
        .appName("CryptoStreamingPipeline")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .getOrCreate()
    )


def get_schema():
    return StructType() \
        .add("id", StringType()) \
        .add("symbol", StringType()) \
        .add("name", StringType()) \
        .add("current_price", DoubleType()) \
        .add("market_cap", DoubleType()) \
        .add("total_volume", DoubleType()) \
        .add("price_change_percentage_24h", DoubleType())


def run_stream():
    logger.info("Starting Spark Streaming...")

    config = load_config()
    spark = get_spark()

    # Read from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config["kafka"]["bootstrap_servers"])
        .option("subscribe", config["kafka"]["topic"])
        .option("startingOffsets", "latest")
        .load()
    )

    # # Convert Kafka value (binary → string)
    # json_df = df.selectExpr("CAST(value AS STRING)")

    # Parse JSON
    schema = get_schema()

    # parsed_df = json_df.select(
    #     from_json(col("value"), schema).alias("data")
    # ).select("data.*")

    # # Basic cleaning
    # cleaned_df = parsed_df.dropna(subset=["current_price"])

    # Convert timestamp (simulate event time)
    df_with_time = df.selectExpr(
    "CAST(value AS STRING)", 
    "timestamp"
    )

    parsed_df = df_with_time.select(
        from_json(col("value"), schema).alias("data"),
        col("timestamp").alias("event_time")
    ).select("data.*", "event_time")

    cleaned_df = parsed_df.dropna(subset=["current_price"])

    windowed_df = cleaned_df \
    .withWatermark("event_time", "2 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol")
    ).agg(
        avg("current_price").alias("avg_price")
    )
    console_query = windowed_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()
    # Write to Parquet continuously
    parquet_query = (
        windowed_df.writeStream
        .format("parquet")
        .option("path", config["paths"]["stream_output"])
        .option("checkpointLocation", config["paths"]["checkpoint"])
        .outputMode("append")
        .start()
    )

    logger.info("Streaming started...")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_stream()
