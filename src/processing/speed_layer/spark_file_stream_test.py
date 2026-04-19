import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


DEFAULT_INPUT_DIR = Path("/tmp/crypto_trend_stream_input")
DEFAULT_CHECKPOINT_DIR = Path("/tmp/crypto_trend_stream_checkpoint")


def get_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("tweet_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("created_at", T.StringType(), True),
            T.StructField("username", T.StringType(), True),
            T.StructField("content", T.StringType(), True),
            T.StructField("hashtags", T.ArrayType(T.StringType()), True),
            T.StructField("cashtags", T.ArrayType(T.StringType()), True),
            T.StructField("like_count", T.IntegerType(), True),
            T.StructField("retweet_count", T.IntegerType(), True),
            T.StructField("reply_count", T.IntegerType(), True),
            T.StructField("lang", T.StringType(), True),
        ]
    )


def build_stream(input_dir: Path):
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spark-file-stream-test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.schema(get_schema())
        .option("maxFilesPerTrigger", 1)
        .json(str(input_dir))
    )

    cleaned = (
        raw_stream.withColumn("event_time", F.try_to_timestamp("created_at"))
        .withColumn("user_id", F.coalesce(F.col("user_id"), F.col("username"), F.lit("unknown")))
        .withColumn("username", F.coalesce(F.col("username"), F.lit("unknown")))
        .withColumn("like_count", F.coalesce(F.col("like_count").cast("int"), F.lit(0)))
        .withColumn("retweet_count", F.coalesce(F.col("retweet_count").cast("int"), F.lit(0)))
        .withColumn("reply_count", F.coalesce(F.col("reply_count").cast("int"), F.lit(0)))
        .withColumn(
            "engagement_score",
            F.col("like_count") + F.col("retweet_count") + F.col("reply_count"),
        )
        .filter(F.col("tweet_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("content").isNotNull())
        .filter(F.size(F.col("cashtags")) > 0)
        .withWatermark("event_time", "2 minutes")
        .dropDuplicates(["tweet_id"])
        .withColumn("symbol", F.explode("cashtags"))
        .withColumn("symbol", F.upper(F.col("symbol")))
    )

    trends = (
        cleaned.groupBy(F.window("event_time", "1 minute"), F.col("symbol"))
        .agg(
            F.count("*").alias("mention_count"),
            F.countDistinct("user_id").alias("unique_authors"),
            F.sum("engagement_score").alias("engagement_score"),
            F.max("event_time").alias("last_seen"),
        )
        .withColumn(
            "trend_score",
            F.round(
                F.col("mention_count") * F.lit(2.0)
                + F.col("unique_authors")
                + (F.col("engagement_score") / F.lit(10.0)),
                2,
            ),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "mention_count",
            "unique_authors",
            "engagement_score",
            "trend_score",
            "last_seen",
        )
    )

    return spark, trends


def main() -> None:
    parser = argparse.ArgumentParser(description="Spark file streaming test")
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--checkpoint-dir", type=Path, default=DEFAULT_CHECKPOINT_DIR)
    args = parser.parse_args()

    args.input_dir.mkdir(parents=True, exist_ok=True)
    args.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    spark, trends = build_stream(args.input_dir)

    query = (
        trends.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 30)
        .option("checkpointLocation", str(args.checkpoint_dir))
        .trigger(processingTime="3 seconds")
        .start()
    )

    print(f"Spark is watching: {args.input_dir}")
    print("Start mock_tweet_producer.py in another terminal to feed data.")
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
