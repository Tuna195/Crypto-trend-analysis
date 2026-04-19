from pyspark.sql import SparkSession
from pyspark.sql import functions as F


SAMPLE_PATH = "/Users/vuquangthang/Crypto-trend-analysis/src/processing/speed_layer/sample_data/tweet_stream.jsonl"


def main() -> None:
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("spark-local-test")
        .getOrCreate()
    )

    df = spark.read.json(SAMPLE_PATH)

    cleaned = (
        df.withColumn("event_time", F.try_to_timestamp("created_at"))
        .withColumn("username", F.coalesce(F.col("username"), F.lit("unknown")))
        .withColumn("like_count", F.coalesce(F.col("like_count").cast("int"), F.lit(0)))
        .withColumn(
            "retweet_count", F.coalesce(F.col("retweet_count").cast("int"), F.lit(0))
        )
        .withColumn("reply_count", F.coalesce(F.col("reply_count").cast("int"), F.lit(0)))
        .withColumn(
            "engagement_score",
            F.col("like_count") + F.col("retweet_count") + F.col("reply_count"),
        )
        .filter(F.col("tweet_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("content").isNotNull())
        .filter(F.size(F.col("cashtags")) > 0)
        .dropDuplicates(["tweet_id"])
        .withColumn("symbol", F.explode("cashtags"))
    )

    result = (
        cleaned.groupBy("symbol")
        .agg(
            F.count("*").alias("mention_count"),
            F.countDistinct("username").alias("unique_authors"),
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
        .orderBy(F.desc("trend_score"))
    )

    print("=== Raw Input ===")
    df.select("tweet_id", "created_at", "content", "cashtags").show(truncate=False)

    print("=== Cleaned Stream ===")
    cleaned.select(
        "tweet_id",
        "event_time",
        "username",
        "symbol",
        "engagement_score",
        "content",
    ).show(truncate=False)

    print("=== Aggregated Trends ===")
    result.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
