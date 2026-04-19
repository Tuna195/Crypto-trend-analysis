"""
Speed layer streaming job for crypto trend analysis.

This module supports two execution modes:

1. `--demo`: pure-Python pipeline that parses sample Kafka messages, cleans them,
   removes duplicates, and aggregates simple trend metrics. This mode works even
   when Spark/Kafka are not installed.
2. default: PySpark Structured Streaming job skeleton for reading from Kafka.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ModuleNotFoundError:  # pragma: no cover - handled by demo mode
    DataFrame = Any  # type: ignore[assignment]
    SparkSession = Any  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    T = None  # type: ignore[assignment]


CASHTAG_PATTERN = re.compile(r"\$([A-Za-z][A-Za-z0-9]{1,9})")
DEFAULT_SAMPLE_PATH = (
    Path(__file__).resolve().parent / "sample_data" / "tweet_stream.jsonl"
)


@dataclass
class CleanTweet:
    tweet_id: str
    user_id: str
    created_at: datetime
    username: str
    content: str
    hashtags: list[str]
    cashtags: list[str]
    like_count: int
    retweet_count: int
    reply_count: int
    lang: str
    engagement_score: int


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_int(value: Any) -> int:
    if value is None or value == "":
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def extract_cashtags(content: str) -> list[str]:
    unique = []
    for match in CASHTAG_PATTERN.findall(content):
        symbol = match.upper()
        if symbol not in unique:
            unique.append(symbol)
    return unique


def ensure_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = [value]

    normalized: list[str] = []
    for item in raw_items:
        text = normalize_text(item).strip("#$")
        if not text:
            continue
        normalized.append(text.upper())
    return normalized


def parse_json_line(line: str) -> dict[str, Any] | None:
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def clean_tweet(record: dict[str, Any]) -> CleanTweet | None:
    tweet_id = normalize_text(record.get("tweet_id"))
    user_id = normalize_text(record.get("user_id")) or normalize_text(record.get("author_id"))
    created_at = parse_iso_datetime(record.get("created_at"))
    username = normalize_text(record.get("username")) or "unknown"
    if not user_id:
        user_id = username
    content = normalize_text(record.get("content"))

    hashtags = ensure_list(record.get("hashtags"))
    cashtags = ensure_list(record.get("cashtags"))
    if not cashtags:
        cashtags = extract_cashtags(content)

    like_count = parse_int(record.get("like_count"))
    retweet_count = parse_int(record.get("retweet_count"))
    reply_count = parse_int(record.get("reply_count"))
    lang = normalize_text(record.get("lang")).lower() or "und"

    if not tweet_id or created_at is None or not content or not cashtags:
        return None

    return CleanTweet(
        tweet_id=tweet_id,
        user_id=user_id,
        created_at=created_at,
        username=username,
        content=content,
        hashtags=hashtags,
        cashtags=cashtags,
        like_count=like_count,
        retweet_count=retweet_count,
        reply_count=reply_count,
        lang=lang,
        engagement_score=like_count + retweet_count + reply_count,
    )


def deduplicate_tweets(tweets: Iterable[CleanTweet]) -> list[CleanTweet]:
    latest_by_id: dict[str, CleanTweet] = {}
    for tweet in tweets:
        current = latest_by_id.get(tweet.tweet_id)
        if current is None or tweet.created_at > current.created_at:
            latest_by_id[tweet.tweet_id] = tweet
    return sorted(latest_by_id.values(), key=lambda item: item.created_at)


def aggregate_trends(tweets: Iterable[CleanTweet]) -> list[dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "mention_count": 0,
            "engagement_score": 0,
            "authors": set(),
            "last_seen": None,
        }
    )

    for tweet in tweets:
        for symbol in tweet.cashtags:
            metric = metrics[symbol]
            metric["mention_count"] += 1
            metric["engagement_score"] += tweet.engagement_score
            metric["authors"].add(tweet.user_id)
            if metric["last_seen"] is None or tweet.created_at > metric["last_seen"]:
                metric["last_seen"] = tweet.created_at

    rows = []
    for symbol, metric in metrics.items():
        mention_count = metric["mention_count"]
        engagement_score = metric["engagement_score"]
        unique_authors = len(metric["authors"])
        trend_score = mention_count * 2 + unique_authors + (engagement_score / 10.0)
        rows.append(
            {
                "symbol": symbol,
                "mention_count": mention_count,
                "unique_authors": unique_authors,
                "engagement_score": engagement_score,
                "trend_score": round(trend_score, 2),
                "last_seen": metric["last_seen"].isoformat(),
            }
        )

    return sorted(
        rows,
        key=lambda item: (item["trend_score"], item["engagement_score"]),
        reverse=True,
    )


def load_demo_messages(path: Path) -> list[str]:
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def run_demo(sample_path: Path) -> int:
    messages = load_demo_messages(sample_path)
    parsed = [parse_json_line(line) for line in messages]
    parsed_valid = [record for record in parsed if record is not None]
    cleaned = [clean_tweet(record) for record in parsed_valid]
    cleaned_valid = [tweet for tweet in cleaned if tweet is not None]
    deduplicated = deduplicate_tweets(cleaned_valid)
    aggregated = aggregate_trends(deduplicated)

    print("=== Demo Summary ===")
    print(f"raw_messages={len(messages)}")
    print(f"parsed_messages={len(parsed_valid)}")
    print(f"clean_messages={len(cleaned_valid)}")
    print(f"deduplicated_messages={len(deduplicated)}")
    print()

    print("=== Clean Tweets ===")
    for tweet in deduplicated:
        print(
            json.dumps(
                {
                    "tweet_id": tweet.tweet_id,
                    "user_id": tweet.user_id,
                    "created_at": tweet.created_at.isoformat(),
                    "username": tweet.username,
                    "cashtags": tweet.cashtags,
                    "engagement_score": tweet.engagement_score,
                    "content": tweet.content,
                },
                ensure_ascii=True,
            )
        )

    print()
    print("=== Aggregated Trends ===")
    for row in aggregated:
        print(json.dumps(row, ensure_ascii=True))

    return 0


def get_tweet_schema() -> Any:
    if T is None:
        raise RuntimeError("PySpark is not installed")

    return T.StructType(
        [
            T.StructField("tweet_id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("author_id", T.StringType(), True),
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


def build_spark_session(app_name: str) -> SparkSession:
    if SparkSession is Any:
        raise RuntimeError("PySpark is not installed")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def create_kafka_stream(
    spark: SparkSession, bootstrap_servers: str, topic: str
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )


def transform_stream(raw_df: DataFrame) -> DataFrame:
    if F is None:
        raise RuntimeError("PySpark is not installed")

    schema = get_tweet_schema()

    parsed = (
        raw_df.selectExpr("CAST(value AS STRING) AS raw_json")
        .withColumn("json_data", F.from_json(F.col("raw_json"), schema))
        .select("raw_json", "json_data.*")
    )

    cleaned = (
        parsed.withColumn("content", F.trim(F.regexp_replace(F.col("content"), r"\s+", " ")))
        .withColumn(
            "user_id",
            F.coalesce(F.col("user_id"), F.col("author_id"), F.col("username"), F.lit("unknown")),
        )
        .withColumn("username", F.coalesce(F.col("username"), F.lit("unknown")))
        .withColumn("lang", F.lower(F.coalesce(F.col("lang"), F.lit("und"))))
        .withColumn("event_time", F.try_to_timestamp("created_at"))
        .withColumn(
            "like_count",
            F.coalesce(F.col("like_count"), F.lit(0)),
        )
        .withColumn(
            "retweet_count",
            F.coalesce(F.col("retweet_count"), F.lit(0)),
        )
        .withColumn(
            "reply_count",
            F.coalesce(F.col("reply_count"), F.lit(0)),
        )
        .withColumn(
            "engagement_score",
            F.col("like_count") + F.col("retweet_count") + F.col("reply_count"),
        )
        .withColumn(
            "cashtags",
            F.when(
                F.size(F.coalesce(F.col("cashtags"), F.array().cast("array<string>"))) > 0,
                F.col("cashtags"),
            ).otherwise(
                F.regexp_extract_all(F.col("content"), r"\$([A-Za-z][A-Za-z0-9]{1,9})", 1)
            ),
        )
        .filter(F.col("tweet_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
        .filter(F.col("content").isNotNull() & (F.length("content") > 0))
        .filter(F.size("cashtags") > 0)
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["tweet_id"])
        .withColumn("symbol", F.explode("cashtags"))
        .withColumn("symbol", F.upper(F.col("symbol")))
    )

    return (
        cleaned.groupBy(
            F.window("event_time", "5 minutes", "1 minute"),
            F.col("symbol"),
        )
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


def run_stream_job(bootstrap_servers: str, topic: str) -> int:
    spark = build_spark_session("crypto-trend-speed-layer")
    raw_stream = create_kafka_stream(spark, bootstrap_servers, topic)
    trend_stream = transform_stream(raw_stream)

    query = (
        trend_stream.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .start()
    )

    query.awaitTermination()
    return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crypto trend streaming demo/job")
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run the pure-Python demo pipeline with sample messages",
    )
    parser.add_argument(
        "--sample-path",
        type=Path,
        default=DEFAULT_SAMPLE_PATH,
        help="Path to the sample .jsonl messages for demo mode",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers for Spark streaming mode",
    )
    parser.add_argument(
        "--topic",
        default="twitter-crypto-stream",
        help="Kafka topic for Spark streaming mode",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()

    if args.demo:
        return run_demo(args.sample_path)

    return run_stream_job(args.bootstrap_servers, args.topic)


if __name__ == "__main__":
    raise SystemExit(main())
