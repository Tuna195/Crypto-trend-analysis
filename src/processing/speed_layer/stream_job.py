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
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession as SparkSessionType
else:
    SparkDataFrame = Any
    SparkSessionType = Any

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except ModuleNotFoundError:  # pragma: no cover - handled by demo mode
    SparkSession = None  # type: ignore[assignment]
    F = None  # type: ignore[assignment]
    T = None  # type: ignore[assignment]


CASHTAG_PATTERN = re.compile(r"\$([A-Za-z][A-Za-z0-9]{1,9})")
DEFAULT_SAMPLE_PATH = (
    Path(__file__).resolve().parent / "sample_data" / "tweet_stream.jsonl"
)
DEFAULT_CHECKPOINT_LOCATION = "/tmp/crypto_trend_kafka_checkpoint"
DEFAULT_TOPICS = "raw-tweets-market,raw-tweets-whales"
WHALE_AUTHORS = {"elonmusk", "saylor", "vitalikbuterin", "whale_alert"}


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
    author_type: str
    author_weight: float
    influence_score: float


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


def parse_float(value: Any, default: float = 1.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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


def normalize_target_coin(value: Any) -> list[str]:
    text = normalize_text(value)
    if not text or text == "WHALE_SIGNAL":
        return []
    return ensure_list(text)


def classify_author(author: str, target_coin: str = "") -> tuple[str, float]:
    normalized_author = normalize_text(author).lower().lstrip("@")
    normalized_target = normalize_text(target_coin).upper()
    if normalized_target == "WHALE_SIGNAL" or normalized_author in WHALE_AUTHORS:
        return "whale", 5.0
    return "market", 1.0


def parse_json_line(line: str) -> dict[str, Any] | None:
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def clean_tweet(record: dict[str, Any]) -> CleanTweet | None:
    tweet_id = normalize_text(record.get("tweet_id")) or normalize_text(record.get("id"))
    author = normalize_text(record.get("author"))
    target_coin = normalize_text(record.get("target_coin"))
    user_id = (
        normalize_text(record.get("user_id"))
        or normalize_text(record.get("author_id"))
        or author
    )
    created_at = parse_iso_datetime(record.get("created_at"))
    username = normalize_text(record.get("username")) or author or "unknown"
    if not user_id:
        user_id = username
    content = normalize_text(record.get("content")) or normalize_text(record.get("text"))

    hashtags = ensure_list(record.get("hashtags"))
    cashtags = ensure_list(record.get("cashtags"))
    if not cashtags:
        cashtags = extract_cashtags(content)
    if not cashtags:
        cashtags = normalize_target_coin(target_coin)

    like_count = parse_int(record.get("like_count"))
    retweet_count = parse_int(record.get("retweet_count"))
    reply_count = parse_int(record.get("reply_count"))
    lang = normalize_text(record.get("lang")).lower() or "und"
    author_type = normalize_text(record.get("author_type")).lower()
    author_weight = parse_float(record.get("author_weight"))
    if not author_type:
        author_type, author_weight = classify_author(username, target_coin)
    engagement_score = like_count + retweet_count + reply_count

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
        engagement_score=engagement_score,
        author_type=author_type,
        author_weight=author_weight,
        influence_score=author_weight * engagement_score,
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
            "influence_score": 0.0,
            "influencer_authors": set(),
            "max_author_weight": 1.0,
            "authors": set(),
            "last_seen": None,
        }
    )

    for tweet in tweets:
        for symbol in tweet.cashtags:
            metric = metrics[symbol]
            metric["mention_count"] += 1
            metric["engagement_score"] += tweet.engagement_score
            metric["influence_score"] += tweet.influence_score
            metric["authors"].add(tweet.user_id)
            if tweet.author_weight > 1.0:
                metric["influencer_authors"].add(tweet.user_id)
            metric["max_author_weight"] = max(metric["max_author_weight"], tweet.author_weight)
            if metric["last_seen"] is None or tweet.created_at > metric["last_seen"]:
                metric["last_seen"] = tweet.created_at

    rows = []
    for symbol, metric in metrics.items():
        mention_count = metric["mention_count"]
        engagement_score = metric["engagement_score"]
        influence_score = metric["influence_score"]
        unique_authors = len(metric["authors"])
        influencer_authors = len(metric["influencer_authors"])
        trend_score = (
            mention_count * 2
            + unique_authors
            + (engagement_score / 10.0)
            + (influence_score / 20.0)
            + (influencer_authors * 3.0)
        )
        rows.append(
            {
                "symbol": symbol,
                "mention_count": mention_count,
                "unique_authors": unique_authors,
                "influencer_authors": influencer_authors,
                "engagement_score": engagement_score,
                "influence_score": round(influence_score, 2),
                "max_author_weight": metric["max_author_weight"],
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
                    "author_type": tweet.author_type,
                    "author_weight": tweet.author_weight,
                    "influence_score": tweet.influence_score,
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
            T.StructField("id", T.StringType(), True),
            T.StructField("user_id", T.StringType(), True),
            T.StructField("author_id", T.StringType(), True),
            T.StructField("created_at", T.StringType(), True),
            T.StructField("username", T.StringType(), True),
            T.StructField("author", T.StringType(), True),
            T.StructField("content", T.StringType(), True),
            T.StructField("text", T.StringType(), True),
            T.StructField("target_coin", T.StringType(), True),
            T.StructField("hashtags", T.ArrayType(T.StringType()), True),
            T.StructField("cashtags", T.ArrayType(T.StringType()), True),
            T.StructField("like_count", T.StringType(), True),
            T.StructField("retweet_count", T.StringType(), True),
            T.StructField("reply_count", T.StringType(), True),
            T.StructField("lang", T.StringType(), True),
            T.StructField("author_type", T.StringType(), True),
            T.StructField("author_weight", T.StringType(), True),
        ]
    )


def build_spark_session(app_name: str) -> SparkSessionType:
    if SparkSession is None:
        raise RuntimeError("PySpark is not installed")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def read_text_file(path: str | None) -> str | None:
    if not path:
        return None
    return Path(path).read_text(encoding="utf-8")


def build_kafka_options(args: argparse.Namespace) -> dict[str, str]:
    options = {
        "kafka.bootstrap.servers": args.bootstrap_servers,
        "subscribe": args.topic,
        "startingOffsets": args.starting_offsets,
    }

    if args.kafka_security_protocol:
        options["kafka.security.protocol"] = args.kafka_security_protocol

    ca = read_text_file(args.kafka_ssl_ca_location)
    cert = read_text_file(args.kafka_ssl_cert_location)
    key = read_text_file(args.kafka_ssl_key_location)
    if ca:
        options["kafka.ssl.truststore.type"] = "PEM"
        options["kafka.ssl.truststore.certificates"] = ca
    if cert and key:
        options["kafka.ssl.keystore.type"] = "PEM"
        options["kafka.ssl.keystore.certificate.chain"] = cert
        options["kafka.ssl.keystore.key"] = key

    return options


def create_kafka_stream(spark: SparkSessionType, options: dict[str, str]) -> SparkDataFrame:
    reader = spark.readStream.format("kafka")
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load()


def build_stream_outputs(raw_df: SparkDataFrame) -> tuple[SparkDataFrame, SparkDataFrame]:
    if F is None:
        raise RuntimeError("PySpark is not installed")

    schema = get_tweet_schema()

    parsed = (
        raw_df.selectExpr("topic", "CAST(value AS STRING) AS raw_json")
        .withColumn("json_data", F.from_json(F.col("raw_json"), schema))
        .select("raw_json", "json_data.*")
    )

    empty_string_array = F.array().cast("array<string>")
    target_coin_symbol = F.regexp_replace(F.upper(F.col("target_coin")), r"^\$", "")
    whale_author_names = F.array(*[F.lit(author) for author in sorted(WHALE_AUTHORS)])

    normalized = (
        parsed.withColumn("tweet_id", F.coalesce(F.col("tweet_id"), F.col("id")))
        .withColumn("content", F.coalesce(F.col("content"), F.col("text")))
        .withColumn("content", F.trim(F.regexp_replace(F.col("content"), r"\s+", " ")))
        .withColumn(
            "user_id",
            F.coalesce(
                F.col("user_id"),
                F.col("author_id"),
                F.col("author"),
                F.col("username"),
                F.lit("unknown"),
            ),
        )
        .withColumn("username", F.coalesce(F.col("username"), F.col("author"), F.lit("unknown")))
        .withColumn("lang", F.lower(F.coalesce(F.col("lang"), F.lit("und"))))
        .withColumn("event_time", F.try_to_timestamp("created_at"))
        .withColumn(
            "like_count",
            F.coalesce(F.col("like_count").cast("int"), F.lit(0)),
        )
        .withColumn(
            "retweet_count",
            F.coalesce(F.col("retweet_count").cast("int"), F.lit(0)),
        )
        .withColumn(
            "reply_count",
            F.coalesce(F.col("reply_count").cast("int"), F.lit(0)),
        )
        .withColumn(
            "engagement_score",
            F.col("like_count") + F.col("retweet_count") + F.col("reply_count"),
        )
        .withColumn(
            "cashtags",
            F.when(
                F.size(F.coalesce(F.col("cashtags"), empty_string_array)) > 0,
                F.col("cashtags"),
            )
            .when(
                F.col("target_coin").isNotNull()
                & (target_coin_symbol != F.lit("WHALE_SIGNAL"))
                & (F.length(target_coin_symbol) > 0),
                F.array(target_coin_symbol),
            )
            .otherwise(
                F.regexp_extract_all(F.col("content"), r"\$([A-Za-z][A-Za-z0-9]{1,9})", 1)
            ),
        )
        .withColumn(
            "author_weight",
            F.coalesce(
                F.col("author_weight").cast("double"),
                F.when(
                    (F.upper(F.col("target_coin")) == F.lit("WHALE_SIGNAL"))
                    | F.array_contains(whale_author_names, F.lower(F.col("username"))),
                    F.lit(5.0),
                ).otherwise(F.lit(1.0)),
            ),
        )
        .withColumn(
            "author_type",
            F.coalesce(
                F.lower(F.col("author_type")),
                F.when(F.col("author_weight") >= F.lit(5.0), F.lit("whale")).otherwise(
                    F.lit("market")
                ),
            ),
        )
        .withColumn("influence_score", F.col("author_weight") * F.col("engagement_score"))
        .withColumn(
            "invalid_reason",
            F.when(F.col("json_data").isNull(), F.lit("bad_json"))
            .when(F.col("tweet_id").isNull() | (F.length("tweet_id") == 0), F.lit("missing_id"))
            .when(F.col("event_time").isNull(), F.lit("bad_timestamp"))
            .when(
                F.col("content").isNull() | (F.length("content") == 0),
                F.lit("missing_content"),
            )
            .when(F.size("cashtags") == 0, F.lit("missing_symbol")),
        )
    )

    invalid_records = normalized.filter(F.col("invalid_reason").isNotNull())
    bad_record_metrics = (
        invalid_records.withColumn("observed_at", F.current_timestamp())
        .withWatermark("observed_at", "10 minutes")
        .groupBy(F.window("observed_at", "5 minutes"), F.col("invalid_reason"))
        .agg(F.count("*").alias("bad_record_count"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "invalid_reason",
            "bad_record_count",
        )
    )

    cleaned = (
        normalized.filter(F.col("invalid_reason").isNull())
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
            F.countDistinct(
                F.when(F.col("author_weight") > F.lit(1.0), F.col("user_id"))
            ).alias("influencer_authors"),
            F.sum("engagement_score").alias("engagement_score"),
            F.round(F.sum("influence_score"), 2).alias("influence_score"),
            F.max("author_weight").alias("max_author_weight"),
            F.max("event_time").alias("last_seen"),
        )
        .withColumn(
            "trend_score",
            F.round(
                F.col("mention_count") * F.lit(2.0)
                + F.col("unique_authors")
                + (F.col("engagement_score") / F.lit(10.0))
                + (F.col("influence_score") / F.lit(20.0))
                + (F.col("influencer_authors") * F.lit(3.0)),
                2,
            ),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "symbol",
            "mention_count",
            "unique_authors",
            "influencer_authors",
            "engagement_score",
            "influence_score",
            "max_author_weight",
            "trend_score",
            "last_seen",
        )
    )

    return trend_metrics, bad_record_metrics


def transform_stream(raw_df: SparkDataFrame) -> SparkDataFrame:
    trend_metrics, _ = build_stream_outputs(raw_df)
    return trend_metrics


def serialize_row(row: Any) -> dict[str, Any]:
    return row.asDict(recursive=True)


def write_batch_to_mongo(
    batch_df: SparkDataFrame, _: int, collection_name: str, mongo_uri: str, mongo_db: str
) -> None:
    from pymongo import MongoClient

    documents = [serialize_row(row) for row in batch_df.toLocalIterator()]
    if not documents:
        return

    client = MongoClient(mongo_uri)
    try:
        client[mongo_db][collection_name].insert_many(documents)
    finally:
        client.close()


def start_console_query(
    stream_df: SparkDataFrame,
    checkpoint_location: str,
    query_name: str,
    output_mode: str = "update",
) -> Any:
    return (
        stream_df.writeStream.queryName(query_name)
        .outputMode(output_mode)
        .format("console")
        .option("truncate", False)
        .option("numRows", 20)
        .option("checkpointLocation", checkpoint_location)
        .start()
    )


def start_mongo_query(
    stream_df: SparkDataFrame,
    checkpoint_location: str,
    query_name: str,
    collection_name: str,
    mongo_uri: str,
    mongo_db: str,
) -> Any:
    return (
        stream_df.writeStream.queryName(query_name)
        .outputMode("update")
        .foreachBatch(
            lambda batch_df, batch_id: write_batch_to_mongo(
                batch_df, batch_id, collection_name, mongo_uri, mongo_db
            )
        )
        .option("checkpointLocation", checkpoint_location)
        .start()
    )


def run_stream_job(args: argparse.Namespace) -> int:
    spark = build_spark_session("crypto-trend-speed-layer")
    raw_stream = create_kafka_stream(spark, build_kafka_options(args))
    trend_stream, bad_record_stream = build_stream_outputs(raw_stream)

    trend_checkpoint = str(Path(args.checkpoint_location) / "trend_metrics")
    bad_checkpoint = str(Path(args.checkpoint_location) / "bad_records")

    if args.output_sink == "mongo":
        start_mongo_query(
            trend_stream,
            trend_checkpoint,
            "trend_metrics_to_mongo",
            args.mongo_trend_collection,
            args.mongo_uri,
            args.mongo_db,
        )
        start_mongo_query(
            bad_record_stream,
            bad_checkpoint,
            "bad_records_to_mongo",
            args.mongo_bad_record_collection,
            args.mongo_uri,
            args.mongo_db,
        )
    else:
        start_console_query(
            trend_stream,
            trend_checkpoint,
            "trend_metrics_to_console",
        )
        start_console_query(
            bad_record_stream,
            bad_checkpoint,
            "bad_records_to_console",
        )

    spark.streams.awaitAnyTermination()
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
        default=DEFAULT_TOPICS,
        help="Kafka topic list for Spark streaming mode",
    )
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=["latest", "earliest"],
        help="Kafka starting offsets",
    )
    parser.add_argument(
        "--kafka-security-protocol",
        default=os.getenv("KAFKA_SECURITY_PROTOCOL", ""),
        help="Kafka security protocol, for example SSL for Aiven Kafka",
    )
    parser.add_argument(
        "--kafka-ssl-ca-location",
        default=os.getenv("KAFKA_SSL_CA_LOCATION", ""),
        help="Path to Kafka CA PEM file",
    )
    parser.add_argument(
        "--kafka-ssl-cert-location",
        default=os.getenv("KAFKA_SSL_CERT_LOCATION", ""),
        help="Path to Kafka client certificate PEM file",
    )
    parser.add_argument(
        "--kafka-ssl-key-location",
        default=os.getenv("KAFKA_SSL_KEY_LOCATION", ""),
        help="Path to Kafka client private key PEM file",
    )
    parser.add_argument(
        "--checkpoint-location",
        default=DEFAULT_CHECKPOINT_LOCATION,
        help="Checkpoint directory for Spark streaming state and Kafka offsets",
    )
    parser.add_argument(
        "--output-sink",
        choices=["console", "mongo"],
        default="console",
        help="Where to write streaming metrics",
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB URI for --output-sink mongo",
    )
    parser.add_argument(
        "--mongo-db",
        default=os.getenv("MONGO_DB", "crypto_trends"),
        help="MongoDB database for --output-sink mongo",
    )
    parser.add_argument(
        "--mongo-trend-collection",
        default="speed_trend_metrics",
        help="MongoDB collection for trend metrics",
    )
    parser.add_argument(
        "--mongo-bad-record-collection",
        default="speed_bad_records",
        help="MongoDB collection for bad record metrics",
    )
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()

    if args.demo:
        return run_demo(args.sample_path)

    return run_stream_job(args)


if __name__ == "__main__":
    raise SystemExit(main())
