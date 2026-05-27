from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    from .config import DEFAULT_KAFKA_PACKAGE, DEFAULT_SPARK_MASTER, WHALE_AUTHORS
except ImportError:  # pragma: no cover - direct script import path
    from config import DEFAULT_KAFKA_PACKAGE, DEFAULT_SPARK_MASTER, WHALE_AUTHORS

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


def build_spark_session(
    app_name: str,
    spark_master: str = DEFAULT_SPARK_MASTER,
    kafka_package: str = DEFAULT_KAFKA_PACKAGE,
) -> SparkSessionType:
    if SparkSession is None:
        raise RuntimeError("PySpark is not installed")

    builder = SparkSession.builder.appName(app_name).config(
        "spark.sql.shuffle.partitions", "2"
    )
    if spark_master:
        builder = builder.master(spark_master)
    if kafka_package and not os.getenv("PYSPARK_SUBMIT_ARGS"):
        builder = builder.config("spark.jars.packages", kafka_package)

    return builder.getOrCreate()


def read_text_file(path: str | None) -> str | None:
    if not path:
        return None
    return Path(path).read_text(encoding="utf-8")


def build_kafka_options(args: argparse.Namespace) -> dict[str, str]:
    options = {
        "kafka.bootstrap.servers": args.bootstrap_servers,
        "subscribe": args.topic,
        "startingOffsets": args.starting_offsets,
        "failOnDataLoss": str(getattr(args, "fail_on_data_loss", False)).lower(),
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
        .select("raw_json", "json_data", "json_data.*")
    )

    empty_string_array = F.array().cast("array<string>")
    target_coin_symbol = F.regexp_replace(F.upper(F.col("target_coin")), r"^\$", "")
    extracted_cashtags = F.regexp_extract_all(
        F.col("content"),
        F.lit(r"\$([A-Za-z][A-Za-z0-9]{1,9})"),
        F.lit(1),
    )
    parsed_event_time = F.coalesce(
        F.try_to_timestamp("created_at"),
        F.to_timestamp(
            F.regexp_replace(F.col("created_at"), r"^[A-Za-z]{3}\s+", ""),
            "MMM dd HH:mm:ss xx yyyy",
        ),
        F.from_unixtime(F.col("created_at").cast("long")).cast("timestamp"),
    )
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
        .withColumn("event_time", parsed_event_time)
        .withColumn("like_count", F.coalesce(F.col("like_count").cast("int"), F.lit(0)))
        .withColumn(
            "retweet_count",
            F.coalesce(F.col("retweet_count").cast("int"), F.lit(0)),
        )
        .withColumn("reply_count", F.coalesce(F.col("reply_count").cast("int"), F.lit(0)))
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
            .when(F.size(extracted_cashtags) > 0, extracted_cashtags)
            .when(
                F.col("target_coin").isNotNull()
                & (~target_coin_symbol.isin("MULTI_CRYPTO", "WHALE_SIGNAL"))
                & (F.length(target_coin_symbol) > 0),
                F.array(target_coin_symbol),
            )
            .otherwise(empty_string_array),
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

    trend_metrics = (
        cleaned.groupBy(
            F.window("event_time", "5 minutes", "1 minute"),
            F.col("symbol"),
        )
        .agg(
            F.count("*").alias("mention_count"),
            F.approx_count_distinct("user_id").alias("unique_authors"),
            F.approx_count_distinct(
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
