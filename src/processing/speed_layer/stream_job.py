"""
CLI entrypoint and compatibility exports for the speed layer.

The implementation lives in focused sibling modules:

- config.py: defaults loaded from .env
- demo_pipeline.py: pure-Python demo parser/aggregator
- spark_pipeline.py: Kafka reader and Spark transformations
- spike_detection.py: baseline and spike enrichment logic
- stream_runtime.py: output sinks and streaming job orchestration
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

try:
    from .config import (
        DEFAULT_BOOTSTRAP_SERVERS,
        DEFAULT_CHECKPOINT_LOCATION,
        DEFAULT_KAFKA_PACKAGE,
        DEFAULT_KAFKA_SECURITY_PROTOCOL,
        DEFAULT_KAFKA_SSL_CA_LOCATION,
        DEFAULT_KAFKA_SSL_CERT_LOCATION,
        DEFAULT_KAFKA_SSL_KEY_LOCATION,
        DEFAULT_SAMPLE_PATH,
        DEFAULT_SPARK_MASTER,
        DEFAULT_TOPICS,
        WHALE_AUTHORS,
    )
    from .demo_pipeline import (
        CASHTAG_PATTERN,
        CleanTweet,
        aggregate_trends,
        clean_tweet,
        classify_author,
        deduplicate_tweets,
        ensure_list,
        extract_cashtags,
        load_demo_messages,
        normalize_target_coin,
        normalize_text,
        parse_float,
        parse_int,
        parse_iso_datetime,
        parse_json_line,
        run_demo,
    )
    from .spark_pipeline import (
        build_kafka_options,
        build_spark_session,
        build_stream_outputs,
        create_kafka_stream,
        get_tweet_schema,
        read_text_file,
        transform_stream,
    )
    from .spike_detection import (
        average,
        calculate_spike_fields,
        enrich_trend_document_with_spike,
        fetch_baseline_mention_count,
        fetch_baseline_stats,
        parse_datetime_value,
        population_stddev,
    )
    from .stream_runtime import (
        run_stream_job,
        serialize_row,
        start_console_query,
        start_mongo_query,
        write_batch_to_mongo,
    )
except ImportError:  # pragma: no cover - direct script execution
    from config import (
        DEFAULT_BOOTSTRAP_SERVERS,
        DEFAULT_CHECKPOINT_LOCATION,
        DEFAULT_KAFKA_PACKAGE,
        DEFAULT_KAFKA_SECURITY_PROTOCOL,
        DEFAULT_KAFKA_SSL_CA_LOCATION,
        DEFAULT_KAFKA_SSL_CERT_LOCATION,
        DEFAULT_KAFKA_SSL_KEY_LOCATION,
        DEFAULT_SAMPLE_PATH,
        DEFAULT_SPARK_MASTER,
        DEFAULT_TOPICS,
        WHALE_AUTHORS,
    )
    from demo_pipeline import (
        CASHTAG_PATTERN,
        CleanTweet,
        aggregate_trends,
        clean_tweet,
        classify_author,
        deduplicate_tweets,
        ensure_list,
        extract_cashtags,
        load_demo_messages,
        normalize_target_coin,
        normalize_text,
        parse_float,
        parse_int,
        parse_iso_datetime,
        parse_json_line,
        run_demo,
    )
    from spark_pipeline import (
        build_kafka_options,
        build_spark_session,
        build_stream_outputs,
        create_kafka_stream,
        get_tweet_schema,
        read_text_file,
        transform_stream,
    )
    from spike_detection import (
        average,
        calculate_spike_fields,
        enrich_trend_document_with_spike,
        fetch_baseline_mention_count,
        fetch_baseline_stats,
        parse_datetime_value,
        population_stddev,
    )
    from stream_runtime import (
        run_stream_job,
        serialize_row,
        start_console_query,
        start_mongo_query,
        write_batch_to_mongo,
    )


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
        "--spark-master",
        default=DEFAULT_SPARK_MASTER,
        help="Spark master URL. Defaults to SPARK_MASTER or local[*].",
    )
    parser.add_argument(
        "--spark-kafka-package",
        default=DEFAULT_KAFKA_PACKAGE,
        help=(
            "Maven coordinate for Spark's Kafka connector. Set empty when the "
            "connector is already provided by spark-submit."
        ),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
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
        "--fail-on-data-loss",
        action="store_true",
        help=(
            "Fail when Kafka offsets/partitions changed before Spark consumed "
            "them. Disabled by default for Aiven topic retention/repartitioning."
        ),
    )
    parser.add_argument(
        "--kafka-security-protocol",
        default=DEFAULT_KAFKA_SECURITY_PROTOCOL,
        help="Kafka security protocol, for example SSL for Aiven Kafka",
    )
    parser.add_argument(
        "--kafka-ssl-ca-location",
        default=DEFAULT_KAFKA_SSL_CA_LOCATION,
        help="Path to Kafka CA PEM file",
    )
    parser.add_argument(
        "--kafka-ssl-cert-location",
        default=DEFAULT_KAFKA_SSL_CERT_LOCATION,
        help="Path to Kafka client certificate PEM file",
    )
    parser.add_argument(
        "--kafka-ssl-key-location",
        default=DEFAULT_KAFKA_SSL_KEY_LOCATION,
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


__all__ = [
    "CASHTAG_PATTERN",
    "CleanTweet",
    "WHALE_AUTHORS",
    "aggregate_trends",
    "average",
    "build_arg_parser",
    "build_kafka_options",
    "build_spark_session",
    "build_stream_outputs",
    "calculate_spike_fields",
    "clean_tweet",
    "classify_author",
    "create_kafka_stream",
    "deduplicate_tweets",
    "ensure_list",
    "enrich_trend_document_with_spike",
    "extract_cashtags",
    "fetch_baseline_mention_count",
    "fetch_baseline_stats",
    "get_tweet_schema",
    "load_demo_messages",
    "main",
    "normalize_target_coin",
    "normalize_text",
    "parse_datetime_value",
    "parse_float",
    "parse_int",
    "parse_iso_datetime",
    "parse_json_line",
    "population_stddev",
    "read_text_file",
    "run_demo",
    "run_stream_job",
    "serialize_row",
    "start_console_query",
    "start_mongo_query",
    "transform_stream",
    "write_batch_to_mongo",
]


if __name__ == "__main__":
    raise SystemExit(main())
