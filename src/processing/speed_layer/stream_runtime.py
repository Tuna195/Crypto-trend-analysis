from __future__ import annotations

import argparse
from pathlib import Path
from typing import TYPE_CHECKING, Any

try:
    from .spark_pipeline import (
        build_kafka_options,
        build_spark_session,
        build_stream_outputs,
        create_kafka_stream,
    )
    from .spike_detection import enrich_trend_document_with_spike
except ImportError:  # pragma: no cover - direct script import path
    from spark_pipeline import (
        build_kafka_options,
        build_spark_session,
        build_stream_outputs,
        create_kafka_stream,
    )
    from spike_detection import enrich_trend_document_with_spike

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame
else:
    SparkDataFrame = Any


def serialize_row(row: Any) -> dict[str, Any]:
    return row.asDict(recursive=True)


def log_mongo_batch(
    collection_name: str,
    batch_id: int,
    rows: int,
    trend_upserts: int = 0,
    plain_inserts: int = 0,
) -> None:
    print(
        "[mongo] "
        f"collection={collection_name} "
        f"batch={batch_id} "
        f"rows={rows} "
        f"upserts={trend_upserts} "
        f"inserts={plain_inserts}",
        flush=True,
    )


def write_batch_to_mongo(
    batch_df: SparkDataFrame, batch_id: int, collection_name: str, mongo_uri: str, mongo_db: str
) -> None:
    from pymongo import MongoClient, UpdateOne

    documents = [serialize_row(row) for row in batch_df.toLocalIterator()]
    if not documents:
        log_mongo_batch(collection_name, batch_id, 0)
        return

    client = MongoClient(mongo_uri)
    try:
        collection = client[mongo_db][collection_name]
        trend_updates = []
        plain_inserts = []
        for document in documents:
            enriched_document = enrich_trend_document_with_spike(document, collection)
            if all(key in enriched_document for key in ("symbol", "window_start", "window_end")):
                trend_updates.append(
                    UpdateOne(
                        {
                            "symbol": enriched_document["symbol"],
                            "window_start": enriched_document["window_start"],
                            "window_end": enriched_document["window_end"],
                        },
                        {"$set": enriched_document},
                        upsert=True,
                    )
                )
            else:
                plain_inserts.append(enriched_document)

        if trend_updates:
            collection.bulk_write(trend_updates, ordered=False)
        if plain_inserts:
            collection.insert_many(plain_inserts)
        log_mongo_batch(
            collection_name,
            batch_id,
            len(documents),
            len(trend_updates),
            len(plain_inserts),
        )
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
    spark = build_spark_session(
        "crypto-trend-speed-layer",
        args.spark_master,
        args.spark_kafka_package,
    )
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
