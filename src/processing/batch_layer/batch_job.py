from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    PYSPARK_AVAILABLE = True
except ModuleNotFoundError:
    SparkSession = None  # type: ignore[assignment]
    F = None             # type: ignore[assignment]
    T = None             # type: ignore[assignment]
    PYSPARK_AVAILABLE = False

# Local imports 
from src.storage.mongo_client import MongoConfig, MongoStorageClient
from src.processing.batch_layer.sentiment_lexicon import SentimentAnalyzer
from src.processing.batch_layer.bot_spam_filter import BotSpamFilter

# Constants
HDFS_RAW_PATH      = "hdfs://namenode:9000/data/crypto/raw_tweets/*/*/*/*.jsonl"
DEMO_SAMPLE_PATH   = Path(__file__).resolve().parent / "sample_data" / "batch_test_sample.jsonl"
DEMO_COLLECTION    = "test_batch_process"
BULLISH_THRESHOLD  =  0.05
BEARISH_THRESHOLD  = -0.05

# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("batch_job")

# SHARED HELPERS

def classify_sentiment(score: float) -> str:
    """Map VADER compound score → bullish / neutral / bearish."""
    if score >= BULLISH_THRESHOLD:
        return "bullish"
    if score <= BEARISH_THRESHOLD:
        return "bearish"
    return "neutral"


def fetch_yesterday_avg_mentions(mongo: MongoStorageClient) -> dict[str, float]:
    now       = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    start     = yesterday.replace(hour=0,  minute=0,  second=0,  microsecond=0)
    end       = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

    try:
        docs = list(mongo.db.sentiment_metrics.find(
            {"window_start": {"$gte": start, "$lte": end}},
            {"_id": 0, "coin": 1, "mention_count": 1},
        ))
    except Exception as exc:
        log.warning("Cannot query yesterday metrics from MongoDB: %s", exc)
        return {}

    buckets: dict[str, list[int]] = defaultdict(list)
    for doc in docs:
        coin  = doc.get("coin", "")
        count = int(doc.get("mention_count", 0))
        if coin:
            buckets[coin].append(count)

    return {coin: sum(v) / len(v) for coin, v in buckets.items()}


def is_trend_spike(
    mention_count: int,
    yesterday_avg: float,
    spike_min_count: int,
    spike_ratio: float,
) -> bool:
    if mention_count < spike_min_count:
        return False
    if yesterday_avg <= 0:
        return True
    return mention_count >= yesterday_avg * spike_ratio


# DEMO MODE

def run_demo(args: argparse.Namespace) -> int:
    log.info("=== DEMO MODE ===")
    sample_path: Path = args.sample_path

    # Stage 1: Load raw tweets
    raw_tweets: list[dict[str, Any]] = []
    for line in sample_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            raw_tweets.append(json.loads(line))
        except json.JSONDecodeError as exc:
            log.warning("Skipping malformed JSON line: %s", exc)

    log.info("Loaded %d raw tweets from %s", len(raw_tweets), sample_path)

    # Stage 2: Bot/spam filter
    bot_filter   = BotSpamFilter(spam_threshold=0.6, bot_threshold=0.5)
    clean_tweets: list[dict[str, Any]] = []
    spam_tweets:  list[dict[str, Any]] = []

    for tweet in raw_tweets:
        content  = tweet.get("content") or tweet.get("text", "")
        username = tweet.get("username", "")
        eng      = int(tweet.get("engagement_score", 0))
        weight   = float(tweet.get("author_weight", 1.0))

        result     = bot_filter.detect_spam(content, username, eng, weight)
        tweet_copy = dict(tweet)
        tweet_copy.update({
            "is_spam":        result.is_spam,
            "is_bot":         result.is_bot,
            "spam_score":     round(result.total_score, 4),
            "filter_reasons": result.reasons,
        })
        (spam_tweets if (result.is_spam or result.is_bot) else clean_tweets).append(tweet_copy)

    log.info("Filter result → clean: %d | spam/bot: %d", len(clean_tweets), len(spam_tweets))

    # Stage 3: Sentiment analysis
    analyzer = SentimentAnalyzer()
    for tweet in clean_tweets:
        score = analyzer.get_score(tweet.get("content") or tweet.get("text", ""))
        tweet["sentiment_score"] = round(score, 4)
        tweet["sentiment_label"] = classify_sentiment(score)

    # Stage 4: Aggregate per coin
    coin_clean: dict[str, list[dict]] = defaultdict(list)
    coin_spam:  dict[str, list[dict]] = defaultdict(list)

    for t in clean_tweets:
        coin_clean[t.get("coin", "UNKNOWN").upper().replace("$", "")].append(t)
    for t in spam_tweets:
        coin_spam[t.get("coin",  "UNKNOWN").upper().replace("$", "")].append(t)

    all_coins = set(coin_clean) | set(coin_spam)

    # Stage 5: Spike detection — query MongoDB for yesterday's data
    mongo_cfg = MongoConfig(uri=args.mongo_uri, database=args.mongo_db)
    mongo     = MongoStorageClient(mongo_cfg)
    yesterday_avgs = fetch_yesterday_avg_mentions(mongo)

    # Stage 6: Build result docs + save to MongoDB
    now_utc      = datetime.now(timezone.utc)
    result_docs: list[dict[str, Any]] = []

    for coin in sorted(all_coins):
        cleans = coin_clean[coin]
        spams  = coin_spam[coin]

        mention_count = len(cleans)
        spam_count    = len(spams)
        total_tweets  = mention_count + spam_count

        scores        = [t["sentiment_score"] for t in cleans]
        avg_sentiment = round(sum(scores) / len(scores), 4) if scores else 0.0
        fear_greed    = round((avg_sentiment + 1) * 50, 2)

        labels         = [t["sentiment_label"] for t in cleans]
        n              = max(mention_count, 1)
        bullish_ratio  = round(labels.count("bullish") / n, 4)
        bearish_ratio  = round(labels.count("bearish") / n, 4)
        neutral_ratio  = round(labels.count("neutral") / n, 4)

        total_engagement = sum(int(t.get("engagement_score", 0)) for t in cleans)
        total_influence  = round(
            sum(float(t.get("author_weight", 1.0)) * int(t.get("engagement_score", 0))
                for t in cleans), 2
        )

        yesterday_avg = yesterday_avgs.get(coin, 0.0)
        spike = is_trend_spike(mention_count, yesterday_avg, args.spike_min_count, args.spike_ratio)

        doc = {
            "coin":                   coin,
            "processed_at":           now_utc,
            "window_date":            now_utc.date().isoformat(),
            "mention_count":          mention_count,
            "spam_count":             spam_count,
            "total_tweets":           total_tweets,
            "spam_ratio":             round(spam_count / max(total_tweets, 1), 4),
            "avg_sentiment":          avg_sentiment,
            "fear_greed_score":       fear_greed,
            "bullish_ratio":          bullish_ratio,
            "bearish_ratio":          bearish_ratio,
            "neutral_ratio":          neutral_ratio,
            "total_engagement":       total_engagement,
            "total_influence":        total_influence,
            "is_trend_spike":         spike,
            "yesterday_avg_mentions": round(yesterday_avg, 2),
            "spike_min_count":        args.spike_min_count,
            "spike_ratio":            args.spike_ratio,
            "spam_usernames":         [t.get("username", "") for t in spams],
        }
        result_docs.append(doc)

        spike_tag = " ⚡ SPIKE" if spike else ""
        log.info(
            "  [%s] clean=%d spam=%d | avg_sent=%.3f | B=%.0f%% Be=%.0f%% N=%.0f%% | FGI=%.1f%s",
            coin, mention_count, spam_count, avg_sentiment,
            bullish_ratio * 100, bearish_ratio * 100, neutral_ratio * 100,
            fear_greed, spike_tag,
        )

    # Storage to MongoDB[test_batch_process]
    if result_docs:
        try:
            inserted = mongo.db[DEMO_COLLECTION].insert_many(result_docs)
            log.info(
                "Saved %d coin documents → MongoDB[%s.%s]",
                len(inserted.inserted_ids), args.mongo_db, DEMO_COLLECTION,
            )
        except Exception as exc:
            log.error("Failed to write to MongoDB: %s", exc)
            mongo.close()
            return 1
    else:
        log.warning("No coin data to save.")

    mongo.close()

    # Summary
    spikes = [d["coin"] for d in result_docs if d["is_trend_spike"]]
    print("\n==DEMO BATCH JOB SUMMARY ==")
    print(f"  Raw tweets loaded  : {len(raw_tweets)}")
    print(f"  Clean tweets       : {len(clean_tweets)}")
    print(f"  Spam/bot tweets    : {len(spam_tweets)}")
    print(f"  Coins processed    : {len(result_docs)}")
    print(f"  Trend spikes       : {spikes if spikes else 'None'}")
    print(f"  Spike thresholds   : min_count>={args.spike_min_count}, ratio>={args.spike_ratio}x")
    print(f"  MongoDB target     : {args.mongo_db}.{DEMO_COLLECTION}")
    return 0


# SPARK MODE — production pipeline reading from HDFS

def _raw_schema():
    return T.StructType([
        T.StructField("tweet_id",         T.StringType(),              True),
        T.StructField("user_id",          T.StringType(),              True),
        T.StructField("username",         T.StringType(),              True),
        T.StructField("content",          T.StringType(),              True),
        T.StructField("created_at",       T.StringType(),              True),
        T.StructField("coin",             T.StringType(),              True),
        T.StructField("engagement_score", T.IntegerType(),             True),
        T.StructField("author_weight",    T.DoubleType(),              True),
        T.StructField("hashtags",         T.ArrayType(T.StringType()), True),
        T.StructField("cashtags",         T.ArrayType(T.StringType()), True),
    ])


def _filter_udf():
    filter_result_type = T.StructType([
        T.StructField("is_spam",    T.BooleanType(),            True),
        T.StructField("is_bot",     T.BooleanType(),            True),
        T.StructField("spam_score", T.DoubleType(),             True),
        T.StructField("reasons",    T.ArrayType(T.StringType()), True),
    ])

    def _fn(content, username, eng, weight):
        f = BotSpamFilter(spam_threshold=0.6, bot_threshold=0.5)
        r = f.detect_spam(
            content or "",
            username=username,
            engagement_score=int(eng or 0),
            author_weight=float(weight or 1.0),
        )
        return (r.is_spam, r.is_bot, float(r.total_score), r.reasons)

    return F.udf(_fn, filter_result_type)


def _sentiment_udf():
    return F.udf(
        lambda text: float(SentimentAnalyzer().get_score(text or "")),
        T.DoubleType(),
    )


def _label_udf():
    return F.udf(classify_sentiment, T.StringType())


def run_spark_job(args: argparse.Namespace) -> int:
    if not PYSPARK_AVAILABLE:
        log.error("PySpark not installed. Use --demo for local testing.")
        return 1

    log.info("=== SPARK MODE ===")
    spark = (
        SparkSession.builder
        .appName("CryptoBatchJob")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Stage 1: Load raw tweets from HDFS
    log.info("Reading tweets from HDFS: %s", args.hdfs_path)
    df = spark.read.schema(_raw_schema()).json(args.hdfs_path)

    # Stage 2: Bot/spam filter
    flt = _filter_udf()
    df = (
        df.withColumn("_f", flt(
            F.col("content"),
            F.col("username"),
            F.coalesce(F.col("engagement_score"), F.lit(0)),
            F.coalesce(F.col("author_weight"),    F.lit(1.0)),
        ))
        .withColumn("is_spam",        F.col("_f.is_spam"))
        .withColumn("is_bot",         F.col("_f.is_bot"))
        .withColumn("spam_score",     F.col("_f.spam_score"))
        .withColumn("filter_reasons", F.col("_f.reasons"))
        .drop("_f")
    )
    df_clean = df.filter(~F.col("is_spam") & ~F.col("is_bot"))
    df_spam  = df.filter( F.col("is_spam") |  F.col("is_bot"))

    # Stage 3: Sentiment analysis
    sent_udf  = _sentiment_udf()
    label_udf = _label_udf()

    df_analyzed = (
        df_clean
        .withColumn("sentiment_score", sent_udf(F.col("content")))
        .withColumn("sentiment_label", label_udf(F.col("sentiment_score")))
        .withColumn(
            "influence_score",
            F.coalesce(F.col("author_weight"), F.lit(1.0))
            * F.coalesce(F.col("engagement_score").cast("double"), F.lit(0.0)),
        )
    )

    # Stage 4: Aggregate metrics per coin
    coin_metrics_df = (
        df_analyzed.groupBy("coin").agg(
            F.count("*")                                           .alias("mention_count"),
            F.round(F.avg("sentiment_score"), 4)                   .alias("avg_sentiment"),
            F.sum(F.when(F.col("sentiment_score") >= BULLISH_THRESHOLD,  1).otherwise(0))
                                                                   .alias("bullish_count"),
            F.sum(F.when(F.col("sentiment_score") <= BEARISH_THRESHOLD,  1).otherwise(0))
                                                                   .alias("bearish_count"),
            F.sum(F.when(
                (F.col("sentiment_score") > BEARISH_THRESHOLD) &
                (F.col("sentiment_score") < BULLISH_THRESHOLD), 1).otherwise(0))
                                                                   .alias("neutral_count"),
            F.sum(F.coalesce(F.col("engagement_score"), F.lit(0))).alias("total_engagement"),
            F.round(F.sum("influence_score"), 2)                   .alias("total_influence"),
        )
        .withColumn("fear_greed_score", F.round((F.col("avg_sentiment") + 1) * 50, 2))
        .withColumn("bullish_ratio",    F.round(F.col("bullish_count") / F.col("mention_count"), 4))
        .withColumn("bearish_ratio",    F.round(F.col("bearish_count") / F.col("mention_count"), 4))
        .withColumn("neutral_ratio",    F.round(F.col("neutral_count") / F.col("mention_count"), 4))
    )

    spam_stats_df = df_spam.groupBy("coin").agg(
        F.count("*").alias("spam_count"),
    )

    coin_rows = coin_metrics_df.collect()
    spam_rows = {r["coin"]: r["spam_count"] for r in spam_stats_df.collect()}

    # Stage 5: Spike detection — fetch yesterday's data from MongoDB
    mongo_cfg      = MongoConfig(uri=args.mongo_uri, database=args.mongo_db)
    mongo          = MongoStorageClient(mongo_cfg)
    yesterday_avgs = fetch_yesterday_avg_mentions(mongo)

    now_utc    = datetime.now(timezone.utc)
    spike_list: list[dict] = []

    # Stage 6: Save to MongoDB
    for row in coin_rows:
        coin          = row["coin"]
        mention_count = int(row["mention_count"])
        spam_count    = spam_rows.get(coin, 0)
        yesterday_avg = yesterday_avgs.get(coin, 0.0)
        spike         = is_trend_spike(mention_count, yesterday_avg, args.spike_min_count, args.spike_ratio)

        try:
            mongo.save_sentiment_metric(
                coin           = coin,
                bullish_ratio  = float(row["bullish_ratio"]),
                bearish_ratio  = float(row["bearish_ratio"]),
                neutral_ratio  = float(row["neutral_ratio"]),
                fear_greed_score = float(row["fear_greed_score"]),
                window_start   = now_utc,
                window_end     = now_utc,
            )
        except Exception as exc:
            log.error("Failed to save sentiment metric for %s: %s", coin, exc)

        if spam_count > 0:
            total_tweets = mention_count + spam_count
            try:
                mongo.save_alert(
                    alert_type = "spam_detected",
                    severity   = "info",
                    message    = f"Detected {spam_count} spam/bot tweets for {coin}",
                    payload    = {
                        "coin":        coin,
                        "spam_count":  spam_count,
                        "total_tweets": total_tweets,
                        "spam_ratio":  round(spam_count / total_tweets, 4),
                    },
                )
            except Exception as exc:
                log.error("Failed to save spam alert for %s: %s", coin, exc)

        if spike:
            spike_list.append(coin)
            try:
                mongo.save_trend_spike(
                    keyword        = coin,
                    mention_count  = mention_count,
                    baseline_count = yesterday_avg,
                    z_score        = (mention_count - yesterday_avg) / max(yesterday_avg, 1),
                    related_coins  = [coin],
                    detected_at    = now_utc,
                )
            except Exception as exc:
                log.error("Failed to save trend spike for %s: %s", coin, exc)

        log.info(
            "  [%s] mentions=%d spam=%d | FGI=%.1f | B=%.0f%% Be=%.0f%% N=%.0f%%%s",
            coin, mention_count, spam_count,
            float(row["fear_greed_score"]),
            float(row["bullish_ratio"]) * 100,
            float(row["bearish_ratio"]) * 100,
            float(row["neutral_ratio"]) * 100,
            " ⚡SPIKE" if spike else "",
        )

    mongo.close()
    spark.stop()

    print("\n===SPARK BATCH JOB SUMMARY ==")
    print(f"  Coins processed : {len(coin_rows)}")
    print(f"  Trend spikes    : {spike_list if spike_list else 'None'}")
    print(f"  Spike thresholds: min_count>={args.spike_min_count}, ratio>={args.spike_ratio}x")
    return 0


# CLI

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Crypto batch processing pipeline")

    p.add_argument("--demo",      action="store_true",
                   help="Run pure-Python demo pipeline (no Spark/HDFS required)")
    p.add_argument("--sample-path", type=Path, default=DEMO_SAMPLE_PATH,
                   help="Path to sample .jsonl file used in --demo mode")

    # HDFS
    p.add_argument("--hdfs-path", default=HDFS_RAW_PATH,
                   help="HDFS glob path to raw tweet JSONL files")

    # MongoDB
    p.add_argument("--mongo-uri", default="mongodb://localhost:27017",
                   help="MongoDB connection URI")
    p.add_argument("--mongo-db",  default="crypto_trends",
                   help="MongoDB database name")

    # Spike detection
    p.add_argument("--spike-min-count", type=int, default=50,
                   help="Absolute minimum mention count to trigger a trend spike (default: 50)")
    p.add_argument("--spike-ratio", type=float, default=2.0,
                   help="Multiplier vs yesterday's avg mentions to trigger a spike (default: 2.0)")

    return p


def main() -> int:
    args = build_arg_parser().parse_args()
    if args.demo:
        return run_demo(args)
    return run_spark_job(args)


if __name__ == "__main__":
    raise SystemExit(main())