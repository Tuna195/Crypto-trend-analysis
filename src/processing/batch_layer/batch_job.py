from __future__ import annotations

import os
import sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYTHONUNBUFFERED"] = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv(), override=True)

import argparse
import json
import re
import pyarrow
import logging
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
HDFS_BASE_PATH     = "hdfs://namenode:9000/data/crypto/raw_tweets"
HDFS_RAW_PATH      = f"{HDFS_BASE_PATH}/"
DEMO_SAMPLE_PATH   = Path(__file__).resolve().parent / "sample_data" / "batch_test_sample.jsonl"
DEMO_COLLECTION    = "test_batch_process"
BULLISH_THRESHOLD      =  0.05
BEARISH_THRESHOLD      = -0.05
WHALE_WEIGHT_THRESHOLD = 2.0   # author_weight >= this → whale, else retail

# Tracked coins — used to extract individual coins from MULTI_CRYPTO tweets
TRACKED_COINS = {"BTC", "ETH", "SOL", "XRP", "ADA", "BNB", "DOGE", "AVAX"}

# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("batch_job")

# SHARED HELPERS

import re
_CASHTAG_RE = re.compile(r"\$([A-Za-z]{2,10})")

def extract_coins_from_text(text: str) -> list[str]:
    """Extract known coin tickers from tweet text via $TICKER patterns.

    Returns a deduplicated list of uppercase coin symbols found in the text.
    If no known coin is found, returns ["UNKNOWN"].
    """
    found = {m.group(1).upper() for m in _CASHTAG_RE.finditer(text or "")}
    matched = [c for c in found if c in TRACKED_COINS]
    return matched if matched else ["UNKNOWN"]


def classify_sentiment(score: float) -> str:
    """Map VADER compound score → bullish / neutral / bearish."""
    if score >= BULLISH_THRESHOLD:
        return "bullish"
    if score <= BEARISH_THRESHOLD:
        return "bearish"
    return "neutral"


def classify_author_type(author_weight: float) -> str:
    """Classify author as whale or retail based on weight threshold."""
    return "whale" if author_weight >= WHALE_WEIGHT_THRESHOLD else "retail"


def compute_segment_metrics(tweets: list[dict]) -> dict[str, Any]:
    """Compute sentiment metrics for a segment (whale or retail) of tweets.

    Returns a dict with keys: mention_count, avg_sentiment, fear_greed_score,
    bullish_ratio, bearish_ratio, neutral_ratio.
    Returns all zeros when the input list is empty.
    """
    n = len(tweets)
    if n == 0:
        return {
            "mention_count": 0,
            "avg_sentiment": 0.0,
            "fear_greed_score": 50.0,
            "bullish_ratio": 0.0,
            "bearish_ratio": 0.0,
            "neutral_ratio": 0.0,
        }
    scores = [t["sentiment_score"] for t in tweets]
    avg    = round(sum(scores) / n, 4)
    labels = [t["sentiment_label"] for t in tweets]
    return {
        "mention_count":    n,
        "avg_sentiment":    avg,
        "fear_greed_score": round((avg + 1) * 50, 2),
        "bullish_ratio":    round(labels.count("bullish") / n, 4),
        "bearish_ratio":    round(labels.count("bearish") / n, 4),
        "neutral_ratio":    round(labels.count("neutral") / n, 4),
    }


def fetch_yesterday_avg_mentions(mongo: MongoStorageClient) -> dict[str, float]:
    now       = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    start     = yesterday.replace(hour=0,  minute=0,  second=0,  microsecond=0)
    end       = yesterday.replace(hour=23, minute=59, second=59, microsecond=0)

    try:
        docs = list(mongo.db.batch_sentiment_metrics.find(
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


def fetch_last_processed_datetime(mongo: MongoStorageClient) -> Optional[datetime]:
    """Query MongoDB tìm window_end lớn nhất đã xử lý. Returns datetime or None."""
    try:
        cursor = (
            mongo.db.batch_sentiment_metrics
            .find({"window_end": {"$exists": True, "$ne": None}}, {"_id": 0, "window_end": 1})
            .sort("window_end", -1)
            .limit(1)
        )
        result = next(cursor, None)
        if result and result.get("window_end"):
            dt = result["window_end"]
            log.info("Last processed window_end: %s", dt)
            return dt
    except Exception as exc:
        log.warning("Cannot query last processed datetime from MongoDB: %s", exc)
    return None


def build_incremental_hdfs_paths(
    last_processed_dt: datetime,
    base_path: str = HDFS_BASE_PATH,
) -> list[str]:
    """Generate HDFS paths for unprocessed date/hour partitions.

    - Ngày ranh giới (cùng ngày với last_processed_dt): chỉ sinh paths cho
      các giờ >= giờ của window_end (vì giờ đó chưa được xử lý tiếp).
    - Các ngày sau đó: sinh path với hour=* (đọc tất cả giờ).

    Ví dụ: window_end = 2026-05-28 03:00:00
      → Ngày 28: hour=03, 04, 05, ..., 23
      → Ngày 29+: hour=*
    """
    now = datetime.now(timezone.utc)
    last_date = last_processed_dt.date()
    last_hour = last_processed_dt.hour  # window_end hour = first unprocessed hour
    end_date = now.date()

    if last_date > end_date:
        return []

    paths = []
    current_date = last_date

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")

        if current_date == last_date:
            # Ngày ranh giới: chỉ lấy từ giờ chưa xử lý trở đi
            for h in range(last_hour, 24):
                paths.append(build_hdfs_path(date_str, f"{h:02d}", base_path))
        else:
            # Ngày mới hoàn toàn: lấy tất cả giờ
            paths.append(build_hdfs_path(date_str, None, base_path))

        current_date += timedelta(days=1)

    return paths


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


def build_hdfs_path(
    target_date: Optional[str] = None,
    target_hour: Optional[str] = None,
    base_path: str = HDFS_BASE_PATH,
) -> str:
    """Build HDFS read path with optional partition pruning.

    When --target-date and/or --target-hour are provided, constructs a
    partition-specific path to avoid scanning the entire dataset.

    Examples
    --------
    >>> build_hdfs_path("2026-05-21", "15")
    'hdfs://namenode:9000/data/crypto/raw_tweets/coin=*/date=2026-05-21/hour=15/*.jsonl'
    >>> build_hdfs_path("2026-05-21")
    'hdfs://namenode:9000/data/crypto/raw_tweets/coin=*/date=2026-05-21/hour=*/*.jsonl'
    >>> build_hdfs_path()
    'hdfs://namenode:9000/data/crypto/raw_tweets/coin=*/date=*/hour=*/*.jsonl'
    """
    coin_part = "coin=*"
    date_part = f"date={target_date}" if target_date else "date=*"
    hour_part = f"hour={target_hour}" if target_hour else "hour=*"
    return f"{base_path}/{coin_part}/{date_part}/{hour_part}/*.jsonl"


def log_batch_run(
    mongo: MongoStorageClient,
    *,
    status: str,
    mode: str,
    target_date: Optional[str] = None,
    target_hour: Optional[str] = None,
    total_tweets: int = 0,
    clean_tweets: int = 0,
    spam_tweets: int = 0,
    coins_processed: int = 0,
    spikes: Optional[list[str]] = None,
    error_message: Optional[str] = None,
    duration_seconds: Optional[float] = None,
) -> None:
    """Record batch job execution metadata to MongoDB for auditing."""
    doc = {
        "job_type":          "batch",
        "mode":              mode,
        "status":            status,
        "target_date":       target_date,
        "target_hour":       target_hour,
        "total_tweets":      total_tweets,
        "clean_tweets":      clean_tweets,
        "spam_tweets":       spam_tweets,
        "coins_processed":   coins_processed,
        "spikes_detected":   spikes or [],
        "error_message":     error_message,
        "duration_seconds":  duration_seconds,
        "executed_at":       datetime.now(timezone.utc),
    }
    try:
        mongo.db["batch_job_runs"].insert_one(doc)
        log.info("Batch run metadata saved → MongoDB[batch_job_runs]")
    except Exception as exc:
        log.warning("Failed to save batch run metadata: %s", exc)


# DEMO MODE

def run_demo(args: argparse.Namespace) -> int:
    log.info("=== DEMO MODE ===")
    job_start = datetime.now(timezone.utc)
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
    bot_filter   = BotSpamFilter(spam_threshold=0.4, bot_threshold=0.4)
    clean_tweets: list[dict[str, Any]] = []
    spam_tweets:  list[dict[str, Any]] = []

    for tweet in raw_tweets:
        content  = tweet.get("content") or tweet.get("text", "")
        username = tweet.get("username") or tweet.get("author", "")
        eng      = int(tweet.get("engagement_score", 0))
        weight   = float(tweet.get("author_weight", 1.0))
        raw_coin = tweet.get("coin") or tweet.get("target_coin", "MIXED")

        # Nếu target_coin là MULTI_CRYPTO / MIXED → trích xuất từng coin riêng lẻ từ text
        if raw_coin.upper() in ("MULTI_CRYPTO", "MIXED"):
            coins = extract_coins_from_text(content)
        else:
            coins = [raw_coin.upper().replace("$", "")]

        result = bot_filter.detect_spam(content, username, eng, weight)

        for coin in coins:
            tweet_copy = dict(tweet)
            tweet_copy["coin"] = coin
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

        # Whale vs. Retail segmentation
        whale_tweets  = [t for t in cleans if classify_author_type(float(t.get("author_weight", 1.0))) == "whale"]
        retail_tweets = [t for t in cleans if classify_author_type(float(t.get("author_weight", 1.0))) == "retail"]
        whale_metrics  = compute_segment_metrics(whale_tweets)
        retail_metrics = compute_segment_metrics(retail_tweets)

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
            # Whale vs. Retail segmented metrics
            "whale_mention_count":    whale_metrics["mention_count"],
            "whale_avg_sentiment":    whale_metrics["avg_sentiment"],
            "whale_fear_greed":       whale_metrics["fear_greed_score"],
            "whale_bullish_ratio":    whale_metrics["bullish_ratio"],
            "whale_bearish_ratio":    whale_metrics["bearish_ratio"],
            "retail_mention_count":   retail_metrics["mention_count"],
            "retail_avg_sentiment":   retail_metrics["avg_sentiment"],
            "retail_fear_greed":      retail_metrics["fear_greed_score"],
            "retail_bullish_ratio":   retail_metrics["bullish_ratio"],
            "retail_bearish_ratio":   retail_metrics["bearish_ratio"],
        }
        result_docs.append(doc)

        spike_tag = " ⚡ SPIKE" if spike else ""
        log.info(
            "  [%s] clean=%d spam=%d | avg_sent=%.3f | B=%.0f%% Be=%.0f%% N=%.0f%% | FGI=%.1f%s",
            coin, mention_count, spam_count, avg_sentiment,
            bullish_ratio * 100, bearish_ratio * 100, neutral_ratio * 100,
            fear_greed, spike_tag,
        )
        log.info(
            "        whale=%d(FGI=%.1f) retail=%d(FGI=%.1f)",
            whale_metrics["mention_count"], whale_metrics["fear_greed_score"],
            retail_metrics["mention_count"], retail_metrics["fear_greed_score"],
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

    # Summary
    spikes = [d["coin"] for d in result_docs if d["is_trend_spike"]]
    duration = (datetime.now(timezone.utc) - job_start).total_seconds()

    # Audit log
    log_batch_run(
        mongo,
        status="success",
        mode="demo",
        total_tweets=len(raw_tweets),
        clean_tweets=len(clean_tweets),
        spam_tweets=len(spam_tweets),
        coins_processed=len(result_docs),
        spikes=spikes,
        duration_seconds=round(duration, 2),
    )
    mongo.close()

    print("\n== DEMO BATCH JOB SUMMARY ==")
    print(f"  Raw tweets loaded  : {len(raw_tweets)}")
    print(f"  Clean tweets       : {len(clean_tweets)}")
    print(f"  Spam/bot tweets    : {len(spam_tweets)}")
    print(f"  Coins processed    : {len(result_docs)}")
    print(f"  Trend spikes       : {spikes if spikes else 'None'}")
    print(f"  Spike thresholds   : min_count>={args.spike_min_count}, ratio>={args.spike_ratio}x")
    print(f"  Duration           : {duration:.1f}s")
    print(f"  MongoDB target     : {args.mongo_db}.{DEMO_COLLECTION}")
    return 0


# SPARK MODE — production pipeline reading from HDFS

def _raw_schema():
    return T.StructType([
        T.StructField("id",               T.StringType(),              True),
        T.StructField("text",             T.StringType(),              True),
        T.StructField("created_at",       T.StringType(),              True),
        T.StructField("author",           T.StringType(),              True),
        T.StructField("target_coin",      T.StringType(),              True),
    ])


_global_spam_filter = None
_global_sentiment_analyzer = None

def get_spam_filter():
    global _global_spam_filter
    if _global_spam_filter is None:
        from src.processing.batch_layer.bot_spam_filter import BotSpamFilter
        _global_spam_filter = BotSpamFilter(spam_threshold=0.4, bot_threshold=0.4)
    return _global_spam_filter

def get_sentiment_analyzer():
    global _global_sentiment_analyzer
    if _global_sentiment_analyzer is None:
        from src.processing.batch_layer.sentiment_lexicon import SentimentAnalyzer
        _global_sentiment_analyzer = SentimentAnalyzer()
    return _global_sentiment_analyzer

def _filter_udf():
    filter_result_type = T.StructType([
        T.StructField("is_spam",    T.BooleanType(),            True),
        T.StructField("is_bot",     T.BooleanType(),            True),
        T.StructField("spam_score", T.DoubleType(),             True),
        T.StructField("reasons",    T.ArrayType(T.StringType()), True),
    ])

    def _fn(content, username, eng, weight):
        f = get_spam_filter()
        r = f.detect_spam(
            content or "",
            username=username,
            engagement_score=int(eng or 0),
            author_weight=float(weight or 1.0),
        )
        return (r.is_spam, r.is_bot, float(r.total_score), r.reasons)

    return F.udf(_fn, filter_result_type)


def _sentiment_udf():
    def _sent_fn(text):
        return float(get_sentiment_analyzer().get_score(text or ""))
    return F.udf(_sent_fn, T.DoubleType())


def _label_udf():
    return F.udf(classify_sentiment, T.StringType())


def run_spark_job(args: argparse.Namespace) -> int:
    if not PYSPARK_AVAILABLE:
        log.error("PySpark not installed. Use --demo for local testing.")
        return 1

    log.info("=== SPARK MODE ===")
    job_start = datetime.now(timezone.utc)
    spark = (
        SparkSession.builder
        .appName("CryptoBatchJob")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.python.worker.memory", "4g")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Stage 1: Load raw tweets from HDFS (with incremental detection)
    mongo_cfg_check = MongoConfig(uri=args.mongo_uri, database=args.mongo_db)
    mongo_check     = MongoStorageClient(mongo_cfg_check)

    if args.target_date:
        hdfs_paths = [build_hdfs_path(args.target_date, args.target_hour)]
        log.info(
            "Manual mode → date=%s hour=%s",
            args.target_date, args.target_hour or "*",
        )
    else:
        last_dt = fetch_last_processed_datetime(mongo_check)
        if last_dt:
            hdfs_paths = build_incremental_hdfs_paths(last_dt)
            if not hdfs_paths:
                log.info("No new data to process. Last window_end: %s", last_dt)
                mongo_check.close()
                spark.stop()
                return 0
            log.info(
                "Incremental mode → %d path(s) from %s to now",
                len(hdfs_paths), last_dt,
            )
        else:
            hdfs_paths = [args.hdfs_path]
            log.info("First run (no history) → full-scan mode")

    mongo_check.close()

    valid_dfs = []
    for p in hdfs_paths:
        log.info("Reading from: %s", p)
        try:
            _df = (
                spark.read.schema(_raw_schema())
                .option("recursiveFileLookup", "true")
                .json(p)
            )
            valid_dfs.append(_df)
        except Exception as exc:
            log.warning("Skip HDFS folder that occur error (maybe no data): %s", exc)

    if not valid_dfs:
        log.warning("No new data to process. Please craw new data !")
        spark.stop()
        return 0

    import functools
    from pyspark.sql import DataFrame
    df = functools.reduce(DataFrame.unionByName, valid_dfs)

    try:
        df = (
            df
            .withColumnRenamed("id", "tweet_id")
            .withColumnRenamed("text", "content")
            .withColumnRenamed("author", "username")
            .withColumnRenamed("target_coin", "coin")
        )

        if "engagement_score" not in df.columns:
            df = df.withColumn("engagement_score", F.lit(0))
        if "author_weight" not in df.columns:
            df = df.withColumn("author_weight", F.lit(1.0))
    except Exception as exc:
        log.error("Failed to process HDFS data schema: %s", exc)
        spark.stop()
        return 1

    log.info(f"Số lượng bản ghi sau khi load: {df.count()}")

    # Stage 1b: Coin Extraction — explode MULTI_CRYPTO tweets into per-coin rows
    tracked_upper = [c.upper() for c in TRACKED_COINS]
    raw_extracted = F.expr(r"regexp_extract_all(content, '\\$([A-Za-z]{2,10})', 1)")
    upper_extracted = F.transform(raw_extracted, lambda x: F.upper(x))
    distinct_extracted = F.array_distinct(upper_extracted)
    extracted_and_filtered = F.filter(distinct_extracted, lambda x: x.isin(tracked_upper))

    # Các giá trị target_coin cần tách riêng từng coin
    multi_coin_labels = ["MULTI_CRYPTO", "MIXED", "WHALE_SIGNAL"]

    df = df.withColumn(
        "_coins",
        F.when(
            F.col("coin").isNotNull() & (~F.upper(F.col("coin")).isin(*multi_coin_labels)),
            F.array(F.regexp_replace(F.upper(F.col("coin")), r"\$", ""))
        ).otherwise(
            F.when(F.size(extracted_and_filtered) > 0, extracted_and_filtered)
             .otherwise(F.array(F.lit("UNKNOWN")))
        )
    )

    df = df.withColumn("coin", F.explode("_coins")).drop("_coins")
    df = df.filter(F.col("coin") != "UNKNOWN")

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
        .withColumn("time_window",    F.date_format(F.date_trunc("hour", F.to_timestamp("created_at", "EEE MMM dd HH:mm:ss Z yyyy")), "yyyy-MM-dd HH:mm:ss"))
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
        .withColumn(
            "author_type",
            F.when(
                F.coalesce(F.col("author_weight"), F.lit(1.0)) >= F.lit(WHALE_WEIGHT_THRESHOLD),
                F.lit("whale"),
            ).otherwise(F.lit("retail")),
        )
    )

    # Stage 4: Aggregate metrics per (coin, time_window) (overall)
    coin_metrics_df = (
        df_analyzed.groupBy("coin", "time_window").agg(
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
    log.info("Overall coin metrics aggregated.")

    # Stage 4b: Aggregate metrics per (coin, time_window, author_type) — Whale vs. Retail
    segment_metrics_df = (
        df_analyzed.groupBy("coin", "time_window", "author_type").agg(
            F.count("*")                           .alias("seg_mention_count"),
            F.round(F.avg("sentiment_score"), 4)   .alias("seg_avg_sentiment"),
            F.sum(F.when(F.col("sentiment_score") >= BULLISH_THRESHOLD, 1).otherwise(0))
                                                   .alias("seg_bullish_count"),
            F.sum(F.when(F.col("sentiment_score") <= BEARISH_THRESHOLD, 1).otherwise(0))
                                                   .alias("seg_bearish_count"),
        )
        .withColumn("seg_fear_greed", F.round((F.col("seg_avg_sentiment") + 1) * 50, 2))
        .withColumn("seg_bullish_ratio", F.round(F.col("seg_bullish_count") / F.col("seg_mention_count"), 4))
        .withColumn("seg_bearish_ratio", F.round(F.col("seg_bearish_count") / F.col("seg_mention_count"), 4))
    )
    
    segment_rows = {}
    for r in segment_metrics_df.collect():
        segment_rows.setdefault((r["coin"], r["time_window"]), {})[r["author_type"]] = r

    spam_stats_df = df_spam.groupBy("coin", "time_window").agg(
        F.count("*").alias("spam_count"),
    )

    coin_rows = coin_metrics_df.collect()
    spam_rows = {(r["coin"], r["time_window"]): r["spam_count"] for r in spam_stats_df.collect()}

    # Stage 5: Spike detection — fetch yesterday's data from MongoDB
    mongo_cfg      = MongoConfig(uri=args.mongo_uri, database=args.mongo_db)
    mongo          = MongoStorageClient(mongo_cfg)
    yesterday_avgs = fetch_yesterday_avg_mentions(mongo)

    now_utc    = datetime.now(timezone.utc)
    spike_list: list[dict] = []

    # Stage 6: Save to MongoDB
    for row in coin_rows:
        coin          = row["coin"]
        time_window_str = row["time_window"]
        if time_window_str is None:
            time_window = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        else:
            time_window = datetime.strptime(time_window_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            
        window_end    = time_window + timedelta(hours=1)
            
        mention_count = int(row["mention_count"])
        total_eng = int(row["total_engagement"] if row["total_engagement"] is not None else 0)
        spam_count    = spam_rows.get((coin, row["time_window"]), 0)
        yesterday_avg = yesterday_avgs.get(coin, 0.0)
        spike         = is_trend_spike(mention_count, yesterday_avg, args.spike_min_count, args.spike_ratio)

        # Whale vs. Retail segment data for this coin/window
        coin_segments = segment_rows.get((coin, row["time_window"]), {})
        w = coin_segments.get("whale")
        r = coin_segments.get("retail")

        try:
            mongo.save_sentiment_metric(
                coin           = coin,
                mention_count  = mention_count,
                bullish_ratio  = float(row["bullish_ratio"]),
                bearish_ratio  = float(row["bearish_ratio"]),
                neutral_ratio  = float(row["neutral_ratio"]),
                fear_greed_score = float(row["fear_greed_score"]),
                total_engagement = total_eng,
                window_start   = time_window,
                window_end     = window_end,
                whale_metrics={
                    "mention_count": int(w["seg_mention_count"]),
                    "avg_sentiment": float(w["seg_avg_sentiment"]),
                    "fear_greed":    float(w["seg_fear_greed"]),
                    "bullish_ratio": float(w["seg_bullish_ratio"]),
                    "bearish_ratio": float(w["seg_bearish_ratio"]),
                } if w else None,
                retail_metrics={
                    "mention_count": int(r["seg_mention_count"]),
                    "avg_sentiment": float(r["seg_avg_sentiment"]),
                    "fear_greed":    float(r["seg_fear_greed"]),
                    "bullish_ratio": float(r["seg_bullish_ratio"]),
                    "bearish_ratio": float(r["seg_bearish_ratio"]),
                } if r else None
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
                    window_start   = time_window,
                    window_end     = window_end,
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
            " SPIKE" if spike else "",
        )
        log.info(
            "        whale=%d(FGI=%.1f) retail=%d(FGI=%.1f)",
            int(w.get("seg_mention_count", 0) if w else 0),
            float(w.get("seg_fear_greed", 50.0) if w else 50.0),
            int(r["seg_mention_count"] if r and r["seg_mention_count"] is not None else 0),
            float(r["seg_fear_greed"] if r and r["seg_fear_greed"] is not None else 50.0),
        )

    duration = (datetime.now(timezone.utc) - job_start).total_seconds()

    # Audit log
    log_batch_run(
        mongo,
        status="success",
        mode="spark",
        target_date=args.target_date,
        target_hour=args.target_hour,
        coins_processed=len(coin_rows),
        spikes=spike_list,
        duration_seconds=round(duration, 2),
    )

    mongo.close()
    spark.stop()

    print("\n=== SPARK BATCH JOB SUMMARY ==")
    print(f"  Coins processed : {len(coin_rows)}")
    print(f"  Trend spikes    : {spike_list if spike_list else 'None'}")
    print(f"  Spike thresholds: min_count>={args.spike_min_count}, ratio>={args.spike_ratio}x")
    print(f"  Target partition: date={args.target_date or '*'} hour={args.target_hour or '*'}")
    print(f"  Duration        : {duration:.1f}s")
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
                   help="HDFS glob path to raw tweet JSONL files (fallback when no --target-date)")

    # Incremental processing (partition pruning)
    p.add_argument("--target-date", type=str, default=None,
                   help="Target date for partition pruning (YYYY-MM-DD). "
                        "When set, only reads data from this date partition on HDFS.")
    p.add_argument("--target-hour", type=str, default=None,
                   help="Target hour for partition pruning (HH, 00-23). "
                        "Requires --target-date. Only reads data from this hour partition.")

    # MongoDB
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
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