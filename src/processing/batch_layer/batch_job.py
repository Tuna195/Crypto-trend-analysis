from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, count, sum as spark_sum, struct
from pyspark.sql.types import FloatType, BooleanType, ArrayType, StringType
from datetime import datetime

# Import from another folder
from src.storage.mongo_client import MongoStorageClient
from src.processing.batch_layer.sentiment_lexicon import SentimentAnalyzer
from src.processing.batch_layer.bot_spam_filter import BotSpamFilter


def apply_bot_spam_filter(content, username, engagement_score, author_weight):
    """UDF wrapper for bot/spam detection."""
    filter_instance = BotSpamFilter(spam_threshold=0.6, bot_threshold=0.5)
    result = filter_instance.detect_spam(
        content or "",
        username=username,
        engagement_score=int(engagement_score or 0),
        author_weight=float(author_weight or 1.0),
    )
    return {
        "is_spam": result.is_spam,
        "is_bot": result.is_bot,
        "spam_score": float(result.total_score),
        "reasons": result.reasons,
    }


def run_full_batch_job():
    spark = SparkSession.builder.appName("Multi_Coin_Batch_Job").getOrCreate()
    
    # Read data (tweet) from twitter
    base_path = "hdfs://namenode:9000/data/crypto/raw_tweets/*/*/*/*.jsonl"
    df = spark.read.json(base_path)
    
    # 1. Apply bot/spam filter
    filter_udf = udf(
        apply_bot_spam_filter,
        "struct<is_spam:boolean,is_bot:boolean,spam_score:double,reasons:array<string>>"
    )
    
    df_with_filter = df.withColumn(
        "filter_result",
        filter_udf(
            col("content").cast("string"),
            col("username").cast("string"),
            col("engagement_score").cast("int"),
            col("author_weight").cast("double"),
        )
    )
    
    # Extract filter fields for easier querying
    df_filtered = df_with_filter.select(
        "*",
        col("filter_result.is_spam").alias("is_spam"),
        col("filter_result.is_bot").alias("is_bot"),
        col("filter_result.spam_score").alias("spam_score"),
        col("filter_result.reasons").alias("filter_reasons"),
    )
    
    # Separate clean tweets from spam/bot
    df_clean = df_filtered.filter((col("is_spam") == False) & (col("is_bot") == False))
    df_spam = df_filtered.filter((col("is_spam") == True) | (col("is_bot") == True))
    
    # 2. Sentiment analysis on clean tweets only
    analyzer = SentimentAnalyzer()
    score_udf = udf(lambda x: analyzer.get_score(x), FloatType())
    
    df_analyzed = df_clean.withColumn(
        "sentiment_score",
        score_udf(col("content").cast("string"))
    )
    # 3. Calculate metrics per coin (from clean tweets)
    coin_metrics = df_analyzed.groupBy("coin").agg(
        avg("sentiment_score").alias("avg_sentiment"),
        count("content").alias("mention_count"),
        spark_sum("engagement_score").alias("total_engagement"),
        spark_sum("influence_score").alias("total_influence"),
    ).collect()

    # 4. Calculate spam statistics per coin
    spam_stats = df_spam.groupBy("coin").agg(
        count("*").alias("spam_count"),
        (spark_sum(col("is_spam").cast("int")) / count("*")).alias("spam_ratio")
    ).collect()

    # 5. Save metrics to MongoDB
    mongo = MongoStorageClient()
    
    # Save clean tweet metrics
    for row in coin_metrics:
        mongo.save_sentiment_metric(
            coin=row['coin'],
            bullish_ratio=0.0,
            bearish_ratio=0.0,
            neutral_ratio=0.0,
            fear_greed_score=(row['avg_sentiment'] + 1) * 50,
            window_start=datetime.utcnow(),
            window_end=datetime.utcnow(),
        )
    
    # Save spam statistics to alerts
    clean_counts = {row['coin']: row['mention_count'] for row in coin_metrics}
    for row in spam_stats:
        coin = row['coin']
        spam_count = row['spam_count']
        total_tweets = spam_count + clean_counts.get(coin, 0) 
        
        mongo.save_alert(
            alert_type="spam_detected",
            severity="info",
            message=f"Detected {spam_count} spam/bot tweets for {coin}",
            payload={
                "coin": coin,
                "spam_count": int(spam_count),
                "total_tweets": int(total_tweets),
            },
        )
    
    # 6. Log summary statistics
    clean_count = df_clean.count()
    spam_count = df_spam.count()
    total_count = df_filtered.count()
    
    print(f"\n========== BATCH JOB SUMMARY ==========")
    print(f"Total tweets processed: {total_count}")
    print(f"Clean tweets: {clean_count} ({100*clean_count/max(total_count,1):.1f}%)")
    print(f"Spam/Bot tweets: {spam_count} ({100*spam_count/max(total_count,1):.1f}%)")
    print(f"Coins analyzed: {len(coin_metrics)}")
    print(f"======================================\n")
    
    spark.stop()

if __name__ == "__main__":
    run_full_batch_job()