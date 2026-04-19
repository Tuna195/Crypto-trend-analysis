from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, avg, count
from pyspark.sql.types import FloatType
from datetime import datetime

# Import from another folder
from src.storage.mongo_client import MongoStorageClient
from src.processing.batch_layer.sentiment_lexicon import SentimentAnalyzer

def run_full_batch_job():
    spark = SparkSession.builder.appName("Multi_Coin_Batch_Job").getOrCreate()
    
    # Read data (tweet) from twitter
    base_path = "hdfs://namenode:9000/data/crypto/raw_tweets/*/*/*/*.jsonl"
    df = spark.read.json(base_path)
    
    # 2. Batch processing 
    analyzer = SentimentAnalyzer()
    score_udf = udf(lambda x: analyzer.get_score(x), FloatType())
    
    df_analyzed = df.withColumn("sentiment_score", score_udf(col("text")))

    # Calculate the metric of coin
    coin_metrics = df_analyzed.groupBy("coin").agg(
        avg("sentiment_score").alias("avg_sentiment"),
        count("text").alias("mention_count")
    ).collect()

    # Storage processed data to mongoDB
    mongo = MongoStorageClient()
    for row in coin_metrics:
        mongo.save_sentiment_metric(
            coin=row['coin'],
            bullish_ratio=0.0,
            bearish_ratio=0.0,
            neutral_ratio=0.0,
            fear_greed_score=(row['avg_sentiment'] + 1) * 50,
            window_start=datetime.utcnow(),
            window_end=datetime.utcnow()
        )
    
    spark.stop()

if __name__ == "__main__":
    run_full_batch_job()