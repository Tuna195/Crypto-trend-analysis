import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
try:
    from src.processing.batch_layer import batch_job as bj
    from src.processing.batch_layer.bot_spam_filter import BotSpamFilter
    from src.processing.batch_layer.sentiment_lexicon import SentimentAnalyzer
except ImportError:
    # Backup trường hợp bạn chạy test bằng cách khác
    from processing.batch_layer import batch_job as bj
    from processing.batch_layer.bot_spam_filter import BotSpamFilter
    from processing.batch_layer.sentiment_lexicon import SentimentAnalyzer


class TestBatchJob(unittest.TestCase):
    """Test suite for batch processing components."""

    def setUp(self):
        """Set up test fixtures."""
        self.filter_engine = BotSpamFilter(spam_threshold=0.3, bot_threshold=0.3)
        self.sentiment_analyzer = SentimentAnalyzer()

    def test_bot_spam_filter_clean_tweets(self):
        """Test that clean tweets pass the filter."""
        clean_tweets = [
            {
                "content": "$BTC showing strong momentum at 46k level. Expecting breakout above 47k.",
                "username": "crypto_trader",
                "engagement_score": 45,
                "author_weight": 1.0,
            },
            {
                "content": "Ethereum network upgrades continue to improve scalability. #eth #defi",
                "username": "blockchain_dev",
                "engagement_score": 23,
                "author_weight": 1.0,
            },
        ]

        for tweet in clean_tweets:
            with self.subTest(tweet=tweet["content"][:30]):
                result = self.filter_engine.detect_spam(**tweet)
                self.assertFalse(result.is_spam, f"Clean tweet flagged as spam: {tweet['content'][:50]}")
                self.assertFalse(result.is_bot, f"Clean tweet flagged as bot: {tweet['content'][:50]}")
                self.assertLess(result.total_score, 0.3, f"Clean tweet has high score: {result.total_score}")

    def test_bot_spam_filter_spam_tweets(self):
        """Test that spam tweets are filtered."""
        spam_tweets = [
            {
                "content": "FOLLOW BACK ✅✅ FREE AIRDROP 🚀🚀 Click link in bio NOW!! DM me",
                "username": "free_tokens_bot",
                "engagement_score": 8000,
                "author_weight": 1.0,
            },
            {
                "content": "$BTC PRESALE 🔥💰 Guaranteed 100x! Limited whitelist slots. Act NOW or miss out! https://scam.com",
                "username": "crypto_presale",
                "engagement_score": 3000,
                "author_weight": 1.0,
            },
            {
                "content": "@user1 @user2 @user3 @user4 @user5 CHECK BIO FOR GIVEAWAY 🌙 #moon #lambo #hodl",
                "username": "mention_spam_bot",
                "engagement_score": 2000,
                "author_weight": 1.0,
            },
        ]

        for tweet in spam_tweets:
            with self.subTest(tweet=tweet["content"][:30]):
                result = self.filter_engine.detect_spam(**tweet)
                self.assertTrue(result.is_spam, f"Spam tweet not flagged: {tweet['content'][:50]}")
                self.assertGreaterEqual(result.total_score, 0.3, f"Spam tweet has low score: {result.total_score}")

    def test_bot_spam_filter_bot_tweets(self):
        """Test that bot tweets are flagged."""
        bot_tweets = [
            {
                "content": "Automated trading signal: $BTC SHORT at 46500. Stop loss 47000. Take profit 45500.",
                "username": "trading_bot_pro",
                "engagement_score": 100,
                "author_weight": 1.0,
            },
            {
                "content": "Price update: BTC is at $46,500. Update time: 14:32:15 UTC. Data source: Binance API",
                "username": "crypto_bot_monitor",
                "engagement_score": 5,
                "author_weight": 1.0,
            },
        ]

        for tweet in bot_tweets:
            with self.subTest(tweet=tweet["content"][:30]):
                result = self.filter_engine.detect_spam(**tweet)
                self.assertTrue(result.is_bot, f"Bot tweet not flagged: {tweet['content'][:50]}")
                self.assertGreaterEqual(len(result.reasons), 1, "Bot tweet should record at least one reason")

    def test_sentiment_lexicon_scoring(self):
        """Test sentiment scoring accuracy."""
        test_cases = [
            ("$BTC is amazing! Love crypto!", 0.5, "Bullish"),
            ("$ETH crashed hard. Terrible day.", -0.5, "Bearish"),
            ("$SOL is stable. Just holding.", 0.0, "Neutral"),
            ("Bitcoin crashed badly today. Very disappointing.", -0.6, "Bearish"),  # Clear negative
        ]

        for text, expected_polarity, label in test_cases:
            with self.subTest(text=text[:30], label=label):
                score = self.sentiment_analyzer.get_score(text)
                if expected_polarity > 0:
                    self.assertGreater(score, 0, f"Expected positive sentiment for: {text}")
                elif expected_polarity < 0:
                    self.assertLess(score, 0, f"Expected negative sentiment for: {text}")
                else:
                    self.assertAlmostEqual(score, 0, delta=0.3, msg=f"Expected neutral sentiment for: {text}")

    def test_batch_filtering_function(self):
        """Test the batch filtering function."""
        tweets = [
            {
                "content": "$BTC clean analysis",
                "username": "trader",
                "engagement_score": 10,
                "author_weight": 1.0,
                "id": "clean1",
            },
            {
                "content": "FREE TOKENS 🚀 DM me",
                "username": "spam_bot",
                "engagement_score": 5000,
                "author_weight": 1.0,
                "id": "spam1",
            },
        ]

        clean_tweets, spam_tweets = self.filter_engine.filter_batch(tweets)

        self.assertEqual(len(clean_tweets), 1, "Should have 1 clean tweet")
        self.assertEqual(len(spam_tweets), 1, "Should have 1 spam tweet")

        self.assertEqual(clean_tweets[0]["id"], "clean1")
        self.assertEqual(spam_tweets[0]["id"], "spam1")
        self.assertTrue(spam_tweets[0]["is_spam"])

    @patch.object(bj, 'MongoStorageClient')
    @patch.object(bj, 'SparkSession')
    @patch.object(bj, 'udf')
    @patch('src.processing.batch_layer.batch_job.col')
    @patch('src.processing.batch_layer.batch_job.avg')
    @patch('src.processing.batch_layer.batch_job.count')
    @patch('src.processing.batch_layer.batch_job.spark_sum')
    def test_batch_job_processing_simulation(self, mock_sum, mock_count, mock_avg, mock_col, mock_udf, mock_spark, mock_mongo):
        """Test complete batch job processing simulation."""
        
        # 1. Giả lập Spark Session
        mock_session = MagicMock()
        mock_spark.builder.appName.return_value.getOrCreate.return_value = mock_session

        # 2. Giả lập UDF và các hàm Spark (Tránh lỗi SESSION_OR_CONTEXT_NOT_EXISTS)
        mock_udf.return_value = MagicMock()
        
        # Quan trọng: Cho các hàm col, avg, count... trả về một Mock có thể thực hiện toán tử (+, -, *, /)
        mock_spark_obj = MagicMock()
        mock_col.return_value = mock_spark_obj
        mock_avg.return_value = mock_spark_obj
        mock_count.return_value = mock_spark_obj
        mock_sum.return_value = mock_spark_obj
        
        # Giả lập các phép tính toán học trên Column (như spam_ratio trong code của bạn)
        mock_spark_obj.__truediv__.return_value = mock_spark_obj
        mock_spark_obj.alias.return_value = mock_spark_obj
        mock_spark_obj.cast.return_value = mock_spark_obj

        # 3. Giả lập DataFrame và các chuỗi xử lý (Method Chaining)
        mock_df = MagicMock()
        mock_session.read.json.return_value = mock_df
        
        mock_df.withColumn.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.filter.return_value = mock_df
        mock_df.groupBy.return_value = mock_df
        mock_df.agg.return_value = mock_df
        
        # Giả lập count() trả về con số để không lỗi phép chia log summary
        mock_df.count.return_value = 10 

        # 4. Giả lập dữ liệu trả về từ HDFS
        # Lần 1: cho coin_metrics | Lần 2: cho spam_stats
        mock_df.collect.side_effect = [
            [{"coin": "BTC", "avg_sentiment": 0.5, "mention_count": 10, "total_engagement": 100, "total_influence": 200.0}],
            [{"coin": "BTC", "spam_count": 2, "spam_ratio": 0.2}]
        ]

        # 5. Thực thi Batch Job
        try:
            bj.run_full_batch_job()
        except Exception as e:
            import traceback
            self.fail(f"Batch job failed logic test: {e}\n{traceback.format_exc()}")

        # 6. Kiểm chứng kết quả
        # Đảm bảo dữ liệu đã được đẩy sang MongoDB
        self.assertTrue(mock_mongo.return_value.save_sentiment_metric.called)
        self.assertTrue(mock_mongo.return_value.save_alert.called)
        
        # Đảm bảo session đã đóng
        mock_session.stop.assert_called_once()

    def test_fear_greed_score_calculation(self):
        """Test Fear & Greed score calculation."""
        # Fear & Greed = (sentiment + 1) * 50
        # Range: -1 to 1 -> 0 to 100

        test_cases = [
            (1.0, 100),   # Very bullish
            (0.5, 75),    # Bullish
            (0.0, 50),    # Neutral
            (-0.5, 25),   # Bearish
            (-1.0, 0),    # Very bearish
        ]

        for sentiment, expected_fg in test_cases:
            with self.subTest(sentiment=sentiment):
                fear_greed = (sentiment + 1) * 50
                self.assertEqual(fear_greed, expected_fg,
                    f"Wrong Fear & Greed calculation for sentiment {sentiment}")

    def test_filter_reasons_logging(self):
        """Test that filter reasons are properly logged."""
        tweet = {
            "content": "FREE AIRDROP 🚀🚀 DM me now for tokens",
            "username": "giveaway_bot",
            "engagement_score": 1000,
            "author_weight": 1.0,
        }

        result = self.filter_engine.detect_spam(**tweet)

        self.assertTrue(result.is_spam)
        self.assertGreater(len(result.reasons), 0, "Should have filter reasons")
        self.assertIn("spam_keywords", str(result.reasons))


if __name__ == "__main__":
    # Run tests with verbose output
    unittest.main(verbosity=2)