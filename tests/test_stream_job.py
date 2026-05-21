from __future__ import annotations

import argparse
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src" / "processing" / "speed_layer"))

import stream_job


class FakeMongoCursor:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def sort(self, *_args) -> "FakeMongoCursor":
        return self

    def limit(self, limit: int) -> list[dict]:
        return self.rows[:limit]


class FakeMongoCollection:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.last_query = None
        self.last_projection = None

    def find(self, query: dict, projection: dict) -> FakeMongoCursor:
        self.last_query = query
        self.last_projection = projection
        return FakeMongoCursor(self.rows)


class StreamJobTest(unittest.TestCase):
    def test_clean_tweet_accepts_friend_kafka_schema(self) -> None:
        tweet = stream_job.clean_tweet(
            {
                "id": "abc-1",
                "text": "$BTC breakout from market feed",
                "created_at": "2026-04-18T08:00:00Z",
                "author": "market_user",
                "target_coin": "$BTC",
            }
        )

        self.assertIsNotNone(tweet)
        assert tweet is not None
        self.assertEqual(tweet.tweet_id, "abc-1")
        self.assertEqual(tweet.content, "$BTC breakout from market feed")
        self.assertEqual(tweet.username, "market_user")
        self.assertEqual(tweet.cashtags, ["BTC"])
        self.assertEqual(tweet.author_type, "market")
        self.assertEqual(tweet.author_weight, 1.0)

    def test_clean_tweet_weights_whale_signal(self) -> None:
        tweet = stream_job.clean_tweet(
            {
                "id": "whale-1",
                "text": "$ETH accumulation signal",
                "created_at": "2026-04-18T08:00:00Z",
                "author": "saylor",
                "target_coin": "WHALE_SIGNAL",
                "like_count": "10",
                "retweet_count": "5",
                "reply_count": "1",
            }
        )

        self.assertIsNotNone(tweet)
        assert tweet is not None
        self.assertEqual(tweet.cashtags, ["ETH"])
        self.assertEqual(tweet.author_type, "whale")
        self.assertEqual(tweet.author_weight, 5.0)
        self.assertEqual(tweet.engagement_score, 16)
        self.assertEqual(tweet.influence_score, 80.0)

    def test_build_kafka_options_reads_ssl_pem_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            ca_path = tmp_path / "ca.pem"
            cert_path = tmp_path / "service.cert"
            key_path = tmp_path / "service.key"
            ca_path.write_text("CA CERT", encoding="utf-8")
            cert_path.write_text("CLIENT CERT", encoding="utf-8")
            key_path.write_text("CLIENT KEY", encoding="utf-8")

            args = argparse.Namespace(
                bootstrap_servers="example.kafka:9093",
                topic="raw-tweets-market,raw-tweets-whales",
                starting_offsets="earliest",
                kafka_security_protocol="SSL",
                kafka_ssl_ca_location=str(ca_path),
                kafka_ssl_cert_location=str(cert_path),
                kafka_ssl_key_location=str(key_path),
            )

            options = stream_job.build_kafka_options(args)

        self.assertEqual(options["kafka.bootstrap.servers"], "example.kafka:9093")
        self.assertEqual(options["subscribe"], "raw-tweets-market,raw-tweets-whales")
        self.assertEqual(options["startingOffsets"], "earliest")
        self.assertEqual(options["kafka.security.protocol"], "SSL")
        self.assertEqual(options["kafka.ssl.truststore.type"], "PEM")
        self.assertEqual(options["kafka.ssl.truststore.certificates"], "CA CERT")
        self.assertEqual(options["kafka.ssl.keystore.type"], "PEM")
        self.assertEqual(options["kafka.ssl.keystore.certificate.chain"], "CLIENT CERT")
        self.assertEqual(options["kafka.ssl.keystore.key"], "CLIENT KEY")

    def test_calculate_spike_fields_marks_large_growth_as_spike(self) -> None:
        fields = stream_job.calculate_spike_fields(35, 3.5)

        self.assertEqual(fields["baseline_mention_count"], 3.5)
        self.assertEqual(fields["growth_rate"], 10.0)
        self.assertTrue(fields["is_spike"])

    def test_calculate_spike_fields_requires_minimum_mentions(self) -> None:
        fields = stream_job.calculate_spike_fields(4, 1)

        self.assertEqual(fields["growth_rate"], 4.0)
        self.assertFalse(fields["is_spike"])

    def test_fetch_baseline_mention_count_averages_recent_windows(self) -> None:
        collection = FakeMongoCollection(
            [
                {"mention_count": 3},
                {"mention_count": 4},
                {"mention_count": 5},
            ]
        )

        baseline = stream_job.fetch_baseline_mention_count(
            collection,
            "DOGE",
            "2026-04-18T08:05:00Z",
        )

        self.assertEqual(baseline, 4.0)
        self.assertEqual(
            collection.last_query,
            {"symbol": "DOGE", "window_end": {"$lt": "2026-04-18T08:05:00Z"}},
        )
        self.assertEqual(collection.last_projection, {"mention_count": 1})


if __name__ == "__main__":
    unittest.main()
