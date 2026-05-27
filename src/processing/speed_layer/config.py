from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env", override=False)

DEFAULT_SAMPLE_PATH = Path(__file__).resolve().parent / "sample_data" / "tweet_stream.jsonl"
DEFAULT_CHECKPOINT_LOCATION = "/tmp/crypto_trend_kafka_checkpoint"
DEFAULT_SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
DEFAULT_KAFKA_PACKAGE = os.getenv(
    "SPARK_KAFKA_PACKAGE",
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1",
)
DEFAULT_BOOTSTRAP_SERVERS = (
    f"{os.getenv('KAFKA_HOST')}:{os.getenv('KAFKA_PORT')}"
    if os.getenv("KAFKA_HOST") and os.getenv("KAFKA_PORT")
    else "localhost:9092"
)
DEFAULT_TOPICS = ",".join(
    [
        os.getenv("KAFKA_TOPIC_MARKET", "raw-tweets-market"),
        os.getenv("KAFKA_TOPIC_WHALES", "raw-tweets-whales"),
    ]
)
DEFAULT_CERTS_DIR = Path(
    os.getenv("KAFKA_CERTS_DIR") or PROJECT_ROOT / "src" / "ingestion" / "certs"
)
DEFAULT_KAFKA_SECURITY_PROTOCOL = os.getenv(
    "KAFKA_SECURITY_PROTOCOL",
    "SSL" if os.getenv("KAFKA_HOST") and os.getenv("KAFKA_PORT") else "",
)
DEFAULT_KAFKA_SSL_CA_LOCATION = os.getenv(
    "KAFKA_SSL_CA_LOCATION", str(DEFAULT_CERTS_DIR / "ca.pem")
)
DEFAULT_KAFKA_SSL_CERT_LOCATION = os.getenv(
    "KAFKA_SSL_CERT_LOCATION", str(DEFAULT_CERTS_DIR / "service.cert")
)
DEFAULT_KAFKA_SSL_KEY_LOCATION = os.getenv(
    "KAFKA_SSL_KEY_LOCATION", str(DEFAULT_CERTS_DIR / "service.key")
)

WHALE_AUTHORS = {"elonmusk", "saylor", "vitalikbuterin", "whale_alert"}

SPIKE_BASELINE_WINDOWS = 6
SPIKE_BASELINE_LOOKBACK_WINDOWS = 72
SPIKE_MIN_MENTION_COUNT = 10
SPIKE_MIN_UNIQUE_AUTHORS = 3
SPIKE_GROWTH_RATE_THRESHOLD = 3.0
SPIKE_Z_SCORE_THRESHOLD = 3.0
SPIKE_SUPPRESSION_MINUTES = 30
