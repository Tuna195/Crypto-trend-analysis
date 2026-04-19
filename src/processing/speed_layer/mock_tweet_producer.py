import argparse
import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_OUTPUT_DIR = Path("/tmp/crypto_trend_stream_input")
SYMBOLS = ["BTC", "ETH", "SOL", "DOGE", "XRP", "BNB"]
USERS = [
    ("u001", "alice"),
    ("u002", "bob"),
    ("u003", "carol"),
    ("u004", "dave"),
    ("u005", "erin"),
    ("u006", "frank"),
]


def build_tweet(batch_id: int, item_id: int) -> dict:
    user_id, username = random.choice(USERS)
    symbols = random.sample(SYMBOLS, k=random.choice([1, 1, 2]))
    symbol_text = " ".join(f"${symbol}" for symbol in symbols)
    tweet_id = f"stream-{batch_id}-{item_id}"

    return {
        "tweet_id": tweet_id,
        "user_id": user_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "username": username,
        "content": f"{symbol_text} realtime momentum update from {username}",
        "hashtags": ["crypto", "trend"],
        "cashtags": symbols,
        "like_count": random.randint(0, 60),
        "retweet_count": random.randint(0, 20),
        "reply_count": random.randint(0, 12),
        "lang": "en",
    }


def write_batch(output_dir: Path, batch_id: int, records_per_batch: int) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    batch_path = output_dir / f"tweets_batch_{batch_id:05d}.jsonl"
    records = [build_tweet(batch_id, item_id) for item_id in range(records_per_batch)]
    batch_path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )
    return batch_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock tweet stream producer")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--records-per-batch", type=int, default=5)
    parser.add_argument("--batches", type=int, default=0, help="0 means run forever")
    args = parser.parse_args()

    batch_id = 1
    while args.batches == 0 or batch_id <= args.batches:
        batch_path = write_batch(args.output_dir, batch_id, args.records_per_batch)
        print(f"wrote {args.records_per_batch} records -> {batch_path}", flush=True)
        batch_id += 1
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
