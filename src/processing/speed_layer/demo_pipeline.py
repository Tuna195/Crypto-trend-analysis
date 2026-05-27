from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from .config import WHALE_AUTHORS
except ImportError:  # pragma: no cover - direct script import path
    from config import WHALE_AUTHORS


CASHTAG_PATTERN = re.compile(r"\$([A-Za-z][A-Za-z0-9]{1,9})")


@dataclass
class CleanTweet:
    tweet_id: str
    user_id: str
    created_at: datetime
    username: str
    content: str
    hashtags: list[str]
    cashtags: list[str]
    like_count: int
    retweet_count: int
    reply_count: int
    lang: str
    engagement_score: int
    author_type: str
    author_weight: float
    influence_score: float


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def parse_iso_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_int(value: Any) -> int:
    if value is None or value == "":
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def parse_float(value: Any, default: float = 1.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def extract_cashtags(content: str) -> list[str]:
    unique = []
    for match in CASHTAG_PATTERN.findall(content):
        symbol = match.upper()
        if symbol not in unique:
            unique.append(symbol)
    return unique


def ensure_list(value: Any) -> list[str]:
    if value is None:
        return []
    raw_items = value if isinstance(value, list) else [value]

    normalized: list[str] = []
    for item in raw_items:
        text = normalize_text(item).strip("#$")
        if text:
            normalized.append(text.upper())
    return normalized


def normalize_target_coin(value: Any) -> list[str]:
    text = normalize_text(value)
    if not text or text in {"MULTI_CRYPTO", "WHALE_SIGNAL"}:
        return []
    return ensure_list(text)


def classify_author(author: str, target_coin: str = "") -> tuple[str, float]:
    normalized_author = normalize_text(author).lower().lstrip("@")
    normalized_target = normalize_text(target_coin).upper()
    if normalized_target == "WHALE_SIGNAL" or normalized_author in WHALE_AUTHORS:
        return "whale", 5.0
    return "market", 1.0


def parse_json_line(line: str) -> dict[str, Any] | None:
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def clean_tweet(record: dict[str, Any]) -> CleanTweet | None:
    tweet_id = normalize_text(record.get("tweet_id")) or normalize_text(record.get("id"))
    author = normalize_text(record.get("author"))
    target_coin = normalize_text(record.get("target_coin"))
    user_id = (
        normalize_text(record.get("user_id"))
        or normalize_text(record.get("author_id"))
        or author
    )
    created_at = parse_iso_datetime(record.get("created_at"))
    username = normalize_text(record.get("username")) or author or "unknown"
    if not user_id:
        user_id = username
    content = normalize_text(record.get("content")) or normalize_text(record.get("text"))

    hashtags = ensure_list(record.get("hashtags"))
    cashtags = ensure_list(record.get("cashtags"))
    if not cashtags:
        cashtags = extract_cashtags(content)
    if not cashtags:
        cashtags = normalize_target_coin(target_coin)

    like_count = parse_int(record.get("like_count"))
    retweet_count = parse_int(record.get("retweet_count"))
    reply_count = parse_int(record.get("reply_count"))
    lang = normalize_text(record.get("lang")).lower() or "und"
    author_type = normalize_text(record.get("author_type")).lower()
    author_weight = parse_float(record.get("author_weight"))
    if not author_type:
        author_type, author_weight = classify_author(username, target_coin)
    engagement_score = like_count + retweet_count + reply_count

    if not tweet_id or created_at is None or not content or not cashtags:
        return None

    return CleanTweet(
        tweet_id=tweet_id,
        user_id=user_id,
        created_at=created_at,
        username=username,
        content=content,
        hashtags=hashtags,
        cashtags=cashtags,
        like_count=like_count,
        retweet_count=retweet_count,
        reply_count=reply_count,
        lang=lang,
        engagement_score=engagement_score,
        author_type=author_type,
        author_weight=author_weight,
        influence_score=author_weight * engagement_score,
    )


def deduplicate_tweets(tweets: Iterable[CleanTweet]) -> list[CleanTweet]:
    latest_by_id: dict[str, CleanTweet] = {}
    for tweet in tweets:
        current = latest_by_id.get(tweet.tweet_id)
        if current is None or tweet.created_at > current.created_at:
            latest_by_id[tweet.tweet_id] = tweet
    return sorted(latest_by_id.values(), key=lambda item: item.created_at)


def aggregate_trends(tweets: Iterable[CleanTweet]) -> list[dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "mention_count": 0,
            "engagement_score": 0,
            "influence_score": 0.0,
            "influencer_authors": set(),
            "max_author_weight": 1.0,
            "authors": set(),
            "last_seen": None,
        }
    )

    for tweet in tweets:
        for symbol in tweet.cashtags:
            metric = metrics[symbol]
            metric["mention_count"] += 1
            metric["engagement_score"] += tweet.engagement_score
            metric["influence_score"] += tweet.influence_score
            metric["authors"].add(tweet.user_id)
            if tweet.author_weight > 1.0:
                metric["influencer_authors"].add(tweet.user_id)
            metric["max_author_weight"] = max(metric["max_author_weight"], tweet.author_weight)
            if metric["last_seen"] is None or tweet.created_at > metric["last_seen"]:
                metric["last_seen"] = tweet.created_at

    rows = []
    for symbol, metric in metrics.items():
        mention_count = metric["mention_count"]
        engagement_score = metric["engagement_score"]
        influence_score = metric["influence_score"]
        unique_authors = len(metric["authors"])
        influencer_authors = len(metric["influencer_authors"])
        trend_score = (
            mention_count * 2
            + unique_authors
            + (engagement_score / 10.0)
            + (influence_score / 20.0)
            + (influencer_authors * 3.0)
        )
        rows.append(
            {
                "symbol": symbol,
                "mention_count": mention_count,
                "unique_authors": unique_authors,
                "influencer_authors": influencer_authors,
                "engagement_score": engagement_score,
                "influence_score": round(influence_score, 2),
                "max_author_weight": metric["max_author_weight"],
                "trend_score": round(trend_score, 2),
                "last_seen": metric["last_seen"].isoformat(),
            }
        )

    return sorted(
        rows,
        key=lambda item: (item["trend_score"], item["engagement_score"]),
        reverse=True,
    )


def load_demo_messages(path: Path) -> list[str]:
    return [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def run_demo(sample_path: Path) -> int:
    messages = load_demo_messages(sample_path)
    parsed = [parse_json_line(line) for line in messages]
    parsed_valid = [record for record in parsed if record is not None]
    cleaned = [clean_tweet(record) for record in parsed_valid]
    cleaned_valid = [tweet for tweet in cleaned if tweet is not None]
    deduplicated = deduplicate_tweets(cleaned_valid)
    aggregated = aggregate_trends(deduplicated)

    print("=== Demo Summary ===")
    print(f"raw_messages={len(messages)}")
    print(f"parsed_messages={len(parsed_valid)}")
    print(f"clean_messages={len(cleaned_valid)}")
    print(f"deduplicated_messages={len(deduplicated)}")
    print()

    print("=== Clean Tweets ===")
    for tweet in deduplicated:
        print(
            json.dumps(
                {
                    "tweet_id": tweet.tweet_id,
                    "user_id": tweet.user_id,
                    "created_at": tweet.created_at.isoformat(),
                    "username": tweet.username,
                    "cashtags": tweet.cashtags,
                    "engagement_score": tweet.engagement_score,
                    "author_type": tweet.author_type,
                    "author_weight": tweet.author_weight,
                    "influence_score": tweet.influence_score,
                    "content": tweet.content,
                },
                ensure_ascii=True,
            )
        )

    print()
    print("=== Aggregated Trends ===")
    for row in aggregated:
        print(json.dumps(row, ensure_ascii=True))

    return 0
