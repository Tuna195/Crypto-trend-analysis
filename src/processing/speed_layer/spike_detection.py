from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

try:
    from .config import (
        SPIKE_BASELINE_LOOKBACK_WINDOWS,
        SPIKE_BASELINE_WINDOWS,
        SPIKE_GROWTH_RATE_THRESHOLD,
        SPIKE_MIN_MENTION_COUNT,
        SPIKE_MIN_UNIQUE_AUTHORS,
        SPIKE_SUPPRESSION_MINUTES,
        SPIKE_Z_SCORE_THRESHOLD,
    )
    from .demo_pipeline import parse_float, parse_int, parse_iso_datetime
except ImportError:  # pragma: no cover - direct script import path
    from config import (
        SPIKE_BASELINE_LOOKBACK_WINDOWS,
        SPIKE_BASELINE_WINDOWS,
        SPIKE_GROWTH_RATE_THRESHOLD,
        SPIKE_MIN_MENTION_COUNT,
        SPIKE_MIN_UNIQUE_AUTHORS,
        SPIKE_SUPPRESSION_MINUTES,
        SPIKE_Z_SCORE_THRESHOLD,
    )
    from demo_pipeline import parse_float, parse_int, parse_iso_datetime


def calculate_spike_fields(
    mention_count: Any,
    baseline_mention_count: Any,
    baseline_stddev: Any = 0.0,
    baseline_sample_count: int = 0,
    unique_authors: Any = None,
    is_suppressed: bool = False,
    min_mention_count: int = SPIKE_MIN_MENTION_COUNT,
    min_unique_authors: int = SPIKE_MIN_UNIQUE_AUTHORS,
    growth_rate_threshold: float = SPIKE_GROWTH_RATE_THRESHOLD,
    z_score_threshold: float = SPIKE_Z_SCORE_THRESHOLD,
) -> dict[str, Any]:
    current_mentions = parse_int(mention_count)
    current_unique_authors = (
        parse_int(unique_authors) if unique_authors is not None else min_unique_authors
    )
    baseline_mentions = max(parse_float(baseline_mention_count, default=0.0), 0.0)
    baseline_deviation = max(parse_float(baseline_stddev, default=0.0), 0.0)
    growth_rate = current_mentions / max(baseline_mentions, 1.0)
    z_score = 0.0
    if baseline_deviation > 0:
        z_score = (current_mentions - baseline_mentions) / baseline_deviation

    has_volume = current_mentions >= min_mention_count
    has_authors = current_unique_authors >= min_unique_authors
    has_statistical_spike = (
        baseline_deviation > 0
        and baseline_sample_count >= 2
        and z_score >= z_score_threshold
    )
    has_growth_spike = growth_rate >= growth_rate_threshold
    is_spike = has_volume and has_authors and not is_suppressed and (
        has_statistical_spike or has_growth_spike
    )

    spike_reasons = []
    if has_statistical_spike:
        spike_reasons.append("z_score")
    if has_growth_spike:
        spike_reasons.append("growth_rate")
    if not has_authors:
        spike_reasons.append("insufficient_unique_authors")
    if is_suppressed:
        spike_reasons.append("suppressed_recent_spike")

    return {
        "baseline_mention_count": round(baseline_mentions, 2),
        "baseline_stddev": round(baseline_deviation, 2),
        "baseline_sample_count": baseline_sample_count,
        "growth_rate": round(growth_rate, 2),
        "z_score": round(z_score, 2),
        "min_unique_authors": min_unique_authors,
        "is_suppressed": is_suppressed,
        "spike_reasons": spike_reasons,
        "is_spike": is_spike,
    }


def parse_datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return parse_iso_datetime(value)


def average(values: list[int]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def population_stddev(values: list[int]) -> float:
    if len(values) < 2:
        return 0.0
    mean = average(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def fetch_baseline_stats(
    collection: Any,
    symbol: str,
    window_end: Any,
    same_hour_limit: int = SPIKE_BASELINE_WINDOWS,
    lookback_limit: int = SPIKE_BASELINE_LOOKBACK_WINDOWS,
    suppression_minutes: int = SPIKE_SUPPRESSION_MINUTES,
) -> dict[str, Any]:
    if not symbol or window_end is None:
        return {
            "baseline_mention_count": 0.0,
            "baseline_stddev": 0.0,
            "baseline_sample_count": 0,
            "is_suppressed": False,
        }

    current_window_end = parse_datetime_value(window_end)

    cursor = (
        collection.find(
            {"symbol": symbol, "window_end": {"$lt": window_end}},
            {
                "mention_count": 1,
                "unique_authors": 1,
                "window_end": 1,
                "is_spike": 1,
            },
        )
        .sort("window_end", -1)
        .limit(lookback_limit)
    )
    rows = list(cursor)

    recent_spike_seen = False
    if current_window_end is not None:
        suppression_seconds = suppression_minutes * 60
        for row in rows:
            previous_window_end = parse_datetime_value(row.get("window_end"))
            if not previous_window_end or not row.get("is_spike"):
                continue
            age_seconds = (current_window_end - previous_window_end).total_seconds()
            if 0 < age_seconds <= suppression_seconds:
                recent_spike_seen = True
                break

    same_hour_rows = rows
    if current_window_end is not None:
        same_hour_rows = [
            row
            for row in rows
            if (parsed := parse_datetime_value(row.get("window_end"))) is not None
            and parsed.hour == current_window_end.hour
        ]

    baseline_rows = same_hour_rows[:same_hour_limit] or rows[:same_hour_limit]
    mention_counts = [parse_int(row.get("mention_count")) for row in baseline_rows]

    return {
        "baseline_mention_count": round(average(mention_counts), 2),
        "baseline_stddev": round(population_stddev(mention_counts), 2),
        "baseline_sample_count": len(mention_counts),
        "is_suppressed": recent_spike_seen,
    }


def fetch_baseline_mention_count(
    collection: Any,
    symbol: str,
    window_end: Any,
    limit: int = SPIKE_BASELINE_WINDOWS,
) -> float:
    return fetch_baseline_stats(
        collection,
        symbol,
        window_end,
        same_hour_limit=limit,
        lookback_limit=max(limit, SPIKE_BASELINE_LOOKBACK_WINDOWS),
    )["baseline_mention_count"]


def enrich_trend_document_with_spike(document: dict[str, Any], collection: Any) -> dict[str, Any]:
    if "symbol" not in document or "mention_count" not in document:
        return document

    baseline_stats = fetch_baseline_stats(
        collection,
        document["symbol"],
        document.get("window_end"),
    )
    return {
        **document,
        **calculate_spike_fields(
            document["mention_count"],
            baseline_stats["baseline_mention_count"],
            baseline_stats["baseline_stddev"],
            baseline_stats["baseline_sample_count"],
            document.get("unique_authors", 1),
            baseline_stats["is_suppressed"],
        ),
        "updated_at": datetime.now(timezone.utc),
    }
