"""
src/crypto-dashboard/backend/main.py
======================================
FastAPI backend — đọc MongoDB URI từ .env ở root project.
Query các collection do Thắng (speed) và Hiệu (batch) ghi vào.

Chạy từ thư mục ROOT của project:
    uvicorn src.crypto-dashboard.backend.main:app --reload --port 8000

Hoặc chạy từ thư mục src/crypto-dashboard/backend/:
    uvicorn main:app --reload --port 8000
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient, DESCENDING, ASCENDING

# ── Đọc .env từ root project (2 cấp trên thư mục này) ──────────────────────
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())   # tự tìm .env từ thư mục hiện tại đi lên

MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://tranduonganttcole_db_user:KrHMQZxRZlRAsA3B@cluster0.rrajasg.mongodb.net/?appName=Cluster0")
MONGO_DB  = os.getenv("MONGO_DB",  "crypto_trends")

# ── Singleton MongoDB connection ─────────────────────────────────────────────
_client: MongoClient | None = None

def get_db():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client[MONGO_DB]

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    yield
    if _client:
        _client.close()

# ── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="CryptoTrend Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ── Helpers ──────────────────────────────────────────────────────────────────
def cutoff(hours: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)

def clean(doc: dict) -> dict:
    doc.pop("_id", None)
    return doc

# ── Pydantic Schemas ─────────────────────────────────────────────────────────
# Ánh xạ 1-1 với document thực tế trong MongoDB

class BatchSentimentMetric(BaseModel):
    """Collection: batch_sentiment_metrics — ghi bởi batch_job.py"""
    coin: str
    mention_count: int
    bullish_ratio: float
    bearish_ratio: float
    neutral_ratio: float
    fear_greed_score: float
    total_engagement: int
    window_start: datetime
    window_end: datetime
    created_at: datetime
    whale_mention_count:  Optional[int]   = None
    whale_avg_sentiment:  Optional[float] = None
    whale_fear_greed:     Optional[float] = None
    whale_bullish_ratio:  Optional[float] = None
    whale_bearish_ratio:  Optional[float] = None
    retail_mention_count: Optional[int]   = None
    retail_avg_sentiment: Optional[float] = None
    retail_fear_greed:    Optional[float] = None
    retail_bullish_ratio: Optional[float] = None
    retail_bearish_ratio: Optional[float] = None

class SpeedTrendMetric(BaseModel):
    """Collection: speed_trend_metrics — ghi bởi stream_runtime.py"""
    symbol: str
    window_start: datetime
    window_end:   datetime
    mention_count:      int
    unique_authors:     int
    influencer_authors: int
    engagement_score:   int
    influence_score:    float
    max_author_weight:  float
    trend_score:        float
    last_seen:          datetime
    baseline_mention_count: Optional[float] = None
    baseline_stddev:        Optional[float] = None
    growth_rate:            Optional[float] = None
    z_score:                Optional[float] = None
    is_spike:               Optional[bool]  = None
    spike_reasons:          Optional[list[str]] = None
    is_suppressed:          Optional[bool]  = None
    updated_at:             Optional[datetime] = None

class BatchTrendSpike(BaseModel):
    """Collection: batch_trend_spikes — ghi bởi MongoStorageClient.save_trend_spike()"""
    keyword:        str
    mention_count:  int
    baseline_count: float
    z_score:        float
    related_coins:  list[str]
    window_start:   Optional[datetime] = None
    window_end:     Optional[datetime] = None
    detected_at:    datetime

class AlertItem(BaseModel):
    """Collection: alerts — ghi bởi MongoStorageClient.save_alert()"""
    alert_type: str
    severity:   str
    message:    str
    status:     str
    payload:    Optional[dict[str, Any]] = None
    created_at: datetime

class DashboardSummary(BaseModel):
    top_trending_coin:  str
    top_trend_score:    float
    total_mentions_1h:  int
    avg_fear_greed:     float
    active_alerts:      int
    active_spikes:      int
    last_updated:       datetime

class TrendRankItem(BaseModel):
    """Aggregate từ batch_sentiment_metrics"""
    coin:             str
    avg_fear_greed:   float
    avg_bullish:      float
    avg_bearish:      float
    total_mentions:   int
    total_engagement: int
    snapshot_count:   int
    latest_at:        Optional[datetime] = None
    avg_whale_fg:     Optional[float]    = None
    avg_retail_fg:    Optional[float]    = None

class BadRecordStat(BaseModel):
    """Collection: speed_bad_records — ghi bởi spark_pipeline.py"""
    window_start:     datetime
    window_end:       datetime
    invalid_reason:   str
    bad_record_count: int

class BatchJobRun(BaseModel):
    """Collection: batch_job_runs — ghi bởi log_batch_run() trong batch_job.py"""
    job_type:         str
    mode:             str
    status:           str
    target_date:      Optional[str]   = None
    target_hour:      Optional[str]   = None
    total_tweets:     int
    clean_tweets:     int
    spam_tweets:      int
    coins_processed:  int
    spikes_detected:  list[str]
    error_message:    Optional[str]   = None
    duration_seconds: Optional[float] = None
    executed_at:      datetime

# ═════════════════════════════════════════════════════════════════════════════
# ENDPOINTS
# ═════════════════════════════════════════════════════════════════════════════

@app.get("/health", tags=["System"])
def health_check():
    """Ping MongoDB và đếm documents từng collection."""
    try:
        db = get_db()
        db.client.admin.command("ping")
        counts = {
            col: db[col].estimated_document_count()
            for col in [
                "batch_sentiment_metrics", "batch_trend_spikes",
                "speed_trend_metrics",     "speed_bad_records",
                "alerts", "test_batch_process", "batch_job_runs",
            ]
        }
        return {"status": "ok", "mongo_uri": MONGO_URI[:30]+"...", "collections": counts}
    except Exception as e:
        raise HTTPException(503, f"MongoDB unreachable: {e}")


@app.get("/api/summary", response_model=DashboardSummary, tags=["Dashboard"])
def get_summary():
    """
    4 KPI cards đầu trang.
    - top coin + mentions + spikes → speed_trend_metrics (Thắng)
    - fear & greed                 → batch_sentiment_metrics (Hiệu)
    - active alerts                → alerts (batch_job.py)
    """
    db  = get_db()
    cut = cutoff(1)

    # Top coin theo trend_score trong 1h
    top = list(db.speed_trend_metrics.aggregate([
        {"$match": {"window_start": {"$gte": cut}}},
        {"$group": {"_id": "$symbol", "max_trend": {"$max": "$trend_score"}, "sum_mention": {"$sum": "$mention_count"}}},
        {"$sort": {"max_trend": -1}},
        {"$limit": 1},
    ]))
    top_coin  = top[0]["_id"]       if top else "N/A"
    top_score = top[0]["max_trend"] if top else 0.0

    # Tổng mentions 1h
    men = list(db.speed_trend_metrics.aggregate([
        {"$match": {"window_start": {"$gte": cut}}},
        {"$group": {"_id": None, "t": {"$sum": "$mention_count"}}},
    ]))
    total_mentions = men[0]["t"] if men else 0

    # Fear & Greed tb 24h từ batch
    fg = list(db.batch_sentiment_metrics.aggregate([
        {"$match": {"window_start": {"$gte": cutoff(24)}}},
        {"$group": {"_id": None, "avg": {"$avg": "$fear_greed_score"}}},
    ]))
    avg_fg = round(fg[0]["avg"], 1) if fg else 50.0

    active_alerts = db.alerts.count_documents({"status": "open"})
    active_spikes = db.speed_trend_metrics.count_documents({"window_start": {"$gte": cut}, "is_spike": True})

    return DashboardSummary(
        top_trending_coin=top_coin,
        top_trend_score=round(top_score, 2),
        total_mentions_1h=total_mentions,
        avg_fear_greed=avg_fg,
        active_alerts=active_alerts,
        active_spikes=active_spikes,
        last_updated=datetime.now(timezone.utc),
    )


@app.get("/api/trends/batch", response_model=list[TrendRankItem], tags=["Trends"])
def get_batch_trends(
    hours: int = Query(24, ge=1, le=168),
    limit: int = Query(20, ge=1, le=100),
):
    """
    Bảng xếp hạng coin từ batch_sentiment_metrics (Hiệu).
    Bao gồm whale_fear_greed và retail_fear_greed cho tab Whales.
    """
    db  = get_db()
    cut = cutoff(hours)

    rows = list(db.batch_sentiment_metrics.aggregate([
        {"$match": {"window_start": {"$gte": cut}}},
        {"$group": {
            "_id":              "$coin",
            "avg_fear_greed":   {"$avg": "$fear_greed_score"},
            "avg_bullish":      {"$avg": "$bullish_ratio"},
            "avg_bearish":      {"$avg": "$bearish_ratio"},
            "total_mentions":   {"$sum": "$mention_count"},
            "total_engagement": {"$sum": "$total_engagement"},
            "snapshot_count":   {"$sum": 1},
            "latest_at":        {"$max": "$window_end"},
            "avg_whale_fg":     {"$avg": "$whale_fear_greed"},
            "avg_retail_fg":    {"$avg": "$retail_fear_greed"},
        }},
        {"$sort": {"avg_fear_greed": -1}},
        {"$limit": limit},
    ]))

    return [
        TrendRankItem(
            coin=r["_id"],
            avg_fear_greed=round(r["avg_fear_greed"] or 0, 1),
            avg_bullish=round(r["avg_bullish"] or 0, 3),
            avg_bearish=round(r["avg_bearish"] or 0, 3),
            total_mentions=int(r["total_mentions"] or 0),
            total_engagement=int(r["total_engagement"] or 0),
            snapshot_count=int(r["snapshot_count"]),
            latest_at=r.get("latest_at"),
            avg_whale_fg=round(r["avg_whale_fg"], 1) if r.get("avg_whale_fg") else None,
            avg_retail_fg=round(r["avg_retail_fg"], 1) if r.get("avg_retail_fg") else None,
        )
        for r in rows
    ]


@app.get("/api/trends/speed", response_model=list[SpeedTrendMetric], tags=["Trends"])
def get_speed_trends(
    hours: int  = Query(1, ge=1, le=24),
    limit: int  = Query(20, ge=1, le=100),
    only_spikes: bool = Query(False),
):
    """
    Top trending từ speed_trend_metrics (Thắng — Spark Streaming).
    Lấy window mới nhất của mỗi symbol.
    """
    db  = get_db()
    cut = cutoff(hours)

    match: dict = {"window_start": {"$gte": cut}}
    if only_spikes:
        match["is_spike"] = True

    rows = list(db.speed_trend_metrics.aggregate([
        {"$match": match},
        {"$sort": {"window_start": DESCENDING}},
        {"$group": {"_id": "$symbol", "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$sort": {"trend_score": DESCENDING}},
        {"$limit": limit},
        {"$project": {"_id": 0}},
    ]))
    return [SpeedTrendMetric(**r) for r in rows]


@app.get("/api/sentiment/{coin}", response_model=list[BatchSentimentMetric], tags=["Sentiment"])
def get_coin_sentiment(
    coin: str,
    hours:  int = Query(6,       ge=1,  le=72),
    source: str = Query("batch", regex="^(batch|test)$"),
):
    """
    Lịch sử sentiment 1 coin theo thời gian.
    source=batch → batch_sentiment_metrics (production)
    source=test  → test_batch_process (demo mode)
    """
    db         = get_db()
    cut        = cutoff(hours)
    coin_upper = coin.upper().replace("$", "")
    collection = "batch_sentiment_metrics" if source == "batch" else "test_batch_process"

    docs = list(
        db[collection]
        .find({"coin": coin_upper, "window_start": {"$gte": cut}}, {"_id": 0})
        .sort("window_start", ASCENDING)
        .limit(200)
    )

    if not docs:
        raise HTTPException(404, f"Không có data cho {coin_upper} trong {hours}h (source={collection})")

    for d in docs:
        d.setdefault("total_engagement", 0)
        d.setdefault("created_at", d.get("window_start"))

    return [BatchSentimentMetric(**d) for d in docs]


@app.get("/api/spikes/batch", response_model=list[BatchTrendSpike], tags=["Spikes"])
def get_batch_spikes(
    hours: int   = Query(24,  ge=1,   le=168),
    min_z: float = Query(2.0, ge=0.0, le=10.0),
    limit: int   = Query(15,  ge=1,   le=50),
):
    """Trend spikes từ batch_trend_spikes (Hiệu), filter theo z-score."""
    db  = get_db()
    cut = cutoff(hours)

    docs = list(
        db.batch_trend_spikes
        .find({"detected_at": {"$gte": cut}, "z_score": {"$gte": min_z}}, {"_id": 0})
        .sort("z_score", DESCENDING)
        .limit(limit)
    )
    return [BatchTrendSpike(**d) for d in docs]


@app.get("/api/spikes/speed", response_model=list[SpeedTrendMetric], tags=["Spikes"])
def get_speed_spikes(
    hours: int = Query(1,  ge=1, le=24),
    limit: int = Query(15, ge=1, le=50),
):
    """Live spikes từ speed_trend_metrics (Thắng), chỉ is_spike=True."""
    db  = get_db()
    cut = cutoff(hours)

    rows = list(db.speed_trend_metrics.aggregate([
        {"$match": {"window_start": {"$gte": cut}, "is_spike": True}},
        {"$sort": {"growth_rate": DESCENDING}},
        {"$limit": limit},
        {"$project": {"_id": 0}},
    ]))
    return [SpeedTrendMetric(**r) for r in rows]


@app.get("/api/alerts", response_model=list[AlertItem], tags=["Alerts"])
def get_alerts(
    status:     Optional[str] = Query(None),
    alert_type: Optional[str] = Query(None),
    limit:      int           = Query(20, ge=1, le=100),
):
    """Alerts feed — ghi bởi batch_job.py (spam) và ingestion (whale signal)."""
    db    = get_db()
    query = {}
    if status:
        query["status"] = status
    if alert_type:
        query["alert_type"] = alert_type

    docs = list(
        db.alerts.find(query, {"_id": 0})
        .sort("created_at", DESCENDING)
        .limit(limit)
    )
    return [AlertItem(**clean(d)) for d in docs]


@app.get("/api/quality/bad-records", response_model=list[BadRecordStat], tags=["Quality"])
def get_bad_records(hours: int = Query(6, ge=1, le=48)):
    """Bad records từ speed_bad_records (Thắng — Spark Streaming)."""
    db  = get_db()
    cut = cutoff(hours)

    docs = list(
        db.speed_bad_records
        .find({"window_start": {"$gte": cut}}, {"_id": 0})
        .sort("window_start", DESCENDING)
        .limit(200)
    )
    return [BadRecordStat(**d) for d in docs]


@app.get("/api/jobs/history", response_model=list[BatchJobRun], tags=["System"])
def get_job_history(limit: int = Query(10, ge=1, le=50)):
    """Lịch sử batch job runs — ghi bởi log_batch_run() trong batch_job.py."""
    db   = get_db()
    docs = list(
        db.batch_job_runs.find({}, {"_id": 0})
        .sort("executed_at", DESCENDING)
        .limit(limit)
    )
    return [BatchJobRun(**d) for d in docs]