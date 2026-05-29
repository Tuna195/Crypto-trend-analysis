"""
CryptoTrend Dashboard — FastAPI Backend (v2 — Fixed)
======================================================
Đã sửa toàn bộ lỗi collection name, field mapping, và connection leak
so với v1. Tham chiếu trực tiếp từ:

  - src/storage/mongo_client.py          → collection names + field schema
  - src/processing/batch_layer/batch_job.py → batch collections + fields
  - src/processing/speed_layer/stream_runtime.py + spark_pipeline.py
                                          → speed_trend_metrics fields
  - src/processing/speed_layer/spike_detection.py → spike fields

Collections thực tế trong DB:
  batch_sentiment_metrics   ← MongoStorageClient.save_sentiment_metric()
  batch_trend_spikes        ← MongoStorageClient.save_trend_spike()
  alerts                    ← MongoStorageClient.save_alert()
  speed_trend_metrics       ← write_batch_to_mongo() (speed layer)
  speed_bad_records         ← bad record metrics (speed layer)
  test_batch_process        ← batch_job.py demo mode
  batch_job_runs            ← log_batch_run() audit trail
  tweets                    ← save_raw_tweets()

Run:
  uvicorn src.dashboard.backend.main:app --reload --port 8000
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pymongo import MongoClient, DESCENDING, ASCENDING

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "crypto_trends")

# ─────────────────────────────────────────────────────────────────────────────
# SINGLETON CONNECTION  (fix connection leak của v1)
# Dùng FastAPI lifespan để mở/đóng MongoClient đúng lúc.
# ─────────────────────────────────────────────────────────────────────────────
_mongo_client: MongoClient | None = None

def get_db():
    """Trả về database instance từ singleton client."""
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _mongo_client[MONGO_DB]

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup — khởi tạo connection
    global _mongo_client
    _mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    yield
    # Shutdown — đóng sạch
    if _mongo_client:
        _mongo_client.close()

# ─────────────────────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="CryptoTrend Dashboard API",
    description="REST API phục vụ React dashboard. Đọc data từ MongoDB được ghi bởi batch + speed layer.",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ─────────────────────────────────────────────────────────────────────────────
# PYDANTIC SCHEMAS
# Mapping 1-1 với document structure thực tế trong từng collection
# ─────────────────────────────────────────────────────────────────────────────

class BatchSentimentMetric(BaseModel):
    """
    Ánh xạ collection: batch_sentiment_metrics
    Ghi bởi: MongoStorageClient.save_sentiment_metric()
             batch_job.py (cả spark mode lẫn demo mode)
    """
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
    # Whale/Retail segmentation (Optional — chỉ có khi batch_job ghi)
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
    """
    Ánh xạ collection: speed_trend_metrics
    Ghi bởi: write_batch_to_mongo() trong stream_runtime.py
    Fields đến từ: spark_pipeline.py → build_stream_outputs() + spike_detection.py
    """
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
    # Spike fields — enrich_trend_document_with_spike()
    baseline_mention_count: Optional[float] = None
    baseline_stddev:        Optional[float] = None
    growth_rate:            Optional[float] = None
    z_score:                Optional[float] = None
    is_spike:               Optional[bool]  = None
    spike_reasons:          Optional[list[str]] = None
    is_suppressed:          Optional[bool]  = None
    updated_at:             Optional[datetime] = None


class BatchTrendSpike(BaseModel):
    """
    Ánh xạ collection: batch_trend_spikes
    Ghi bởi: MongoStorageClient.save_trend_spike()
    """
    keyword:        str
    mention_count:  int
    baseline_count: float
    z_score:        float
    related_coins:  list[str]
    window_start:   Optional[datetime] = None
    window_end:     Optional[datetime] = None
    detected_at:    datetime


class AlertItem(BaseModel):
    """
    Ánh xạ collection: alerts
    Ghi bởi: MongoStorageClient.save_alert()
    severity trong project dùng: "info", "low", "medium", "high", "critical"
    """
    alert_type: str
    severity:   str   # không dùng Literal cứng vì batch_job ghi "info"
    message:    str
    status:     str
    payload:    Optional[dict[str, Any]] = None
    created_at: datetime


class DashboardSummary(BaseModel):
    """KPI tổng hợp cho 4 metric cards đầu trang."""
    top_trending_coin:    str
    top_trend_score:      float
    total_mentions_1h:    int
    avg_fear_greed:       float
    active_alerts:        int
    active_spikes:        int   # is_spike=True trong speed_trend_metrics
    last_updated:         datetime


class TrendRankItem(BaseModel):
    """1 dòng trong bảng trending — aggregate từ batch_sentiment_metrics."""
    coin:            str
    avg_fear_greed:  float
    avg_bullish:     float
    avg_bearish:     float
    total_mentions:  int
    total_engagement: int
    snapshot_count:  int
    latest_at:       Optional[datetime] = None
    # Whale segment summary
    avg_whale_fg:    Optional[float] = None
    avg_retail_fg:   Optional[float] = None


class BadRecordStat(BaseModel):
    """Ánh xạ collection: speed_bad_records — chất lượng data stream."""
    window_start:     datetime
    window_end:       datetime
    invalid_reason:   str
    bad_record_count: int


class BatchJobRun(BaseModel):
    """Ánh xạ collection: batch_job_runs — audit log mỗi lần chạy batch."""
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


# ─────────────────────────────────────────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────────────────────────────────────────
def _cutoff(hours: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)

def _clean(doc: dict) -> dict:
    """Xóa _id trước khi parse vào Pydantic."""
    doc.pop("_id", None)
    return doc


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health_check():
    """Kiểm tra kết nối MongoDB + liệt kê các collection có dữ liệu."""
    try:
        db = get_db()
        db.client.admin.command("ping")
        collections = {
            col: db[col].estimated_document_count()
            for col in [
                "batch_sentiment_metrics", "batch_trend_spikes",
                "speed_trend_metrics", "speed_bad_records",
                "alerts", "test_batch_process", "batch_job_runs", "tweets",
            ]
        }
        return {"status": "ok", "mongo": "connected", "collections": collections}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB unreachable: {exc}")


# ── Dashboard Summary ─────────────────────────────────────────────────────────
@app.get("/api/summary", response_model=DashboardSummary, tags=["Dashboard"])
def get_dashboard_summary():
    """
    4 KPI cards đầu trang dashboard.

    Nguồn data:
      - top_trending_coin   → speed_trend_metrics (trend_score cao nhất 1h)
      - total_mentions_1h   → speed_trend_metrics (sum mention_count 1h)
      - avg_fear_greed      → batch_sentiment_metrics (avg fear_greed_score 1h)
      - active_alerts       → alerts (status=open)
      - active_spikes       → speed_trend_metrics (is_spike=True 1h)
    """
    db  = get_db()
    cut = _cutoff(1)

    # Top coin theo trend_score từ speed_trend_metrics (real-time hơn)
    top_pipeline = [
        {"$match": {"window_start": {"$gte": cut}}},
        {"$group": {
            "_id":        "$symbol",
            "max_trend":  {"$max": "$trend_score"},
            "sum_mention": {"$sum": "$mention_count"},
        }},
        {"$sort": {"max_trend": -1}},
        {"$limit": 1},
    ]
    top = list(db.speed_trend_metrics.aggregate(top_pipeline))
    top_coin  = top[0]["_id"]        if top else "N/A"
    top_score = top[0]["max_trend"]  if top else 0.0

    # Tổng mentions 1h từ speed_trend_metrics
    mentions_agg = list(db.speed_trend_metrics.aggregate([
        {"$match": {"window_start": {"$gte": cut}}},
        {"$group": {"_id": None, "total": {"$sum": "$mention_count"}}},
    ]))
    total_mentions = mentions_agg[0]["total"] if mentions_agg else 0

    # Fear & Greed trung bình từ batch_sentiment_metrics (chuẩn hơn)
    fg_agg = list(db.batch_sentiment_metrics.aggregate([
        {"$match": {"window_start": {"$gte": _cutoff(24)}}},
        {"$group": {"_id": None, "avg": {"$avg": "$fear_greed_score"}}},
    ]))
    avg_fg = round(fg_agg[0]["avg"], 1) if fg_agg else 50.0

    # Alerts đang mở
    active_alerts = db.alerts.count_documents({"status": "open"})

    # Spikes đang active
    active_spikes = db.speed_trend_metrics.count_documents({
        "window_start": {"$gte": cut},
        "is_spike": True,
    })

    return DashboardSummary(
        top_trending_coin=top_coin,
        top_trend_score=round(top_score, 2),
        total_mentions_1h=total_mentions,
        avg_fear_greed=avg_fg,
        active_alerts=active_alerts,
        active_spikes=active_spikes,
        last_updated=datetime.now(timezone.utc),
    )


# ── Trending Coins (Batch Layer) ──────────────────────────────────────────────
@app.get("/api/trends/batch", response_model=list[TrendRankItem], tags=["Trends"])
def get_batch_trends(
    hours: int = Query(default=24, ge=1, le=168, description="Lookback window (giờ)"),
    limit: int = Query(default=20, ge=1, le=100),
):
    """
    Bảng xếp hạng từ batch_sentiment_metrics.
    Bao gồm: fear & greed, bullish/bearish ratio, whale vs retail segment.
    Dùng cho: Trending Table chính trên dashboard.
    """
    db  = get_db()
    cut = _cutoff(hours)

    pipeline = [
        {"$match": {"window_start": {"$gte": cut}}},
        {"$group": {
            "_id":             "$coin",
            "avg_fear_greed":  {"$avg": "$fear_greed_score"},
            "avg_bullish":     {"$avg": "$bullish_ratio"},
            "avg_bearish":     {"$avg": "$bearish_ratio"},
            "total_mentions":  {"$sum": "$mention_count"},
            "total_engagement":{"$sum": "$total_engagement"},
            "snapshot_count":  {"$sum": 1},
            "latest_at":       {"$max": "$window_end"},
            # Whale / Retail segment
            "avg_whale_fg":    {"$avg": "$whale_fear_greed"},
            "avg_retail_fg":   {"$avg": "$retail_fear_greed"},
        }},
        {"$sort": {"avg_fear_greed": -1}},
        {"$limit": limit},
    ]

    rows = list(db.batch_sentiment_metrics.aggregate(pipeline))
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


# ── Trending Coins (Speed Layer — real-time) ──────────────────────────────────
@app.get("/api/trends/speed", response_model=list[SpeedTrendMetric], tags=["Trends"])
def get_speed_trends(
    hours: int  = Query(default=1, ge=1, le=24),
    limit: int  = Query(default=20, ge=1, le=100),
    only_spikes: bool = Query(default=False, description="Chỉ lấy những coin đang spike"),
):
    """
    Top trending từ speed_trend_metrics (Spark Streaming output).
    Real-time hơn batch — cập nhật mỗi 5 phút.
    Dùng cho: ticker strip, live trending widget.
    """
    db  = get_db()
    cut = _cutoff(hours)

    match: dict = {"window_start": {"$gte": cut}}
    if only_spikes:
        match["is_spike"] = True

    # Lấy window mới nhất của mỗi symbol
    pipeline = [
        {"$match": match},
        {"$sort": {"window_start": DESCENDING}},
        {"$group": {
            "_id": "$symbol",
            "doc": {"$first": "$$ROOT"},
        }},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$sort": {"trend_score": DESCENDING}},
        {"$limit": limit},
        {"$project": {"_id": 0}},
    ]

    rows = list(db.speed_trend_metrics.aggregate(pipeline))
    return [SpeedTrendMetric(**r) for r in rows]


# ── Sentiment History (1 coin) ────────────────────────────────────────────────
@app.get("/api/sentiment/{coin}", response_model=list[BatchSentimentMetric], tags=["Sentiment"])
def get_coin_sentiment(
    coin: str,
    hours: int = Query(default=6, ge=1, le=72),
    source: str = Query(default="batch", description="'batch' hoặc 'test'"),
):
    """
    Lịch sử sentiment của 1 coin cụ thể theo thời gian.

    source='batch' → batch_sentiment_metrics (production)
    source='test'  → test_batch_process (demo mode output)

    Dùng cho: Line chart detail khi click vào coin trong bảng.
    """
    db         = get_db()
    cut        = _cutoff(hours)
    coin_upper = coin.upper().replace("$", "")
    collection = "batch_sentiment_metrics" if source == "batch" else "test_batch_process"

    docs = list(
        db[collection].find(
            {"coin": coin_upper, "window_start": {"$gte": cut}},
            {"_id": 0},
        )
        .sort("window_start", ASCENDING)   # ascending → chart đọc trái→phải
        .limit(200)
    )

    if not docs:
        raise HTTPException(
            status_code=404,
            detail=f"Không có data cho {coin_upper} trong {hours}h qua (source={collection}).",
        )

    # Đảm bảo các field bắt buộc có giá trị mặc định nếu thiếu
    result = []
    for d in docs:
        d.setdefault("total_engagement", 0)
        d.setdefault("created_at", d.get("window_start"))
        result.append(BatchSentimentMetric(**d))
    return result


# ── Batch Trend Spikes ────────────────────────────────────────────────────────
@app.get("/api/spikes/batch", response_model=list[BatchTrendSpike], tags=["Spikes"])
def get_batch_spikes(
    hours: int  = Query(default=24, ge=1, le=168),
    min_z: float = Query(default=2.0, description="Ngưỡng z-score tối thiểu"),
    limit: int  = Query(default=15, ge=1, le=50),
):
    """
    Trend spikes từ batch_trend_spikes.
    Ghi bởi: MongoStorageClient.save_trend_spike()
    Dùng cho: Spike alert panel trên dashboard.
    """
    db  = get_db()
    cut = _cutoff(hours)

    docs = list(
        db.batch_trend_spikes.find(
            {"detected_at": {"$gte": cut}, "z_score": {"$gte": min_z}},
            {"_id": 0},
        )
        .sort("z_score", DESCENDING)
        .limit(limit)
    )

    return [BatchTrendSpike(**d) for d in docs]


# ── Speed Layer Spikes (real-time) ────────────────────────────────────────────
@app.get("/api/spikes/speed", response_model=list[SpeedTrendMetric], tags=["Spikes"])
def get_speed_spikes(
    hours: int  = Query(default=1, ge=1, le=24),
    limit: int  = Query(default=15, ge=1, le=50),
):
    """
    Spikes real-time từ speed_trend_metrics (is_spike=True).
    Được enrich bởi spike_detection.py: growth_rate, z_score, spike_reasons.
    Dùng cho: live spike strip ở đầu dashboard.
    """
    db  = get_db()
    cut = _cutoff(hours)

    pipeline = [
        {"$match": {"window_start": {"$gte": cut}, "is_spike": True}},
        {"$sort": {"growth_rate": DESCENDING}},
        {"$limit": limit},
        {"$project": {"_id": 0}},
    ]

    return [SpeedTrendMetric(**r) for r in db.speed_trend_metrics.aggregate(pipeline)]


# ── Alerts Feed ───────────────────────────────────────────────────────────────
@app.get("/api/alerts", response_model=list[AlertItem], tags=["Alerts"])
def get_alerts(
    status:     Optional[str] = Query(default=None, description="'open' hoặc 'closed'"),
    alert_type: Optional[str] = Query(default=None, description="Filter theo loại: 'spam_detected', 'whale_signal'..."),
    limit:      int           = Query(default=20, ge=1, le=100),
):
    """
    Alerts feed từ collection alerts.
    Ghi bởi: MongoStorageClient.save_alert() — batch layer + ingestion.
    Dùng cho: Whale/Spam alerts feed.
    """
    db    = get_db()
    query: dict = {}
    if status:
        query["status"] = status
    if alert_type:
        query["alert_type"] = alert_type

    docs = list(
        db.alerts.find(query, {"_id": 0})
        .sort("created_at", DESCENDING)
        .limit(limit)
    )

    return [AlertItem(**_clean(d)) for d in docs]


# ── Bad Record Stats ──────────────────────────────────────────────────────────
@app.get("/api/quality/bad-records", response_model=list[BadRecordStat], tags=["Data Quality"])
def get_bad_records(
    hours: int = Query(default=6, ge=1, le=48),
):
    """
    Thống kê bad records từ speed_bad_records.
    Ghi bởi: Spark Streaming → bad_record_metrics trong spark_pipeline.py.
    Dùng cho: Data quality monitoring panel.
    """
    db  = get_db()
    cut = _cutoff(hours)

    docs = list(
        db.speed_bad_records.find(
            {"window_start": {"$gte": cut}},
            {"_id": 0},
        )
        .sort("window_start", DESCENDING)
        .limit(200)
    )

    return [BadRecordStat(**d) for d in docs]


# ── Batch Job Audit Log ───────────────────────────────────────────────────────
@app.get("/api/jobs/history", response_model=list[BatchJobRun], tags=["System"])
def get_job_history(limit: int = Query(default=10, ge=1, le=50)):
    """
    Lịch sử các lần chạy batch job từ batch_job_runs.
    Ghi bởi: log_batch_run() trong batch_job.py.
    Dùng cho: Admin / monitoring panel.
    """
    db   = get_db()
    docs = list(
        db.batch_job_runs.find({}, {"_id": 0})
        .sort("executed_at", DESCENDING)
        .limit(limit)
    )
    return [BatchJobRun(**d) for d in docs]