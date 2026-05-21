"""
CryptoTrend Dashboard — FastAPI Backend
========================================
Cung cấp REST API cho React dashboard.
Kết nối trực tiếp vào MongoStorageClient có sẵn trong project.

Tech stack: FastAPI + PyMongo + Pydantic
Run: uvicorn backend.main:app --reload --port 8000
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient, DESCENDING

# ──────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB  = os.getenv("MONGO_DB",  "crypto_trends")

app = FastAPI(
    title="CryptoTrend Dashboard API",
    description="REST API phục vụ React dashboard phân tích xu hướng crypto từ Twitter.",
    version="1.0.0",
)

# Cho phép React dev server (localhost:5173) gọi API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────
# DATABASE CONNECTION (singleton)
# ──────────────────────────────────────────
def get_db():
    """Trả về MongoDB database instance."""
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]


# ──────────────────────────────────────────
# RESPONSE SCHEMAS (Pydantic)
# ──────────────────────────────────────────
class SentimentMetric(BaseModel):
    coin: str
    bullish_ratio: float
    bearish_ratio: float
    neutral_ratio: float
    fear_greed_score: float
    window_start: datetime
    window_end: datetime


class TrendItem(BaseModel):
    coin: str
    avg_fear_greed: float
    avg_bullish: float
    avg_bearish: float
    latest_at: datetime
    snapshot_count: int


class TrendSpike(BaseModel):
    keyword: str
    mention_count: int
    z_score: float
    related_coins: list[str]
    detected_at: datetime


class AlertItem(BaseModel):
    alert_type: str
    severity: Literal["low", "medium", "high", "critical"]
    message: str
    status: str
    created_at: datetime


class DashboardSummary(BaseModel):
    top_trending_coin: str
    total_mentions_1h: int
    avg_fear_greed: float
    active_whale_alerts: int
    last_updated: datetime


# ──────────────────────────────────────────
# ENDPOINTS
# ──────────────────────────────────────────

@app.get("/health")
def health_check():
    """Kiểm tra kết nối MongoDB."""
    try:
        db = get_db()
        db.client.admin.command("ping")
        return {"status": "ok", "mongo": "connected"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB unreachable: {exc}")


@app.get("/api/summary", response_model=DashboardSummary)
def get_dashboard_summary():
    """
    KPI tổng hợp cho 4 metric cards đầu trang:
    - Top trending coin
    - Tổng tweet mentions trong 1 giờ qua
    - Fear & Greed trung bình
    - Số whale alerts đang mở
    """
    db = get_db()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

    # Tính top coin dựa trên fear_greed_score cao nhất trong 1h
    pipeline = [
        {"$match": {"window_start": {"$gte": cutoff}}},
        {"$group": {
            "_id": "$coin",
            "avg_fg": {"$avg": "$fear_greed_score"},
        }},
        {"$sort": {"avg_fg": -1}},
        {"$limit": 1},
    ]
    top = list(db.sentiment_metrics.aggregate(pipeline))
    top_coin = top[0]["_id"] if top else "N/A"

    # Tổng mentions estimate từ trend_spikes
    total_mentions = db.trend_spikes.aggregate([
        {"$match": {"detected_at": {"$gte": cutoff}}},
        {"$group": {"_id": None, "total": {"$sum": "$mention_count"}}},
    ])
    total = list(total_mentions)
    total_mentions_val = total[0]["total"] if total else 0

    # Fear & Greed trung bình toàn thị trường
    fg_agg = list(db.sentiment_metrics.aggregate([
        {"$match": {"window_start": {"$gte": cutoff}}},
        {"$group": {"_id": None, "avg": {"$avg": "$fear_greed_score"}}},
    ]))
    avg_fg = round(fg_agg[0]["avg"], 1) if fg_agg else 50.0

    # Số whale alerts đang mở
    whale_count = db.alerts.count_documents({"status": "open"})

    return DashboardSummary(
        top_trending_coin=top_coin,
        total_mentions_1h=total_mentions_val,
        avg_fear_greed=avg_fg,
        active_whale_alerts=whale_count,
        last_updated=datetime.now(timezone.utc),
    )


@app.get("/api/trends", response_model=list[TrendItem])
def get_trends(
    hours: int = Query(default=24, ge=1, le=168, description="Khoảng thời gian nhìn lại (giờ)"),
    limit: int = Query(default=20, ge=1, le=100, description="Số coin tối đa trả về"),
):
    """
    Bảng xếp hạng Top Trending Coins.
    Aggregate từ collection `sentiment_metrics`, nhóm theo coin.
    Dùng cho: Trending Table + Top Gainers widget.
    """
    db = get_db()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    pipeline = [
        {"$match": {"window_start": {"$gte": cutoff}}},
        {"$group": {
            "_id": "$coin",
            "avg_fear_greed": {"$avg": "$fear_greed_score"},
            "avg_bullish":    {"$avg": "$bullish_ratio"},
            "avg_bearish":    {"$avg": "$bearish_ratio"},
            "latest_at":      {"$max": "$window_end"},
            "snapshot_count": {"$sum": 1},
        }},
        {"$sort": {"avg_fear_greed": -1}},
        {"$limit": limit},
    ]

    results = list(db.sentiment_metrics.aggregate(pipeline))

    return [
        TrendItem(
            coin=r["_id"],
            avg_fear_greed=round(r["avg_fear_greed"], 1),
            avg_bullish=round(r["avg_bullish"], 3),
            avg_bearish=round(r["avg_bearish"], 3),
            latest_at=r["latest_at"],
            snapshot_count=r["snapshot_count"],
        )
        for r in results
    ]


@app.get("/api/sentiment/{coin}", response_model=list[SentimentMetric])
def get_coin_sentiment(
    coin: str,
    hours: int = Query(default=6, ge=1, le=72),
):
    """
    Lịch sử sentiment theo thời gian của 1 coin cụ thể.
    Dùng cho: Line chart trên trang detail của coin.
    """
    db = get_db()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    coin_upper = coin.upper().replace("$", "")

    docs = list(
        db.sentiment_metrics.find(
            {"coin": coin_upper, "window_start": {"$gte": cutoff}},
            {"_id": 0},
        ).sort("window_start", DESCENDING).limit(200)
    )

    if not docs:
        raise HTTPException(status_code=404, detail=f"Không có data cho coin: {coin_upper}")

    return [SentimentMetric(**d) for d in docs]


@app.get("/api/spikes", response_model=list[TrendSpike])
def get_trend_spikes(
    hours: int = Query(default=24, ge=1, le=168),
    min_z: float = Query(default=2.0, description="Ngưỡng z-score tối thiểu"),
    limit: int = Query(default=15, ge=1, le=50),
):
    """
    Danh sách trend spikes (từ speed layer) được lọc theo z-score.
    Dùng cho: Spike alerts panel, Gem detection widget.
    """
    db = get_db()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    docs = list(
        db.trend_spikes.find(
            {"detected_at": {"$gte": cutoff}, "z_score": {"$gte": min_z}},
            {"_id": 0},
        ).sort("z_score", DESCENDING).limit(limit)
    )

    return [
        TrendSpike(
            keyword=d["keyword"],
            mention_count=d["mention_count"],
            z_score=round(d["z_score"], 2),
            related_coins=d.get("related_coins", []),
            detected_at=d["detected_at"],
        )
        for d in docs
    ]


@app.get("/api/alerts", response_model=list[AlertItem])
def get_alerts(
    status: Optional[str] = Query(default=None, description="'open' hoặc 'closed'"),
    limit: int = Query(default=20, ge=1, le=100),
):
    """
    Danh sách alerts (whale signals, correlation alerts...).
    Dùng cho: Whale Alerts Feed ở dashboard.
    """
    db = get_db()
    query = {}
    if status:
        query["status"] = status

    docs = list(
        db.alerts.find(query, {"_id": 0})
        .sort("created_at", DESCENDING)
        .limit(limit)
    )

    return [
        AlertItem(
            alert_type=d.get("alert_type", "unknown"),
            severity=d.get("severity", "low"),
            message=d.get("message", ""),
            status=d.get("status", "open"),
            created_at=d["created_at"],
        )
        for d in docs
    ]
