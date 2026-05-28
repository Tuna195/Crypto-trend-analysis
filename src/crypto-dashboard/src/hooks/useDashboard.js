/**
 * useDashboard.js  (v2 — synced với backend v2)
 * ===============================================
 * Custom React hooks để fetch data từ FastAPI backend.
 * Mỗi hook quản lý: loading, error, data, auto-refresh.
 *
 * Endpoint map (backend v2):
 *   /api/summary              → useSummary()
 *   /api/trends/batch         → useBatchTrends()
 *   /api/trends/speed         → useSpeedTrends()
 *   /api/sentiment/:coin      → useCoinSentiment()
 *   /api/spikes/batch         → useBatchSpikes()
 *   /api/spikes/speed         → useSpeedSpikes()
 *   /api/alerts               → useAlerts()
 *   /api/quality/bad-records  → useBadRecords()
 *   /api/jobs/history         → useJobHistory()
 */

import { useState, useEffect, useCallback, useRef } from "react";

const BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ─── Generic fetcher ─────────────────────────────────────────────────────────
async function apiFetch(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

// ─── Base hook ────────────────────────────────────────────────────────────────
function useApiData(path, refreshMs = 0) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const timerRef = useRef(null);

  const doFetch = useCallback(async () => {
    if (!path) return;           // coin chưa chọn → không fetch
    try {
      setError(null);
      const json = await apiFetch(path);
      setData(json);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [path]);

  useEffect(() => {
    setLoading(true);
    setData(null);
    doFetch();
    if (refreshMs > 0) {
      timerRef.current = setInterval(doFetch, refreshMs);
    }
    return () => clearInterval(timerRef.current);
  }, [doFetch, refreshMs]);

  return { data, loading, error, refetch: doFetch };
}

// ─── Public hooks ─────────────────────────────────────────────────────────────

/**
 * 4 KPI summary cards — refresh 15s
 * Fields: top_trending_coin, top_trend_score, total_mentions_1h,
 *         avg_fear_greed, active_alerts, active_spikes, last_updated
 */
export function useSummary() {
  return useApiData("/api/summary", 15_000);
}

/**
 * Bảng xếp hạng từ batch_sentiment_metrics — refresh 60s
 * Fields: coin, avg_fear_greed, avg_bullish, avg_bearish,
 *         total_mentions, total_engagement, snapshot_count,
 *         avg_whale_fg, avg_retail_fg, latest_at
 */
export function useBatchTrends(hours = 24, limit = 20) {
  return useApiData(`/api/trends/batch?hours=${hours}&limit=${limit}`, 60_000);
}

/**
 * Real-time trends từ speed_trend_metrics — refresh 10s
 * Fields: symbol, mention_count, unique_authors, influencer_authors,
 *         trend_score, engagement_score, influence_score, is_spike,
 *         growth_rate, z_score, spike_reasons, window_start, window_end
 */
export function useSpeedTrends(hours = 1, limit = 20, onlySpikes = false) {
  const qs = `hours=${hours}&limit=${limit}&only_spikes=${onlySpikes}`;
  return useApiData(`/api/trends/speed?${qs}`, 10_000);
}

/**
 * Sentiment history của 1 coin — refresh 60s
 * source: 'batch' (production) | 'test' (demo mode)
 */
export function useCoinSentiment(coin, hours = 6, source = "batch") {
  const path = coin ? `/api/sentiment/${coin}?hours=${hours}&source=${source}` : null;
  return useApiData(path, 60_000);
}

/**
 * Batch trend spikes từ batch_trend_spikes — refresh 30s
 * Fields: keyword, mention_count, baseline_count, z_score,
 *         related_coins, window_start, window_end, detected_at
 */
export function useBatchSpikes(hours = 24, minZ = 2.0) {
  return useApiData(`/api/spikes/batch?hours=${hours}&min_z=${minZ}`, 30_000);
}

/**
 * Real-time spikes từ speed_trend_metrics (is_spike=True) — refresh 10s
 * Fields: giống SpeedTrendMetric + growth_rate, spike_reasons
 */
export function useSpeedSpikes(hours = 1) {
  return useApiData(`/api/spikes/speed?hours=${hours}`, 10_000);
}

/**
 * Alerts feed từ collection alerts — refresh 10s
 * Fields: alert_type, severity, message, status, payload, created_at
 */
export function useAlerts(status = "open", alertType = null) {
  const qs = status ? `status=${status}` : "";
  const typeQs = alertType ? `&alert_type=${alertType}` : "";
  return useApiData(`/api/alerts?${qs}${typeQs}&limit=20`, 10_000);
}

/**
 * Bad record stats từ speed_bad_records — refresh 60s
 * Dùng cho data quality monitoring panel
 */
export function useBadRecords(hours = 6) {
  return useApiData(`/api/quality/bad-records?hours=${hours}`, 60_000);
}

/**
 * Lịch sử batch job runs — refresh 120s
 * Dùng cho admin/audit panel
 */
export function useJobHistory(limit = 10) {
  return useApiData(`/api/jobs/history?limit=${limit}`, 120_000);
}