/**
 * src/crypto-dashboard/src/hooks/useDashboard.js
 * ================================================
 * Gọi FastAPI backend (src/crypto-dashboard/backend/main.py).
 * Vite proxy chuyển /api/* → http://localhost:8000 nên không bị CORS.
 */

import { useState, useEffect, useCallback, useRef } from "react";

async function apiFetch(path) {
  const res = await fetch(path);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

function useApiData(path, refreshMs = 0) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const timer = useRef(null);

  const doFetch = useCallback(async () => {
    if (!path) { setLoading(false); return; }
    try {
      setError(null);
      setData(await apiFetch(path));
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
    if (refreshMs > 0) timer.current = setInterval(doFetch, refreshMs);
    return () => clearInterval(timer.current);
  }, [doFetch, refreshMs]);

  return { data, loading, error, refetch: doFetch };
}

// ── KPI cards — speed + batch ─────────────────────────────────────────────
export const useSummary = () => useApiData("/api/summary", 15_000);

// ── Bảng trending — batch_sentiment_metrics ──────────────────────────────
export const useBatchTrends = (hours = 24, limit = 20) =>
  useApiData(`/api/trends/batch?hours=${hours}&limit=${limit}`, 60_000);

// ── Ticker + spikes — speed_trend_metrics ────────────────────────────────
export const useSpeedTrends = (hours = 1, limit = 20, onlySpikes = false) =>
  useApiData(`/api/trends/speed?hours=${hours}&limit=${limit}&only_spikes=${onlySpikes}`, 10_000);

// ── Line chart — batch_sentiment_metrics ─────────────────────────────────
export const useCoinSentiment = (coin, hours = 6, source = "batch") =>
  useApiData(
    coin ? `/api/sentiment/${encodeURIComponent(coin)}?hours=${hours}&source=${source}` : null,
    60_000,
  );

// ── Batch spikes — batch_trend_spikes ────────────────────────────────────
export const useBatchSpikes = (hours = 24, minZ = 2.0) =>
  useApiData(`/api/spikes/batch?hours=${hours}&min_z=${minZ}`, 30_000);

// ── Live spikes — speed_trend_metrics (is_spike=True) ────────────────────
export const useSpeedSpikes = (hours = 1) =>
  useApiData(`/api/spikes/speed?hours=${hours}`, 10_000);

// ── Alerts — alerts collection ────────────────────────────────────────────
export const useAlerts = (status = "open", alertType = null) => {
  const qs = [status && `status=${status}`, alertType && `alert_type=${alertType}`]
    .filter(Boolean).join("&");
  return useApiData(`/api/alerts?${qs}&limit=20`, 10_000);
};

// ── Bad records — speed_bad_records ──────────────────────────────────────
export const useBadRecords = (hours = 6) =>
  useApiData(`/api/quality/bad-records?hours=${hours}`, 60_000);

// ── Job history — batch_job_runs ─────────────────────────────────────────
export const useJobHistory = (limit = 10) =>
  useApiData(`/api/jobs/history?limit=${limit}`, 120_000);
