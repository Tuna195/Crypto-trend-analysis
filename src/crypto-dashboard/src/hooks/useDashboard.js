/**
 * useDashboard.js
 * ================
 * Custom React hooks để fetch data từ FastAPI backend.
 * Mỗi hook quản lý: loading state, error state, data, và auto-refresh.
 *
 * Pattern: { data, loading, error, refetch }
 */

import { useState, useEffect, useCallback, useRef } from "react";

const BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

// ─── Generic fetcher ────────────────────────────────────────────────────────
async function apiFetch(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  return res.json();
}

// ─── Base hook ──────────────────────────────────────────────────────────────
function useApiData(path, refreshMs = 0) {
  const [data, setData]       = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]     = useState(null);
  const timerRef              = useRef(null);

  const fetch_ = useCallback(async () => {
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
    fetch_();
    if (refreshMs > 0) {
      timerRef.current = setInterval(fetch_, refreshMs);
    }
    return () => clearInterval(timerRef.current);
  }, [fetch_, refreshMs]);

  return { data, loading, error, refetch: fetch_ };
}

// ─── Public hooks ────────────────────────────────────────────────────────────

/** 4 KPI cards — tự refresh mỗi 15 giây */
export function useSummary() {
  return useApiData("/api/summary", 15_000);
}

/** Bảng trending coins — refresh mỗi 30 giây */
export function useTrends(hours = 24, limit = 20) {
  return useApiData(`/api/trends?hours=${hours}&limit=${limit}`, 30_000);
}

/** Sentiment history của 1 coin cụ thể */
export function useCoinSentiment(coin, hours = 6) {
  return useApiData(coin ? `/api/sentiment/${coin}?hours=${hours}` : null, 60_000);
}

/** Trend spikes — refresh mỗi 20 giây */
export function useSpikes(hours = 24, minZ = 2.0) {
  return useApiData(`/api/spikes?hours=${hours}&min_z=${minZ}`, 20_000);
}

/** Whale alerts feed — refresh mỗi 10 giây */
export function useAlerts(status = "open") {
  return useApiData(`/api/alerts?status=${status}&limit=20`, 10_000);
}
