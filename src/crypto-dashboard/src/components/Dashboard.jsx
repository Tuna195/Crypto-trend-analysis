/**
 * Dashboard.jsx  (v2 — synced với backend v2 + hooks v2)
 * ========================================================
 * Tất cả field names khớp với MongoDB schema thực tế của project:
 *
 *   KpiCards        → useSummary()         → /api/summary
 *   SpeedTickerBar  → useSpeedTrends()     → /api/trends/speed
 *   TrendingTable   → useBatchTrends()     → /api/trends/batch
 *   SentimentChart  → useCoinSentiment()   → /api/sentiment/:coin
 *   SpikesPanel     → useSpeedSpikes()     → /api/spikes/speed
 *   AlertsFeed      → useAlerts()          → /api/alerts
 */

import { useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine,
} from "recharts";
import {
  useSummary, useBatchTrends, useSpeedTrends,
  useCoinSentiment, useSpeedSpikes, useAlerts,
} from "../hooks/useDashboard";
import {
  SkeletonKpiCard, SkeletonTable, SkeletonChart, SkeletonAlertItem,
} from "./Skeleton";

// ─── Design Tokens ────────────────────────────────────────────────────────────
const C = {
  bgVoid: "#0a0d14",
  bgSurface: "#111620",
  bgElevated: "#1a2030",
  border: "#1e2a40",
  borderHov: "#2a3a58",
  textPri: "#e2e8f0",
  textMuted: "#7a90b0",
  textDim: "#3d5275",
  neonTeal: "#00f5a0",
  limeGreen: "#22c97a",
  electricBl: "#5b8cff",
  neonPurple: "#a78bfa",
  neonRed: "#f05252",
  amber: "#f59e0b",
};

const COIN_COLORS = ["#00f5a0", "#5b8cff", "#a78bfa", "#f59e0b", "#f05252", "#22c97a"];

// ─── Helpers ──────────────────────────────────────────────────────────────────
function fgColor(score) {
  if (!score && score !== 0) return C.textMuted;
  if (score >= 70) return C.neonTeal;
  if (score >= 55) return C.limeGreen;
  if (score >= 45) return C.amber;
  return C.neonRed;
}
function fgLabel(score) {
  if (!score && score !== 0) return "—";
  if (score >= 70) return "Greed";
  if (score >= 55) return "Neutral+";
  if (score >= 45) return "Neutral";
  return "Fear";
}
function severityColor(s) {
  const m = { critical: C.neonRed, high: C.amber, medium: C.electricBl, info: C.neonTeal, low: C.textMuted };
  return m[s] ?? C.textMuted;
}
function timeAgo(iso) {
  if (!iso) return "—";
  const diff = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}
function fmt(n, dec = 1) {
  if (n == null) return "—";
  return Number(n).toFixed(dec);
}
function fmtInt(n) {
  if (n == null) return "—";
  return Number(n).toLocaleString("vi-VN");
}

// ─── Primitives ───────────────────────────────────────────────────────────────
function NeonBadge({ children, color = C.neonTeal, small = false }) {
  return (
    <span style={{
      display: "inline-block",
      padding: small ? "1px 7px" : "2px 10px",
      borderRadius: 20,
      fontSize: small ? 10 : 11,
      fontWeight: 600,
      letterSpacing: "0.04em",
      color,
      background: color + "18",
      border: `0.5px solid ${color}55`,
      whiteSpace: "nowrap",
    }}>
      {children}
    </span>
  );
}

function SectionTitle({ children }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
      <div style={{ width: 3, height: 18, borderRadius: 2, background: C.electricBl, flexShrink: 0 }} />
      <span style={{
        fontSize: 12, fontWeight: 600, color: C.textPri,
        letterSpacing: "0.08em", textTransform: "uppercase",
      }}>
        {children}
      </span>
    </div>
  );
}

function ErrorBox({ message }) {
  return (
    <div style={{
      padding: "14px 18px", borderRadius: 10,
      background: C.neonRed + "10", border: `0.5px solid ${C.neonRed}44`,
      color: C.neonRed, fontSize: 12,
    }}>
      ⚠ {message}
    </div>
  );
}

function Card({ children, style = {} }) {
  return (
    <div style={{
      background: C.bgSurface,
      border: `0.5px solid ${C.border}`,
      borderRadius: 12,
      ...style,
    }}>
      {children}
    </div>
  );
}

// ─── Speed Ticker Bar ─────────────────────────────────────────────────────────
// Dùng speed_trend_metrics — field: symbol, trend_score, mention_count, is_spike
function SpeedTickerBar() {
  const { data } = useSpeedTrends(1, 10, false);
  if (!data?.length) return null;

  return (
    <div style={{
      background: C.bgElevated,
      borderBottom: `0.5px solid ${C.border}`,
      padding: "0 32px",
      height: 34,
      display: "flex",
      alignItems: "center",
      gap: 28,
      overflowX: "auto",
    }}>
      {data.map(item => (
        <div key={item.symbol} style={{ display: "flex", alignItems: "center", gap: 7, flexShrink: 0 }}>
          <span style={{ fontSize: 11, color: C.textMuted, fontWeight: 500 }}>${item.symbol}</span>
          <span style={{ fontSize: 11, fontFamily: "monospace", color: C.textPri }}>
            {fmtInt(item.mention_count)} tweets
          </span>
          {item.is_spike && <NeonBadge color={C.amber} small>⚡ SPIKE</NeonBadge>}
          <span style={{ fontSize: 10, color: C.textDim }}>
            score {fmt(item.trend_score)}
          </span>
        </div>
      ))}
    </div>
  );
}

// ─── KPI Cards ────────────────────────────────────────────────────────────────
// Dùng useSummary() → /api/summary
// Fields: top_trending_coin, top_trend_score, total_mentions_1h,
//         avg_fear_greed, active_alerts, active_spikes
function KpiCards() {
  const { data, loading, error } = useSummary();

  const cards = data ? [
    {
      label: "Top Trending",
      value: `$${data.top_trending_coin}`,
      sub: `trend score ${fmt(data.top_trend_score)}`,
      color: C.neonTeal,
    },
    {
      label: "Mentions / 1h",
      value: fmtInt(data.total_mentions_1h),
      sub: "từ speed_trend_metrics",
      color: C.electricBl,
    },
    {
      label: "Fear & Greed",
      value: fmt(data.avg_fear_greed),
      sub: fgLabel(data.avg_fear_greed),
      color: fgColor(data.avg_fear_greed),
    },
    {
      label: "Active Spikes",
      value: data.active_spikes,
      sub: `${data.active_alerts} alerts mở`,
      color: C.amber,
    },
  ] : [];

  if (error) return <ErrorBox message={error} />;

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14 }}>
      {loading
        ? Array(4).fill(0).map((_, i) => <SkeletonKpiCard key={i} />)
        : cards.map(({ label, value, sub, color }) => (
          <div key={label} style={{
            background: C.bgSurface,
            border: `0.5px solid ${C.border}`,
            borderRadius: 12,
            padding: "20px 22px",
            position: "relative",
            overflow: "hidden",
            transition: "border-color 0.2s",
            cursor: "default",
          }}
            onMouseEnter={e => e.currentTarget.style.borderColor = color + "55"}
            onMouseLeave={e => e.currentTarget.style.borderColor = C.border}
          >
            <div style={{
              position: "absolute", top: 0, right: 0,
              width: 56, height: 56, borderRadius: "0 12px 0 56px",
              background: color + "10",
            }} />
            <div style={{ fontSize: 10, color: C.textMuted, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 8 }}>
              {label}
            </div>
            <div style={{ fontSize: 28, fontWeight: 700, color, fontFamily: "monospace", marginBottom: 5 }}>
              {value}
            </div>
            <div style={{ fontSize: 11, color: C.textDim }}>{sub}</div>
          </div>
        ))
      }
    </div>
  );
}

// ─── Sentiment Chart ──────────────────────────────────────────────────────────
// Dùng useCoinSentiment() → /api/sentiment/:coin
// Fields: fear_greed_score, bullish_ratio, bearish_ratio, window_start
// Optional: whale_fear_greed, retail_fear_greed
function SentimentChart({ coin }) {
  const [hours, setHours] = useState(6);
  const { data, loading, error } = useCoinSentiment(coin, hours);

  const chartData = data
    ? data.map(d => ({
      time: new Date(d.window_start).toLocaleTimeString("vi-VN", {
        hour: "2-digit", minute: "2-digit",
      }),
      "F&G Overall": Math.round(d.fear_greed_score),
      "Bullish %": Math.round((d.bullish_ratio ?? 0) * 100),
      "Bearish %": Math.round((d.bearish_ratio ?? 0) * 100),
      // whale/retail — chỉ có khi batch_job ghi segment data
      ...(d.whale_fear_greed != null && { "F&G Whale": Math.round(d.whale_fear_greed) }),
      ...(d.retail_fear_greed != null && { "F&G Retail": Math.round(d.retail_fear_greed) }),
    }))
    : [];

  const hasSegment = chartData.some(d => d["F&G Whale"] != null);

  return (
    <Card style={{ flex: 1, padding: "20px 22px" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <SectionTitle>
          Sentiment — {coin ? `$${coin}` : "chọn coin ↓"}
        </SectionTitle>
        {coin && (
          <div style={{ display: "flex", gap: 5 }}>
            {[1, 3, 6, 24].map(h => (
              <button key={h} onClick={() => setHours(h)} style={{
                padding: "3px 10px", borderRadius: 20, fontSize: 10, cursor: "pointer",
                background: hours === h ? C.electricBl + "20" : "transparent",
                border: `0.5px solid ${hours === h ? C.electricBl : C.border}`,
                color: hours === h ? C.electricBl : C.textMuted,
                transition: "all 0.15s",
              }}>
                {h}h
              </button>
            ))}
          </div>
        )}
      </div>

      {!coin && <div style={{ color: C.textMuted, fontSize: 12, textAlign: "center", padding: "40px 0" }}>👆 Nhấp vào một coin trong bảng để xem biểu đồ</div>}
      {coin && loading && <SkeletonChart height={190} />}
      {coin && error && <ErrorBox message={error} />}

      {coin && !loading && !error && chartData.length > 0 && (
        <ResponsiveContainer width="100%" height={190}>
          <LineChart data={chartData} margin={{ top: 4, right: 6, left: -22, bottom: 0 }}>
            <CartesianGrid strokeDasharray="2 3" stroke={C.border} />
            <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 9 }} tickLine={false} />
            <YAxis domain={[0, 100]} tick={{ fill: C.textMuted, fontSize: 9 }} tickLine={false} axisLine={false} />
            <ReferenceLine y={50} stroke={C.textDim} strokeDasharray="4 3" />
            <Tooltip
              contentStyle={{ background: C.bgElevated, border: `0.5px solid ${C.border}`, borderRadius: 8, fontSize: 11 }}
              labelStyle={{ color: C.textPri }}
            />
            <Legend wrapperStyle={{ fontSize: 10, color: C.textMuted }} />
            <Line type="monotone" dataKey="F&G Overall" stroke={C.neonTeal} strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="Bullish %" stroke={C.limeGreen} strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
            <Line type="monotone" dataKey="Bearish %" stroke={C.neonRed} strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
            {hasSegment && <>
              <Line type="monotone" dataKey="F&G Whale" stroke={C.amber} strokeWidth={1.5} dot={false} />
              <Line type="monotone" dataKey="F&G Retail" stroke={C.neonPurple} strokeWidth={1.5} dot={false} />
            </>}
          </LineChart>
        </ResponsiveContainer>
      )}

      {coin && !loading && chartData.length === 0 && (
        <div style={{ color: C.textMuted, fontSize: 12, textAlign: "center", padding: "40px 0" }}>
          Chưa có data sentiment cho ${coin} trong {hours}h.
        </div>
      )}
    </Card>
  );
}

// ─── Trending Table ───────────────────────────────────────────────────────────
// Dùng useBatchTrends() → /api/trends/batch
// Fields: coin, avg_fear_greed, avg_bullish, avg_bearish,
//         total_mentions, total_engagement, snapshot_count,
//         avg_whale_fg, avg_retail_fg, latest_at
function TrendingTable({ onCoinSelect, selectedCoin }) {
  const [hours, setHours] = useState(24);
  const { data, loading, error } = useBatchTrends(hours, 20);

  const th = {
    padding: "10px 14px",
    fontSize: 10, fontWeight: 600,
    color: C.textMuted,
    textAlign: "left",
    letterSpacing: "0.08em",
    textTransform: "uppercase",
    borderBottom: `0.5px solid ${C.border}`,
    whiteSpace: "nowrap",
  };

  return (
    <Card style={{ overflow: "hidden" }}>
      <div style={{ padding: "18px 22px 0", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <SectionTitle>Trending Coins — Batch Layer</SectionTitle>
        <div style={{ display: "flex", gap: 5, marginBottom: 16 }}>
          {[1, 6, 24, 72].map(h => (
            <button key={h} onClick={() => setHours(h)} style={{
              padding: "3px 10px", borderRadius: 20, fontSize: 10, cursor: "pointer",
              background: hours === h ? C.electricBl + "20" : "transparent",
              border: `0.5px solid ${hours === h ? C.electricBl : C.border}`,
              color: hours === h ? C.electricBl : C.textMuted,
            }}>
              {h}h
            </button>
          ))}
        </div>
      </div>

      {error && <div style={{ padding: "0 22px 16px" }}><ErrorBox message={error} /></div>}

      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th style={th}>#</th>
              <th style={th}>Coin</th>
              <th style={th}>Fear & Greed</th>
              <th style={th}>Bullish</th>
              <th style={th}>Bearish</th>
              <th style={th}>Mentions</th>
              <th style={th}>Engagement</th>
              <th style={th}>🐋 Whale FG</th>
              <th style={th}>🛒 Retail FG</th>
              <th style={th}>Updated</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <SkeletonTable rows={8} />
            ) : data?.map((row, i) => {
              const sel = selectedCoin === row.coin;
              const color = COIN_COLORS[i % COIN_COLORS.length];
              return (
                <tr
                  key={row.coin}
                  onClick={() => onCoinSelect(row.coin)}
                  style={{
                    cursor: "pointer",
                    background: sel ? C.electricBl + "0e" : "transparent",
                    borderLeft: sel ? `2px solid ${C.electricBl}` : "2px solid transparent",
                    transition: "background 0.12s",
                  }}
                  onMouseEnter={e => { if (!sel) e.currentTarget.style.background = C.bgElevated; }}
                  onMouseLeave={e => { if (!sel) e.currentTarget.style.background = "transparent"; }}
                >
                  <td style={{ padding: "13px 14px", fontSize: 11, color: C.textDim, fontFamily: "monospace" }}>
                    {i + 1}
                  </td>
                  <td style={{ padding: "13px 14px" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 9 }}>
                      <div style={{
                        width: 28, height: 28, borderRadius: 14,
                        background: color + "18",
                        border: `0.5px solid ${color}55`,
                        display: "flex", alignItems: "center", justifyContent: "center",
                        fontSize: 9, fontWeight: 700, color,
                        flexShrink: 0,
                      }}>
                        {row.coin.slice(0, 3)}
                      </div>
                      <div>
                        <div style={{ fontSize: 13, fontWeight: 600, color: C.textPri }}>${row.coin}</div>
                        <div style={{ fontSize: 10, color: C.textDim }}>{row.snapshot_count} snapshots</div>
                      </div>
                    </div>
                  </td>
                  <td style={{ padding: "13px 14px" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
                      <div style={{
                        width: 34, height: 34, borderRadius: 17,
                        border: `2px solid ${fgColor(row.avg_fear_greed)}55`,
                        display: "flex", alignItems: "center", justifyContent: "center",
                        fontSize: 11, fontWeight: 700,
                        color: fgColor(row.avg_fear_greed),
                        flexShrink: 0,
                      }}>
                        {fmt(row.avg_fear_greed, 0)}
                      </div>
                      <NeonBadge color={fgColor(row.avg_fear_greed)} small>
                        {fgLabel(row.avg_fear_greed)}
                      </NeonBadge>
                    </div>
                  </td>
                  <td style={{ padding: "13px 14px", fontSize: 12, fontFamily: "monospace", color: C.limeGreen }}>
                    {fmt(row.avg_bullish * 100)}%
                  </td>
                  <td style={{ padding: "13px 14px", fontSize: 12, fontFamily: "monospace", color: C.neonRed }}>
                    {fmt(row.avg_bearish * 100)}%
                  </td>
                  <td style={{ padding: "13px 14px", fontSize: 12, color: C.textPri, fontFamily: "monospace" }}>
                    {fmtInt(row.total_mentions)}
                  </td>
                  <td style={{ padding: "13px 14px", fontSize: 12, color: C.textMuted, fontFamily: "monospace" }}>
                    {fmtInt(row.total_engagement)}
                  </td>
                  <td style={{ padding: "13px 14px" }}>
                    {row.avg_whale_fg != null
                      ? <NeonBadge color={fgColor(row.avg_whale_fg)} small>{fmt(row.avg_whale_fg, 0)}</NeonBadge>
                      : <span style={{ fontSize: 11, color: C.textDim }}>—</span>}
                  </td>
                  <td style={{ padding: "13px 14px" }}>
                    {row.avg_retail_fg != null
                      ? <NeonBadge color={fgColor(row.avg_retail_fg)} small>{fmt(row.avg_retail_fg, 0)}</NeonBadge>
                      : <span style={{ fontSize: 11, color: C.textDim }}>—</span>}
                  </td>
                  <td style={{ padding: "13px 14px", fontSize: 10, color: C.textDim }}>
                    {timeAgo(row.latest_at)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

// ─── Speed Spikes Panel ───────────────────────────────────────────────────────
// Dùng useSpeedSpikes() → /api/spikes/speed
// Fields: symbol, mention_count, growth_rate, z_score, spike_reasons,
//         unique_authors, trend_score, window_start
function SpeedSpikesPanel() {
  const { data, loading } = useSpeedSpikes(1);
  if (loading || !data?.length) return null;

  return (
    <Card style={{ padding: "18px 22px" }}>
      <SectionTitle>⚡ Live Spikes — Speed Layer</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
        {data.map(spike => (
          <div key={spike.symbol + spike.window_start} style={{
            background: C.bgElevated,
            border: `0.5px solid ${C.amber}33`,
            borderRadius: 10,
            padding: "11px 16px",
            display: "flex",
            flexDirection: "column",
            gap: 5,
            minWidth: 160,
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ fontSize: 14, fontWeight: 700, color: C.amber }}>${spike.symbol}</span>
              <NeonBadge color={C.amber} small>growth ×{fmt(spike.growth_rate)}</NeonBadge>
            </div>
            <div style={{ fontSize: 11, color: C.textMuted }}>
              {fmtInt(spike.mention_count)} mentions · {spike.unique_authors} authors
            </div>
            {spike.spike_reasons?.length > 0 && (
              <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                {spike.spike_reasons.map(r => (
                  <span key={r} style={{
                    fontSize: 9, padding: "1px 6px", borderRadius: 10,
                    background: C.amber + "15", color: C.amber,
                    border: `0.5px solid ${C.amber}44`,
                  }}>
                    {r}
                  </span>
                ))}
              </div>
            )}
            <div style={{ fontSize: 10, color: C.textDim }}>{timeAgo(spike.window_start)}</div>
          </div>
        ))}
      </div>
    </Card>
  );
}

// ─── Alerts Feed ──────────────────────────────────────────────────────────────
// Dùng useAlerts() → /api/alerts
// Fields: alert_type, severity, message, status, payload, created_at
function AlertsFeed() {
  const { data, loading, error } = useAlerts("open");

  return (
    <Card style={{ width: 300, flexShrink: 0, padding: "18px 20px" }}>
      <SectionTitle>🚨 Alerts Feed</SectionTitle>

      {error && <ErrorBox message={error} />}
      {loading && Array(4).fill(0).map((_, i) => <SkeletonAlertItem key={i} />)}

      {!loading && !error && !data?.length && (
        <div style={{ fontSize: 12, color: C.textMuted }}>Không có alert nào đang mở.</div>
      )}

      {!loading && data?.map((alert, i) => {
        const col = severityColor(alert.severity);
        const icon = alert.severity === "critical" ? "🚨"
          : alert.severity === "high" ? "⚡"
            : alert.alert_type === "spam_detected" ? "🤖"
              : "📡";
        // payload có thể chứa coin, spam_count, spam_ratio từ batch_job
        const payload = alert.payload ?? {};
        return (
          <div key={i} style={{
            display: "flex", alignItems: "flex-start", gap: 10,
            padding: "11px 0",
            borderBottom: i < data.length - 1 ? `0.5px solid ${C.border}` : "none",
          }}>
            <div style={{
              width: 34, height: 34, borderRadius: 17, flexShrink: 0,
              background: col + "15",
              border: `0.5px solid ${col}44`,
              display: "flex", alignItems: "center", justifyContent: "center",
              fontSize: 14,
            }}>
              {icon}
            </div>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
                <span style={{ fontSize: 10, fontWeight: 600, color: col, textTransform: "uppercase" }}>
                  {alert.alert_type}
                </span>
                <span style={{ fontSize: 10, color: C.textDim }}>{timeAgo(alert.created_at)}</span>
              </div>
              <p style={{ fontSize: 11, color: C.textMuted, lineHeight: 1.5, margin: "0 0 4px" }}>
                {alert.message}
              </p>
              {/* Hiển thị payload nếu có (spam_count, coin...) */}
              {payload.coin && (
                <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
                  {payload.coin && <NeonBadge color={col} small>${payload.coin}</NeonBadge>}
                  {payload.spam_count && (
                    <NeonBadge color={C.textDim} small>{payload.spam_count} spam</NeonBadge>
                  )}
                </div>
              )}
            </div>
          </div>
        );
      })}
    </Card>
  );
}

// ─── MAIN DASHBOARD ───────────────────────────────────────────────────────────
export default function Dashboard() {
  const [selectedCoin, setSelectedCoin] = useState(null);

  return (
    <div style={{
      minHeight: "100vh",
      background: C.bgVoid,
      color: C.textPri,
      fontFamily: "'DM Mono', 'Fira Code', monospace",
    }}>
      {/* ── Header ── */}
      <header style={{
        borderBottom: `0.5px solid ${C.border}`,
        padding: "0 32px",
        display: "flex", alignItems: "center", height: 52,
        background: C.bgSurface,
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <span style={{ fontSize: 20 }}>⬡</span>
          <span style={{ fontSize: 14, fontWeight: 700, color: C.textPri, letterSpacing: "0.05em" }}>
            CryptoTrend
          </span>
          <NeonBadge color={C.neonTeal}>LIVE</NeonBadge>
        </div>
        <nav style={{ marginLeft: "auto", display: "flex", gap: 24 }}>
          {["Trending", "Whales", "Alerts", "API Docs"].map(item => (
            <a key={item} href="#" style={{
              fontSize: 11, color: C.textMuted,
              textDecoration: "none", letterSpacing: "0.06em",
            }}>
              {item}
            </a>
          ))}
        </nav>
      </header>

      {/* ── Speed Ticker Bar ── */}
      <SpeedTickerBar />

      {/* ── Body ── */}
      <main style={{ padding: "24px 32px 48px", display: "flex", flexDirection: "column", gap: 18 }}>

        {/* Row 1: KPI Cards */}
        <KpiCards />

        {/* Row 2: Chart + Alerts */}
        <div style={{ display: "flex", gap: 16 }}>
          <SentimentChart coin={selectedCoin} />
          <AlertsFeed />
        </div>

        {/* Row 3: Live Spikes */}
        <SpeedSpikesPanel />

        {/* Row 4: Trending Table */}
        <TrendingTable onCoinSelect={setSelectedCoin} selectedCoin={selectedCoin} />

      </main>
    </div>
  );
}
