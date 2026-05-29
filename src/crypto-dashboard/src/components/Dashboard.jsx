/**
 * Dashboard.jsx  (v3)
 * ────────────────────
 * Fixes:
 *  1. AlertsFeed có chiều cao cố định + scroll nội bộ → chart không bị kéo dài
 *  2. Nav header hoạt động thực sự: Trending / Whales / Alerts / API Docs
 *     mỗi tab render đúng view tương ứng
 */

import { useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine,
} from "recharts";
import {
  useSummary, useBatchTrends, useSpeedTrends,
  useCoinSentiment, useSpeedSpikes, useAlerts, useBadRecords, useJobHistory,
} from "../hooks/useDashboard";
import {
  SkeletonKpiCard, SkeletonTable, SkeletonChart, SkeletonAlertItem, SkeletonBox,
} from "./Skeleton";

// ─── Design Tokens ────────────────────────────────────────────────────────────
const C = {
  bgVoid: "#0a0d14",
  bgSurface: "#111620",
  bgElevated: "#1a2030",
  border: "#1e2a40",
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
const fgColor = s =>
  !s && s !== 0 ? C.textMuted
    : s >= 70 ? C.neonTeal : s >= 55 ? C.limeGreen : s >= 45 ? C.amber : C.neonRed;

const fgLabel = s =>
  !s && s !== 0 ? "—"
    : s >= 70 ? "Greed" : s >= 55 ? "Neutral+" : s >= 45 ? "Neutral" : "Fear";

const severityColor = s =>
  ({ critical: C.neonRed, high: C.amber, medium: C.electricBl, info: C.neonTeal, low: C.textMuted })[s] ?? C.textMuted;

const timeAgo = iso => {
  if (!iso) return "—";
  const d = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (d < 60) return `${d}s ago`;
  if (d < 3600) return `${Math.floor(d / 60)}m ago`;
  return `${Math.floor(d / 3600)}h ago`;
};

const fmt = (n, dec = 1) => n == null ? "—" : Number(n).toFixed(dec);
const fmtInt = n => n == null ? "—" : Number(n).toLocaleString("vi-VN");

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
      <span style={{ fontSize: 12, fontWeight: 600, color: C.textPri, letterSpacing: "0.08em", textTransform: "uppercase" }}>
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
    <div style={{ background: C.bgSurface, border: `0.5px solid ${C.border}`, borderRadius: 12, ...style }}>
      {children}
    </div>
  );
}

function TimeFilterBar({ value, onChange, options = [1, 6, 24, 72] }) {
  return (
    <div style={{ display: "flex", gap: 5 }}>
      {options.map(h => (
        <button key={h} onClick={() => onChange(h)} style={{
          padding: "3px 10px", borderRadius: 20, fontSize: 10, cursor: "pointer",
          background: value === h ? C.electricBl + "20" : "transparent",
          border: `0.5px solid ${value === h ? C.electricBl : C.border}`,
          color: value === h ? C.electricBl : C.textMuted,
          transition: "all 0.15s",
        }}>
          {h}h
        </button>
      ))}
    </div>
  );
}

// ─── Speed Ticker Bar ─────────────────────────────────────────────────────────
function SpeedTickerBar() {
  const { data } = useSpeedTrends(1, 10, false);
  if (!data?.length) return null;
  return (
    <div style={{
      background: C.bgElevated, borderBottom: `0.5px solid ${C.border}`,
      padding: "0 32px", height: 34,
      display: "flex", alignItems: "center", gap: 28, overflowX: "auto",
    }}>
      {data.map(item => (
        <div key={item.symbol} style={{ display: "flex", alignItems: "center", gap: 7, flexShrink: 0 }}>
          <span style={{ fontSize: 11, color: C.textMuted, fontWeight: 500 }}>${item.symbol}</span>
          <span style={{ fontSize: 11, fontFamily: "monospace", color: C.textPri }}>
            {fmtInt(item.mention_count)} tweets
          </span>
          {item.is_spike && <NeonBadge color={C.amber} small>⚡ SPIKE</NeonBadge>}
          <span style={{ fontSize: 10, color: C.textDim }}>score {fmt(item.trend_score)}</span>
        </div>
      ))}
    </div>
  );
}

// ─── KPI Cards ────────────────────────────────────────────────────────────────
function KpiCards() {
  const { data, loading, error } = useSummary();
  if (error) return <ErrorBox message={error} />;
  const cards = data ? [
    { label: "Top Trending", value: `$${data.top_trending_coin}`, sub: `trend score ${fmt(data.top_trend_score)}`, color: C.neonTeal },
    { label: "Mentions / 1h", value: fmtInt(data.total_mentions_1h), sub: "từ speed layer", color: C.electricBl },
    { label: "Fear & Greed", value: fmt(data.avg_fear_greed), sub: fgLabel(data.avg_fear_greed), color: fgColor(data.avg_fear_greed) },
    { label: "Active Spikes", value: data.active_spikes, sub: `${data.active_alerts} alerts mở`, color: C.amber },
  ] : [];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14 }}>
      {loading
        ? Array(4).fill(0).map((_, i) => <SkeletonKpiCard key={i} />)
        : cards.map(({ label, value, sub, color }) => (
          <div key={label} style={{
            background: C.bgSurface, border: `0.5px solid ${C.border}`, borderRadius: 12,
            padding: "20px 22px", position: "relative", overflow: "hidden",
            transition: "border-color 0.2s", cursor: "default",
          }}
            onMouseEnter={e => e.currentTarget.style.borderColor = color + "55"}
            onMouseLeave={e => e.currentTarget.style.borderColor = C.border}
          >
            <div style={{ position: "absolute", top: 0, right: 0, width: 56, height: 56, borderRadius: "0 12px 0 56px", background: color + "10" }} />
            <div style={{ fontSize: 10, color: C.textMuted, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 8 }}>{label}</div>
            <div style={{ fontSize: 28, fontWeight: 700, color, fontFamily: "monospace", marginBottom: 5 }}>{value}</div>
            <div style={{ fontSize: 11, color: C.textDim }}>{sub}</div>
          </div>
        ))
      }
    </div>
  );
}

// ─── Sentiment Chart ──────────────────────────────────────────────────────────
function SentimentChart({ coin }) {
  const [hours, setHours] = useState(6);
  const { data, loading, error } = useCoinSentiment(coin, hours);

  const chartData = data ? data.map(d => ({
    time: new Date(d.window_start).toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" }),
    "F&G Overall": Math.round(d.fear_greed_score),
    "Bullish %": Math.round((d.bullish_ratio ?? 0) * 100),
    "Bearish %": Math.round((d.bearish_ratio ?? 0) * 100),
    ...(d.whale_fear_greed != null && { "F&G Whale": Math.round(d.whale_fear_greed) }),
    ...(d.retail_fear_greed != null && { "F&G Retail": Math.round(d.retail_fear_greed) }),
  })) : [];

  const hasSegment = chartData.some(d => d["F&G Whale"] != null);

  // ── chiều cao cố định để không bị kéo dài theo AlertsFeed ──
  return (
    <Card style={{ flex: 1, padding: "20px 22px", minHeight: 0 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <SectionTitle>Sentiment — {coin ? `$${coin}` : "chọn coin ↓"}</SectionTitle>
        {coin && <TimeFilterBar value={hours} onChange={setHours} options={[1, 3, 6, 24]} />}
      </div>

      {!coin && <div style={{ color: C.textMuted, fontSize: 12, textAlign: "center", paddingTop: 60 }}>👆 Nhấp vào một coin trong bảng bên dưới để xem biểu đồ</div>}
      {coin && loading && <SkeletonChart height={200} />}
      {coin && error && <ErrorBox message={error} />}

      {coin && !loading && !error && chartData.length > 0 && (
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={chartData} margin={{ top: 4, right: 6, left: -22, bottom: 0 }}>
            <CartesianGrid strokeDasharray="2 3" stroke={C.border} />
            <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 9 }} tickLine={false} />
            <YAxis domain={[0, 100]} tick={{ fill: C.textMuted, fontSize: 9 }} tickLine={false} axisLine={false} />
            <ReferenceLine y={50} stroke={C.textDim} strokeDasharray="4 3" />
            <Tooltip contentStyle={{ background: C.bgElevated, border: `0.5px solid ${C.border}`, borderRadius: 8, fontSize: 11 }} labelStyle={{ color: C.textPri }} />
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
        <div style={{ color: C.textMuted, fontSize: 12, textAlign: "center", paddingTop: 60 }}>
          Chưa có data sentiment cho ${coin} trong {hours}h.
        </div>
      )}
    </Card>
  );
}

// ─── Alerts Feed ──────────────────────────────────────────────────────────────
// FIX: chiều cao cố định 320px + overflowY scroll → không kéo dài chart
function AlertsFeed() {
  const { data, loading, error } = useAlerts("open");

  return (
    <Card style={{ width: 300, flexShrink: 0, display: "flex", flexDirection: "column" }}>
      {/* Header cố định */}
      <div style={{ padding: "18px 20px 12px", borderBottom: `0.5px solid ${C.border}`, flexShrink: 0 }}>
        <SectionTitle>🚨 Alerts Feed</SectionTitle>
      </div>

      {/* Body cuộn — cố định chiều cao 320px */}
      <div style={{
        height: 320,
        overflowY: "auto",
        padding: "8px 20px 16px",
        // custom scrollbar
        scrollbarWidth: "thin",
        scrollbarColor: `${C.border} transparent`,
      }}>
        {error && <ErrorBox message={error} />}
        {loading && Array(4).fill(0).map((_, i) => <SkeletonAlertItem key={i} />)}

        {!loading && !error && !data?.length && (
          <div style={{ fontSize: 12, color: C.textMuted, paddingTop: 16 }}>Không có alert nào đang mở.</div>
        )}

        {!loading && data?.map((alert, i) => {
          const col = severityColor(alert.severity);
          const icon = alert.severity === "critical" ? "🚨"
            : alert.severity === "high" ? "⚡"
              : alert.alert_type === "spam_detected" ? "🤖" : "📡";
          const payload = alert.payload ?? {};
          return (
            <div key={i} style={{
              display: "flex", alignItems: "flex-start", gap: 10,
              padding: "11px 0",
              borderBottom: i < data.length - 1 ? `0.5px solid ${C.border}` : "none",
            }}>
              <div style={{
                width: 32, height: 32, borderRadius: 16, flexShrink: 0,
                background: col + "15", border: `0.5px solid ${col}44`,
                display: "flex", alignItems: "center", justifyContent: "center", fontSize: 13,
              }}>
                {icon}
              </div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 2 }}>
                  <span style={{ fontSize: 10, fontWeight: 600, color: col, textTransform: "uppercase" }}>
                    {alert.alert_type}
                  </span>
                  <span style={{ fontSize: 10, color: C.textDim }}>{timeAgo(alert.created_at)}</span>
                </div>
                <p style={{ fontSize: 11, color: C.textMuted, lineHeight: 1.5, margin: "0 0 4px" }}>
                  {alert.message}
                </p>
                {payload.coin && (
                  <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                    <NeonBadge color={col} small>${payload.coin}</NeonBadge>
                    {payload.spam_count && <NeonBadge color={C.textDim} small>{payload.spam_count} spam</NeonBadge>}
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {/* Footer: badge tổng số */}
      {!loading && data?.length > 0 && (
        <div style={{
          padding: "8px 20px", borderTop: `0.5px solid ${C.border}`,
          fontSize: 10, color: C.textDim, flexShrink: 0,
        }}>
          {data.length} alert{data.length > 1 ? "s" : ""} đang mở · cuộn để xem thêm
        </div>
      )}
    </Card>
  );
}

// ─── Trending Table ───────────────────────────────────────────────────────────
function TrendingTable({ onCoinSelect, selectedCoin }) {
  const [hours, setHours] = useState(24);
  const { data, loading, error } = useBatchTrends(hours, 20);

  const th = {
    padding: "10px 14px", fontSize: 10, fontWeight: 600,
    color: C.textMuted, textAlign: "left",
    letterSpacing: "0.08em", textTransform: "uppercase",
    borderBottom: `0.5px solid ${C.border}`, whiteSpace: "nowrap",
  };

  return (
    <Card style={{ overflow: "hidden" }}>
      <div style={{ padding: "18px 22px 0", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <SectionTitle>Trending Coins — Batch Layer</SectionTitle>
        <div style={{ marginBottom: 16 }}>
          <TimeFilterBar value={hours} onChange={setHours} options={[1, 6, 24, 72]} />
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
              <th style={th}>🐋 Whale</th>
              <th style={th}>🛒 Retail</th>
              <th style={th}>Updated</th>
            </tr>
          </thead>
          <tbody>
            {loading ? <SkeletonTable rows={8} /> : data?.map((row, i) => {
              const sel = selectedCoin === row.coin;
              const color = COIN_COLORS[i % COIN_COLORS.length];
              return (
                <tr key={row.coin} onClick={() => onCoinSelect(row.coin)} style={{
                  cursor: "pointer",
                  background: sel ? C.electricBl + "0e" : "transparent",
                  borderLeft: sel ? `2px solid ${C.electricBl}` : "2px solid transparent",
                  transition: "background 0.12s",
                }}
                  onMouseEnter={e => { if (!sel) e.currentTarget.style.background = C.bgElevated; }}
                  onMouseLeave={e => { if (!sel) e.currentTarget.style.background = "transparent"; }}
                >
                  <td style={{ padding: "12px 14px", fontSize: 11, color: C.textDim, fontFamily: "monospace" }}>{i + 1}</td>
                  <td style={{ padding: "12px 14px" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 9 }}>
                      <div style={{
                        width: 28, height: 28, borderRadius: 14,
                        background: color + "18", border: `0.5px solid ${color}55`,
                        display: "flex", alignItems: "center", justifyContent: "center",
                        fontSize: 9, fontWeight: 700, color, flexShrink: 0,
                      }}>
                        {row.coin.slice(0, 3)}
                      </div>
                      <div>
                        <div style={{ fontSize: 13, fontWeight: 600, color: C.textPri }}>${row.coin}</div>
                        <div style={{ fontSize: 10, color: C.textDim }}>{row.snapshot_count} snapshots</div>
                      </div>
                    </div>
                  </td>
                  <td style={{ padding: "12px 14px" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
                      <div style={{
                        width: 32, height: 32, borderRadius: 16,
                        border: `2px solid ${fgColor(row.avg_fear_greed)}55`,
                        display: "flex", alignItems: "center", justifyContent: "center",
                        fontSize: 11, fontWeight: 700, color: fgColor(row.avg_fear_greed), flexShrink: 0,
                      }}>
                        {fmt(row.avg_fear_greed, 0)}
                      </div>
                      <NeonBadge color={fgColor(row.avg_fear_greed)} small>{fgLabel(row.avg_fear_greed)}</NeonBadge>
                    </div>
                  </td>
                  <td style={{ padding: "12px 14px", fontSize: 12, fontFamily: "monospace", color: C.limeGreen }}>{fmt(row.avg_bullish * 100)}%</td>
                  <td style={{ padding: "12px 14px", fontSize: 12, fontFamily: "monospace", color: C.neonRed }}>{fmt(row.avg_bearish * 100)}%</td>
                  <td style={{ padding: "12px 14px", fontSize: 12, color: C.textPri, fontFamily: "monospace" }}>{fmtInt(row.total_mentions)}</td>
                  <td style={{ padding: "12px 14px", fontSize: 12, color: C.textMuted, fontFamily: "monospace" }}>{fmtInt(row.total_engagement)}</td>
                  <td style={{ padding: "12px 14px" }}>
                    {row.avg_whale_fg != null
                      ? <NeonBadge color={fgColor(row.avg_whale_fg)} small>{fmt(row.avg_whale_fg, 0)}</NeonBadge>
                      : <span style={{ fontSize: 11, color: C.textDim }}>—</span>}
                  </td>
                  <td style={{ padding: "12px 14px" }}>
                    {row.avg_retail_fg != null
                      ? <NeonBadge color={fgColor(row.avg_retail_fg)} small>{fmt(row.avg_retail_fg, 0)}</NeonBadge>
                      : <span style={{ fontSize: 11, color: C.textDim }}>—</span>}
                  </td>
                  <td style={{ padding: "12px 14px", fontSize: 10, color: C.textDim }}>{timeAgo(row.latest_at)}</td>
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
function SpeedSpikesPanel() {
  const { data, loading } = useSpeedSpikes(1);
  if (loading || !data?.length) return null;
  return (
    <Card style={{ padding: "18px 22px" }}>
      <SectionTitle>⚡ Live Spikes — Speed Layer</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
        {data.map(spike => (
          <div key={spike.symbol + spike.window_start} style={{
            background: C.bgElevated, border: `0.5px solid ${C.amber}33`,
            borderRadius: 10, padding: "11px 16px",
            display: "flex", flexDirection: "column", gap: 5, minWidth: 160,
          }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ fontSize: 14, fontWeight: 700, color: C.amber }}>${spike.symbol}</span>
              <NeonBadge color={C.amber} small>×{fmt(spike.growth_rate)}</NeonBadge>
            </div>
            <div style={{ fontSize: 11, color: C.textMuted }}>
              {fmtInt(spike.mention_count)} mentions · {spike.unique_authors} authors
            </div>
            {spike.spike_reasons?.length > 0 && (
              <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                {spike.spike_reasons.map(r => (
                  <span key={r} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 10, background: C.amber + "15", color: C.amber, border: `0.5px solid ${C.amber}44` }}>
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

// ═══════════════════════════════════════════════════════════════════════════════
// VIEWS — mỗi tab nav render 1 view riêng
// ═══════════════════════════════════════════════════════════════════════════════

// ── View: Trending (trang chủ) ────────────────────────────────────────────────
function ViewTrending() {
  const [selectedCoin, setSelectedCoin] = useState(null);
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <KpiCards />
      {/* Chart + Alerts nằm cạnh nhau, alignItems stretch để cùng chiều cao */}
      <div style={{ display: "flex", gap: 16, alignItems: "stretch" }}>
        <SentimentChart coin={selectedCoin} />
        <AlertsFeed />
      </div>
      <SpeedSpikesPanel />
      <TrendingTable onCoinSelect={setSelectedCoin} selectedCoin={selectedCoin} />
    </div>
  );
}

// ── View: Whales ──────────────────────────────────────────────────────────────
function ViewWhales() {
  const { data, loading, error } = useSpeedTrends(1, 20, false);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 4 }}>
        <span style={{ fontSize: 22 }}>🐋</span>
        <div>
          <div style={{ fontSize: 16, fontWeight: 700, color: C.textPri }}>Whale Signal Monitor</div>
          <div style={{ fontSize: 11, color: C.textMuted }}>Real-time top coins từ speed_trend_metrics — cập nhật mỗi 10s</div>
        </div>
      </div>

      {error && <ErrorBox message={error} />}

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))", gap: 12 }}>
        {loading
          ? Array(8).fill(0).map((_, i) => (
            <Card key={i} style={{ padding: "16px 18px" }}>
              <SkeletonBox w="50%" h={12} style={{ marginBottom: 10 }} />
              <SkeletonBox w="70%" h={24} style={{ marginBottom: 8 }} />
              <SkeletonBox w="90%" h={10} />
            </Card>
          ))
          : data?.map((item, i) => (
            <Card key={item.symbol} style={{
              padding: "16px 18px",
              border: item.is_spike ? `0.5px solid ${C.amber}66` : `0.5px solid ${C.border}`,
              transition: "border-color 0.2s",
            }}
              onMouseEnter={e => e.currentTarget.style.borderColor = COIN_COLORS[i % COIN_COLORS.length] + "66"}
              onMouseLeave={e => e.currentTarget.style.borderColor = item.is_spike ? C.amber + "66" : C.border}
            >
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 8 }}>
                <span style={{ fontSize: 15, fontWeight: 700, color: COIN_COLORS[i % COIN_COLORS.length] }}>
                  ${item.symbol}
                </span>
                {item.is_spike && <NeonBadge color={C.amber} small>⚡ SPIKE</NeonBadge>}
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "6px 0", fontSize: 11 }}>
                <span style={{ color: C.textMuted }}>Mentions</span>
                <span style={{ color: C.textPri, textAlign: "right", fontFamily: "monospace" }}>
                  {fmtInt(item.mention_count)}
                </span>
                <span style={{ color: C.textMuted }}>Authors</span>
                <span style={{ color: C.textPri, textAlign: "right", fontFamily: "monospace" }}>
                  {fmtInt(item.unique_authors)}
                </span>
                <span style={{ color: C.textMuted }}>Influencers</span>
                <span style={{ color: C.amber, textAlign: "right", fontFamily: "monospace" }}>
                  {fmtInt(item.influencer_authors)}
                </span>
                <span style={{ color: C.textMuted }}>Trend Score</span>
                <span style={{ color: C.neonTeal, textAlign: "right", fontFamily: "monospace" }}>
                  {fmt(item.trend_score)}
                </span>
                {item.growth_rate != null && <>
                  <span style={{ color: C.textMuted }}>Growth</span>
                  <span style={{ color: item.growth_rate >= 3 ? C.neonRed : C.limeGreen, textAlign: "right", fontFamily: "monospace" }}>
                    ×{fmt(item.growth_rate)}
                  </span>
                </>}
              </div>

              <div style={{ marginTop: 8, fontSize: 10, color: C.textDim }}>
                {timeAgo(item.window_start)}
              </div>
            </Card>
          ))
        }
      </div>
    </div>
  );
}

// ── View: Alerts ──────────────────────────────────────────────────────────────
function ViewAlerts() {
  const [statusFilter, setStatusFilter] = useState("open");
  const { data, loading, error } = useAlerts(statusFilter);

  const statusColors = { open: C.neonRed, closed: C.limeGreen };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <div style={{ fontSize: 16, fontWeight: 700, color: C.textPri, marginBottom: 4 }}>🚨 Alert Center</div>
          <div style={{ fontSize: 11, color: C.textMuted }}>Toàn bộ alerts từ batch layer + speed layer</div>
        </div>
        {/* Filter status */}
        <div style={{ display: "flex", gap: 8 }}>
          {["open", "closed", ""].map(s => (
            <button key={s || "all"} onClick={() => setStatusFilter(s)} style={{
              padding: "5px 14px", borderRadius: 20, fontSize: 11, cursor: "pointer",
              background: statusFilter === s ? C.electricBl + "20" : "transparent",
              border: `0.5px solid ${statusFilter === s ? C.electricBl : C.border}`,
              color: statusFilter === s ? C.electricBl : C.textMuted,
            }}>
              {s === "open" ? "🔴 Open" : s === "closed" ? "✅ Closed" : "All"}
            </button>
          ))}
        </div>
      </div>

      {error && <ErrorBox message={error} />}
      {loading && <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
        {Array(5).fill(0).map((_, i) => <SkeletonAlertItem key={i} />)}
      </div>}

      {!loading && !data?.length && (
        <Card style={{ padding: "40px", textAlign: "center" }}>
          <div style={{ fontSize: 32, marginBottom: 12 }}>✅</div>
          <div style={{ color: C.textMuted, fontSize: 13 }}>Không có alert nào {statusFilter && `(status: ${statusFilter})`}</div>
        </Card>
      )}

      {/* Danh sách alert đầy đủ — không bị giới hạn chiều cao */}
      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {!loading && data?.map((alert, i) => {
          const col = severityColor(alert.severity);
          const icon = alert.severity === "critical" ? "🚨"
            : alert.severity === "high" ? "⚡"
              : alert.alert_type === "spam_detected" ? "🤖" : "📡";
          const payload = alert.payload ?? {};
          return (
            <Card key={i} style={{ padding: "14px 20px" }}>
              <div style={{ display: "flex", alignItems: "flex-start", gap: 14 }}>
                <div style={{
                  width: 38, height: 38, borderRadius: 19, flexShrink: 0,
                  background: col + "15", border: `0.5px solid ${col}44`,
                  display: "flex", alignItems: "center", justifyContent: "center", fontSize: 16,
                }}>
                  {icon}
                </div>
                <div style={{ flex: 1 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <span style={{ fontSize: 12, fontWeight: 600, color: col, textTransform: "uppercase" }}>
                        {alert.alert_type}
                      </span>
                      <NeonBadge color={statusColors[alert.status] ?? C.textMuted} small>
                        {alert.status}
                      </NeonBadge>
                      <NeonBadge color={col} small>{alert.severity}</NeonBadge>
                    </div>
                    <span style={{ fontSize: 11, color: C.textDim }}>{timeAgo(alert.created_at)}</span>
                  </div>
                  <p style={{ fontSize: 12, color: C.textMuted, lineHeight: 1.6, margin: "0 0 6px" }}>
                    {alert.message}
                  </p>
                  {Object.keys(payload).length > 0 && (
                    <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                      {payload.coin && <NeonBadge color={col} small>${payload.coin}</NeonBadge>}
                      {payload.spam_count && <NeonBadge color={C.textMuted} small>{payload.spam_count} spam</NeonBadge>}
                      {payload.spam_ratio && <NeonBadge color={C.textMuted} small>{(payload.spam_ratio * 100).toFixed(1)}% spam ratio</NeonBadge>}
                    </div>
                  )}
                </div>
              </div>
            </Card>
          );
        })}
      </div>
    </div>
  );
}

// ── View: API Docs ────────────────────────────────────────────────────────────
const API_ENDPOINTS = [
  { method: "GET", path: "/health", desc: "Kiểm tra kết nối MongoDB + đếm documents mỗi collection", tag: "System" },
  { method: "GET", path: "/api/summary", desc: "4 KPI cards: top coin, mentions/1h, fear & greed, active spikes", tag: "Dashboard" },
  { method: "GET", path: "/api/trends/batch", desc: "Bảng xếp hạng từ batch_sentiment_metrics, hỗ trợ ?hours= & ?limit=", tag: "Trends" },
  { method: "GET", path: "/api/trends/speed", desc: "Real-time top coins từ speed_trend_metrics (Spark Streaming), hỗ trợ ?only_spikes=true", tag: "Trends" },
  { method: "GET", path: "/api/sentiment/:coin", desc: "Lịch sử sentiment 1 coin, hỗ trợ ?hours= & ?source=batch|test", tag: "Sentiment" },
  { method: "GET", path: "/api/spikes/batch", desc: "Trend spikes từ batch_trend_spikes, hỗ trợ ?min_z= để filter", tag: "Spikes" },
  { method: "GET", path: "/api/spikes/speed", desc: "Live spikes từ speed_trend_metrics (is_spike=True) với growth_rate", tag: "Spikes" },
  { method: "GET", path: "/api/alerts", desc: "Alerts feed, hỗ trợ ?status=open|closed & ?alert_type=", tag: "Alerts" },
  { method: "GET", path: "/api/quality/bad-records", desc: "Bad records từ speed_bad_records — monitor chất lượng data stream", tag: "Quality" },
  { method: "GET", path: "/api/jobs/history", desc: "Audit log từ batch_job_runs — lịch sử mỗi lần chạy batch job", tag: "System" },
];

const TAG_COLORS = {
  System: C.textMuted, Dashboard: C.neonTeal, Trends: C.electricBl,
  Sentiment: C.neonPurple, Spikes: C.amber, Alerts: C.neonRed, Quality: C.limeGreen,
};

function ViewApiDocs() {
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <div>
        <div style={{ fontSize: 16, fontWeight: 700, color: C.textPri, marginBottom: 4 }}>📖 API Reference</div>
        <div style={{ fontSize: 11, color: C.textMuted }}>
          Base URL: <code style={{ color: C.neonTeal, background: C.bgElevated, padding: "1px 6px", borderRadius: 4 }}>http://localhost:8000</code>
          &nbsp;·&nbsp; FastAPI docs tại&nbsp;
          <a href="http://localhost:8000/docs" target="_blank" rel="noreferrer" style={{ color: C.electricBl }}>
            /docs
          </a>
        </div>
      </div>

      <Card style={{ overflow: "hidden" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              {["Method", "Endpoint", "Tag", "Mô tả"].map(h => (
                <th key={h} style={{
                  padding: "10px 16px", fontSize: 10, fontWeight: 600,
                  color: C.textMuted, textAlign: "left",
                  letterSpacing: "0.08em", textTransform: "uppercase",
                  borderBottom: `0.5px solid ${C.border}`, whiteSpace: "nowrap",
                }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {API_ENDPOINTS.map((ep, i) => (
              <tr key={i} style={{ borderBottom: `0.5px solid ${C.border}` }}
                onMouseEnter={e => e.currentTarget.style.background = C.bgElevated}
                onMouseLeave={e => e.currentTarget.style.background = "transparent"}
              >
                <td style={{ padding: "12px 16px" }}>
                  <span style={{
                    fontSize: 10, fontWeight: 700, padding: "2px 8px", borderRadius: 4,
                    background: C.limeGreen + "18", color: C.limeGreen,
                    border: `0.5px solid ${C.limeGreen}44`, fontFamily: "monospace",
                  }}>
                    {ep.method}
                  </span>
                </td>
                <td style={{ padding: "12px 16px" }}>
                  <code style={{ fontSize: 12, color: C.electricBl, fontFamily: "monospace" }}>{ep.path}</code>
                </td>
                <td style={{ padding: "12px 16px" }}>
                  <NeonBadge color={TAG_COLORS[ep.tag] ?? C.textMuted} small>{ep.tag}</NeonBadge>
                </td>
                <td style={{ padding: "12px 16px", fontSize: 12, color: C.textMuted }}>{ep.desc}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>

      {/* Collection map */}
      <Card style={{ padding: "20px 24px" }}>
        <SectionTitle>MongoDB Collections</SectionTitle>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 10 }}>
          {[
            { col: "batch_sentiment_metrics", writer: "batch_job.py", note: "Sentiment theo coin/window" },
            { col: "batch_trend_spikes", writer: "mongo_client.save_trend_spike()", note: "Spike detection kết quả" },
            { col: "speed_trend_metrics", writer: "stream_runtime.write_batch_to_mongo()", note: "Real-time Spark output" },
            { col: "speed_bad_records", writer: "spark_pipeline.py", note: "Bad records từ stream" },
            { col: "alerts", writer: "mongo_client.save_alert()", note: "Whale + spam alerts" },
            { col: "batch_job_runs", writer: "batch_job.log_batch_run()", note: "Audit log" },
            { col: "test_batch_process", writer: "batch_job.py --demo", note: "Demo mode output" },
            { col: "tweets", writer: "mongo_client.save_raw_tweets()", note: "Raw tweet storage" },
          ].map(({ col, writer, note }) => (
            <div key={col} style={{
              background: C.bgElevated, border: `0.5px solid ${C.border}`,
              borderRadius: 8, padding: "12px 14px",
            }}>
              <div style={{ fontSize: 12, fontWeight: 600, color: C.electricBl, fontFamily: "monospace", marginBottom: 4 }}>
                {col}
              </div>
              <div style={{ fontSize: 10, color: C.neonTeal, marginBottom: 2 }}>← {writer}</div>
              <div style={{ fontSize: 11, color: C.textMuted }}>{note}</div>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN DASHBOARD — Tab routing
// ═══════════════════════════════════════════════════════════════════════════════
const NAV_TABS = [
  { id: "trending", label: "Trending", icon: "📈" },
  { id: "whales", label: "Whales", icon: "🐋" },
  { id: "alerts", label: "Alerts", icon: "🚨" },
  { id: "api", label: "API Docs", icon: "📖" },
];

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState("trending");

  // Badge số alerts đang mở trên nav
  const { data: alertData } = useAlerts("open");
  const alertCount = alertData?.length ?? 0;

  return (
    <div style={{ minHeight: "100vh", background: C.bgVoid, color: C.textPri, fontFamily: "'DM Mono', 'Fira Code', monospace" }}>

      {/* ── Header ── */}
      <header style={{
        borderBottom: `0.5px solid ${C.border}`,
        padding: "0 32px",
        display: "flex", alignItems: "center", height: 52,
        background: C.bgSurface,
        position: "sticky", top: 0, zIndex: 100,
      }}>
        {/* Logo */}
        <div style={{ display: "flex", alignItems: "center", gap: 10, cursor: "pointer" }}
          onClick={() => setActiveTab("trending")}
        >
          <span style={{ fontSize: 20 }}>⬡</span>
          <span style={{ fontSize: 14, fontWeight: 700, color: C.textPri, letterSpacing: "0.05em" }}>CryptoTrend</span>
          <NeonBadge color={C.neonTeal}>LIVE</NeonBadge>
        </div>

        {/* Nav tabs */}
        <nav style={{ marginLeft: "auto", display: "flex", gap: 4 }}>
          {NAV_TABS.map(tab => {
            const isActive = activeTab === tab.id;
            return (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                style={{
                  position: "relative",
                  display: "flex", alignItems: "center", gap: 6,
                  padding: "6px 14px",
                  borderRadius: 8,
                  fontSize: 12,
                  fontWeight: isActive ? 600 : 400,
                  cursor: "pointer",
                  border: "none",
                  background: isActive ? C.electricBl + "18" : "transparent",
                  color: isActive ? C.electricBl : C.textMuted,
                  transition: "all 0.15s",
                  fontFamily: "inherit",
                  // underline neon khi active
                  borderBottom: isActive ? `2px solid ${C.electricBl}` : "2px solid transparent",
                  borderRadius: 0,
                }}
                onMouseEnter={e => { if (!isActive) e.currentTarget.style.color = C.textPri; }}
                onMouseLeave={e => { if (!isActive) e.currentTarget.style.color = C.textMuted; }}
              >
                <span>{tab.icon}</span>
                <span style={{ letterSpacing: "0.04em" }}>{tab.label}</span>
                {/* Badge số alert */}
                {tab.id === "alerts" && alertCount > 0 && (
                  <span style={{
                    position: "absolute", top: 4, right: 6,
                    width: 16, height: 16, borderRadius: 8,
                    background: C.neonRed, color: "#fff",
                    fontSize: 9, fontWeight: 700,
                    display: "flex", alignItems: "center", justifyContent: "center",
                  }}>
                    {alertCount > 9 ? "9+" : alertCount}
                  </span>
                )}
              </button>
            );
          })}
        </nav>
      </header>

      {/* ── Ticker ── */}
      <SpeedTickerBar />

      {/* ── Content ── */}
      <main style={{ padding: "24px 32px 56px" }}>
        {activeTab === "trending" && <ViewTrending />}
        {activeTab === "whales" && <ViewWhales />}
        {activeTab === "alerts" && <ViewAlerts />}
        {activeTab === "api" && <ViewApiDocs />}
      </main>
    </div>
  );
}
