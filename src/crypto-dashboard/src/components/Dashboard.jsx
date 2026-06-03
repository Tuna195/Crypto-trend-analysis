/**
 * Dashboard.jsx — CryptoTrend Frontend
 * ======================================
 * Gọi backend/main.py (FastAPI) → query MongoDB của An
 * Dữ liệu do Thắng (speed layer) và Hiệu (batch layer) ghi vào.
 *
 * Pages:
 *   📈 Trending  — KPI, chart, heatmap, bảng coin
 *   🐋 Whales    — so sánh whale vs retail sentiment
 *   🚨 Alerts    — alerts đầy đủ
 *   📊 Pipeline  — trạng thái batch/speed + job history
 */

import { useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine,
  BarChart, Bar, Cell,
} from "recharts";
import {
  useSummary, useBatchTrends, useSpeedTrends,
  useCoinSentiment, useSpeedSpikes, useAlerts,
  useBadRecords, useJobHistory,
} from "../hooks/useDashboard";
import {
  SkeletonKpiCard, SkeletonTable, SkeletonChart,
  SkeletonAlertItem, SkeletonBox, SkeletonCard,
} from "./Skeleton";
import TrendHeatmap from "./TrendHeatmap";

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
const COIN_COLORS = ["#00f5a0", "#5b8cff", "#a78bfa", "#f59e0b", "#f05252", "#22c97a", "#38bdf8", "#fb7185"];

// ─── Helpers ──────────────────────────────────────────────────────────────────
const fgColor = s => s == null ? C.textMuted : s >= 70 ? C.neonTeal : s >= 55 ? C.limeGreen : s >= 45 ? C.amber : C.neonRed;
const fgLabel = s => s == null ? "—" : s >= 70 ? "Extreme Greed" : s >= 55 ? "Greed" : s >= 45 ? "Neutral" : s >= 30 ? "Fear" : "Extreme Fear";
const sevColor = s => ({ critical: C.neonRed, high: C.amber, medium: C.electricBl, info: C.neonTeal, low: C.textMuted })[s] ?? C.textMuted;
const timeAgo = iso => {
  if (!iso) return "—";
  const d = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (d < 60) return `${d}s ago`;
  if (d < 3600) return `${Math.floor(d / 60)}m ago`;
  if (d < 86400) return `${Math.floor(d / 3600)}h ago`;
  return `${Math.floor(d / 86400)}d ago`;
};
const fmt = (n, dec = 1) => n == null ? "—" : Number(n).toFixed(dec);
const fmtInt = n => n == null ? "—" : Number(n).toLocaleString("vi-VN");

// ─── UI Primitives ────────────────────────────────────────────────────────────
function NeonBadge({ children, color = C.neonTeal, small = false }) {
  return (
    <span style={{
      display: "inline-block", padding: small ? "1px 7px" : "2px 10px",
      borderRadius: 20, fontSize: small ? 10 : 11, fontWeight: 600,
      letterSpacing: "0.04em", color,
      background: color + "18", border: `0.5px solid ${color}55`, whiteSpace: "nowrap",
    }}>{children}</span>
  );
}

function LayerBadge({ layer }) {
  const cfg = {
    speed: { label: "SPEED", color: C.neonTeal, title: "Nguồn: speed_trend_metrics (Spark Streaming)" },
    batch: { label: "BATCH", color: C.electricBl, title: "Nguồn: batch_sentiment_metrics (Spark Batch)" },
    both: { label: "BATCH+SPEED", color: C.neonPurple, title: "Kết hợp cả hai layer" },
  }[layer];
  if (!cfg) return null;
  return (
    <span title={cfg.title} style={{
      fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 4,
      background: cfg.color + "18", color: cfg.color,
      border: `0.5px solid ${cfg.color}44`, letterSpacing: "0.06em", cursor: "help",
    }}>⬡ {cfg.label}</span>
  );
}

function SectionTitle({ children, layer }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
      <div style={{ width: 3, height: 18, borderRadius: 2, background: C.electricBl, flexShrink: 0 }} />
      <span style={{ fontSize: 12, fontWeight: 600, color: C.textPri, letterSpacing: "0.08em", textTransform: "uppercase" }}>
        {children}
      </span>
      {layer && <LayerBadge layer={layer} />}
    </div>
  );
}

function Card({ children, style = {} }) {
  return <div style={{ background: C.bgSurface, border: `0.5px solid ${C.border}`, borderRadius: 12, ...style }}>{children}</div>;
}

function ErrorBox({ message }) {
  return (
    <div style={{
      padding: "14px 18px", borderRadius: 10,
      background: C.neonRed + "10", border: `0.5px solid ${C.neonRed}44`,
      color: C.neonRed, fontSize: 12, lineHeight: 1.5,
    }}>
      <strong>⚠ Lỗi kết nối backend:</strong> {message}
      <div style={{ marginTop: 6, fontSize: 11, color: C.neonRed + "aa" }}>
        Kiểm tra FastAPI đang chạy tại localhost:8000 và MongoDB đã có dữ liệu.
      </div>
    </div>
  );
}

function TimeFilterBar({ value, onChange, options = [1, 6, 24, 72] }) {
  return (
    <div style={{ display: "flex", gap: 5 }}>
      {options.map(h => (
        <button key={h} onClick={() => onChange(h)} style={{
          padding: "3px 10px", borderRadius: 20, fontSize: 10,
          cursor: "pointer", fontFamily: "inherit",
          background: value === h ? C.electricBl + "20" : "transparent",
          border: `0.5px solid ${value === h ? C.electricBl : C.border}`,
          color: value === h ? C.electricBl : C.textMuted, transition: "all 0.15s",
        }}>{h}h</button>
      ))}
    </div>
  );
}

// ─── Ticker Bar ───────────────────────────────────────────────────────────────
// Nguồn: speed_trend_metrics (Thắng)
function TickerBar() {
  const { data } = useSpeedTrends(1, 12);
  if (!data?.length) return null;
  return (
    <div style={{
      background: C.bgElevated, borderBottom: `0.5px solid ${C.border}`,
      padding: "0 32px", height: 36,
      display: "flex", alignItems: "center", gap: 24, overflowX: "auto",
    }}>
      <span style={{
        fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 4,
        background: C.neonTeal + "18", color: C.neonTeal,
        border: `0.5px solid ${C.neonTeal}44`, flexShrink: 0, letterSpacing: "0.06em",
      }}>⬡ SPEED</span>
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
function KpiCards() {
  const { data, loading, error } = useSummary();
  if (error) return <ErrorBox message={error} />;

  const cards = data ? [
    { label: "Top Trending", value: `$${data.top_trending_coin}`, sub: `trend score ${fmt(data.top_trend_score)}`, color: C.neonTeal, layer: "speed" },
    { label: "Mentions / 1h", value: fmtInt(data.total_mentions_1h), sub: "tweets · speed layer", color: C.electricBl, layer: "speed" },
    { label: "Fear & Greed", value: fmt(data.avg_fear_greed), sub: fgLabel(data.avg_fear_greed), color: fgColor(data.avg_fear_greed), layer: "batch" },
    { label: "Active Spikes", value: data.active_spikes, sub: `${data.active_alerts} alerts đang mở`, color: C.amber, layer: "speed" },
  ] : [];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4,1fr)", gap: 14 }}>
      {loading
        ? Array(4).fill(0).map((_, i) => <SkeletonKpiCard key={i} />)
        : cards.map(({ label, value, sub, color, layer }) => (
          <div key={label} style={{
            background: C.bgSurface, border: `0.5px solid ${C.border}`, borderRadius: 12,
            padding: "20px 22px", position: "relative", overflow: "hidden",
            transition: "border-color 0.2s",
          }}
            onMouseEnter={e => e.currentTarget.style.borderColor = color + "55"}
            onMouseLeave={e => e.currentTarget.style.borderColor = C.border}
          >
            <div style={{ position: "absolute", top: 0, right: 0, width: 56, height: 56, borderRadius: "0 12px 0 56px", background: color + "10" }} />
            <div style={{ position: "absolute", top: 10, right: 12 }}><LayerBadge layer={layer} /></div>
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
// Nguồn: batch_sentiment_metrics (Hiệu)
function SentimentChart({ coin }) {
  const [hours, setHours] = useState(6);
  const { data, loading, error } = useCoinSentiment(coin, hours);

  const chartData = (data ?? []).map(d => ({
    time: new Date(d.window_start).toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" }),
    "F&G": Math.round(d.fear_greed_score),
    "Bullish": Math.round((d.bullish_ratio ?? 0) * 100),
    "Bearish": Math.round((d.bearish_ratio ?? 0) * 100),
    ...(d.whale_fear_greed != null && { "Whale": Math.round(d.whale_fear_greed) }),
    ...(d.retail_fear_greed != null && { "Retail": Math.round(d.retail_fear_greed) }),
  }));
  const hasSegment = chartData.some(d => d["Whale"] != null);

  return (
    <Card style={{ flex: 1, padding: "20px 22px", minHeight: 0 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <SectionTitle layer="batch">
          Sentiment — {coin ? `$${coin}` : "chọn coin ↓"}
        </SectionTitle>
        {coin && <TimeFilterBar value={hours} onChange={setHours} options={[1, 3, 6, 24]} />}
      </div>

      {!coin && (
        <div style={{ color: C.textMuted, fontSize: 12, textAlign: "center", paddingTop: 60 }}>
          👆 Nhấp vào một coin trong bảng để xem biểu đồ
        </div>
      )}
      {coin && loading && <SkeletonChart height={200} />}
      {coin && error && <ErrorBox message={error} />}
      {coin && !loading && !error && chartData.length === 0 && (
        <div style={{ color: C.textMuted, fontSize: 12, textAlign: "center", paddingTop: 60 }}>
          Chưa có dữ liệu cho ${coin} trong {hours}h qua. Thử chọn khoảng thời gian khác.
        </div>
      )}
      {coin && !loading && !error && chartData.length > 0 && (
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={chartData} margin={{ top: 4, right: 6, left: -22, bottom: 0 }}>
            <CartesianGrid strokeDasharray="2 3" stroke={C.border} />
            <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 9 }} tickLine={false} />
            <YAxis domain={[0, 100]} tick={{ fill: C.textMuted, fontSize: 9 }} tickLine={false} axisLine={false} />
            <ReferenceLine y={50} stroke={C.textDim} strokeDasharray="4 3" />
            <Tooltip contentStyle={{ background: C.bgElevated, border: `0.5px solid ${C.border}`, borderRadius: 8, fontSize: 11 }} labelStyle={{ color: C.textPri }} />
            <Legend wrapperStyle={{ fontSize: 10, color: C.textMuted }} />
            <Line type="monotone" dataKey="F&G" stroke={C.neonTeal} strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="Bullish" stroke={C.limeGreen} strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
            <Line type="monotone" dataKey="Bearish" stroke={C.neonRed} strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
            {hasSegment && <>
              <Line type="monotone" dataKey="Whale" stroke={C.amber} strokeWidth={1.5} dot={false} />
              <Line type="monotone" dataKey="Retail" stroke={C.neonPurple} strokeWidth={1.5} dot={false} />
            </>}
          </LineChart>
        </ResponsiveContainer>
      )}
    </Card>
  );
}

// ─── Alerts Feed ──────────────────────────────────────────────────────────────
// Nguồn: alerts collection (batch_job.py)
function AlertsFeed() {
  const { data, loading, error } = useAlerts("open");
  return (
    <Card style={{ width: 300, flexShrink: 0, display: "flex", flexDirection: "column" }}>
      <div style={{ padding: "18px 20px 12px", borderBottom: `0.5px solid ${C.border}`, flexShrink: 0 }}>
        <SectionTitle layer="batch">🚨 Alerts</SectionTitle>
      </div>
      <div style={{
        height: 320, overflowY: "auto", padding: "8px 20px 16px",
        scrollbarWidth: "thin", scrollbarColor: `${C.border} transparent`
      }}>
        {error && <ErrorBox message={error} />}
        {loading && Array(3).fill(0).map((_, i) => <SkeletonAlertItem key={i} />)}
        {!loading && !error && !data?.length && (
          <div style={{ fontSize: 12, color: C.textMuted, paddingTop: 16 }}>Không có alert nào đang mở.</div>
        )}
        {!loading && data?.map((a, i) => {
          const col = sevColor(a.severity);
          const icon = a.severity === "critical" ? "🚨" : a.severity === "high" ? "⚡" : a.alert_type === "spam_detected" ? "🤖" : "📡";
          const payload = a.payload ?? {};
          return (
            <div key={i} style={{ display: "flex", alignItems: "flex-start", gap: 10, padding: "10px 0", borderBottom: i < data.length - 1 ? `0.5px solid ${C.border}` : "none" }}>
              <div style={{ width: 32, height: 32, borderRadius: 16, flexShrink: 0, background: col + "15", border: `0.5px solid ${col}44`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 13 }}>{icon}</div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 2 }}>
                  <span style={{ fontSize: 10, fontWeight: 600, color: col, textTransform: "uppercase" }}>{a.alert_type}</span>
                  <span style={{ fontSize: 10, color: C.textDim }}>{timeAgo(a.created_at)}</span>
                </div>
                <p style={{ fontSize: 11, color: C.textMuted, lineHeight: 1.5, margin: "0 0 4px" }}>{a.message}</p>
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
      {!loading && (data?.length ?? 0) > 0 && (
        <div style={{ padding: "8px 20px", borderTop: `0.5px solid ${C.border}`, fontSize: 10, color: C.textDim, flexShrink: 0 }}>
          {data.length} alert đang mở · cuộn để xem thêm
        </div>
      )}
    </Card>
  );
}

// ─── Trending Table ───────────────────────────────────────────────────────────
// Nguồn: batch_sentiment_metrics (Hiệu)
function TrendingTable({ onCoinSelect, selectedCoin }) {
  const [hours, setHours] = useState(24);
  const { data, loading, error } = useBatchTrends(hours, 20);

  const th = { padding: "10px 14px", fontSize: 10, fontWeight: 600, color: C.textMuted, textAlign: "left", letterSpacing: "0.08em", textTransform: "uppercase", borderBottom: `0.5px solid ${C.border}`, whiteSpace: "nowrap" };

  return (
    <Card style={{ overflow: "hidden" }}>
      <div style={{ padding: "18px 22px 0", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <SectionTitle layer="batch">Trending Coins</SectionTitle>
        <div style={{ marginBottom: 16 }}>
          <TimeFilterBar value={hours} onChange={setHours} options={[1, 6, 24, 72]} />
        </div>
      </div>
      {error && <div style={{ padding: "0 22px 16px" }}><ErrorBox message={error} /></div>}
      {!loading && !error && !data?.length && (
        <div style={{ padding: "32px", textAlign: "center", color: C.textMuted, fontSize: 13 }}>
          Chưa có dữ liệu trong {hours}h qua. Thử chọn khoảng thời gian khác.
        </div>
      )}
      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead><tr>
            <th style={th}>#</th><th style={th}>Coin</th>
            <th style={th}>Fear & Greed</th><th style={th}>Bullish</th><th style={th}>Bearish</th>
            <th style={th}>Mentions</th><th style={th}>Engagement</th>
            <th style={th}>🐋 Whale</th><th style={th}>🛒 Retail</th>
            <th style={th}>Updated</th>
          </tr></thead>
          <tbody>
            {loading ? <SkeletonTable rows={8} cols={10} /> : data?.map((row, i) => {
              const sel = selectedCoin === row.coin;
              const color = COIN_COLORS[i % COIN_COLORS.length];
              return (
                <tr key={row.coin} onClick={() => onCoinSelect(row.coin)}
                  style={{ cursor: "pointer", background: sel ? C.electricBl + "0e" : "transparent", borderLeft: sel ? `2px solid ${C.electricBl}` : "2px solid transparent", transition: "background 0.12s" }}
                  onMouseEnter={e => { if (!sel) e.currentTarget.style.background = C.bgElevated; }}
                  onMouseLeave={e => { if (!sel) e.currentTarget.style.background = "transparent"; }}
                >
                  <td style={{ padding: "12px 14px", fontSize: 11, color: C.textDim, fontFamily: "monospace" }}>{i + 1}</td>
                  <td style={{ padding: "12px 14px" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 9 }}>
                      <div style={{ width: 28, height: 28, borderRadius: 14, background: color + "18", border: `0.5px solid ${color}55`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 9, fontWeight: 700, color, flexShrink: 0 }}>{row.coin.slice(0, 3)}</div>
                      <div>
                        <div style={{ fontSize: 13, fontWeight: 600, color: C.textPri }}>${row.coin}</div>
                        <div style={{ fontSize: 10, color: C.textDim }}>{row.snapshot_count} snapshots</div>
                      </div>
                    </div>
                  </td>
                  <td style={{ padding: "12px 14px" }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 7 }}>
                      <div style={{ width: 32, height: 32, borderRadius: 16, border: `2px solid ${fgColor(row.avg_fear_greed)}55`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 11, fontWeight: 700, color: fgColor(row.avg_fear_greed), flexShrink: 0 }}>{fmt(row.avg_fear_greed, 0)}</div>
                      <NeonBadge color={fgColor(row.avg_fear_greed)} small>{fgLabel(row.avg_fear_greed)}</NeonBadge>
                    </div>
                  </td>
                  <td style={{ padding: "12px 14px", fontSize: 12, fontFamily: "monospace", color: C.limeGreen }}>{fmt(row.avg_bullish * 100)}%</td>
                  <td style={{ padding: "12px 14px", fontSize: 12, fontFamily: "monospace", color: C.neonRed }}>{fmt(row.avg_bearish * 100)}%</td>
                  <td style={{ padding: "12px 14px", fontSize: 12, color: C.textPri, fontFamily: "monospace" }}>{fmtInt(row.total_mentions)}</td>
                  <td style={{ padding: "12px 14px", fontSize: 12, color: C.textMuted, fontFamily: "monospace" }}>{fmtInt(row.total_engagement)}</td>
                  <td style={{ padding: "12px 14px" }}>
                    {row.avg_whale_fg != null ? <NeonBadge color={fgColor(row.avg_whale_fg)} small>{fmt(row.avg_whale_fg, 0)}</NeonBadge> : <span style={{ fontSize: 11, color: C.textDim }}>—</span>}
                  </td>
                  <td style={{ padding: "12px 14px" }}>
                    {row.avg_retail_fg != null ? <NeonBadge color={fgColor(row.avg_retail_fg)} small>{fmt(row.avg_retail_fg, 0)}</NeonBadge> : <span style={{ fontSize: 11, color: C.textDim }}>—</span>}
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

// ─── Spikes Panel ─────────────────────────────────────────────────────────────
// Nguồn: speed_trend_metrics với is_spike=True (Thắng)
function SpikesPanel() {
  const { data, loading } = useSpeedSpikes(1);
  if (loading || !data?.length) return null;
  return (
    <Card style={{ padding: "18px 22px" }}>
      <SectionTitle layer="speed">⚡ Live Spikes</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
        {data.map(spike => (
          <div key={spike.symbol + spike.window_start} style={{ background: C.bgElevated, border: `0.5px solid ${C.amber}33`, borderRadius: 10, padding: "11px 16px", display: "flex", flexDirection: "column", gap: 5, minWidth: 160 }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <span style={{ fontSize: 14, fontWeight: 700, color: C.amber }}>${spike.symbol}</span>
              <NeonBadge color={C.amber} small>×{fmt(spike.growth_rate)}</NeonBadge>
            </div>
            <div style={{ fontSize: 11, color: C.textMuted }}>{fmtInt(spike.mention_count)} mentions · {spike.unique_authors} authors</div>
            {spike.spike_reasons?.length > 0 && (
              <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                {spike.spike_reasons.map(r => <span key={r} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 10, background: C.amber + "15", color: C.amber, border: `0.5px solid ${C.amber}44` }}>{r}</span>)}
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
// PAGES
// ═══════════════════════════════════════════════════════════════════════════════

function PageTrending() {
  const [coin, setCoin] = useState(null);
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <KpiCards />
      <div style={{ display: "flex", gap: 16, alignItems: "stretch" }}>
        <SentimentChart coin={coin} />
        <AlertsFeed />
      </div>
      <TrendHeatmap />
      <SpikesPanel />
      <TrendingTable onCoinSelect={setCoin} selectedCoin={coin} />
    </div>
  );
}

function PageWhales() {
  const [hours, setHours] = useState(24);
  const { data, loading, error } = useBatchTrends(hours, 20);

  const whaleCoins = (data ?? []).filter(r => r.avg_whale_fg != null);
  const divergent = whaleCoins.filter(r => r.avg_retail_fg != null && Math.abs((r.avg_whale_fg ?? 50) - (r.avg_retail_fg ?? 50)) >= 15);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      {/* Header */}
      <Card style={{ padding: "18px 24px", border: `0.5px solid ${C.amber}33`, display: "flex", gap: 20, alignItems: "flex-start" }}>
        <span style={{ fontSize: 28, lineHeight: 1 }}>🐋</span>
        <div style={{ flex: 1 }}>
          <div style={{ fontSize: 15, fontWeight: 700, color: C.textPri, marginBottom: 6 }}>Whale Signal Monitor</div>
          <div style={{ fontSize: 12, color: C.textMuted, lineHeight: 1.7 }}>
            So sánh tâm lý <span style={{ color: C.amber }}>Whale</span> (<code style={{ fontSize: 11 }}>author_weight ≥ 2.0</code>: saylor, VitalikButerin...) vs <span style={{ color: C.electricBl }}>Retail</span>.<br />
            Divergence ≥ 15 điểm F&G = tín hiệu âm thầm tích lũy hoặc phân phối.
          </div>
          <div style={{ marginTop: 6, fontSize: 11, color: C.textDim }}>
            Nguồn: <code style={{ color: C.neonTeal }}>batch_sentiment_metrics</code> · field <code style={{ color: C.neonTeal }}>whale_fear_greed</code> + <code style={{ color: C.neonTeal }}>retail_fear_greed</code>
          </div>
        </div>
        <TimeFilterBar value={hours} onChange={setHours} options={[6, 24, 72]} />
      </Card>

      {error && <ErrorBox message={error} />}

      {/* Divergence banner */}
      {!loading && divergent.length > 0 && (
        <div style={{ background: C.amber + "0e", border: `0.5px solid ${C.amber}55`, borderRadius: 10, padding: "12px 20px", display: "flex", alignItems: "center", gap: 12 }}>
          <span style={{ fontSize: 18 }}>⚠️</span>
          <div>
            <div style={{ fontSize: 12, fontWeight: 600, color: C.amber, marginBottom: 3 }}>{divergent.length} Divergence Signal phát hiện</div>
            <div style={{ fontSize: 11, color: C.textMuted }}>
              Whale và Retail đi ngược chiều ≥15 điểm:&nbsp;
              {divergent.map(r => <span key={r.coin} style={{ color: C.amber, fontWeight: 600 }}>${r.coin} </span>)}
            </div>
          </div>
        </div>
      )}

      {/* Empty state */}
      {!loading && !error && whaleCoins.length === 0 && (
        <Card style={{ padding: "40px 32px", textAlign: "center" }}>
          <div style={{ fontSize: 36, marginBottom: 12 }}>🐋</div>
          <div style={{ fontSize: 14, fontWeight: 600, color: C.textPri, marginBottom: 10 }}>Chưa có dữ liệu Whale</div>
          <div style={{ fontSize: 12, color: C.textMuted, lineHeight: 1.8, maxWidth: 480, margin: "0 auto" }}>
            Cần chạy batch job để phân tách whale/retail sentiment:<br />
            <code style={{ color: C.neonTeal }}>python src/processing/batch_layer/batch_job.py --demo</code>
          </div>
        </Card>
      )}

      {/* Skeletons */}
      {loading && (
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(300px,1fr))", gap: 12 }}>
          {Array(6).fill(0).map((_, i) => <SkeletonCard key={i} height={180} />)}
        </div>
      )}

      {/* Whale cards */}
      {!loading && whaleCoins.length > 0 && (
        <>
          <div style={{ display: "flex", gap: 16, fontSize: 11 }}>
            {[{ c: C.amber, l: "Whale (≥ 2.0 weight)" }, { c: C.electricBl, l: "Retail (< 2.0 weight)" }, { c: C.neonRed, l: "Divergence ≥ 15 điểm" }].map(({ c, l }) => (
              <div key={l} style={{ display: "flex", alignItems: "center", gap: 6 }}>
                <div style={{ width: 10, height: 10, borderRadius: 5, background: c }} />
                <span style={{ color: C.textMuted }}>{l}</span>
              </div>
            ))}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(300px,1fr))", gap: 12 }}>
            {whaleCoins.map((row, i) => {
              const wFG = row.avg_whale_fg ?? 50;
              const rFG = row.avg_retail_fg ?? null;
              const diff = rFG != null ? Math.abs(wFG - rFG) : 0;
              const isDivergent = diff >= 15;
              return (
                <Card key={row.coin} style={{ padding: "18px 20px", border: isDivergent ? `0.5px solid ${C.neonRed}66` : `0.5px solid ${C.border}`, position: "relative", overflow: "hidden" }}>
                  {isDivergent && <div style={{ position: "absolute", top: 0, right: 0, width: 40, height: 40, borderRadius: "0 12px 0 40px", background: C.neonRed + "20" }} />}
                  {/* Header */}
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 14 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <div style={{ width: 28, height: 28, borderRadius: 14, background: COIN_COLORS[i % COIN_COLORS.length] + "20", border: `0.5px solid ${COIN_COLORS[i % COIN_COLORS.length]}55`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 9, fontWeight: 700, color: COIN_COLORS[i % COIN_COLORS.length] }}>{row.coin.slice(0, 3)}</div>
                      <span style={{ fontSize: 14, fontWeight: 700, color: C.textPri }}>${row.coin}</span>
                    </div>
                    {isDivergent && <NeonBadge color={C.neonRed} small>⚡ Divergence {fmt(diff, 0)}pt</NeonBadge>}
                  </div>
                  {/* Whale bar */}
                  <div style={{ marginBottom: 8 }}>
                    <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3, fontSize: 10 }}>
                      <span style={{ color: C.amber }}>🐋 Whale F&G</span>
                      <span style={{ color: fgColor(wFG), fontWeight: 600, fontFamily: "monospace" }}>{fmt(wFG, 0)} — {fgLabel(wFG)}</span>
                    </div>
                    <div style={{ height: 6, borderRadius: 3, background: C.bgElevated, overflow: "hidden" }}>
                      <div style={{ height: "100%", borderRadius: 3, width: `${wFG}%`, background: `linear-gradient(90deg,${fgColor(wFG)}88,${fgColor(wFG)})`, transition: "width 0.6s ease" }} />
                    </div>
                  </div>
                  {/* Retail bar */}
                  {rFG != null && (
                    <div style={{ marginBottom: 8 }}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3, fontSize: 10 }}>
                        <span style={{ color: C.electricBl }}>🛒 Retail F&G</span>
                        <span style={{ color: fgColor(rFG), fontWeight: 600, fontFamily: "monospace" }}>{fmt(rFG, 0)} — {fgLabel(rFG)}</span>
                      </div>
                      <div style={{ height: 6, borderRadius: 3, background: C.bgElevated, overflow: "hidden" }}>
                        <div style={{ height: "100%", borderRadius: 3, width: `${rFG}%`, background: `linear-gradient(90deg,${fgColor(rFG)}88,${fgColor(rFG)})`, transition: "width 0.6s ease" }} />
                      </div>
                    </div>
                  )}
                  {/* Stats */}
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 6, marginTop: 10 }}>
                    {[{ l: "Mentions", v: fmtInt(row.total_mentions), c: C.textPri }, { l: "Bullish", v: `${fmt(row.avg_bullish * 100)}%`, c: C.limeGreen }, { l: "Bearish", v: `${fmt(row.avg_bearish * 100)}%`, c: C.neonRed }].map(({ l, v, c }) => (
                      <div key={l} style={{ background: C.bgElevated, borderRadius: 6, padding: "6px 8px", textAlign: "center" }}>
                        <div style={{ fontSize: 9, color: C.textDim, marginBottom: 2 }}>{l}</div>
                        <div style={{ fontSize: 12, fontWeight: 600, color: c, fontFamily: "monospace" }}>{v}</div>
                      </div>
                    ))}
                  </div>
                  <div style={{ marginTop: 8, fontSize: 10, color: C.textDim }}>Updated {timeAgo(row.latest_at)}</div>
                </Card>
              );
            })}
          </div>
        </>
      )}
    </div>
  );
}

function PageAlerts() {
  const [statusFilter, setStatusFilter] = useState("open");
  const { data, loading, error } = useAlerts(statusFilter);
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <div style={{ fontSize: 16, fontWeight: 700, color: C.textPri, marginBottom: 4 }}>🚨 Alert Center</div>
          <div style={{ fontSize: 11, color: C.textMuted }}>Nguồn: <code style={{ color: C.neonTeal }}>alerts</code> collection — spam detection + whale signals từ batch_job.py</div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          {[["open", "🔴 Open"], ["closed", "✅ Closed"], ["", "All"]].map(([s, l]) => (
            <button key={s} onClick={() => setStatusFilter(s)} style={{ padding: "5px 14px", borderRadius: 20, fontSize: 11, cursor: "pointer", fontFamily: "inherit", background: statusFilter === s ? C.electricBl + "20" : "transparent", border: `0.5px solid ${statusFilter === s ? C.electricBl : C.border}`, color: statusFilter === s ? C.electricBl : C.textMuted }}>{l}</button>
          ))}
        </div>
      </div>
      {error && <ErrorBox message={error} />}
      {!loading && !data?.length && (
        <Card style={{ padding: "40px", textAlign: "center" }}>
          <div style={{ fontSize: 32, marginBottom: 12 }}>✅</div>
          <div style={{ color: C.textMuted, fontSize: 13 }}>Không có alert nào {statusFilter && `(${statusFilter})`}</div>
        </Card>
      )}
      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {!loading && data?.map((a, i) => {
          const col = sevColor(a.severity);
          const icon = a.severity === "critical" ? "🚨" : a.severity === "high" ? "⚡" : a.alert_type === "spam_detected" ? "🤖" : "📡";
          const payload = a.payload ?? {};
          return (
            <Card key={i} style={{ padding: "14px 20px" }}>
              <div style={{ display: "flex", alignItems: "flex-start", gap: 14 }}>
                <div style={{ width: 38, height: 38, borderRadius: 19, flexShrink: 0, background: col + "15", border: `0.5px solid ${col}44`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 16 }}>{icon}</div>
                <div style={{ flex: 1 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                      <span style={{ fontSize: 12, fontWeight: 600, color: col, textTransform: "uppercase" }}>{a.alert_type}</span>
                      <NeonBadge color={{ open: C.neonRed, closed: C.limeGreen }[a.status] ?? C.textMuted} small>{a.status}</NeonBadge>
                      <NeonBadge color={col} small>{a.severity}</NeonBadge>
                    </div>
                    <span style={{ fontSize: 11, color: C.textDim }}>{timeAgo(a.created_at)}</span>
                  </div>
                  <p style={{ fontSize: 12, color: C.textMuted, lineHeight: 1.6, margin: "0 0 6px" }}>{a.message}</p>
                  {Object.keys(payload).length > 0 && (
                    <div style={{ display: "flex", gap: 6, flexWrap: "wrap" }}>
                      {payload.coin && <NeonBadge color={col} small>${payload.coin}</NeonBadge>}
                      {payload.spam_count && <NeonBadge color={C.textMuted} small>{payload.spam_count} spam</NeonBadge>}
                      {payload.spam_ratio && <NeonBadge color={C.textMuted} small>{(payload.spam_ratio * 100).toFixed(1)}% ratio</NeonBadge>}
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

// ─── Pipeline Status Page ─────────────────────────────────────────────────────
function PagePipeline() {
  const { data: batchData, loading: bL } = useBatchTrends(24, 1);
  const { data: speedData, loading: sL } = useSpeedTrends(1, 1);
  const { data: jobs, loading: jL } = useJobHistory(10);
  const { data: badRec, loading: bRL } = useBadRecords(6);

  const batchAlive = (batchData?.length ?? 0) > 0;
  const speedAlive = (speedData?.length ?? 0) > 0;

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
      <div>
        <div style={{ fontSize: 16, fontWeight: 700, color: C.textPri, marginBottom: 4 }}>📊 Pipeline Status</div>
        <div style={{ fontSize: 11, color: C.textMuted }}>Trạng thái dữ liệu trong MongoDB — do An setup, Thắng và Hiệu ghi</div>
      </div>

      {/* Layer status cards */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        {[
          {
            label: "Batch Layer", alive: batchAlive, loading: bL, color: C.electricBl,
            collections: ["batch_sentiment_metrics", "batch_trend_spikes", "alerts", "batch_job_runs"],
            cmd: "python src/processing/batch_layer/batch_job.py --demo",
            desc: batchAlive ? "Data có sẵn trong MongoDB" : "Chưa có data",
            refresh: "Định kỳ (hourly/daily)"
          },
          {
            label: "Speed Layer", alive: speedAlive, loading: sL, color: C.neonTeal,
            collections: ["speed_trend_metrics", "speed_bad_records"],
            cmd: "python src/processing/speed_layer/stream_job.py --demo",
            desc: speedAlive ? "Spark Streaming đang ghi data" : "Chưa có data",
            refresh: "Real-time (5-phút micro-batch)"
          },
        ].map(({ label, alive, loading: ld, color, collections, cmd, desc, refresh }) => (
          <Card key={label} style={{ padding: "18px 22px", border: `0.5px solid ${alive ? color + "55" : C.border}` }}>
            <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 12 }}>
              <div style={{ width: 38, height: 38, borderRadius: 19, background: alive ? color + "18" : C.bgElevated, border: `0.5px solid ${alive ? color + "55" : C.border}`, display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18 }}>
                {ld ? "…" : alive ? "✅" : "⭕"}
              </div>
              <div>
                <div style={{ fontSize: 14, fontWeight: 700, color: alive ? color : C.textMuted }}>{label}</div>
                <span style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, fontWeight: 700, background: alive ? color + "18" : C.bgElevated, color: alive ? color : C.textDim, border: `0.5px solid ${alive ? color + "44" : C.border}` }}>
                  {alive ? "● ACTIVE" : "○ INACTIVE"}
                </span>
              </div>
            </div>
            <div style={{ fontSize: 12, color: C.textMuted, marginBottom: 10 }}>{desc}</div>
            <div style={{ display: "flex", gap: 5, flexWrap: "wrap", marginBottom: 10 }}>
              {collections.map(c => <code key={c} style={{ fontSize: 9, padding: "1px 6px", borderRadius: 4, background: C.bgElevated, color: C.textDim, border: `0.5px solid ${C.border}` }}>{c}</code>)}
            </div>
            <div style={{ fontSize: 10, color: C.textDim, marginBottom: alive ? 0 : 10 }}>🔄 {refresh}</div>
            {!alive && (
              <div style={{ background: C.bgElevated, border: `0.5px solid ${C.border}`, borderRadius: 6, padding: "8px 12px", marginTop: 8 }}>
                <div style={{ fontSize: 9, color: C.textDim, marginBottom: 4 }}>Lệnh khởi động:</div>
                <code style={{ fontSize: 10, color }}>{cmd}</code>
              </div>
            )}
          </Card>
        ))}
      </div>

      {/* Bad records chart */}
      {!bRL && (badRec?.length ?? 0) > 0 && (
        <Card style={{ padding: "18px 22px" }}>
          <SectionTitle layer="speed">Bad Records (6h qua)</SectionTitle>
          <ResponsiveContainer width="100%" height={160}>
            <BarChart data={badRec} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="2 3" stroke={C.border} />
              <XAxis dataKey="invalid_reason" tick={{ fill: C.textMuted, fontSize: 9 }} tickLine={false} />
              <YAxis tick={{ fill: C.textMuted, fontSize: 9 }} tickLine={false} axisLine={false} />
              <Tooltip contentStyle={{ background: C.bgElevated, border: `0.5px solid ${C.border}`, borderRadius: 8, fontSize: 11 }} />
              <Bar dataKey="bad_record_count" fill={C.neonRed} radius={[3, 3, 0, 0]} opacity={0.8} />
            </BarChart>
          </ResponsiveContainer>
        </Card>
      )}

      {/* Job history */}
      <Card style={{ overflow: "hidden" }}>
        <div style={{ padding: "18px 22px 0" }}>
          <SectionTitle layer="batch">Lịch sử Batch Job Runs</SectionTitle>
        </div>
        {jL && <div style={{ padding: "16px 22px" }}><SkeletonBox h={12} style={{ marginBottom: 8 }} /><SkeletonBox h={12} w="80%" /></div>}
        {!jL && !(jobs?.length) && <div style={{ padding: "16px 22px", fontSize: 12, color: C.textMuted }}>Chưa có lịch sử.</div>}
        {!jL && jobs?.length > 0 && (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr>
                {["Mode", "Status", "Date", "Tweets", "Clean", "Spam", "Coins", "Spikes", "Duration", "Ran at"].map(h => (
                  <th key={h} style={{ padding: "10px 14px", fontSize: 10, fontWeight: 600, color: C.textMuted, textAlign: "left", letterSpacing: "0.06em", textTransform: "uppercase", borderBottom: `0.5px solid ${C.border}`, whiteSpace: "nowrap" }}>{h}</th>
                ))}
              </tr></thead>
              <tbody>
                {jobs.map((j, i) => (
                  <tr key={i} style={{ borderBottom: `0.5px solid ${C.border}` }}
                    onMouseEnter={e => e.currentTarget.style.background = C.bgElevated}
                    onMouseLeave={e => e.currentTarget.style.background = "transparent"}
                  >
                    <td style={{ padding: "10px 14px" }}><NeonBadge color={C.electricBl} small>{j.mode}</NeonBadge></td>
                    <td style={{ padding: "10px 14px" }}><NeonBadge color={j.status === "success" ? C.limeGreen : C.neonRed} small>{j.status}</NeonBadge></td>
                    <td style={{ padding: "10px 14px", fontSize: 11, color: C.textMuted, fontFamily: "monospace" }}>{j.target_date ?? "-"}</td>
                    <td style={{ padding: "10px 14px", fontSize: 11, color: C.textPri, fontFamily: "monospace" }}>{fmtInt(j.total_tweets)}</td>
                    <td style={{ padding: "10px 14px", fontSize: 11, color: C.limeGreen, fontFamily: "monospace" }}>{fmtInt(j.clean_tweets)}</td>
                    <td style={{ padding: "10px 14px", fontSize: 11, color: C.neonRed, fontFamily: "monospace" }}>{fmtInt(j.spam_tweets)}</td>
                    <td style={{ padding: "10px 14px", fontSize: 11, color: C.textPri, fontFamily: "monospace" }}>{j.coins_processed}</td>
                    <td style={{ padding: "10px 14px", fontSize: 11, color: C.amber, fontFamily: "monospace" }}>{j.spikes_detected?.length ?? 0}</td>
                    <td style={{ padding: "10px 14px", fontSize: 11, color: C.textMuted, fontFamily: "monospace" }}>{j.duration_seconds ? `${j.duration_seconds.toFixed(1)}s` : "—"}</td>
                    <td style={{ padding: "10px 14px", fontSize: 10, color: C.textDim }}>{timeAgo(j.executed_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}

// ═══════════════════════════════════════════════════════════════════════════════
// ROOT
// ═══════════════════════════════════════════════════════════════════════════════
const TABS = [
  { id: "trending", label: "Trending", icon: "📈" },
  //  { id: "whales", label: "Whales", icon: "🐋" },
  { id: "alerts", label: "Alerts", icon: "🚨" },
  { id: "pipeline", label: "Pipeline", icon: "📊" },
];

export default function Dashboard() {
  const [tab, setTab] = useState("trending");
  const { data: alertData } = useAlerts("open");
  const alertCount = alertData?.length ?? 0;

  return (
    <div style={{ minHeight: "100vh", background: C.bgVoid, color: C.textPri, fontFamily: "'DM Mono','Fira Code',monospace" }}>
      {/* Header */}
      <header style={{ borderBottom: `0.5px solid ${C.border}`, padding: "0 32px", display: "flex", alignItems: "center", height: 52, background: C.bgSurface, position: "sticky", top: 0, zIndex: 100 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, cursor: "pointer" }} onClick={() => setTab("trending")}>
          <span style={{ fontSize: 20 }}>⬡</span>
          <span style={{ fontSize: 14, fontWeight: 700, color: C.textPri, letterSpacing: "0.05em" }}>CryptoTrend</span>
          <NeonBadge color={C.neonTeal}>LIVE</NeonBadge>
        </div>
        <nav style={{ marginLeft: "auto", display: "flex", gap: 4 }}>
          {TABS.map(t => {
            const isActive = tab === t.id;
            return (
              <button key={t.id} onClick={() => setTab(t.id)} style={{
                position: "relative", display: "flex", alignItems: "center", gap: 6,
                padding: "6px 14px", fontSize: 12, fontWeight: isActive ? 600 : 400,
                cursor: "pointer", border: "none", fontFamily: "inherit",
                background: isActive ? C.electricBl + "18" : "transparent",
                color: isActive ? C.electricBl : C.textMuted,
                borderBottom: isActive ? `2px solid ${C.electricBl}` : "2px solid transparent",
                borderRadius: 0, transition: "all 0.15s",
              }}
                onMouseEnter={e => { if (!isActive) e.currentTarget.style.color = C.textPri; }}
                onMouseLeave={e => { if (!isActive) e.currentTarget.style.color = C.textMuted; }}
              >
                <span>{t.icon}</span>
                <span style={{ letterSpacing: "0.04em" }}>{t.label}</span>
                {t.id === "alerts" && alertCount > 0 && (
                  <span style={{ position: "absolute", top: 4, right: 6, width: 16, height: 16, borderRadius: 8, background: C.neonRed, color: "#fff", fontSize: 9, fontWeight: 700, display: "flex", alignItems: "center", justifyContent: "center" }}>
                    {alertCount > 9 ? "9+" : alertCount}
                  </span>
                )}
              </button>
            );
          })}
        </nav>
      </header>

      <TickerBar />

      <main style={{ padding: "24px 32px 56px" }}>
        {tab === "trending" && <PageTrending />}
        {tab === "whales" && <PageWhales />}
        {tab === "alerts" && <PageAlerts />}
        {tab === "pipeline" && <PagePipeline />}
      </main>
    </div>
  );
}
