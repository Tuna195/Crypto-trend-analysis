/**
 * Dashboard.jsx
 * ==============
 * Trang Dashboard chính — "Void Market" Web3 Dark Theme.
 *
 * Cấu trúc layout:
 *   ┌─ TickerBar (live price strip) ─────────────────────────┐
 *   ├─ KPI Cards × 4 ─────────────────────────────────────────┤
 *   ├─ TrendChart (Recharts)  │  AI Sentiment Panel ──────────┤
 *   ├─ TrendingTable (full)   │  WhaleAlertsFeed ─────────────┤
 *   └─ SpikeAlerts row ───────────────────────────────────────┘
 */

import { useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import {
  useSummary, useTrends, useCoinSentiment,
  useSpikes, useAlerts,
} from "../hooks/useDashboard";
import {
  SkeletonKpiCard, SkeletonTable, SkeletonChart, SkeletonAlertItem,
} from "./Skeleton";

// ─── Design Tokens ──────────────────────────────────────────────────────────
const C = {
  bgVoid:     "#0a0d14",
  bgSurface:  "#111620",
  bgElevated: "#1a2030",
  border:     "#1e2a40",
  borderHov:  "#2a3a58",
  textPri:    "#e2e8f0",
  textMuted:  "#7a90b0",
  textDim:    "#3d5275",
  neonTeal:   "#00f5a0",
  limeGreen:  "#22c97a",
  electricBl: "#5b8cff",
  neonPurple: "#a78bfa",
  neonRed:    "#f05252",
  amber:      "#f59e0b",
};

// ─── Helpers ─────────────────────────────────────────────────────────────────
function fgColor(score) {
  if (score >= 70) return C.neonTeal;
  if (score >= 55) return C.limeGreen;
  if (score >= 45) return C.amber;
  return C.neonRed;
}

function fgLabel(score) {
  if (score >= 70) return "Greed";
  if (score >= 55) return "Neutral+";
  if (score >= 45) return "Neutral";
  return "Fear";
}

function severityColor(s) {
  return { critical: C.neonRed, high: C.amber, medium: C.electricBl, low: C.textMuted }[s] ?? C.textMuted;
}

function timeAgo(iso) {
  const diff = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

// ─── Sub-components ──────────────────────────────────────────────────────────

/** Glowing neon label tag */
function NeonBadge({ children, color = C.neonTeal }) {
  return (
    <span style={{
      display: "inline-block",
      padding: "2px 10px",
      borderRadius: 20,
      fontSize: 11,
      fontWeight: 600,
      letterSpacing: "0.05em",
      color,
      background: color + "18",
      border: `0.5px solid ${color}55`,
    }}>
      {children}
    </span>
  );
}

/** Section heading với neon accent bar */
function SectionTitle({ children }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 16 }}>
      <div style={{ width: 3, height: 18, borderRadius: 2, background: C.electricBl }} />
      <span style={{ fontSize: 13, fontWeight: 600, color: C.textPri, letterSpacing: "0.06em", textTransform: "uppercase" }}>
        {children}
      </span>
    </div>
  );
}

/** Error state */
function ErrorBox({ message }) {
  return (
    <div style={{
      padding: "16px 20px", borderRadius: 10,
      background: C.neonRed + "12", border: `0.5px solid ${C.neonRed}44`,
      color: C.neonRed, fontSize: 13,
    }}>
      ⚠ {message}
    </div>
  );
}

// ─── KPI Cards ───────────────────────────────────────────────────────────────
function KpiCards() {
  const { data, loading, error } = useSummary();

  const cards = data ? [
    { label: "Top Trending",     value: `$${data.top_trending_coin}`, sub: "by Fear & Greed score", color: C.neonTeal },
    { label: "Tweets / 1h",      value: data.total_mentions_1h.toLocaleString(), sub: "across all tracked coins", color: C.electricBl },
    { label: "Fear & Greed",     value: data.avg_fear_greed.toFixed(1), sub: fgLabel(data.avg_fear_greed), color: fgColor(data.avg_fear_greed) },
    { label: "Whale Alerts",     value: data.active_whale_alerts, sub: "alerts open", color: C.amber },
  ] : [];

  if (error) return <ErrorBox message={error} />;

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14, marginBottom: 20 }}>
      {loading
        ? Array(4).fill(0).map((_, i) => <SkeletonKpiCard key={i} />)
        : cards.map(({ label, value, sub, color }) => (
            <div key={label} style={{
              background: C.bgSurface,
              border: `0.5px solid ${C.border}`,
              borderRadius: 12,
              padding: "20px 24px",
              position: "relative",
              overflow: "hidden",
              transition: "border-color 0.2s",
            }}
              onMouseEnter={e => e.currentTarget.style.borderColor = color + "66"}
              onMouseLeave={e => e.currentTarget.style.borderColor = C.border}
            >
              {/* glow accent corner */}
              <div style={{
                position: "absolute", top: 0, right: 0,
                width: 60, height: 60, borderRadius: "0 12px 0 60px",
                background: color + "12",
              }} />
              <div style={{ fontSize: 10, color: C.textMuted, letterSpacing: "0.1em", textTransform: "uppercase", marginBottom: 8 }}>
                {label}
              </div>
              <div style={{ fontSize: 28, fontWeight: 700, color, fontFamily: "monospace", marginBottom: 6 }}>
                {value}
              </div>
              <div style={{ fontSize: 11, color: C.textDim }}>
                {sub}
              </div>
            </div>
          ))
      }
    </div>
  );
}

// ─── Trend Chart ─────────────────────────────────────────────────────────────
const COIN_COLORS = ["#00f5a0", "#5b8cff", "#a78bfa", "#f59e0b", "#f05252"];

function TrendChart({ selectedCoin, onCoinClick }) {
  const { data, loading, error } = useCoinSentiment(selectedCoin, 6);

  // Flatten + format cho Recharts
  const chartData = data
    ? data.slice().reverse().map(d => ({
        time: new Date(d.window_start).toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" }),
        "Fear & Greed": Math.round(d.fear_greed_score),
        "Bullish %":    Math.round(d.bullish_ratio * 100),
        "Bearish %":    Math.round(d.bearish_ratio * 100),
      }))
    : [];

  return (
    <div style={{
      background: C.bgSurface, border: `0.5px solid ${C.border}`,
      borderRadius: 12, padding: "20px 24px", flex: 1,
    }}>
      <SectionTitle>
        Sentiment Timeline — {selectedCoin ? `$${selectedCoin}` : "Chọn coin"}
      </SectionTitle>

      {!selectedCoin && (
        <div style={{ color: C.textMuted, fontSize: 13, textAlign: "center", paddingTop: 40 }}>
          👆 Nhấp vào một coin trong bảng để xem biểu đồ
        </div>
      )}

      {selectedCoin && loading && <SkeletonChart height={200} />}
      {selectedCoin && error   && <ErrorBox message={error} />}

      {selectedCoin && !loading && !error && chartData.length > 0 && (
        <ResponsiveContainer width="100%" height={200}>
          <LineChart data={chartData} margin={{ top: 4, right: 8, left: -20, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke={C.border} />
            <XAxis dataKey="time" tick={{ fill: C.textMuted, fontSize: 10 }} tickLine={false} />
            <YAxis tick={{ fill: C.textMuted, fontSize: 10 }} tickLine={false} axisLine={false} />
            <Tooltip
              contentStyle={{ background: C.bgElevated, border: `0.5px solid ${C.border}`, borderRadius: 8, fontSize: 12 }}
              labelStyle={{ color: C.textPri }}
              itemStyle={{ color: C.textMuted }}
            />
            <Legend wrapperStyle={{ fontSize: 11, color: C.textMuted }} />
            <Line type="monotone" dataKey="Fear & Greed" stroke={C.neonTeal}   strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="Bullish %"    stroke={C.limeGreen}  strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
            <Line type="monotone" dataKey="Bearish %"    stroke={C.neonRed}    strokeWidth={1.5} dot={false} strokeDasharray="4 2" />
          </LineChart>
        </ResponsiveContainer>
      )}

      {selectedCoin && !loading && chartData.length === 0 && (
        <div style={{ color: C.textMuted, fontSize: 13, textAlign: "center", paddingTop: 40 }}>
          Chưa có data sentiment cho {selectedCoin} trong 6 giờ qua.
        </div>
      )}
    </div>
  );
}

// ─── Trending Table ───────────────────────────────────────────────────────────
function TrendingTable({ onCoinSelect, selectedCoin }) {
  const [hours, setHours] = useState(24);
  const { data, loading, error } = useTrends(hours, 20);

  const thStyle = {
    padding: "10px 16px", fontSize: 10, fontWeight: 600, color: C.textMuted,
    textAlign: "left", letterSpacing: "0.08em", textTransform: "uppercase",
    borderBottom: `0.5px solid ${C.border}`, whiteSpace: "nowrap",
  };

  return (
    <div style={{
      background: C.bgSurface, border: `0.5px solid ${C.border}`,
      borderRadius: 12, overflow: "hidden", flex: 1,
    }}>
      <div style={{ padding: "20px 24px 0", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <SectionTitle>Trending Coins</SectionTitle>
        <div style={{ display: "flex", gap: 6, marginBottom: 16 }}>
          {[1, 6, 24].map(h => (
            <button key={h} onClick={() => setHours(h)} style={{
              padding: "4px 12px", borderRadius: 20, fontSize: 11, cursor: "pointer",
              background: hours === h ? C.electricBl + "20" : "transparent",
              border: `0.5px solid ${hours === h ? C.electricBl : C.border}`,
              color: hours === h ? C.electricBl : C.textMuted,
              transition: "all 0.15s",
            }}>
              {h}h
            </button>
          ))}
        </div>
      </div>

      {error && <div style={{ padding: "0 24px 20px" }}><ErrorBox message={error} /></div>}

      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th style={thStyle}>#</th>
              <th style={thStyle}>Coin</th>
              <th style={thStyle}>Fear & Greed</th>
              <th style={thStyle}>Bullish</th>
              <th style={thStyle}>Bearish</th>
              <th style={thStyle}>Snapshots</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <SkeletonTable rows={8} />
            ) : (
              data?.map((row, i) => {
                const isSelected = selectedCoin === row.coin;
                return (
                  <tr
                    key={row.coin}
                    onClick={() => onCoinSelect(row.coin)}
                    style={{
                      cursor: "pointer",
                      background: isSelected ? C.electricBl + "10" : "transparent",
                      borderLeft: isSelected ? `2px solid ${C.electricBl}` : "2px solid transparent",
                      transition: "background 0.15s",
                    }}
                    onMouseEnter={e => { if (!isSelected) e.currentTarget.style.background = C.bgElevated; }}
                    onMouseLeave={e => { if (!isSelected) e.currentTarget.style.background = "transparent"; }}
                  >
                    <td style={{ padding: "14px 16px", fontSize: 12, color: C.textDim, fontFamily: "monospace" }}>
                      {i + 1}
                    </td>
                    <td style={{ padding: "14px 16px" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                        <div style={{
                          width: 28, height: 28, borderRadius: 14,
                          background: COIN_COLORS[i % COIN_COLORS.length] + "20",
                          border: `0.5px solid ${COIN_COLORS[i % COIN_COLORS.length]}55`,
                          display: "flex", alignItems: "center", justifyContent: "center",
                          fontSize: 10, fontWeight: 700, color: COIN_COLORS[i % COIN_COLORS.length],
                        }}>
                          {row.coin.slice(0, 2)}
                        </div>
                        <span style={{ fontSize: 13, fontWeight: 600, color: C.textPri }}>
                          ${row.coin}
                        </span>
                      </div>
                    </td>
                    <td style={{ padding: "14px 16px" }}>
                      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                        <div style={{
                          width: 36, height: 36, borderRadius: 18,
                          border: `2px solid ${fgColor(row.avg_fear_greed)}55`,
                          display: "flex", alignItems: "center", justifyContent: "center",
                          fontSize: 11, fontWeight: 700, color: fgColor(row.avg_fear_greed),
                        }}>
                          {row.avg_fear_greed.toFixed(0)}
                        </div>
                        <NeonBadge color={fgColor(row.avg_fear_greed)}>
                          {fgLabel(row.avg_fear_greed)}
                        </NeonBadge>
                      </div>
                    </td>
                    <td style={{ padding: "14px 16px", fontSize: 13, fontFamily: "monospace", color: C.limeGreen }}>
                      {(row.avg_bullish * 100).toFixed(1)}%
                    </td>
                    <td style={{ padding: "14px 16px", fontSize: 13, fontFamily: "monospace", color: C.neonRed }}>
                      {(row.avg_bearish * 100).toFixed(1)}%
                    </td>
                    <td style={{ padding: "14px 16px", fontSize: 12, color: C.textMuted }}>
                      {row.snapshot_count}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Whale Alerts Feed ────────────────────────────────────────────────────────
function WhaleAlertsFeed() {
  const { data, loading, error } = useAlerts("open");

  return (
    <div style={{
      background: C.bgSurface, border: `0.5px solid ${C.border}`,
      borderRadius: 12, padding: "20px 24px", width: 300, flexShrink: 0,
    }}>
      <SectionTitle>🐋 Whale Alerts</SectionTitle>

      {error   && <ErrorBox message={error} />}
      {loading && Array(4).fill(0).map((_, i) => <SkeletonAlertItem key={i} />)}

      {!loading && !error && data?.length === 0 && (
        <div style={{ color: C.textMuted, fontSize: 13 }}>Không có alert nào đang mở.</div>
      )}

      {!loading && data?.map((alert, i) => (
        <div key={i} style={{
          display: "flex", alignItems: "flex-start", gap: 12,
          padding: "12px 0",
          borderBottom: i < data.length - 1 ? `0.5px solid ${C.border}` : "none",
        }}>
          {/* severity dot */}
          <div style={{
            width: 36, height: 36, borderRadius: 18, flexShrink: 0,
            background: severityColor(alert.severity) + "18",
            border: `0.5px solid ${severityColor(alert.severity)}55`,
            display: "flex", alignItems: "center", justifyContent: "center",
            fontSize: 14,
          }}>
            {alert.severity === "critical" ? "🚨" : alert.severity === "high" ? "⚡" : "📡"}
          </div>
          <div style={{ flex: 1 }}>
            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
              <span style={{ fontSize: 11, fontWeight: 600, color: severityColor(alert.severity), textTransform: "uppercase" }}>
                {alert.alert_type}
              </span>
              <span style={{ fontSize: 10, color: C.textDim }}>
                {timeAgo(alert.created_at)}
              </span>
            </div>
            <p style={{ fontSize: 12, color: C.textMuted, lineHeight: 1.5, margin: 0 }}>
              {alert.message}
            </p>
          </div>
        </div>
      ))}
    </div>
  );
}

// ─── Spike Alerts Strip ───────────────────────────────────────────────────────
function SpikeStrip() {
  const { data, loading } = useSpikes(24, 2.0);

  if (loading || !data?.length) return null;

  return (
    <div style={{
      background: C.bgSurface, border: `0.5px solid ${C.amber}33`,
      borderRadius: 12, padding: "16px 24px",
    }}>
      <SectionTitle>⚡ Trend Spikes Detected</SectionTitle>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 10 }}>
        {data.map(spike => (
          <div key={spike.keyword} style={{
            background: C.bgElevated, border: `0.5px solid ${C.border}`,
            borderRadius: 10, padding: "10px 16px",
            display: "flex", alignItems: "center", gap: 12,
          }}>
            <div>
              <div style={{ fontSize: 13, fontWeight: 700, color: C.amber, marginBottom: 2 }}>
                {spike.keyword}
              </div>
              <div style={{ fontSize: 11, color: C.textMuted }}>
                {spike.mention_count.toLocaleString()} mentions
              </div>
            </div>
            <NeonBadge color={C.amber}>z={spike.z_score}</NeonBadge>
          </div>
        ))}
      </div>
    </div>
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
      fontFamily: "'DM Mono', 'Fira Code', 'Consolas', monospace",
      padding: "0 0 40px",
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
          <span style={{ fontSize: 15, fontWeight: 700, color: C.textPri, letterSpacing: "0.05em" }}>
            CryptoTrend
          </span>
          <NeonBadge color={C.neonTeal}>LIVE</NeonBadge>
        </div>
        <nav style={{ marginLeft: "auto", display: "flex", gap: 24 }}>
          {["Trending", "Whales", "Alerts", "API Docs"].map(item => (
            <a key={item} href="#" style={{
              fontSize: 12, color: C.textMuted, textDecoration: "none",
              letterSpacing: "0.06em",
            }}>
              {item}
            </a>
          ))}
        </nav>
      </header>

      {/* ── Body ── */}
      <main style={{ padding: "28px 32px", display: "flex", flexDirection: "column", gap: 20 }}>

        {/* Row 1: KPI Cards */}
        <KpiCards />

        {/* Row 2: Chart + Whale Alerts */}
        <div style={{ display: "flex", gap: 16 }}>
          <TrendChart selectedCoin={selectedCoin} />
          <WhaleAlertsFeed />
        </div>

        {/* Row 3: Trending Table (full width) */}
        <TrendingTable onCoinSelect={setSelectedCoin} selectedCoin={selectedCoin} />

        {/* Row 4: Spike Strip */}
        <SpikeStrip />

      </main>
    </div>
  );
}
