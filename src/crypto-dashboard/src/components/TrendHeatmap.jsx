/**
 * components/TrendHeatmap.jsx
 * ============================
 * Heatmap xu hướng — visualize Fear & Greed của nhiều coin
 * theo trục thời gian (24 giờ × N coins).
 *
 * Màu sắc:
 *   Đỏ đậm   (0–30)  → Extreme Fear
 *   Cam      (30–45) → Fear
 *   Vàng     (45–55) → Neutral
 *   Xanh lá  (55–70) → Greed
 *   Xanh neon (70–100) → Extreme Greed
 */

import { useBatchTrends, useCoinSentiment } from "../hooks/useDashboard";
import { useState, useEffect, useRef } from "react";
import { SkeletonBox } from "./Skeleton";

// ─── Tokens ──────────────────────────────────────────────────────────────────
const C = {
    bgSurface: "#111620",
    bgElevated: "#1a2030",
    border: "#1e2a40",
    textPri: "#e2e8f0",
    textMuted: "#7a90b0",
    textDim: "#3d5275",
    electricBl: "#5b8cff",
};

// Fear & Greed → màu ô heatmap
function fgToColor(score, alpha = 1) {
    if (score == null) return `rgba(30,42,64,${alpha})`;
    if (score >= 70) return `rgba(0,245,160,${alpha})`;   // Extreme Greed — neon teal
    if (score >= 55) return `rgba(34,201,122,${alpha})`;  // Greed — lime
    if (score >= 45) return `rgba(245,158,11,${alpha})`;  // Neutral — amber
    if (score >= 30) return `rgba(239,100,60,${alpha})`;  // Fear — orange-red
    return `rgba(240,82,82,${alpha})`;                     // Extreme Fear — red
}

function fgLabel(score) {
    if (score == null) return "N/A";
    if (score >= 70) return "Extreme Greed";
    if (score >= 55) return "Greed";
    if (score >= 45) return "Neutral";
    if (score >= 30) return "Fear";
    return "Extreme Fear";
}

// ─── Tooltip ─────────────────────────────────────────────────────────────────
function HeatTooltip({ cell, pos }) {
    if (!cell) return null;
    return (
        <div style={{
            position: "fixed",
            left: pos.x + 12, top: pos.y - 8,
            background: "#1a2030", border: "0.5px solid #2a3a58",
            borderRadius: 8, padding: "8px 12px", fontSize: 11,
            color: C.textPri, pointerEvents: "none", zIndex: 999,
            boxShadow: "0 4px 20px rgba(0,0,0,0.5)",
            minWidth: 140,
        }}>
            <div style={{ fontWeight: 700, color: fgToColor(cell.fg), marginBottom: 4 }}>
                ${cell.coin} · {cell.hour}:00
            </div>
            <div style={{ color: C.textMuted }}>
                F&G: <span style={{ color: fgToColor(cell.fg), fontWeight: 600 }}>{cell.fg?.toFixed(1) ?? "—"}</span>
                {" · "}{fgLabel(cell.fg)}
            </div>
            {cell.bull != null && (
                <div style={{ color: C.textMuted, marginTop: 3 }}>
                    Bullish {(cell.bull * 100).toFixed(0)}% · Bearish {(cell.bear * 100).toFixed(0)}%
                </div>
            )}
        </div>
    );
}

// ─── Single coin row — fetch its own sentiment ────────────────────────────────
function CoinRow({ coin, hours, onHover }) {
    const { data } = useCoinSentiment(coin, hours);

    // Nhóm data theo giờ (lấy giá trị trung bình nếu có nhiều snapshot/giờ)
    const byHour = {};
    (data ?? []).forEach(d => {
        const h = new Date(d.window_start).getHours();
        if (!byHour[h]) byHour[h] = [];
        byHour[h].push(d);
    });

    const now = new Date();
    const cells = Array.from({ length: hours }, (_, i) => {
        const h = new Date(now.getTime() - (hours - 1 - i) * 3_600_000).getHours();
        const pts = byHour[h] ?? [];
        const fg = pts.length ? pts.reduce((a, d) => a + d.fear_greed_score, 0) / pts.length : null;
        const bull = pts.length ? pts.reduce((a, d) => a + (d.bullish_ratio ?? 0), 0) / pts.length : null;
        const bear = pts.length ? pts.reduce((a, d) => a + (d.bearish_ratio ?? 0), 0) / pts.length : null;
        return { coin, hour: h, fg, bull, bear };
    });

    return (
        <div style={{ display: "contents" }}>
            {/* Coin label */}
            <div style={{
                fontSize: 11, fontWeight: 600, color: C.textMuted,
                display: "flex", alignItems: "center", paddingRight: 8,
                fontFamily: "monospace",
            }}>
                ${coin}
            </div>
            {/* Heatmap cells */}
            {cells.map((cell, i) => (
                <div
                    key={i}
                    title={`${coin} ${cell.hour}:00 — F&G ${cell.fg?.toFixed(1) ?? "N/A"}`}
                    onMouseEnter={e => onHover(cell, { x: e.clientX, y: e.clientY })}
                    onMouseLeave={() => onHover(null, null)}
                    style={{
                        height: 28,
                        background: fgToColor(cell.fg, cell.fg != null ? 0.85 : 0.2),
                        borderRadius: 3,
                        cursor: "crosshair",
                        transition: "opacity 0.15s, transform 0.1s",
                        border: "1px solid rgba(0,0,0,0.2)",
                        position: "relative",
                    }}
                    onMouseOver={e => { e.currentTarget.style.transform = "scale(1.08)"; e.currentTarget.style.zIndex = 2; }}
                    onMouseOut={e => { e.currentTarget.style.transform = "scale(1)"; e.currentTarget.style.zIndex = 0; }}
                />
            ))}
        </div>
    );
}

// ─── Main Heatmap ─────────────────────────────────────────────────────────────
export default function TrendHeatmap() {
    const [hours, setHours] = useState(24);
    const [hoveredCell, setHoveredCell] = useState(null);
    const [hoverPos, setHoverPos] = useState(null);
    const { data: trends, loading } = useBatchTrends(hours, 12);

    // Chỉ lấy top 8 coin có nhiều snapshot nhất
    const coins = (trends ?? [])
        .sort((a, b) => b.snapshot_count - a.snapshot_count)
        .slice(0, 8)
        .map(r => r.coin);

    const now = new Date();
    const hourLabels = Array.from({ length: hours }, (_, i) =>
        new Date(now.getTime() - (hours - 1 - i) * 3_600_000).getHours()
    );

    const handleHover = (cell, pos) => {
        setHoveredCell(cell);
        setHoverPos(pos);
    };

    return (
        <div style={{ background: C.bgSurface, border: `0.5px solid ${C.border}`, borderRadius: 12, padding: "20px 24px" }}>

            {/* Header */}
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 18 }}>
                <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
                    <div style={{ width: 3, height: 18, borderRadius: 2, background: C.electricBl }} />
                    <span style={{ fontSize: 12, fontWeight: 600, color: C.textPri, letterSpacing: "0.08em", textTransform: "uppercase" }}>
                        Sentiment Heatmap
                    </span>
                    <span style={{
                        fontSize: 9, fontWeight: 700, padding: "2px 7px", borderRadius: 4,
                        background: "#5b8cff18", color: C.electricBl,
                        border: "0.5px solid #5b8cff44", letterSpacing: "0.06em",
                    }}>
                        ⬡ BATCH
                    </span>
                </div>
                {/* Hours filter */}
                <div style={{ display: "flex", gap: 5 }}>
                    {[12, 24, 48].map(h => (
                        <button key={h} onClick={() => setHours(h)} style={{
                            padding: "3px 10px", borderRadius: 20, fontSize: 10, cursor: "pointer",
                            background: hours === h ? C.electricBl + "20" : "transparent",
                            border: `0.5px solid ${hours === h ? C.electricBl : C.border}`,
                            color: hours === h ? C.electricBl : C.textMuted,
                            fontFamily: "inherit", transition: "all 0.15s",
                        }}>
                            {h}h
                        </button>
                    ))}
                </div>
            </div>

            {/* Legend */}
            <div style={{ display: "flex", gap: 6, alignItems: "center", marginBottom: 16, flexWrap: "wrap" }}>
                <span style={{ fontSize: 10, color: C.textDim, marginRight: 4 }}>Fear & Greed:</span>
                {[
                    { label: "Extreme Fear", color: fgToColor(20) },
                    { label: "Fear", color: fgToColor(38) },
                    { label: "Neutral", color: fgToColor(50) },
                    { label: "Greed", color: fgToColor(62) },
                    { label: "Extreme Greed", color: fgToColor(80) },
                ].map(({ label, color }) => (
                    <div key={label} style={{ display: "flex", alignItems: "center", gap: 4 }}>
                        <div style={{ width: 12, height: 12, borderRadius: 3, background: color }} />
                        <span style={{ fontSize: 9, color: C.textDim }}>{label}</span>
                    </div>
                ))}
                <span style={{ fontSize: 9, color: C.textDim, marginLeft: 8 }}>· Hover ô để xem chi tiết</span>
            </div>

            {/* Grid */}
            {loading || coins.length === 0 ? (
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                    {Array(6).fill(0).map((_, i) => (
                        <SkeletonBox key={i} h={28} style={{ borderRadius: 3 }} />
                    ))}
                </div>
            ) : (
                <div style={{
                    display: "grid",
                    gridTemplateColumns: `60px repeat(${hours}, 1fr)`,
                    gap: "3px",
                    overflowX: "auto",
                }}>
                    {/* Hour labels header */}
                    <div /> {/* empty top-left corner */}
                    {hourLabels.map((h, i) => (
                        <div key={i} style={{
                            fontSize: 8, color: C.textDim, textAlign: "center",
                            paddingBottom: 4, fontFamily: "monospace",
                            // Chỉ hiện mỗi 4 giờ để không chật
                            opacity: h % 4 === 0 ? 1 : 0,
                        }}>
                            {String(h).padStart(2, "0")}h
                        </div>
                    ))}

                    {/* Coin rows */}
                    {coins.map(coin => (
                        <CoinRow key={coin} coin={coin} hours={hours} onHover={handleHover} />
                    ))}
                </div>
            )}

            {/* Tooltip */}
            <HeatTooltip cell={hoveredCell} pos={hoverPos} />
        </div>
    );
}
