/**
 * Skeleton.jsx
 * =============
 * Skeleton loading components — hiệu ứng shimmer "xương" chạy mờ.
 * Dùng trong lúc chờ API trả về data.
 *
 * Components:
 *  <SkeletonBox />         — khối chữ nhật bất kỳ
 *  <SkeletonKpiCard />     — KPI card (4 cái ở đầu trang)
 *  <SkeletonTableRow />    — 1 hàng trong trending table
 *  <SkeletonTable rows />  — toàn bộ trending table
 *  <SkeletonChart />       — placeholder cho line chart
 *  <SkeletonAlertItem />   — 1 item trong whale alerts feed
 */

const shimmer = `
  @keyframes shimmer {
    0%   { background-position: -680px 0; }
    100% { background-position: 680px 0; }
  }
`;

// Inject keyframes một lần duy nhất
if (typeof document !== "undefined" && !document.getElementById("sk-shimmer")) {
  const style = document.createElement("style");
  style.id = "sk-shimmer";
  style.textContent = shimmer;
  document.head.appendChild(style);
}

// Base shimmer style
const skBase = {
  borderRadius: "6px",
  background: "linear-gradient(90deg, #1a2030 25%, #243050 50%, #1a2030 75%)",
  backgroundSize: "680px 100%",
  animation: "shimmer 1.6s infinite linear",
};

// ─── Primitive ───────────────────────────────────────────────────────────────
export function SkeletonBox({ w = "100%", h = 16, r = 6, style = {} }) {
  return (
    <div
      style={{
        ...skBase,
        width: w,
        height: h,
        borderRadius: r,
        flexShrink: 0,
        ...style,
      }}
    />
  );
}

// ─── KPI Card ────────────────────────────────────────────────────────────────
export function SkeletonKpiCard() {
  return (
    <div style={{
      background: "#111620",
      border: "0.5px solid #1e2a40",
      borderRadius: 12,
      padding: "20px 24px",
    }}>
      <SkeletonBox w="50%" h={11} style={{ marginBottom: 12 }} />
      <SkeletonBox w="70%" h={28} r={4} style={{ marginBottom: 10 }} />
      <SkeletonBox w="40%" h={11} />
    </div>
  );
}

// ─── Table Row ───────────────────────────────────────────────────────────────
export function SkeletonTableRow() {
  return (
    <tr>
      {/* rank */}
      <td style={{ padding: "14px 16px" }}><SkeletonBox w={20} h={12} /></td>
      {/* coin */}
      <td style={{ padding: "14px 16px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
          <SkeletonBox w={28} h={28} r={14} />
          <SkeletonBox w={60} h={13} />
        </div>
      </td>
      {/* fear & greed */}
      <td style={{ padding: "14px 16px" }}><SkeletonBox w={80} h={20} r={10} /></td>
      {/* bullish */}
      <td style={{ padding: "14px 16px" }}><SkeletonBox w={48} h={13} /></td>
      {/* bearish */}
      <td style={{ padding: "14px 16px" }}><SkeletonBox w={48} h={13} /></td>
      {/* snapshots */}
      <td style={{ padding: "14px 16px" }}><SkeletonBox w={36} h={13} /></td>
    </tr>
  );
}

export function SkeletonTable({ rows = 8 }) {
  return (
    <>
      {Array.from({ length: rows }).map((_, i) => (
        <SkeletonTableRow key={i} />
      ))}
    </>
  );
}

// ─── Chart ───────────────────────────────────────────────────────────────────
export function SkeletonChart({ height = 220 }) {
  const bars = [55, 72, 48, 83, 61, 90, 74, 66, 88, 52, 79, 95];
  return (
    <div style={{
      height,
      display: "flex",
      alignItems: "flex-end",
      gap: 6,
      padding: "0 8px",
    }}>
      {bars.map((pct, i) => (
        <div
          key={i}
          style={{
            ...skBase,
            flex: 1,
            height: `${pct}%`,
            borderRadius: "4px 4px 0 0",
            animationDelay: `${i * 0.08}s`,
          }}
        />
      ))}
    </div>
  );
}

// ─── Alert Item ──────────────────────────────────────────────────────────────
export function SkeletonAlertItem() {
  return (
    <div style={{
      display: "flex",
      alignItems: "flex-start",
      gap: 12,
      padding: "12px 0",
      borderBottom: "0.5px solid #1e2a40",
    }}>
      <SkeletonBox w={36} h={36} r={18} style={{ flexShrink: 0 }} />
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 8 }}>
        <SkeletonBox w="60%" h={12} />
        <SkeletonBox w="85%" h={11} />
      </div>
    </div>
  );
}
