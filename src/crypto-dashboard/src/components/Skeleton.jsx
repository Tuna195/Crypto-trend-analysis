/**
 * components/Skeleton.jsx
 * ========================
 * Shimmer skeleton components cho mọi loading state.
 */

const shimmerCSS = `
  @keyframes shimmer {
    0%   { background-position: -680px 0; }
    100% { background-position:  680px 0; }
  }
`;
if (typeof document !== "undefined" && !document.getElementById("sk-shimmer")) {
  const s = document.createElement("style");
  s.id = "sk-shimmer";
  s.textContent = shimmerCSS;
  document.head.appendChild(s);
}

const skBase = {
  borderRadius: 6,
  background: "linear-gradient(90deg,#1a2030 25%,#243050 50%,#1a2030 75%)",
  backgroundSize: "680px 100%",
  animation: "shimmer 1.6s infinite linear",
};

export function SkeletonBox({ w = "100%", h = 16, r = 6, style = {} }) {
  return <div style={{ ...skBase, width: w, height: h, borderRadius: r, flexShrink: 0, ...style }} />;
}

export function SkeletonKpiCard() {
  return (
    <div style={{ background: "#111620", border: "0.5px solid #1e2a40", borderRadius: 12, padding: "20px 24px" }}>
      <SkeletonBox w="50%" h={11} style={{ marginBottom: 12 }} />
      <SkeletonBox w="70%" h={28} r={4} style={{ marginBottom: 10 }} />
      <SkeletonBox w="40%" h={11} />
    </div>
  );
}

export function SkeletonTableRow({ cols = 6 }) {
  return (
    <tr>
      {Array.from({ length: cols }).map((_, i) => (
        <td key={i} style={{ padding: "14px 16px" }}>
          <SkeletonBox w={i === 1 ? 80 : i === 0 ? 20 : 48} h={13} />
        </td>
      ))}
    </tr>
  );
}

export function SkeletonTable({ rows = 8, cols = 6 }) {
  return <>{Array.from({ length: rows }).map((_, i) => <SkeletonTableRow key={i} cols={cols} />)}</>;
}

export function SkeletonChart({ height = 220 }) {
  const bars = [55, 72, 48, 83, 61, 90, 74, 66, 88, 52, 79, 95];
  return (
    <div style={{ height, display: "flex", alignItems: "flex-end", gap: 6, padding: "0 8px" }}>
      {bars.map((pct, i) => (
        <div key={i} style={{ ...skBase, flex: 1, height: `${pct}%`, borderRadius: "4px 4px 0 0", animationDelay: `${i * 0.08}s` }} />
      ))}
    </div>
  );
}

export function SkeletonCard({ height = 120 }) {
  return (
    <div style={{ background: "#111620", border: "0.5px solid #1e2a40", borderRadius: 12, padding: "18px 20px", height }}>
      <SkeletonBox w="40%" h={12} style={{ marginBottom: 12 }} />
      <SkeletonBox w="80%" h={18} style={{ marginBottom: 8 }} />
      <SkeletonBox w="60%" h={10} />
    </div>
  );
}

export function SkeletonAlertItem() {
  return (
    <div style={{ display: "flex", alignItems: "flex-start", gap: 12, padding: "12px 0", borderBottom: "0.5px solid #1e2a40" }}>
      <SkeletonBox w={36} h={36} r={18} style={{ flexShrink: 0 }} />
      <div style={{ flex: 1, display: "flex", flexDirection: "column", gap: 8 }}>
        <SkeletonBox w="60%" h={12} />
        <SkeletonBox w="85%" h={11} />
      </div>
    </div>
  );
}
