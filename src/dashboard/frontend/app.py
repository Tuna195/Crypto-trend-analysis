"""
Twitter Trend Analysis Dashboard
=================================
CoinMarketCap-inspired analytics dashboard built with Streamlit + Plotly.
Uses rich mock data by default. To connect MongoDB, swap load_data() below.

Install:
    pip install streamlit pandas plotly

Run:
    streamlit run app.py
"""

import time
import random
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────
# PAGE CONFIG  (must be the very first Streamlit call)
# ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Twitter Trend Dashboard",
    page_icon="🐦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────
# GLOBAL CSS  — dark CoinMarketCap-style theme
# ─────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
      html, body, [class*="css"] { font-family: 'Segoe UI', 'Inter', sans-serif; }
      .block-container { padding-top: 1.2rem; }

      .metric-card {
          background: linear-gradient(135deg,#1c2333 0%,#0d1117 100%);
          border: 1px solid #30363d;
          border-radius: 14px;
          padding: 22px 20px 18px;
          text-align: center;
          margin-bottom: 4px;
      }
      .metric-label {
          font-size: 11px; text-transform: uppercase;
          letter-spacing: 1.4px; color: #8b949e; margin-bottom: 10px;
      }
      .metric-value { font-size: 30px; font-weight: 800; color: #e6edf3; }
      .metric-sub   { font-size: 12px; color: #58a6ff; margin-top: 5px; }

      .section-header {
          font-size: 15px; font-weight: 700; color: #e6edf3;
          border-left: 3px solid #1d9bf0;
          padding-left: 10px; margin: 28px 0 14px;
      }

      .trend-table { width: 100%; border-collapse: collapse; font-size: 13px; }
      .trend-table th {
          background: #161b22; color: #8b949e; text-align: left;
          padding: 9px 12px; font-size: 10px; text-transform: uppercase;
          letter-spacing: 1.1px; border-bottom: 1px solid #30363d;
      }
      .trend-table td { padding: 9px 12px; border-bottom: 1px solid #21262d; vertical-align: middle; }
      .trend-table tr:hover td { background: #1c2333; }
      .trend-table tr:last-child td { border-bottom: none; }

      .cell-rank    { color: #8b949e; font-weight: 700; }
      .cell-hashtag { color: #58a6ff; font-weight: 700; }
      .cell-count   { color: #e6edf3; }
      .cell-pos     { color: #3fb950; font-weight: 700; }
      .cell-neg     { color: #f85149; font-weight: 700; }
      .cell-neu     { color: #8b949e; }
      .cell-chg-pos { color: #3fb950; }
      .cell-chg-neg { color: #f85149; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
HASHTAGS = [
    "#AI", "#Python", "#ChatGPT", "#MachineLearning", "#DataScience",
    "#Crypto", "#Bitcoin", "#Web3", "#OpenSource", "#Streamlit",
    "#OpenAI", "#LLM", "#BigData", "#CloudComputing", "#DevOps",
    "#Kubernetes", "#NLP", "#GenerativeAI", "#MLOps", "#TechTrends",
]

GEO_CENTRES = [
    (40.71, -74.01), (51.51, -0.13), (35.68, 139.69), (48.85, 2.35),
    (37.77, -122.42), (-23.55, -46.63), (28.61, 77.21), (55.75, 37.62),
    (-33.87, 151.21), (1.35, 103.82), (19.08, 72.88), (39.91, 116.39),
    (41.00, 28.97), (52.52, 13.40), (-34.60, -58.38),
]

_PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#c9d1d9", family="Segoe UI"),
    margin=dict(l=10, r=10, t=44, b=10),
    xaxis=dict(gridcolor="#21262d", linecolor="#30363d", zeroline=False),
    yaxis=dict(gridcolor="#21262d", linecolor="#30363d", zeroline=False),
    title_font=dict(size=14, color="#e6edf3"),
)


# ─────────────────────────────────────────────────────────────
# DATA LAYER
# ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def generate_mock_data(seed: int) -> pd.DataFrame:
    """
    Produce one row per (hashtag × hour) for the last 24 hours.
    Seed rotates every 10 s to simulate live stream.

    MongoDB swap-in:
        from pymongo import MongoClient
        client = MongoClient(st.secrets["MONGO_URI"])
        col = client["twitter"]["trends"]
        df = pd.DataFrame(list(col.find({}, {"_id": 0})))
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        return df
    """
    rng = random.Random(seed)
    rows = []
    now = datetime.utcnow()

    for tag in HASHTAGS:
        base_count = rng.randint(800, 8_000)
        base_sentiment = rng.uniform(-0.6, 0.8)
        for hour in range(24):
            ts = now - timedelta(hours=23 - hour)
            lat, lon = rng.choice(GEO_CENTRES)
            rows.append({
                "hashtag": tag,
                "count": max(0, int(base_count + rng.gauss(0, 300) + hour * rng.uniform(20, 120))),
                "sentiment": round(max(-1, min(1, base_sentiment + rng.gauss(0, 0.15))), 3),
                "timestamp": ts,
                "lat": round(lat + rng.uniform(-6, 6), 3),
                "lon": round(lon + rng.uniform(-6, 6), 3),
            })

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df


def load_data() -> pd.DataFrame:
    return generate_mock_data(int(time.time() // 10))


# ─────────────────────────────────────────────────────────────
# FILTER LOGIC
# ─────────────────────────────────────────────────────────────
def apply_filters(df, selected_tags, date_range, sentiment_filter) -> pd.DataFrame:
    if selected_tags:
        df = df[df["hashtag"].isin(selected_tags)]

    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        s, e = date_range
        df = df[(df["timestamp"].dt.date >= s) & (df["timestamp"].dt.date <= e)]

    if sentiment_filter == "Positive (≥ 0)":
        df = df[df["sentiment"] >= 0]
    elif sentiment_filter == "Negative (< 0)":
        df = df[df["sentiment"] < 0]

    return df


# ─────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────
def render_sidebar(df: pd.DataFrame):
    st.sidebar.markdown("## ⚙️ Filters")

    all_tags = sorted(df["hashtag"].unique().tolist())
    selected_tags = st.sidebar.multiselect(
        "Hashtags", all_tags, default=all_tags[:10],
        help="Choose hashtags to include in all views",
    )

    min_dt = df["timestamp"].min().date()
    max_dt = df["timestamp"].max().date()
    date_range = st.sidebar.date_input(
        "Date Range", value=(min_dt, max_dt), min_value=min_dt, max_value=max_dt,
    )

    sentiment_filter = st.sidebar.radio(
        "Sentiment", ["All", "Positive (≥ 0)", "Negative (< 0)"], index=0,
    )

    st.sidebar.divider()
    auto_refresh = st.sidebar.toggle("⚡ Auto-refresh (10 s)", value=True)
    st.sidebar.caption("Dashboard syncs every 10 s when enabled.")

    return selected_tags, date_range, sentiment_filter, auto_refresh


# ─────────────────────────────────────────────────────────────
# HEADER — KPI cards
# ─────────────────────────────────────────────────────────────
def render_header(df: pd.DataFrame):
    total_tweets = int(df["count"].sum())
    num_trends = int(df["hashtag"].nunique())
    avg_sent = float(df["sentiment"].mean()) if not df.empty else 0.0
    top_tag = df.groupby("hashtag")["count"].sum().idxmax() if not df.empty else "—"

    sent_label = "Positive 📈" if avg_sent >= 0 else "Negative 📉"
    sent_color = "#3fb950" if avg_sent >= 0 else "#f85149"

    st.markdown(
        """
        <div style="display:flex;align-items:center;gap:14px;margin-bottom:20px">
          <span style="font-size:30px">🐦</span>
          <div>
            <div style="font-size:23px;font-weight:800;color:#e6edf3;letter-spacing:-.3px">
              Twitter Trend Dashboard
            </div>
            <div style="font-size:11px;color:#8b949e;letter-spacing:.4px">
              REAL-TIME HASHTAG ANALYTICS · AUTO-REFRESH EVERY 10 S
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)
    for col, label, val, sub, color in [
        (c1, "Total Tweets",      f"{total_tweets:,}", "across all filters", ""),
        (c2, "Trending Hashtags", str(num_trends),     "unique tags",        ""),
        (c3, "Avg Sentiment",     f"{avg_sent:+.3f}",  sent_label,           sent_color),
        (c4, "Top Hashtag",       top_tag,             "by tweet volume",    ""),
    ]:
        col.markdown(
            f"""
            <div class="metric-card">
              <div class="metric-label">{label}</div>
              <div class="metric-value" style="{'color:'+color if color else ''}">{val}</div>
              <div class="metric-sub">{sub}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ─────────────────────────────────────────────────────────────
# MAIN TABLE
# ─────────────────────────────────────────────────────────────
def render_table(df: pd.DataFrame):
    st.markdown('<div class="section-header">📋 Trending Hashtags</div>', unsafe_allow_html=True)

    if df.empty:
        st.info("No data matches the current filters.")
        return

    agg = (
        df.groupby("hashtag")
        .agg(total_count=("count", "sum"), avg_sentiment=("sentiment", "mean"))
        .reset_index()
        .sort_values("total_count", ascending=False)
        .reset_index(drop=True)
    )
    agg["rank"] = agg.index + 1

    # Deterministic mock 24h change per tag
    agg["change_24h"] = agg["hashtag"].map(
        lambda t: round((hash(t) % 3001 - 1500) / 100, 2)
    )

    ctrl_l, ctrl_r = st.columns([3, 1])
    with ctrl_l:
        query = st.text_input("search", placeholder="🔍 Search hashtag…", label_visibility="collapsed")
    with ctrl_r:
        page_size = st.selectbox("Rows", [10, 20, 50], index=0, label_visibility="collapsed")

    if query.strip():
        agg = agg[agg["hashtag"].str.lower().str.contains(query.strip().lower())]

    total_pages = max(1, -(-len(agg) // page_size))
    page_num = st.number_input("Page", min_value=1, max_value=total_pages, value=1, label_visibility="collapsed")
    page_df = agg.iloc[(page_num - 1) * page_size : page_num * page_size]

    rows_html = ""
    for _, row in page_df.iterrows():
        s = row["avg_sentiment"]
        s_cls = "cell-pos" if s >= 0.1 else ("cell-neg" if s < -0.1 else "cell-neu")
        bar_pct = int((s + 1) / 2 * 100)
        bar_color = "#3fb950" if s >= 0 else "#f85149"

        ch = row["change_24h"]
        ch_cls = "cell-chg-pos" if ch >= 0 else "cell-chg-neg"
        ch_str = f"▲ {ch:.2f}%" if ch >= 0 else f"▼ {abs(ch):.2f}%"

        rows_html += f"""
        <tr>
          <td class="cell-rank">#{int(row['rank'])}</td>
          <td class="cell-hashtag">{row['hashtag']}</td>
          <td class="cell-count">{int(row['total_count']):,}</td>
          <td>
            <span class="{s_cls}">{s:+.3f}</span>
            <div style="margin-top:4px;background:#21262d;border-radius:3px;height:3px;width:72px">
              <div style="background:{bar_color};width:{bar_pct}%;height:3px;border-radius:3px"></div>
            </div>
          </td>
          <td class="{ch_cls}">{ch_str}</td>
        </tr>
        """

    st.markdown(
        f"""
        <div style="overflow-x:auto;border:1px solid #30363d;border-radius:12px">
          <table class="trend-table">
            <thead>
              <tr>
                <th>#</th><th>Hashtag</th><th>Tweet Count</th>
                <th>Sentiment Score</th><th>24h Change</th>
              </tr>
            </thead>
            <tbody>{rows_html}</tbody>
          </table>
        </div>
        <div style="text-align:right;color:#8b949e;font-size:11px;margin-top:6px">
          Page {page_num} of {total_pages} &nbsp;·&nbsp; {len(agg)} results
        </div>
        """,
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────
# CHART BUILDERS
# ─────────────────────────────────────────────────────────────
def chart_top_trends(df: pd.DataFrame) -> go.Figure:
    agg = (
        df.groupby("hashtag")["count"].sum().reset_index()
        .sort_values("count", ascending=False).head(15)
    )
    fig = px.bar(
        agg, x="hashtag", y="count", color="count",
        color_continuous_scale=[[0, "#1d9bf0"], [0.5, "#7c5cfc"], [1, "#3fb950"]],
        labels={"count": "Tweet Count", "hashtag": ""},
        title="Top Trending Hashtags",
    )
    fig.update_layout(**_PLOT_LAYOUT, coloraxis_showscale=False)
    fig.update_traces(marker_line_width=0, hovertemplate="%{x}<br>%{y:,} tweets<extra></extra>")
    return fig


def chart_sentiment_dist(df: pd.DataFrame) -> go.Figure:
    fig = px.histogram(
        df, x="sentiment", nbins=50, color_discrete_sequence=["#1d9bf0"],
        labels={"sentiment": "Sentiment Score"}, title="Sentiment Distribution",
    )
    fig.add_vline(x=0, line_dash="dash", line_color="#f85149", opacity=0.7,
                  annotation_text="Neutral", annotation_position="top right",
                  annotation_font_color="#8b949e")
    fig.update_layout(**_PLOT_LAYOUT, bargap=0.04)
    return fig


def chart_trend_over_time(df: pd.DataFrame) -> go.Figure:
    top_tags = df.groupby("hashtag")["count"].sum().nlargest(8).index.tolist()
    agg = (
        df[df["hashtag"].isin(top_tags)]
        .groupby(["timestamp", "hashtag"])["count"].sum().reset_index()
    )
    fig = px.line(
        agg, x="timestamp", y="count", color="hashtag",
        labels={"count": "Tweet Count", "timestamp": ""},
        title="Trend Over Time (Top 8)",
    )
    fig.update_layout(**_PLOT_LAYOUT, legend_title_text="",
                      legend=dict(bgcolor="rgba(0,0,0,0)", borderwidth=0))
    fig.update_traces(line_width=2)
    return fig


def chart_sentiment_by_tag(df: pd.DataFrame) -> go.Figure:
    agg = (
        df.groupby("hashtag")["sentiment"].mean().reset_index()
        .sort_values("sentiment", ascending=False).head(15)
    )
    fig = go.Figure(go.Bar(
        x=agg["hashtag"], y=agg["sentiment"],
        marker_color=["#3fb950" if s >= 0 else "#f85149" for s in agg["sentiment"]],
        hovertemplate="%{x}<br>Sentiment: %{y:+.3f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dot", line_color="#8b949e", opacity=0.5)
    layout = dict(**_PLOT_LAYOUT)
    layout["yaxis"] = dict(gridcolor="#21262d", linecolor="#30363d", zeroline=False, range=[-1, 1])
    fig.update_layout(**layout, title="Avg Sentiment by Hashtag")
    return fig


def chart_heatmap(df: pd.DataFrame):
    geo = df.dropna(subset=["lat", "lon"])
    if geo.empty:
        return None
    fig = px.density_mapbox(
        geo, lat="lat", lon="lon", z="count", radius=20,
        center=dict(lat=20, lon=5), zoom=1,
        mapbox_style="carto-darkmatter",
        color_continuous_scale="Turbo",
        title="Global Tweet Density",
        labels={"count": "Tweets"},
    )
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=44, b=0),
        font=dict(color="#c9d1d9"),
        title_font=dict(size=14, color="#e6edf3"),
        coloraxis_colorbar=dict(tickfont=dict(color="#c9d1d9"), title="Tweets"),
    )
    return fig


# ─────────────────────────────────────────────────────────────
# CHARTS SECTION
# ─────────────────────────────────────────────────────────────
def render_charts(df: pd.DataFrame):
    st.markdown('<div class="section-header">📊 Analytics</div>', unsafe_allow_html=True)

    if df.empty:
        st.info("No data for the current filters.")
        return

    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(chart_top_trends(df), use_container_width=True)
    with col2:
        st.plotly_chart(chart_sentiment_dist(df), use_container_width=True)

    col3, col4 = st.columns([2, 1])
    with col3:
        st.plotly_chart(chart_trend_over_time(df), use_container_width=True)
    with col4:
        st.plotly_chart(chart_sentiment_by_tag(df), use_container_width=True)

    st.markdown('<div class="section-header">🌍 Geo Heatmap</div>', unsafe_allow_html=True)
    heatmap = chart_heatmap(df)
    if heatmap:
        st.plotly_chart(heatmap, use_container_width=True)
    else:
        st.info("No geo-tagged records in the current dataset.")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    df_raw = load_data()
    selected_tags, date_range, sentiment_filter, auto_refresh = render_sidebar(df_raw)
    df = apply_filters(df_raw, selected_tags, date_range, sentiment_filter)

    render_header(df)
    st.divider()
    render_table(df)
    render_charts(df)

    st.markdown(
        f"<div style='text-align:center;color:#8b949e;font-size:11px;margin-top:40px'>"
        f"Last refreshed: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"
        f"</div>",
        unsafe_allow_html=True,
    )

    if auto_refresh:
        time.sleep(10)
        st.rerun()


if __name__ == "__main__":
    main()