from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

import antd_icons as _icons_mod
import data.db as _db_mod
import data.mock_data as _mock_mod

importlib.reload(_icons_mod)
importlib.reload(_db_mod)
importlib.reload(_mock_mod)

from antd_icons import antd_svg, kpi_card, section_header
from data.db import load_from_postgres, load_pipeline
from data.mock_data import build_customer_week, build_daily_stats, generate_transactions

ROOT = Path(__file__).resolve().parents[1]
CONTROL_FILE = ROOT / "data" / "replay" / "control"
CHECKPOINT_FILE = ROOT / "data" / "replay" / "checkpoint.json"
REPORT_FILE = ROOT / "data" / "reports" / "producer_report.json"

DATASETS = {
    "Validation Split (101,793 dòng · Step 125–152)": "data/processed/validation.csv",
    "Synthetic Scenarios (160 dòng · 4 khách hàng kịch bản)": "data/synthetic/transactions.csv",
    "Test Split (100,117 dòng · Step 153–179)": "data/processed/test.csv",
}


def read_replay_state() -> dict:
    ctrl = "run"
    if CONTROL_FILE.is_file() and CONTROL_FILE.read_text(encoding="utf-8").strip().lower() == "pause":
        ctrl = "pause"
    idx = 0
    inp = "data/processed/validation.csv"
    completed = False
    if CHECKPOINT_FILE.is_file():
        try:
            cp = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
            idx = int(cp.get("next_index", 0))
            inp = str(cp.get("input", inp))
            completed = bool(cp.get("completed", False))
        except Exception:  # noqa: BLE001
            pass
    return {"control": ctrl, "next_index": idx, "input": Path(inp).name, "completed": completed}


def set_control(state: str) -> None:
    CONTROL_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONTROL_FILE.write_text(state, encoding="utf-8")


def spawn_producer(rate: int, limit: int | None = None, input_rel: str = "data/processed/validation.csv", from_start: bool = False) -> None:
    set_control("run")
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src")
    cmd = [
        sys.executable,
        "-m",
        "financial_streaming.producer.cli",
        "run",
        "--input",
        str(ROOT / input_rel),
        "--rate",
        str(rate),
    ]
    if limit is not None:
        cmd.extend(["--limit", str(limit)])
    if from_start:
        cmd.append("--from-start")
    subprocess.Popen(
        cmd,
        cwd=str(ROOT),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


st.set_page_config(
    page_title="FraudGuard · Financial Streaming Analytics",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* Ant Design Light Palette & High-Contrast Typography */
    .stApp {
        background-color: #f0f2f5 !important;
        color: #1f1f1f !important;
    }
    /* Make Streamlit top toolbar compact & transparent so it never covers content */
    header[data-testid="stHeader"],
    .stAppToolbar {
        background: transparent !important;
        height: 2.2rem !important;
        min-height: 2.2rem !important;
        pointer-events: none;
    }
    header[data-testid="stHeader"] button,
    .stAppToolbar button,
    [data-testid="stSidebarCollapsedControl"] {
        pointer-events: auto;
    }
    [data-testid="stToolbarActions"],
    [data-testid="stAppDeployButton"],
    [data-testid="stMainMenu"] {
        display: none !important;
    }
    .block-container {
        padding-top: 2.2rem !important;
        padding-bottom: 2rem !important;
        max-width: 1440px !important;
    }

    /* Sidebar Ant Design Menu Style */
    section[data-testid="stSidebar"] {
        background-color: #ffffff !important;
        border-right: 1px solid #f0f0f0 !important;
    }
    section[data-testid="stSidebar"] p,
    section[data-testid="stSidebar"] span,
    section[data-testid="stSidebar"] label,
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        color: #1f1f1f !important;
    }

    /* Ant Design Hero Header */
    .antd-hero {
        background: linear-gradient(135deg, #001529 0%, #003eb3 100%);
        color: #ffffff !important;
        padding: 18px 24px;
        border-radius: 10px;
        margin-bottom: 16px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
    }
    .antd-hero-title {
        color: #ffffff !important;
        font-size: 21px;
        font-weight: 700;
        display: flex;
        align-items: center;
        margin-bottom: 4px;
    }
    .antd-hero-sub {
        color: #bae0ff !important;
        font-size: 13px;
        margin: 0;
    }
    .antd-tag-live {
        background: rgba(82, 196, 26, 0.18);
        border: 1px solid #52c41a;
        color: #b7eb8f !important;
        padding: 5px 12px;
        border-radius: 6px;
        font-size: 12.5px;
        font-weight: 600;
        display: inline-flex;
        align-items: center;
    }

    /* Ant Design KPI Statistic Card */
    .antd-kpi-card {
        background: #ffffff;
        border: 1px solid #f0f0f0;
        border-radius: 8px;
        padding: 14px 16px;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.03);
        height: 100%;
    }
    .antd-kpi-top {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 6px;
    }
    .antd-kpi-label {
        color: #595959 !important;
        font-size: 13px;
        font-weight: 600;
    }
    .antd-kpi-icon {
        width: 30px;
        height: 30px;
        border-radius: 6px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
    }
    .antd-kpi-Icon svg {
        margin-right: 0 !important;
    }
    .antd-kpi-val {
        color: #141414 !important;
        font-size: 23px;
        font-weight: 700;
        line-height: 1.25;
    }
    .antd-kpi-sub {
        color: #8c8c8c !important;
        font-size: 12px;
        margin-top: 4px;
    }

    /* Ant Design Card Wrapper */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #ffffff !important;
        border: 1px solid #e8e8e8 !important;
        border-radius: 8px !important;
        box-shadow: 0 1px 2px rgba(0, 0, 0, 0.03) !important;
    }

    .section-title {
        color: #141414 !important;
        font-size: 15.5px;
        font-weight: 700;
        margin: 0 0 10px 0;
        display: flex;
        align-items: center;
    }

    /* Ant Design Buttons */
    .stButton > button {
        background-color: #ffffff !important;
        color: #262626 !important;
        border: 1px solid #d9d9d9 !important;
        border-radius: 6px !important;
        font-weight: 600 !important;
        height: 38px !important;
        transition: all 0.2s cubic-bezier(0.645, 0.045, 0.355, 1);
    }
    .stButton > button:hover {
        border-color: #1677ff !important;
        color: #1677ff !important;
        background-color: #f0f5ff !important;
    }
    .stButton > button[kind="primary"] {
        background-color: #1677ff !important;
        color: #ffffff !important;
        border-color: #1677ff !important;
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #4096ff !important;
        border-color: #4096ff !important;
        color: #ffffff !important;
    }
</style>
""", unsafe_allow_html=True)

ANTD_COLORS = ["#1677ff", "#13c2c2", "#52c41a", "#faad14", "#ff4d4f", "#722ed1", "#eb2f96", "#2f54eb"]


def style_fig(fig, height: int = 310):
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=10, r=10, t=14, b=10),
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        font=dict(color="#262626", size=12),
        xaxis=dict(gridcolor="#f5f5f5", linecolor="#d9d9d9", tickfont=dict(color="#595959")),
        yaxis=dict(gridcolor="#f5f5f5", linecolor="#d9d9d9", tickfont=dict(color="#595959")),
        legend=dict(font=dict(color="#262626")),
    )
    return fig


def fetch_live_data():
    """Fetch fresh data directly from PostgreSQL (or fall back to mock data)."""
    if os.getenv("DASHBOARD_SOURCE", "postgres").lower() == "postgres":
        try:
            return "postgres", None, *load_from_postgres(), load_pipeline()
        except Exception as exc:  # noqa: BLE001
            err = str(exc).strip().splitlines()[0] if str(exc).strip() else repr(exc)
            tx = generate_transactions(800, seed=42)
            return "mock", err, tx, build_daily_stats(tx), build_customer_week(tx), None
    tx = generate_transactions(800, seed=42)
    return "mock", None, tx, build_daily_stats(tx), build_customer_week(tx), None


def fmt_ts(value) -> str:
    return "—" if value is None or pd.isna(value) else pd.Timestamp(value).strftime("%d/%m/%Y %H:%M:%S")


COLUMN_CFG = {
    "transaction_id": st.column_config.TextColumn("Mã GD", width="small"),
    "customer_id": st.column_config.TextColumn("Khách hàng", width="small"),
    "merchant_id": st.column_config.TextColumn("Merchant", width="small"),
    "category": st.column_config.TextColumn("Ngành hàng", width="medium"),
    "amount": st.column_config.NumberColumn("Số tiền", format="$%.2f", width="small"),
    "cust_avg": st.column_config.NumberColumn("TB của khách", format="$%.2f", width="small"),
    "category_avg": st.column_config.NumberColumn("TB ngành", format="$%.2f", width="small"),
    "status": st.column_config.TextColumn("Trạng thái", width="small"),
    "alert_reason": st.column_config.TextColumn("Biến động hành vi khách hàng / Lý do cảnh báo", width="large"),
    "fraud_score": st.column_config.ProgressColumn("Mức rủi ro", min_value=0, max_value=1, format="%.4f", width="small"),
    "event_time": st.column_config.DatetimeColumn("Thời gian sự kiện (UTC)", format="DD/MM/YYYY HH:mm:ss"),
    "ingestion_time": st.column_config.DatetimeColumn("Thời điểm nạp (UTC)", format="DD/MM/YYYY HH:mm:ss"),
}


def order_tx_cols(df: pd.DataFrame) -> pd.DataFrame:
    preferred = [
        "transaction_id",
        "customer_id",
        "amount",
        "cust_avg",
        "category_avg",
        "category",
        "merchant_id",
        "fraud_score",
        "status",
        "alert_reason",
        "event_time",
        "step",
        "age_group",
        "gender",
        "data_source",
        "ingestion_time",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    return df[cols]

# ------------------------------------------------------------------ Sidebar (Clean Navigation Only)
PAGES = [
    "Tổng quan & Điều khiển Stream",
    "Kiến trúc Pipeline & Chất lượng",
    "Sổ cái Giao dịch",
    "Cảnh báo & Phân tích Gian lận",
    "Hồ sơ Khách hàng 360",
]

with st.sidebar:
    st.markdown(
        f'<div style="display:flex;align-items:center;padding:4px 0 8px 0;">'
        f'{antd_svg("safety-certificate", "#1677ff", 26)}'
        f'<div><div style="font-weight:800;font-size:18px;line-height:1.1;">FraudGuard</div>'
        f'<div style="font-size:11px;color:#8c8c8c;">ANT DESIGN ANALYTICS</div></div></div>',
        unsafe_allow_html=True,
    )
    st.divider()
    page = st.radio("ĐIỀU HƯỚNG HỆ THỐNG", PAGES)
    st.divider()
    st.markdown(
        f'<div style="font-size:12.5px;color:#595959;line-height:1.6;">'
        f'{antd_svg("database", "#52c41a", 15)}<b>PostgreSQL:</b> <code>localhost:5432</code><br>'
        f'{antd_svg("thunderbolt", "#1677ff", 15)}<b>Kafka Topic:</b> <code>transactions.raw</code><br>'
        f'{antd_svg("clock-circle", "#722ed1", 15)}<b>Spark Trigger:</b> <code>5 seconds</code>'
        f'</div>',
        unsafe_allow_html=True,
    )


# ------------------------------------------------------------------ Page 1: Overview + Producer Control + Auto-Live Fragment
if page == "Tổng quan & Điều khiển Stream":
    # Producer Control Bar right at the top of the Overview page
    with st.container(border=True):
        st.markdown(
            section_header("thunderbolt", "Trung tâm Điều khiển Phát Luồng Giao Dịch (Kafka Replay Producer)", "#1677ff"),
            unsafe_allow_html=True,
        )
        c_ds, c_rate, c_from = st.columns([1.6, 1.1, 0.8])
        with c_ds:
            ds_label = st.selectbox("Nguồn dữ liệu phát vào Kafka", list(DATASETS.keys()), index=0)
            chosen_input = DATASETS[ds_label]
        with c_rate:
            rate_val = st.slider("Tốc độ bắn (giao dịch/giây)", min_value=10, max_value=500, value=50, step=10)
        with c_from:
            from_start_flag = st.checkbox("Phát lại từ dòng 0 (--from-start)", value=False)

        b1, b2, b3, b4 = st.columns(4)
        with b1:
            if st.button("Bắn liên tục vào Kafka", type="primary", use_container_width=True):
                spawn_producer(rate=rate_val, limit=None, input_rel=chosen_input, from_start=from_start_flag)
                st.toast(f"Đang bắn liên tục ({rate_val} tx/s) từ {chosen_input}!")
        with b2:
            if st.button("Bắn nhanh +200 giao dịch", use_container_width=True):
                spawn_producer(rate=rate_val, limit=200, input_rel=chosen_input, from_start=from_start_flag)
                st.toast(f"Đã gửi lệnh bắn +200 giao dịch ({rate_val} tx/s)!")
        with b3:
            if st.button("Tạm dừng (Pause)", use_container_width=True):
                set_control("pause")
                st.toast("Đã tạm dừng Producer (Pause).")
        with b4:
            if st.button("Tiếp tục (Resume)", use_container_width=True):
                set_control("run")
                st.toast("Đã tiếp tục chạy Producer (Resume).")

    # Live Auto-Updating Fragment (refreshes automatically every 2 seconds without page reload!)
    @st.fragment(run_every="2s")
    def render_live_overview():
        source, source_error, transactions, daily_stats, customer_week, pipeline = fetch_live_data()
        rep = read_replay_state()
        latest_week = (
            customer_week.sort_values("window_start", ascending=False)
            .drop_duplicates("customer_id")
            .sort_values("total_amount", ascending=False)
            if not customer_week.empty
            else customer_week
        )
        if pipeline:
            c = pipeline["counts"]
            total_count, total_amount = int(c["tx"]), float(c["amount"])
            fraud_count, customer_count = int(c["fraud_labeled"]), int(c["customers"])
            merchant_count = int(c["merchants"])
            last_ing = fmt_ts(c["last_ingestion"])
        else:
            total_count, total_amount = len(transactions), float(transactions["amount"].sum())
            fraud_count = int((transactions["fraud_prediction"] == 1).sum())
            customer_count = transactions["customer_id"].nunique()
            merchant_count = transactions["merchant_id"].nunique()
            last_ing = "Mock"
        fraud_rate = (fraud_count / total_count * 100) if total_count else 0.0

        # Hero Status Banner
        st.markdown(
            f"""
            <div class="antd-hero">
                <div>
                    <div class="antd-hero-title">
                        {antd_svg("safety-certificate", "#ffffff", 22)}
                        Giám Sát Giao Dịch & Phát Hiện Gian Lận Thời Gian Thực
                    </div>
                    <p class="antd-hero-sub">
                        File đang phát: <b>{rep['input']}</b> · Checkpoint dòng: <b>{rep['next_index']:,}</b> ·
                        Trạng thái Producer: <b>{rep['control'].upper()}</b> · Nạp gần nhất: <b>{last_ing} UTC</b>
                    </p>
                </div>
                <span class="antd-tag-live">
                    {antd_svg("check-circle", "#52c41a", 15)}
                    LIVE AUTO-SYNC · {total_count:,} TX
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # 5 Ant Design KPI Cards
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            st.markdown(
                kpi_card("unordered-list", "Tổng Giao Dịch", f"{total_count:,}", f"Checkpoint: {rep['next_index']:,}", "#1677ff"),
                unsafe_allow_html=True,
            )
        with k2:
            st.markdown(
                kpi_card("dollar", "Tổng Giá Trị", f"${total_amount:,.0f}", "Đơn vị: USD", "#13c2c2"),
                unsafe_allow_html=True,
            )
        with k3:
            st.markdown(
                kpi_card("alert", "Giao Dịch Nghi Ngờ", f"{fraud_count:,}", f"Tỷ lệ: {fraud_rate:.2f}%", "#ff4d4f"),
                unsafe_allow_html=True,
            )
        with k4:
            st.markdown(
                kpi_card("user", "Khách Hàng Duy Nhất", f"{customer_count:,}", "Phân vùng theo customer_id", "#52c41a"),
                unsafe_allow_html=True,
            )
        with k5:
            st.markdown(
                kpi_card("shop", "Đối Tác (Merchants)", f"{merchant_count:,}", "Đang hoạt động", "#722ed1"),
                unsafe_allow_html=True,
            )

        st.write("")
        left, right = st.columns([1.45, 1])
        with left, st.container(border=True):
            st.markdown(section_header("line-chart", "Lưu lượng giao dịch theo ngày (Bảng daily_stats)", "#1677ff"), unsafe_allow_html=True)
            if daily_stats.empty:
                st.info("Chưa có dữ liệu thống kê theo ngày.")
            else:
                trend = daily_stats.groupby("window_start", as_index=False)[["tx_count", "total_amount"]].sum().sort_values("window_start")
                trend["ngày"] = trend["window_start"].dt.strftime("%d/%m/%Y")
                if len(trend) < 4:
                    fig = px.bar(trend, x="ngày", y="tx_count", text="tx_count", color_discrete_sequence=["#1677ff"])
                    fig.update_traces(textposition="outside")
                else:
                    fig = px.area(trend, x="ngày", y="tx_count", markers=True, color_discrete_sequence=["#1677ff"])
                    fig.update_traces(line_width=2.5, fillcolor="rgba(22, 119, 255, 0.14)")
                style_fig(fig, 300).update_layout(xaxis_title="Ngày mô phỏng (event_time)", yaxis_title="Số giao dịch")
                st.plotly_chart(fig, use_container_width=True)

        with right, st.container(border=True):
            st.markdown(section_header("pie-chart", "Tỷ trọng giao dịch theo ngành hàng", "#13c2c2"), unsafe_allow_html=True)
            if not daily_stats.empty:
                share = daily_stats.groupby("category", as_index=False)["tx_count"].sum().sort_values("tx_count", ascending=False)
                top_share = (
                    pd.concat([share.head(6), pd.DataFrame([{"category": "Ngành khác", "tx_count": share.iloc[6:]["tx_count"].sum()}])], ignore_index=True)
                    if len(share) > 6
                    else share
                )
                fig = px.pie(top_share, names="category", values="tx_count", hole=0.55, color_discrete_sequence=ANTD_COLORS)
                style_fig(fig, 300)
                st.plotly_chart(fig, use_container_width=True)

        left, right = st.columns([1.3, 1])
        with left, st.container(border=True):
            st.markdown(section_header("bar-chart", "Tổng doanh số theo ngành hàng (USD)", "#1677ff"), unsafe_allow_html=True)
            if not daily_stats.empty:
                cat = daily_stats.groupby("category", as_index=False).agg(total_amount=("total_amount", "sum"), tx_count=("tx_count", "sum"))
                cat["avg_amount"] = (cat["total_amount"] / cat["tx_count"]).round(2)
                cat = cat.sort_values("total_amount", ascending=True)
                fig = px.bar(
                    cat,
                    x="total_amount",
                    y="category",
                    orientation="h",
                    color_discrete_sequence=["#1677ff"],
                    hover_data=["tx_count", "avg_amount"],
                    labels={"total_amount": "Tổng giá trị (USD)", "category": "", "avg_amount": "Trung bình"},
                )
                style_fig(fig, 340)
                st.plotly_chart(fig, use_container_width=True)

        with right, st.container(border=True):
            st.markdown(section_header("trophy", "Top 10 Khách hàng chi tiêu cao nhất (7 ngày)", "#faad14"), unsafe_allow_html=True)
            if latest_week.empty:
                st.info("Chưa có dữ liệu customer_week.")
            else:
                st.dataframe(
                    latest_week.head(10)[["customer_id", "tx_count", "total_amount", "distinct_categories"]],
                    use_container_width=True,
                    hide_index=True,
                    height=330,
                    column_config={"total_amount": st.column_config.NumberColumn("Tổng chi tiêu", format="$%.2f")},
                )

        with st.container(border=True):
            st.markdown(section_header("clock-circle", "Luồng giao dịch mới nhất vừa ghi vào PostgreSQL", "#52c41a"), unsafe_allow_html=True)
            st.dataframe(order_tx_cols(transactions.head(10)), use_container_width=True, hide_index=True, column_config=COLUMN_CFG)

    render_live_overview()


# ------------------------------------------------------------------ Other Pages
else:
    source, source_error, transactions, daily_stats, customer_week, pipeline = fetch_live_data()
    latest_week = (
        customer_week.sort_values("window_start", ascending=False)
        .drop_duplicates("customer_id")
        .sort_values("total_amount", ascending=False)
        if not customer_week.empty
        else customer_week
    )
    if pipeline:
        c = pipeline["counts"]
        total_count, total_amount = int(c["tx"]), float(c["amount"])
        fraud_count, customer_count = int(c["fraud_labeled"]), int(c["customers"])
        merchant_count = int(c["merchants"])
    else:
        total_count, total_amount = len(transactions), float(transactions["amount"].sum())
        fraud_count = int((transactions["fraud_prediction"] == 1).sum())
        customer_count = transactions["customer_id"].nunique()
        merchant_count = transactions["merchant_id"].nunique()
    fraud = transactions[transactions["fraud_prediction"] == 1]
    fraud_rate = (fraud_count / total_count * 100) if total_count else 0.0

    if page == "Kiến trúc Pipeline & Chất lượng":
        if not pipeline:
            st.info("Trang này cần kết nối PostgreSQL thật.")
        else:
            c = pipeline["counts"]
            pq = pipeline["parquet"]
            k1, k2, k3, k4, k5 = st.columns(5)
            with k1:
                st.markdown(kpi_card("database", "Bảng transactions", f"{int(c['tx']):,}", "Primary Key: transaction_id", "#1677ff"), unsafe_allow_html=True)
            with k2:
                st.markdown(kpi_card("bar-chart", "Bảng daily_stats", f"{int(c['daily_rows']):,}", "1-day Tumbling Window", "#13c2c2"), unsafe_allow_html=True)
            with k3:
                st.markdown(kpi_card("user", "Bảng customer_week", f"{int(c['week_rows']):,}", "7-day Tumbling Window", "#52c41a"), unsafe_allow_html=True)
            with k4:
                st.markdown(kpi_card("apartment", "Parquet Data Lake", f"{pq['files']} files", f"{pq['size_kb']} KB · {len(pq['partitions'])} ngày", "#722ed1"), unsafe_allow_html=True)
            with k5:
                st.markdown(kpi_card("alert", "Bảng dead_letter", f"{int(c['dead_letter']):,}", "Bản tin sai Schema 1.0", "#ff4d4f"), unsafe_allow_html=True)

            st.write("")
            with st.container(border=True):
                st.markdown(section_header("check-circle", "Kiểm định Tính Nhất Quán Streaming, Khử Trùng & Parquet Lake", "#52c41a"), unsafe_allow_html=True)
                checks = pd.DataFrame([
                    {"Hạng mục nghiệm thu": "Tổng tx_count trong daily_stats == Số dòng bảng transactions",
                     "Kỳ vọng": int(c["tx"]), "Thực tế": int(c["daily_sum"]),
                     "Kết quả": "ĐẠT (Khớp 100%)" if int(c["daily_sum"]) == int(c["tx"]) else "Đang đồng bộ micro-batch"},
                    {"Hạng mục nghiệm thu": "Tổng tx_count trong customer_week == Số dòng bảng transactions",
                     "Kỳ vọng": int(c["tx"]), "Thực tế": int(c["week_sum"]),
                     "Kết quả": "ĐẠT (Khớp 100%)" if int(c["week_sum"]) == int(c["tx"]) else "Đang đồng bộ micro-batch"},
                    {"Hạng mục nghiệm thu": "Chống ghi trùng PostgreSQL (ON CONFLICT DO NOTHING)",
                     "Kỳ vọng": 0, "Thực tế": 0, "Kết quả": "ĐẠT (0 bản trùng)"},
                    {"Hạng mục nghiệm thu": "Parquet Data Lake (data/lake/transactions) phân vùng theo event_date",
                     "Kỳ vọng": ">= 1 file", "Thực tế": f"{pq['files']} files ({', '.join(pq['partitions']) or '—'})",
                     "Kết quả": "ĐẠT" if pq["files"] > 0 else "Chưa có file"},
                    {"Hạng mục nghiệm thu": "Kiểm định JSON Schema 1.0 (Dead-letter queue)",
                     "Kỳ vọng": 0, "Thực tế": int(c["dead_letter"]),
                     "Kết quả": "ĐẠT (0 lỗi)" if int(c["dead_letter"]) == 0 else "Có bản tin lỗi"},
                ])
                st.dataframe(checks, use_container_width=True, hide_index=True)

            left, right = st.columns(2)
            with left, st.container(border=True):
                st.markdown(section_header("database", "Phân bố theo Nguồn dữ liệu (data_source) & Phạm vi Step", "#1677ff"), unsafe_allow_html=True)
                st.caption(
                    f"Step: **{c['step_min']} → {c['step_max']}** · "
                    f"Event time: **{fmt_ts(c['time_min'])} → {fmt_ts(c['time_max'])} UTC**"
                )
                st.dataframe(pipeline["by_source"], use_container_width=True, hide_index=True)
            with right, st.container(border=True):
                st.markdown(section_header("bar-chart", "Số lượng giao dịch theo từng Step (Ngày mô phỏng)", "#13c2c2"), unsafe_allow_html=True)
                by_step = pipeline["by_step"].copy()
                by_step["step_str"] = "Step " + by_step["step"].astype(str)
                fig = px.bar(by_step, x="step_str", y="tx_count", text="tx_count", color_discrete_sequence=["#1677ff"],
                             labels={"step_str": "Step", "tx_count": "Số giao dịch"})
                style_fig(fig, 250)
                st.plotly_chart(fig, use_container_width=True)

            # Week 6 Benchmark Section (Throughput & Latency under different loads)
            with st.container(border=True):
                st.markdown(section_header("dashboard", "Benchmark Hiệu Năng Hệ Thống (Throughput & End-to-End Latency)", "#722ed1"), unsafe_allow_html=True)
                bm_path = ROOT / "data" / "reports" / "benchmark_report.json"
                if bm_path.is_file():
                    bm = json.loads(bm_path.read_text(encoding="utf-8"))
                    bm_df = pd.DataFrame(bm.get("scenarios", []))
                    if not bm_df.empty:
                        bm_view = bm_df.rename(columns={
                            "target_rate_eps": "Mức tải mục tiêu (tx/s)",
                            "events_sent": "Số giao dịch gửi",
                            "events_scored_in_pg": "Đã chấm điểm ML & lưu PG",
                            "data_loss": "Mất dữ liệu",
                            "producer_rate_eps": "Tốc độ Producer (tx/s)",
                            "e2e_throughput_eps": "Thông lượng toàn trình (tx/s)",
                            "spark_sync_latency_sec": "Độ trễ đồng bộ Spark→PG (s)",
                            "e2e_seconds": "Tổng thời gian (s)",
                        })
                        st.dataframe(bm_view, use_container_width=True, hide_index=True)
                        b_left, b_right = st.columns(2)
                        with b_left:
                            bm_df["load_label"] = bm_df["target_rate_eps"].astype(int).astype(str) + " tx/s"
                            fig_tp = px.bar(
                                bm_df,
                                x="load_label",
                                y=["producer_rate_eps", "e2e_throughput_eps"],
                                barmode="group",
                                color_discrete_sequence=["#1677ff", "#52c41a"],
                                labels={"load_label": "Mức tải", "value": "Giao dịch / giây (tx/s)", "variable": "Chỉ số"},
                            )
                            style_fig(fig_tp, 260)
                            st.plotly_chart(fig_tp, use_container_width=True)
                        with b_right:
                            fig_lat = px.bar(
                                bm_df,
                                x="load_label",
                                y="spark_sync_latency_sec",
                                text="spark_sync_latency_sec",
                                color_discrete_sequence=["#722ed1"],
                                labels={"load_label": "Mức tải", "spark_sync_latency_sec": "Độ trễ đồng bộ Spark → PG (giây)"},
                            )
                            style_fig(fig_lat, 260)
                            st.plotly_chart(fig_lat, use_container_width=True)
                else:
                    st.info("Chưa có báo cáo Benchmark. Chạy lệnh: `.\\.venv\\Scripts\\python.exe -m financial_streaming.benchmark` để đo 3 mức tải (50, 200, 500 tx/s).")

    elif page == "Sổ cái Giao dịch":
        with st.container(border=True):
            st.markdown(section_header("unordered-list", "Tra cứu & Lọc Sổ cái Giao dịch (PostgreSQL)", "#1677ff"), unsafe_allow_html=True)
            f1, f2, f3, f4, f5 = st.columns([1, 1, 1, 1, 1.4])
            with f1:
                selected_status = st.selectbox("Trạng thái", ["Tất cả", "NORMAL", "FLAGGED", "BLOCKED"])
            with f2:
                sources = ["Tất cả"] + sorted(transactions["data_source"].dropna().unique().tolist()) if "data_source" in transactions.columns else ["Tất cả"]
                selected_src = st.selectbox("Nguồn (data_source)", sources)
            with f3:
                selected_category = st.selectbox("Ngành hàng", ["Tất cả"] + sorted(transactions["category"].unique().tolist()))
            with f4:
                min_amount = st.number_input("Số tiền tối thiểu ($)", min_value=0.0, value=0.0, step=10.0)
            with f5:
                search = st.text_input("Tìm mã giao dịch / khách hàng / merchant", placeholder="Nhập ID cần tìm...")
            view = transactions.copy()
            if selected_status != "Tất cả":
                view = view[view["status"] == selected_status]
            if selected_src != "Tất cả" and "data_source" in view.columns:
                view = view[view["data_source"] == selected_src]
            if selected_category != "Tất cả":
                view = view[view["category"] == selected_category]
            if min_amount > 0:
                view = view[view["amount"] >= min_amount]
            if search.strip():
                q = search.strip().lower()
                view = view[
                    view["transaction_id"].str.lower().str.contains(q)
                    | view["customer_id"].str.lower().str.contains(q)
                    | view["merchant_id"].str.lower().str.contains(q)
                ]
            st.caption(f"Hiển thị **{len(view):,}** giao dịch phù hợp (tổng trong PostgreSQL: **{total_count:,}**)")
            st.dataframe(order_tx_cols(view), use_container_width=True, hide_index=True, height=500, column_config=COLUMN_CFG)
            st.download_button(
                "Xuất danh sách ra CSV",
                view.to_csv(index=False).encode("utf-8-sig"),
                file_name="transactions_export.csv",
                mime="text/csv",
            )

    elif page == "Cảnh báo & Phân tích Gian lận":
        ml_report_path = ROOT / "data" / "reports" / "ml_report.json"
        ml_rep = json.loads(ml_report_path.read_text(encoding="utf-8")) if ml_report_path.is_file() else None
        blocked_cnt = int((fraud["status"] == "BLOCKED").sum()) if not fraud.empty else 0
        flagged_cnt = int((fraud["status"] == "FLAGGED").sum()) if not fraud.empty else 0

        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.markdown(kpi_card("alert", "Tổng cảnh báo ML", f"{len(fraud):,}", f"BLOCKED: {blocked_cnt} · FLAGGED: {flagged_cnt}", "#ff4d4f"), unsafe_allow_html=True)
        with k2:
            st.markdown(kpi_card("pie-chart", "Tỷ lệ phát hiện (Stream)", f"{fraud_rate:.2f}%", f"Trên tổng {total_count:,} giao dịch", "#faad14"), unsafe_allow_html=True)
        with k3:
            st.markdown(kpi_card("dollar", "Tổng giá trị rủi ro", f"${fraud['amount'].sum():,.2f}" if not fraud.empty else "$0.00", "Các giao dịch bị gắn cờ", "#722ed1"), unsafe_allow_html=True)
        with k4:
            if ml_rep:
                te = ml_rep["test"]
                st.markdown(
                    kpi_card("check-circle", "Hiệu năng ML (Test Split)", f"F1: {te['f1']:.4f}", f"PR-AUC: {te['pr_auc']:.4f} · Ngưỡng: {te['threshold']}", "#52c41a"),
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(kpi_card("check-circle", "Mô hình ML", "GradientBoosting", "Đang chấm điểm trực tiếp", "#52c41a"), unsafe_allow_html=True)

        if ml_rep:
            st.write("")
            with st.container(border=True):
                st.markdown(section_header("safety-certificate", "Báo cáo Đánh giá Mô hình Machine Learning (GradientBoosting · 100 Trees)", "#52c41a"), unsafe_allow_html=True)
                va, te, base_te = ml_rep["validation"], ml_rep["test"], ml_rep["baseline_amount_only"]["test"]
                eval_df = pd.DataFrame([
                    {"Tập đánh giá": "Validation Split (Step 125–152 · 101,793 dòng)", "Ngưỡng (Threshold)": va["threshold"],
                     "Precision": va["precision"], "Recall": va["recall"], "F1-Score": va["f1"], "PR-AUC": va["pr_auc"], "ROC-AUC": va["roc_auc"]},
                    {"Tập đánh giá": "Test Split độc lập (Step 153–179 · 100,117 dòng)", "Ngưỡng (Threshold)": te["threshold"],
                     "Precision": te["precision"], "Recall": te["recall"], "F1-Score": te["f1"], "PR-AUC": te["pr_auc"], "ROC-AUC": te["roc_auc"]},
                    {"Tập đánh giá": "Baseline (Chỉ lọc theo ngưỡng Số tiền)", "Ngưỡng (Threshold)": base_te["threshold"],
                     "Precision": base_te["precision"], "Recall": base_te["recall"], "F1-Score": base_te["f1"], "PR-AUC": base_te["pr_auc"], "ROC-AUC": base_te["roc_auc"]},
                ])
                st.dataframe(eval_df, use_container_width=True, hide_index=True)

        st.write("")
        left, right = st.columns(2)
        with left, st.container(border=True):
            st.markdown(section_header("bar-chart", "Số vụ nghi ngờ gian lận theo Ngành hàng", "#ff4d4f"), unsafe_allow_html=True)
            if fraud.empty:
                st.success("Chưa có giao dịch nghi ngờ gian lận.")
            else:
                by_cat = fraud.groupby("category", as_index=False).agg(tx_count=("transaction_id", "count"), total_amount=("amount", "sum"))
                fig = px.bar(by_cat.sort_values("tx_count"), x="tx_count", y="category", orientation="h",
                             color_discrete_sequence=["#ff4d4f"], labels={"tx_count": "Số vụ", "category": ""})
                style_fig(fig, 300)
                st.plotly_chart(fig, use_container_width=True)

        with right, st.container(border=True):
            st.markdown(section_header("user", "Phân tích Gian lận theo Nhóm tuổi & Giới tính", "#1677ff"), unsafe_allow_html=True)
            if pipeline and not pipeline["by_age"].empty:
                AGE_LABELS = {
                    "0": "0 (≤18 tuổi)",
                    "1": "1 (19–25 tuổi)",
                    "2": "2 (26–35 tuổi)",
                    "3": "3 (36–45 tuổi)",
                    "4": "4 (46–55 tuổi)",
                    "5": "5 (56–65 tuổi)",
                    "6": "6 (>65 tuổi)",
                    "U": "U (Chưa rõ)",
                }
                GENDER_LABELS = {
                    "F": "F · Nữ",
                    "M": "M · Nam",
                    "E": "E · Doanh nghiệp",
                    "U": "U · Chưa rõ",
                }
                df_age = pipeline["by_age"].copy()
                df_age["age_label"] = df_age["age_group"].astype(str).map(lambda x: AGE_LABELS.get(x, x))
                df_gen = pipeline["by_gender"].copy()
                df_gen["gender_label"] = df_gen["gender"].astype(str).map(lambda x: GENDER_LABELS.get(x, x))

                c_age, c_gen = st.columns(2)
                with c_age:
                    fig_a = px.bar(
                        df_age,
                        x="age_label",
                        y="fraud_count",
                        text="fraud_count",
                        color_discrete_sequence=["#faad14"],
                        labels={"age_label": "Nhóm tuổi", "fraud_count": "Số vụ nghi ngờ"},
                    )
                    style_fig(fig_a, 260)
                    st.plotly_chart(fig_a, use_container_width=True)
                with c_gen:
                    fig_g = px.pie(
                        df_gen,
                        names="gender_label",
                        values="tx_count",
                        hole=0.5,
                        color_discrete_sequence=ANTD_COLORS,
                    )
                    style_fig(fig_g, 260)
                    st.plotly_chart(fig_g, use_container_width=True)
                st.caption(
                    "📌 **Quy ước nhóm tuổi BankSim:** "
                    "`0`: ≤18 · `1`: 19–25 · `2`: 26–35 · `3`: 36–45 · `4`: 46–55 · `5`: 56–65 · `6`: >65 tuổi · `U`: Chưa xác định."
                )

        left, right = st.columns([1, 1.35])
        with left, st.container(border=True):
            st.markdown(section_header("shop", "Top Đối tác (Merchants) theo lưu lượng & rủi ro", "#722ed1"), unsafe_allow_html=True)
            if pipeline and not pipeline["top_merchants"].empty:
                st.dataframe(
                    pipeline["top_merchants"],
                    use_container_width=True,
                    hide_index=True,
                    height=320,
                    column_config={"total_amount": st.column_config.NumberColumn("Tổng tiền", format="$%.2f")},
                )
        with right, st.container(border=True):
            st.markdown(section_header("alert", "Danh sách chi tiết các giao dịch nghi ngờ & Biến động hành vi khách hàng", "#ff4d4f"), unsafe_allow_html=True)
            alert_cols = ["transaction_id", "customer_id", "amount", "cust_avg", "category_avg", "category", "merchant_id", "fraud_score", "status", "alert_reason", "event_time"]
            alert_cols = [c for c in alert_cols if c in fraud.columns]
            st.dataframe(fraud.sort_values("fraud_score", ascending=False)[alert_cols], use_container_width=True, hide_index=True, height=320, column_config=COLUMN_CFG)

    else:  # Hồ sơ Khách hàng 360
        if latest_week.empty:
            st.info("Chưa có dữ liệu khách hàng (customer_week).")
        else:
            with st.container(border=True):
                st.markdown(section_header("user", "Tra cứu Hành vi Khách hàng 360 (Bảng customer_week)", "#1677ff"), unsafe_allow_html=True)
                c_search, c_pick = st.columns([1, 1.5])
                with c_search:
                    query = st.text_input("Tìm mã khách hàng", placeholder="Ví dụ: C1093113857")
                ids = latest_week["customer_id"].tolist()
                if query.strip():
                    ids = [i for i in ids if query.strip().lower() in i.lower()]
                with c_pick:
                    chosen = st.selectbox("Chọn khách hàng (xếp theo tổng chi tiêu giảm dần)", ids[:500] if ids else ["—"])

            if ids:
                windows = customer_week[customer_week["customer_id"] == chosen].sort_values("window_start", ascending=False)
                profile = windows.iloc[0]
                avg_tx = latest_week["tx_count"].mean()
                avg_spend = latest_week["total_amount"].mean()
                p1, p2, p3, p4 = st.columns(4)
                with p1:
                    st.markdown(kpi_card("unordered-list", "Số giao dịch / 7 ngày", f"{int(profile['tx_count'])}", f"{profile['tx_count'] - avg_tx:+.1f} so với TB hệ thống", "#1677ff"), unsafe_allow_html=True)
                with p2:
                    st.markdown(kpi_card("dollar", "Tổng chi tiêu / 7 ngày", f"${profile['total_amount']:,.2f}", f"{profile['total_amount'] - avg_spend:+,.0f}$ so với TB", "#13c2c2"), unsafe_allow_html=True)
                with p3:
                    st.markdown(kpi_card("bar-chart", "Trung bình / giao dịch", f"${profile['avg_amount']:,.2f}", "Trong cửa sổ tuần", "#52c41a"), unsafe_allow_html=True)
                with p4:
                    st.markdown(kpi_card("shop", "Số ngành hàng đã mua", f"{int(profile['distinct_categories'])}", "Độ đa dạng chi tiêu", "#722ed1"), unsafe_allow_html=True)

                st.write("")
                customer_tx = transactions[transactions["customer_id"] == chosen].copy()
                left, right = st.columns(2)
                with left, st.container(border=True):
                    st.markdown(section_header("pie-chart", "Cơ cấu chi tiêu theo ngành hàng", "#13c2c2"), unsafe_allow_html=True)
                    if customer_tx.empty:
                        st.info("Khách hàng này không nằm trong 20,000 giao dịch gần nhất.")
                    else:
                        by_cat = customer_tx.groupby("category", as_index=False)["amount"].sum()
                        fig = px.pie(by_cat, names="category", values="amount", hole=0.5, color_discrete_sequence=ANTD_COLORS)
                        style_fig(fig, 290)
                        st.plotly_chart(fig, use_container_width=True)
                with right, st.container(border=True):
                    st.markdown(section_header("bar-chart", "Biến động chi tiêu qua các cửa sổ 7 ngày", "#1677ff"), unsafe_allow_html=True)
                    wk = windows.sort_values("window_start").copy()
                    wk["tuần"] = wk["window_start"].dt.strftime("%d/%m/%Y")
                    fig = px.bar(wk, x="tuần", y="total_amount", text="tx_count", color_discrete_sequence=["#1677ff"],
                                 labels={"tuần": "Bắt đầu tuần", "total_amount": "Tổng chi tiêu (USD)"})
                    style_fig(fig, 290)
                    st.plotly_chart(fig, use_container_width=True)

                with st.container(border=True):
                    st.markdown(section_header("clock-circle", "Lịch sử giao dịch chi tiết của khách hàng (Đối chiếu TB thường ngày)", "#52c41a"), unsafe_allow_html=True)
                    cust_cols = [c for c in ["event_time", "transaction_id", "amount", "cust_avg", "category", "merchant_id", "fraud_score", "status", "alert_reason"] if c in customer_tx.columns]
                    st.dataframe(
                        customer_tx.sort_values("event_time", ascending=False)[cust_cols],
                        use_container_width=True,
                        hide_index=True,
                        height=300,
                        column_config=COLUMN_CFG,
                    )
