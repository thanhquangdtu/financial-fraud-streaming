from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data.mock_data import build_customer_week, build_daily_stats, generate_transactions

st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .stApp { background: #f5f7fb; }
    [data-testid="stSidebar"] { background: #101b33; }
    [data-testid="stSidebar"] * { color: #eef3ff !important; }
    .main-title { font-size: 30px; font-weight: 800; color: #14213d; margin-bottom: 0; }
    .sub-title { color: #64748b; margin-top: 3px; margin-bottom: 22px; }
    div[data-testid="stMetric"] { background: white; border: 1px solid #e6eaf2; padding: 16px 18px; border-radius: 13px; box-shadow: 0 3px 12px rgba(20,33,61,.04); }
    div[data-testid="stMetricLabel"] p { color: #64748b; font-weight: 600; }
    div[data-testid="stMetricValue"] { color: #14213d; }
    .section-title { color: #14213d; font-size: 19px; font-weight: 750; margin: 8px 0 12px 0; }
    .small-note { color: #64748b; font-size: 12px; }
    .stButton>button { border-radius: 9px; font-weight: 650; }
</style>
""", unsafe_allow_html=True)


@st.cache_data(show_spinner=False)
def initial_data() -> pd.DataFrame:
    return generate_transactions(800, seed=42)


if "transactions" not in st.session_state:
    st.session_state.transactions = initial_data()

transactions = st.session_state.transactions.copy()
transactions["event_time"] = pd.to_datetime(transactions["event_time"], utc=True)
daily_stats = build_daily_stats(transactions)
customer_week = build_customer_week(transactions)

# Sidebar
with st.sidebar:
    st.markdown("# 🛡️ FraudGuard")
    st.caption("FINANCIAL FRAUD MONITORING")
    st.divider()
    page = st.radio(
        "ĐIỀU HƯỚNG",
        ["Tổng quan", "Giao dịch", "Cảnh báo gian lận", "Khách hàng 360"],
        label_visibility="visible",
    )
    st.divider()
    st.markdown("**Trạng thái hệ thống**")
    st.markdown("🟢 Mock data đang hoạt động")
    st.caption("Dữ liệu mô phỏng – chưa kết nối Kafka/PostgreSQL")
    st.divider()
    if st.button("＋ Tạo giao dịch mô phỏng", use_container_width=True):
        new_rows = generate_transactions(8, seed=int(pd.Timestamp.now().timestamp()) % 1_000_000)
        now = pd.Timestamp.now(tz="UTC")
        new_rows["event_time"] = [now - pd.Timedelta(seconds=i * 7) for i in range(len(new_rows))]
        st.session_state.transactions = pd.concat(
            [new_rows, st.session_state.transactions], ignore_index=True
        ).drop_duplicates("transaction_id").sort_values("event_time", ascending=False).head(1500).reset_index(drop=True)
        st.rerun()
    if st.button("↻ Làm mới dashboard", use_container_width=True):
        st.rerun()


# Header
st.markdown('<p class="main-title">Financial Fraud Detection</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-title">Dashboard giám sát giao dịch và phát hiện gian lận tài chính · Dữ liệu giả lập</p>', unsafe_allow_html=True)

fraud = transactions[transactions["fraud_prediction"] == 1]
normal_count = int((transactions["fraud_prediction"] == 0).sum())
total_count = len(transactions)
fraud_rate = (len(fraud) / total_count * 100) if total_count else 0


def money(value: float) -> str:
    return f"{value:,.0f}"


def show_kpis() -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng giao dịch", f"{total_count:,}", help="Số giao dịch trong bộ dữ liệu mock")
    c2.metric("Tổng giá trị", f"{money(transactions['amount'].sum())} USD")
    c3.metric("Giao dịch nghi ngờ", f"{len(fraud):,}", f"{fraud_rate:.2f}% tổng giao dịch")
    c4.metric("Giao dịch hợp lệ", f"{normal_count:,}")


if page == "Tổng quan":
    show_kpis()
    st.write("")
    left, right = st.columns([1.55, 1])
    with left:
        st.markdown('<div class="section-title">📈 Số lượng giao dịch theo ngày</div>', unsafe_allow_html=True)
        if not daily_stats.empty:
            trend = daily_stats.groupby("window_start", as_index=False)["tx_count"].sum().sort_values("window_start")
            trend["ngày"] = trend["window_start"].dt.strftime("%d/%m")
            fig = px.area(trend, x="ngày", y="tx_count", markers=True)
            fig.update_traces(line_width=3, fillcolor="rgba(59,130,246,0.14)")
            fig.update_layout(height=330, margin=dict(l=10, r=10, t=15, b=10),
                              paper_bgcolor="white", plot_bgcolor="white",
                              xaxis_title="", yaxis_title="Số giao dịch")
            st.plotly_chart(fig, use_container_width=True)
    with right:
        st.markdown('<div class="section-title">🍩 Tỷ lệ trạng thái</div>', unsafe_allow_html=True)
        status_df = transactions["status"].value_counts().rename_axis("status").reset_index(name="count")
        fig = px.pie(status_df, names="status", values="count", hole=.62,
                     color="status", color_discrete_map={"NORMAL": "#22c55e", "FLAGGED": "#f59e0b", "BLOCKED": "#ef4444"})
        fig.update_layout(height=330, margin=dict(l=5, r=5, t=15, b=10), paper_bgcolor="white",
                          legend_title_text="Trạng thái")
        st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns([1.2, 1])
    with left:
        st.markdown('<div class="section-title">💳 Giá trị giao dịch theo ngành hàng</div>', unsafe_allow_html=True)
        cat = transactions.groupby("category", as_index=False)["amount"].sum().sort_values("amount", ascending=True)
        fig = px.bar(cat, x="amount", y="category", orientation="h", color="amount", color_continuous_scale="Blues")
        fig.update_layout(height=350, margin=dict(l=5, r=5, t=10, b=10), paper_bgcolor="white",
                          plot_bgcolor="white", xaxis_title="Tổng giá trị (USD)", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)
    with right:
        st.markdown('<div class="section-title">🚨 Cảnh báo mới nhất</div>', unsafe_allow_html=True)
        alerts = fraud.sort_values("event_time", ascending=False).head(7)
        if alerts.empty:
            st.success("Chưa có cảnh báo gian lận.")
        else:
            st.dataframe(alerts[["transaction_id", "customer_id", "amount", "fraud_score", "status"]],
                         use_container_width=True, hide_index=True,
                         column_config={
                             "amount": st.column_config.NumberColumn("Số tiền", format="$%.2f"),
                             "fraud_score": st.column_config.ProgressColumn("Fraud score", min_value=0, max_value=1, format="%.4f"),
                         })

elif page == "Giao dịch":
    show_kpis()
    st.markdown('<div class="section-title">📋 Danh sách giao dịch</div>', unsafe_allow_html=True)
    f1, f2, f3 = st.columns([1, 1, 1.2])
    with f1:
        selected_status = st.selectbox("Trạng thái", ["Tất cả", "NORMAL", "FLAGGED", "BLOCKED"])
    with f2:
        selected_category = st.selectbox("Ngành hàng", ["Tất cả"] + sorted(transactions["category"].unique().tolist()))
    with f3:
        search = st.text_input("Tìm mã giao dịch / khách hàng", placeholder="Nhập transaction_id hoặc customer_id")
    view = transactions.copy()
    if selected_status != "Tất cả":
        view = view[view["status"] == selected_status]
    if selected_category != "Tất cả":
        view = view[view["category"] == selected_category]
    if search.strip():
        q = search.strip().lower()
        view = view[view["transaction_id"].str.lower().str.contains(q) | view["customer_id"].str.lower().str.contains(q)]
    st.caption(f"Hiển thị {len(view):,} / {total_count:,} giao dịch")
    st.dataframe(view, use_container_width=True, hide_index=True, height=520,
                 column_config={
                     "event_time": st.column_config.DatetimeColumn("Thời gian (UTC)", format="DD/MM/YYYY HH:mm:ss"),
                     "amount": st.column_config.NumberColumn("Số tiền", format="$%.2f"),
                     "fraud_score": st.column_config.ProgressColumn("Fraud score", min_value=0, max_value=1, format="%.4f"),
                 })
    st.download_button("⬇️ Tải giao dịch CSV", view.to_csv(index=False).encode("utf-8-sig"),
                       file_name="transactions_mock.csv", mime="text/csv")

elif page == "Cảnh báo gian lận":
    st.markdown('<div class="section-title">🚨 Fraud Alerts</div>', unsafe_allow_html=True)
    a, b, c = st.columns(3)
    a.metric("Tổng cảnh báo", f"{len(fraud):,}")
    b.metric("Đã chặn", f"{int((fraud['status'] == 'BLOCKED').sum()):,}")
    c.metric("Đang gắn cờ", f"{int((fraud['status'] == 'FLAGGED').sum()):,}")
    threshold = st.slider("Ngưỡng điểm rủi ro", 0.0, 1.0, 0.75, 0.05)
    alerts = fraud[fraud["fraud_score"] >= threshold].sort_values("fraud_score", ascending=False)
    st.caption(f"Có {len(alerts):,} giao dịch đạt ngưỡng rủi ro đã chọn.")
    st.dataframe(alerts, use_container_width=True, hide_index=True, height=500,
                 column_config={
                     "event_time": st.column_config.DatetimeColumn("Thời gian (UTC)", format="DD/MM/YYYY HH:mm:ss"),
                     "amount": st.column_config.NumberColumn("Số tiền", format="$%.2f"),
                     "fraud_score": st.column_config.ProgressColumn("Fraud score", min_value=0, max_value=1, format="%.4f"),
                 })

else:  # Customer 360
    st.markdown('<div class="section-title">👤 Customer 360 – Tra cứu hành vi khách hàng</div>', unsafe_allow_html=True)
    if customer_week.empty:
        st.info("Chưa có dữ liệu khách hàng trong 7 ngày gần nhất.")
    else:
        customer_ids = customer_week["customer_id"].tolist()
        chosen_customer = st.selectbox("Chọn mã khách hàng", customer_ids)
        profile = customer_week[customer_week["customer_id"] == chosen_customer].iloc[0]
        p1, p2, p3, p4 = st.columns(4)
        p1.metric("Số giao dịch / 7 ngày", f"{int(profile['tx_count'])}")
        p2.metric("Tổng chi tiêu", f"${profile['total_amount']:,.2f}")
        p3.metric("Trung bình / giao dịch", f"${profile['avg_amount']:,.2f}")
        p4.metric("Số ngành hàng", f"{int(profile['distinct_categories'])}")
        customer_tx = transactions[transactions["customer_id"] == chosen_customer].copy()
        left, right = st.columns([1, 1])
        with left:
            st.markdown("**Chi tiêu theo ngành hàng**")
            by_cat = customer_tx.groupby("category", as_index=False)["amount"].sum()
            fig = px.pie(by_cat, names="category", values="amount", hole=.45)
            fig.update_layout(height=330, margin=dict(l=5, r=5, t=10, b=10), paper_bgcolor="white")
            st.plotly_chart(fig, use_container_width=True)
        with right:
            st.markdown("**Lịch sử giao dịch**")
            st.dataframe(customer_tx.sort_values("event_time", ascending=False)[
                ["event_time", "transaction_id", "category", "amount", "status"]
            ], use_container_width=True, hide_index=True, height=330,
                column_config={"event_time": st.column_config.DatetimeColumn("Thời gian", format="DD/MM HH:mm"),
                               "amount": st.column_config.NumberColumn("Số tiền", format="$%.2f")})

st.divider()
st.markdown('<p class="small-note">© 2026 Financial Fraud Detection · Streamlit UI · Mock data only</p>', unsafe_allow_html=True)
