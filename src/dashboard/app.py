import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import time
import io
from datetime import datetime, date

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────
API_BASE = "http://fastapi:8000"

st.set_page_config(
    page_title="AML Transaction Monitoring",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
    div[data-testid="stMetricValue"] { font-size: 2rem; }
</style>
""",
    unsafe_allow_html=True,
)


# ─────────────────────────────────────────
# API helpers with retry
# ─────────────────────────────────────────
def api_get(path, params=None, timeout=60, retries=3):
    url = f"{API_BASE}{path}"
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            return None
        except Exception:
            if i < retries - 1:
                time.sleep(2)
    return None


def api_post(path, json=None, timeout=30):
    try:
        r = requests.post(f"{API_BASE}{path}", json=json, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


def api_patch(path, json=None, timeout=15):
    try:
        r = requests.patch(f"{API_BASE}{path}", json=json, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception:
        return None


@st.cache_data(ttl=300)
def get_summary():
    return api_get("/analytics/summary", timeout=60) or {}


@st.cache_data(ttl=300)
def get_risk_distribution():
    return api_get("/analytics/risk-distribution", timeout=60) or {}


@st.cache_data(ttl=300)
def get_daily_stats():
    return api_get("/analytics/daily-stats", timeout=60) or []


@st.cache_data(ttl=300)
def get_payment_type_stats():
    return api_get("/analytics/payment-type-stats", timeout=60) or []


@st.cache_data(ttl=60)
def get_alerts(
    status=None,
    typology=None,
    limit=500,
    date_from=None,
    date_to=None,
    min_score=None,
    max_score=None,
):
    params = {"limit": limit}
    if status:
        params["status"] = status
    if typology:
        params["typology"] = typology
    data = api_get("/alerts", params=params, timeout=60)
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if df.empty:
        return df
    df["created_at"] = pd.to_datetime(df["created_at"])
    if date_from:
        df = df[df["created_at"].dt.date >= date_from]
    if date_to:
        df = df[df["created_at"].dt.date <= date_to]
    if min_score is not None:
        df = df[df["risk_score"] >= min_score]
    if max_score is not None:
        df = df[df["risk_score"] <= max_score]
    return df


def check_health():
    return api_get("/health", timeout=10) or {"status": "unreachable"}


# ─────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/5/51/IBM_logo.svg", width=80
    )
    st.title("AML Monitoring")
    st.caption("IBM Consulting — AML Pipeline v2.0")
    st.divider()

    page = st.radio(
        "Navigation",
        [
            "📊 Executive Summary",
            "🚨 Alert Management",
            "📈 Risk Analytics",
            "🤖 Model Performance",
            "🔎 Transaction Search",
        ],
        label_visibility="collapsed",
    )

    st.divider()
    health = check_health()
    status_color = "🟢" if health.get("status") == "ok" else "🔴"
    st.caption(f"{status_color} API: {health.get('status', 'unknown')}")
    st.caption(f"Scores: {health.get('scores_count', 0):,}")
    st.caption(f"Model: {'✅' if health.get('model_loaded') else '❌'}")

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()


# ─────────────────────────────────────────
# Page 1: Executive Summary
# ─────────────────────────────────────────
if page == "📊 Executive Summary":
    st.title("📊 Executive Summary")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    with st.spinner("Loading summary..."):
        summary = get_summary()
        dist = get_risk_distribution()

    if not summary:
        st.error("Cannot connect to API — please wait and refresh")
        st.stop()

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Transactions", f"{summary.get('total_transactions', 0):,}")
    with col2:
        st.metric(
            "Laundering Cases",
            f"{summary.get('total_laundering', 0):,}",
            delta=f"{summary.get('laundering_rate_pct', 0):.4f}%",
        )
    with col3:
        st.metric("Total Alerts", f"{summary.get('total_alerts', 0):,}")
    with col4:
        st.metric(
            "Open Alerts", f"{summary.get('open_alerts', 0):,}", delta_color="inverse"
        )
    with col5:
        critical = dist.get("risk_levels", {}).get("CRITICAL", 0)
        st.metric("CRITICAL Risk", f"{critical:,}", delta_color="inverse")

    st.divider()

    st.subheader("📅 Daily Trend")
    with st.spinner("Loading daily stats..."):
        daily_data = get_daily_stats()

    if daily_data:
        daily_df = pd.DataFrame(daily_data)
        daily_df["date"] = pd.to_datetime(daily_df["date"])

        col1, col2 = st.columns(2)
        with col1:
            date_from = st.date_input(
                "From",
                value=date(2022, 9, 1),
                min_value=date(2022, 9, 1),
                max_value=date(2022, 9, 17),
            )
        with col2:
            date_to = st.date_input(
                "To",
                value=date(2022, 9, 17),
                min_value=date(2022, 9, 1),
                max_value=date(2022, 9, 17),
            )

        mask = (daily_df["date"].dt.date >= date_from) & (
            daily_df["date"].dt.date <= date_to
        )
        filtered_daily = daily_df[mask]

        col1, col2 = st.columns(2)
        with col1:
            fig = px.line(
                filtered_daily,
                x="date",
                y="transaction_count",
                title="Daily Transaction Volume",
                markers=True,
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                filtered_daily,
                x="date",
                y="alert_count",
                title="Daily Alert Count",
                color="alert_count",
                color_continuous_scale="Reds",
            )
            fig.update_layout(height=300, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Risk Level Distribution")
        if dist.get("risk_levels"):
            risk_df = pd.DataFrame(
                [
                    {
                        "Risk Level": k,
                        "Count": v,
                        "Pct": round(v / dist["total"] * 100, 2),
                    }
                    for k, v in dist["risk_levels"].items()
                ]
            )
            colors = {
                "LOW": "#4CAF50",
                "MEDIUM": "#FFC107",
                "HIGH": "#FF9800",
                "CRITICAL": "#f44336",
            }
            fig = px.bar(
                risk_df,
                x="Risk Level",
                y="Count",
                color="Risk Level",
                color_discrete_map=colors,
                text="Pct",
                title="Transactions by Risk Level",
            )
            fig.update_traces(texttemplate="%{text}%", textposition="outside")
            fig.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("ML Score Distribution")
        if dist.get("histogram"):
            hist_df = pd.DataFrame(dist["histogram"])
            fig = px.bar(
                hist_df,
                x="bin",
                y="count",
                title="ML Probability Histogram",
                color="count",
                color_continuous_scale="Reds",
            )
            fig.update_layout(height=350, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Alerts by Typology")
        typology_data = summary.get("alerts_by_typology", [])
        if typology_data:
            typ_df = pd.DataFrame(typology_data)
            fig = px.pie(
                typ_df,
                values="count",
                names="typology",
                title="Alert Distribution by Typology",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top Risk Senders")
        senders = summary.get("top_risk_senders", [])
        if senders:
            sender_df = pd.DataFrame(senders)
            fig = px.bar(
                sender_df,
                x="avg_rule_score",
                y="sender_account_masked",
                orientation="h",
                title="Top 10 High-Risk Senders",
                color="avg_rule_score",
                color_continuous_scale="Reds",
                text="tx_count",
            )
            fig.update_traces(texttemplate="(%{text} txns)", textposition="outside")
            fig.update_layout(
                height=350,
                coloraxis_showscale=False,
                yaxis={"categoryorder": "total ascending"},
            )
            st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────
# Page 2: Alert Management
# ─────────────────────────────────────────
elif page == "🚨 Alert Management":
    st.title("🚨 Alert Management")
    st.caption("Compliance Officer Workflow")

    col1, col2, col3 = st.columns(3)
    with col1:
        filter_status = st.selectbox(
            "Filter by Status", ["All", "OPEN", "INVESTIGATING", "CLOSED"]
        )
    with col2:
        filter_typology = st.selectbox(
            "Filter by Typology",
            ["All", "cross_currency_high_risk", "structuring_rapid", "structuring"],
        )
    with col3:
        limit = st.slider("Max Records", 50, 500, 100, 50)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        date_from = st.date_input(
            "Alert Date From", value=date(2022, 5, 1), key="alert_date_from"
        )
    with col2:
        date_to = st.date_input(
            "Alert Date To", value=date(2026, 12, 31), key="alert_date_to"
        )
    with col3:
        min_score = st.number_input("Min Risk Score", 0.0, 1.0, 0.0, 0.05)
    with col4:
        max_score = st.number_input("Max Risk Score", 0.0, 1.0, 1.0, 0.05)

    status_param = None if filter_status == "All" else filter_status
    typology_param = None if filter_typology == "All" else filter_typology

    with st.spinner("Loading alerts..."):
        df = get_alerts(
            status=status_param,
            typology=typology_param,
            limit=limit,
            date_from=date_from,
            date_to=date_to,
            min_score=min_score,
            max_score=max_score,
        )

    if df.empty:
        st.info("No alerts found")
        st.stop()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Showing", f"{len(df):,} alerts")
    with col2:
        st.metric("OPEN", f"{len(df[df['status'] == 'OPEN']):,}")
    with col3:
        st.metric("INVESTIGATING", f"{len(df[df['status'] == 'INVESTIGATING']):,}")
    with col4:
        st.metric("CLOSED", f"{len(df[df['status'] == 'CLOSED']):,}")

    st.divider()

    with st.expander("⚡ Bulk Update Status"):
        col1, col2 = st.columns(2)
        with col1:
            selected_ids = st.text_area(
                "Alert IDs (one per line)",
                height=100,
                placeholder="paste alert IDs here...",
            )
        with col2:
            new_status = st.selectbox("New Status", ["INVESTIGATING", "CLOSED", "OPEN"])
            if st.button("Update Selected", type="primary"):
                ids = [x.strip() for x in selected_ids.strip().split("\n") if x.strip()]
                success = 0
                for aid in ids:
                    result = api_patch(f"/alerts/{aid}", json={"status": new_status})
                    if result and result.get("updated"):
                        success += 1
                st.success(f"Updated {success}/{len(ids)} alerts")
                st.cache_data.clear()
                st.rerun()

    st.subheader(f"Alerts ({len(df):,})")

    def color_status(val):
        colors = {
            "OPEN": "background-color: #f44336; color: white",
            "INVESTIGATING": "background-color: #FF9800; color: white",
            "CLOSED": "background-color: #4CAF50; color: white",
        }
        return colors.get(val, "")

    display_df = df[
        ["alert_id", "transaction_id", "risk_score", "typology", "status", "created_at"]
    ].copy()
    display_df["risk_score"] = display_df["risk_score"].round(4)
    display_df["created_at"] = pd.to_datetime(display_df["created_at"]).dt.strftime(
        "%Y-%m-%d %H:%M"
    )

    st.dataframe(
        display_df.style.applymap(color_status, subset=["status"]),
        use_container_width=True,
        height=400,
    )

    csv = display_df.to_csv(index=False)
    st.download_button(
        "📥 Export CSV",
        csv,
        f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        "text/csv",
    )

    st.divider()
    st.subheader("Update Individual Alert")
    col1, col2, col3 = st.columns(3)
    with col1:
        alert_id_input = st.text_input("Alert ID")
    with col2:
        new_status_single = st.selectbox(
            "New Status", ["OPEN", "INVESTIGATING", "CLOSED"], key="single_status"
        )
    with col3:
        st.write("")
        st.write("")
        if st.button("Update", type="primary"):
            if alert_id_input:
                result = api_patch(
                    f"/alerts/{alert_id_input}", json={"status": new_status_single}
                )
                if result and result.get("updated"):
                    st.success(f"✅ Updated → {new_status_single}")
                    st.cache_data.clear()
                else:
                    st.error("Update failed")


# ─────────────────────────────────────────
# Page 3: Risk Analytics
# ─────────────────────────────────────────
elif page == "📈 Risk Analytics":
    st.title("📈 Risk Analytics")

    with st.spinner("Loading analytics..."):
        summary = get_summary()
        dist = get_risk_distribution()

    if not summary or not dist:
        st.error("Cannot connect to API — please wait and refresh")
        st.stop()

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Avg ML Score", f"{dist.get('avg_score', 0):.4f}")
    with col2:
        st.metric("Threshold", f"{dist.get('threshold', 0.9):.2f}")
    with col3:
        total = dist.get("total", 1)
        critical = dist.get("risk_levels", {}).get("CRITICAL", 0)
        st.metric("CRITICAL Rate", f"{critical/total*100:.2f}%")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Risk Level Breakdown")
        risk_levels = dist.get("risk_levels", {})
        if risk_levels:
            fig = go.Figure(
                go.Funnel(
                    y=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                    x=[
                        risk_levels.get("CRITICAL", 0),
                        risk_levels.get("HIGH", 0),
                        risk_levels.get("MEDIUM", 0),
                        risk_levels.get("LOW", 0),
                    ],
                    textinfo="value+percent initial",
                    marker=dict(color=["#f44336", "#FF9800", "#FFC107", "#4CAF50"]),
                )
            )
            fig.update_layout(title="Risk Funnel", height=400)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Alert Score Distribution by Typology")
        typology_data = summary.get("alerts_by_typology", [])
        if typology_data:
            typ_df = pd.DataFrame(typology_data)
            colors = ["#2196F3", "#FF9800", "#f44336"]
            fig = go.Figure()
            for i, row in typ_df.iterrows():
                fig.add_trace(
                    go.Bar(
                        name=row["typology"],
                        x=[row["typology"]],
                        y=[row["count"]],
                        text=[f"Avg Score: {row['avg_score']}"],
                        marker_color=colors[i % len(colors)],
                    )
                )
            fig.update_layout(title="Alerts by Typology", height=400, showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("💳 Payment Type Risk Analysis")
    with st.spinner("Loading payment stats..."):
        payment_data = get_payment_type_stats()

    if payment_data:
        pay_df = pd.DataFrame(payment_data)
        col1, col2 = st.columns(2)
        with col1:
            fig = px.bar(
                pay_df.sort_values("avg_rule_score", ascending=True),
                x="avg_rule_score",
                y="payment_type",
                orientation="h",
                title="Avg Risk Score by Payment Type",
                color="avg_rule_score",
                color_continuous_scale="RdYlGn_r",
                text="tx_count",
            )
            fig.update_traces(texttemplate="%{text:,}", textposition="outside")
            fig.update_layout(height=400, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            fig = px.bar(
                pay_df.sort_values("laundering_rate", ascending=True),
                x="laundering_rate",
                y="payment_type",
                orientation="h",
                title="Laundering Rate by Payment Type (%)",
                color="laundering_rate",
                color_continuous_scale="Reds",
            )
            fig.update_layout(height=400, coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("ML Score Histogram (Detail)")
    if dist.get("histogram"):
        hist_df = pd.DataFrame(dist["histogram"])
        fig = px.bar(
            hist_df,
            x="bin",
            y="count",
            title="Full ML Probability Distribution",
            color="count",
            color_continuous_scale="RdYlGn_r",
            labels={"bin": "ML Score Range", "count": "Transaction Count"},
        )
        fig.add_vline(
            x=7.5,
            line_dash="dash",
            line_color="red",
            annotation_text=f"Threshold={dist.get('threshold', 0.9):.2f}",
        )
        fig.update_layout(height=400, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("Top Risk Senders")
    senders = summary.get("top_risk_senders", [])
    if senders:
        sender_df = pd.DataFrame(senders)
        st.dataframe(
            sender_df.rename(
                columns={
                    "sender_account_masked": "Account",
                    "tx_count": "Transactions",
                    "avg_rule_score": "Avg Risk Score",
                }
            ),
            use_container_width=True,
            hide_index=True,
        )


# ─────────────────────────────────────────
# Page 4: Model Performance
# ─────────────────────────────────────────
elif page == "🤖 Model Performance":
    st.title("🤖 Model Performance")
    st.caption("XGBoost AML Risk Model — IBM AML Dataset")

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Val AUC-ROC", "0.9362", help="Higher is better. Random=0.5")
    with col2:
        st.metric(
            "Val AUC-PR", "0.0066", help="Expected low due to 0.05% positive rate"
        )
    with col3:
        st.metric("Val Recall", "27.83%", help="% of laundering cases detected")
    with col4:
        st.metric("Val Precision", "0.83%", help="Low due to extreme class imbalance")
    with col5:
        st.metric("Best Threshold", "0.90", help="Optimized for F1 score")

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Model Metrics Comparison")
        metrics_df = pd.DataFrame(
            {
                "Metric": ["AUC-ROC", "AUC-PR", "Recall", "F1"],
                "Val": [0.9362, 0.0066, 0.2783, 0.0162],
                "Random Baseline": [0.5, 0.0005, 0.5, 0.001],
            }
        )
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                name="Our Model",
                x=metrics_df["Metric"],
                y=metrics_df["Val"],
                marker_color="#2196F3",
                text=[f"{v:.4f}" for v in metrics_df["Val"]],
                textposition="outside",
            )
        )
        fig.add_trace(
            go.Bar(
                name="Random Baseline",
                x=metrics_df["Metric"],
                y=metrics_df["Random Baseline"],
                marker_color="#9E9E9E",
                text=[f"{v:.4f}" for v in metrics_df["Random Baseline"]],
                textposition="outside",
            )
        )
        fig.update_layout(barmode="group", title="Model vs Random Baseline", height=400)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Confusion Matrix (Val Set)")
        fig = px.imshow(
            [[1523311, 25430], [555, 214]],
            labels=dict(x="Predicted", y="Actual", color="Count"),
            x=["Not Laundering", "Laundering"],
            y=["Not Laundering", "Laundering"],
            color_continuous_scale="Blues",
            text_auto=True,
            title="Confusion Matrix",
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("SHAP Feature Importance")
        shap_df = pd.DataFrame(
            {
                "feature": [
                    "rule_score",
                    "amount_log",
                    "payment_type_risk",
                    "sender_amount_sum_1h",
                    "amount_vs_sender_avg",
                    "sender_avg_amount",
                    "sender_tx_count_1h",
                    "is_structuring",
                    "amount",
                    "is_cross_currency",
                    "is_high_risk_type",
                    "tx_hour",
                    "is_round_amount",
                    "tx_day_of_week",
                    "is_weekend",
                ],
                "importance": [
                    0.0842,
                    0.0731,
                    0.0698,
                    0.0621,
                    0.0589,
                    0.0534,
                    0.0498,
                    0.0445,
                    0.0412,
                    0.0389,
                    0.0334,
                    0.0298,
                    0.0267,
                    0.0234,
                    0.0198,
                ],
            }
        ).sort_values("importance", ascending=True)
        fig = px.bar(
            shap_df,
            x="importance",
            y="feature",
            orientation="h",
            title="SHAP Feature Importance",
            color="importance",
            color_continuous_scale="Blues",
        )
        fig.update_layout(height=450, coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Model Configuration")
        config_df = pd.DataFrame(
            list(
                {
                    "Algorithm": "XGBoost (binary:logistic)",
                    "Training Rows": "5,091,431",
                    "Val Rows": "1,549,510",
                    "Scale Pos Weight": "2,155.47",
                    "Max Depth": "6",
                    "Learning Rate": "0.1",
                    "Subsample": "0.8",
                    "ColSample ByTree": "0.8",
                    "Early Stopping": "Round 65",
                    "Best Threshold": "0.90",
                    "Split Strategy": "Time-based (no leakage)",
                    "Train Period": "Sep 01-07, 2022",
                    "Val Period": "Sep 08-09, 2022",
                    "Test Period": "Sep 10-17, 2022",
                }.items()
            ),
            columns=["Parameter", "Value"],
        )
        st.dataframe(config_df, use_container_width=True, hide_index=True, height=420)

    st.divider()

    st.subheader("🎛️ Threshold Simulator")
    st.caption("ปรับ threshold เพื่อดู tradeoff ระหว่าง Precision และ Recall")

    dist = get_risk_distribution()
    sim_threshold = st.slider("Threshold", 0.05, 0.95, 0.90, 0.05)

    if dist.get("histogram"):
        bins = [h["count"] for h in dist["histogram"]]
        total = dist["total"]
        bins_above = sum(bins[int(sim_threshold * 10) :])
        est_flagged_pct = round(bins_above / total * 100, 2)
        tp_rate = min(0.95, max(0, (0.90 - sim_threshold) * 0.5 + 0.2783))
        precision = 0.0083 * (0.90 / max(sim_threshold, 0.05))

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Threshold", f"{sim_threshold:.2f}")
        with col2:
            st.metric("Est. Flagged", f"{bins_above:,} ({est_flagged_pct}%)")
        with col3:
            st.metric(
                "Est. Recall",
                f"{tp_rate*100:.1f}%",
                delta=f"{(tp_rate-0.2783)*100:+.1f}% vs current",
            )
        with col4:
            st.metric("Est. Precision", f"{precision*100:.2f}%")

        thresholds = [i / 20 for i in range(1, 19)]
        recalls = [min(0.95, max(0, (0.90 - t) * 0.5 + 0.2783)) for t in thresholds]
        precisions = [0.0083 * (0.90 / max(t, 0.05)) for t in thresholds]

        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=thresholds,
                y=recalls,
                name="Recall",
                line=dict(color="#2196F3", width=2),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=thresholds,
                y=precisions,
                name="Precision",
                line=dict(color="#f44336", width=2),
            )
        )
        fig.add_vline(
            x=sim_threshold,
            line_dash="dash",
            line_color="yellow",
            annotation_text=f"Current: {sim_threshold:.2f}",
        )
        fig.update_layout(
            title="Precision-Recall Tradeoff by Threshold",
            xaxis_title="Threshold",
            yaxis_title="Score",
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader("📝 Model Interpretation")
    st.info("""
    **Why AUC-ROC is high (0.93) but Recall is low (28%)?**

    This is expected behavior for extreme class imbalance (1:2,155 ratio).
    AUC-ROC measures the model's ability to **rank** suspicious transactions higher — our model does this well.
    Recall is low because threshold=0.90 minimizes false positives for compliance officers.

    **In production, we would:**
    - Lower threshold to 0.50 to increase recall
    - Use ensemble with rule-based alerts
    - Apply active learning from compliance officer feedback
    - Retrain monthly with new labeled data
    """)


# ─────────────────────────────────────────
# Page 5: Transaction Search
# ─────────────────────────────────────────
elif page == "🔎 Transaction Search":
    st.title("🔎 Transaction Search")
    st.caption("Lookup individual transaction risk score")

    tx_id = st.text_input(
        "Transaction ID",
        placeholder="Enter transaction ID...",
        help="MD5 hash transaction ID",
    )

    if st.button("🔍 Search", type="primary") and tx_id:
        with st.spinner("Looking up transaction..."):
            tx = api_get(f"/transactions/{tx_id.strip()}", timeout=15)
            alert = api_get(f"/alerts/by-transaction/{tx_id.strip()}", timeout=10)

        if not tx:
            st.error(f"Transaction `{tx_id}` not found")
        else:
            risk_level = tx.get("risk_level", "UNKNOWN")
            risk_colors = {
                "CRITICAL": "error",
                "HIGH": "warning",
                "MEDIUM": "warning",
                "LOW": "success",
            }
            getattr(st, risk_colors.get(risk_level, "info"))(
                f"Risk Level: **{risk_level}** | ML Score: **{tx.get('ml_probability', 'N/A')}**"
            )

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric(
                    "ML Probability",
                    (
                        f"{tx.get('ml_probability', 0):.4f}"
                        if tx.get("ml_probability")
                        else "N/A"
                    ),
                )
            with col2:
                st.metric(
                    "Rule Score",
                    f"{tx.get('rule_score', 0):.4f}" if tx.get("rule_score") else "N/A",
                )
            with col3:
                st.metric("Amount", f"${tx.get('amount', 0):,.2f}")
            with col4:
                st.metric(
                    "Known Laundering", "⚠️ YES" if tx.get("is_laundering") else "✅ NO"
                )

            st.divider()

            if alert:
                st.subheader("🚨 Associated Alert")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Alert ID", str(alert.get("alert_id", ""))[:8] + "...")
                with col2:
                    st.metric("Typology", alert.get("typology", ""))
                with col3:
                    status = alert.get("status", "")
                    color = {"OPEN": "🔴", "INVESTIGATING": "🟠", "CLOSED": "🟢"}.get(
                        status, "⚪"
                    )
                    st.metric("Status", f"{color} {status}")
                with col4:
                    st.metric("Risk Score", f"{alert.get('risk_score', 0):.4f}")

                new_status = st.selectbox(
                    "Update Alert Status",
                    ["OPEN", "INVESTIGATING", "CLOSED"],
                    index=["OPEN", "INVESTIGATING", "CLOSED"].index(
                        alert.get("status", "OPEN")
                    ),
                )
                if st.button("Update Alert", type="primary"):
                    result = api_patch(
                        f"/alerts/{alert.get('alert_id')}", json={"status": new_status}
                    )
                    if result and result.get("updated"):
                        st.success(f"✅ Alert updated → {new_status}")
                        st.cache_data.clear()
                    else:
                        st.error("Update failed")
            else:
                st.info("No alert associated with this transaction")

            st.divider()
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Transaction Details")
                for k, v in {
                    "Transaction ID": tx.get("transaction_id", ""),
                    "Timestamp": tx.get("timestamp", ""),
                    "Payment Type": tx.get("payment_type", ""),
                    "Payment Currency": tx.get("payment_currency", ""),
                    "Receiving Currency": tx.get("receiving_currency", ""),
                    "Cross Currency": "Yes" if tx.get("is_cross_currency") else "No",
                }.items():
                    st.text(f"{k}: {v}")

            with col2:
                st.subheader("Account Details")
                for k, v in {
                    "Sender Account": tx.get("sender_account_masked", ""),
                    "Sender Bank": tx.get("sender_bank", ""),
                    "Receiver Account": tx.get("receiver_account_masked", ""),
                    "Receiver Bank": tx.get("receiver_bank", ""),
                }.items():
                    st.text(f"{k}: {v}")

            st.divider()
            st.subheader("Risk Score Gauge")
            ml_prob = tx.get("ml_probability", 0) or 0
            fig = go.Figure(
                go.Indicator(
                    mode="gauge+number+delta",
                    value=ml_prob,
                    domain={"x": [0, 1], "y": [0, 1]},
                    title={"text": "ML Risk Score"},
                    delta={"reference": 0.9},
                    gauge={
                        "axis": {"range": [0, 1]},
                        "bar": {"color": "#2196F3"},
                        "steps": [
                            {"range": [0, 0.4], "color": "#4CAF50"},
                            {"range": [0.4, 0.7], "color": "#FFC107"},
                            {"range": [0.7, 0.9], "color": "#FF9800"},
                            {"range": [0.9, 1.0], "color": "#f44336"},
                        ],
                        "threshold": {
                            "line": {"color": "red", "width": 4},
                            "thickness": 0.75,
                            "value": 0.9,
                        },
                    },
                )
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Enter a Transaction ID to search")
        st.divider()
        st.subheader("💡 Real-time Scoring")
        st.caption("Score a new transaction using the ML model")

        with st.form("predict_form"):
            col1, col2, col3 = st.columns(3)
            with col1:
                amount = st.number_input("Amount", value=9500.0, min_value=0.0)
                amount_log = st.number_input("Amount Log", value=9.16, min_value=0.0)
                is_cross_currency = st.selectbox("Cross Currency", [0, 1])
                is_structuring = st.selectbox("Is Structuring", [0, 1])
            with col2:
                tx_hour = st.slider("Transaction Hour", 0, 23, 10)
                tx_day_of_week = st.slider("Day of Week", 0, 6, 2)
                is_weekend = st.selectbox("Is Weekend", [0, 1])
                is_round_amount = st.selectbox("Round Amount", [0, 1])
            with col3:
                sender_tx_count_1h = st.number_input(
                    "Sender TX Count 1h", value=3, min_value=0
                )
                sender_amount_sum_1h = st.number_input(
                    "Sender Amount Sum 1h", value=28500.0
                )
                sender_avg_amount = st.number_input("Sender Avg Amount", value=9500.0)
                amount_vs_sender_avg = st.number_input(
                    "Amount vs Sender Avg", value=1.0
                )

            col1, col2 = st.columns(2)
            with col1:
                payment_type_risk = st.number_input(
                    "Payment Type Risk", value=0.5, min_value=0.0, max_value=1.0
                )
                is_high_risk_type = st.selectbox("High Risk Type", [0, 1])
            with col2:
                rule_score = st.number_input(
                    "Rule Score", value=0.5, min_value=0.0, max_value=1.0
                )

            submitted = st.form_submit_button("🎯 Predict Risk", type="primary")

        if submitted:
            payload = {
                "amount": amount,
                "amount_log": amount_log,
                "tx_hour": tx_hour,
                "tx_day_of_week": tx_day_of_week,
                "is_weekend": is_weekend,
                "is_cross_currency": is_cross_currency,
                "sender_tx_count_1h": sender_tx_count_1h,
                "sender_amount_sum_1h": sender_amount_sum_1h,
                "sender_avg_amount": sender_avg_amount,
                "amount_vs_sender_avg": amount_vs_sender_avg,
                "payment_type_risk": payment_type_risk,
                "is_high_risk_type": is_high_risk_type,
                "is_structuring": is_structuring,
                "is_round_amount": is_round_amount,
                "rule_score": rule_score,
            }
            result = api_post("/predict", json=payload)
            if result:
                risk_level = result.get("risk_level", "UNKNOWN")
                risk_colors = {
                    "CRITICAL": "error",
                    "HIGH": "warning",
                    "MEDIUM": "warning",
                    "LOW": "success",
                }
                getattr(st, risk_colors.get(risk_level, "info"))(
                    f"Risk Level: **{risk_level}** | "
                    f"ML Score: **{result.get('ml_probability', 0):.4f}** | "
                    f"Suspicious: **{'YES ⚠️' if result.get('is_suspicious') else 'NO ✅'}**"
                )
            else:
                st.error("Prediction failed — check API connection")
