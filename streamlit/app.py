"""
Banking Data Platform — Streamlit Dashboard (Complete)
=====================================================
Full-featured dashboard với dbt + Trino integration
Multi-page, filters, interactive charts, export, drill-down
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from trino.dbapi import connect
from datetime import datetime, timedelta
import io

# =============================================================================
# PAGE CONFIG
# =============================================================================
st.set_page_config(
    page_title="Banking Data Platform",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# CUSTOM CSS
# =============================================================================
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
    }
    div[data-testid="stMetric"] {
        background-color: #f8f9fa;
        padding: 20px;
        border-radius: 12px;
        border: 2px solid #e0e0e0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    div[data-testid="stMetric"] label {
        color: #666666;
        font-size: 0.95rem;
        font-weight: 500;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #1f77b4;
        font-size: 1.8rem;
        font-weight: bold;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricDelta"] {
        font-size: 0.9rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
        border-radius: 8px 8px 0 0;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# DATABASE CONNECTION
# =============================================================================
@st.cache_resource
def get_connection():
    """Create Trino connection"""
    return connect(
        host="host.docker.internal",
        port=8085,
        user="admin",
        catalog="lakehouse"
    )

def query_data(sql: str) -> pd.DataFrame:
    """Execute query and return DataFrame"""
    from decimal import Decimal
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=columns)
    # Convert Decimal columns to float for arithmetic operations
    for col in df.columns:
        if df[col].dtype == object:
            if len(df) > 0 and isinstance(df[col].iloc[0], Decimal):
                df[col] = df[col].astype(float)
            elif len(df) > 0 and hasattr(df[col].iloc[0], 'as_integer_ratio'):
                try:
                    df[col] = df[col].apply(lambda x: float(x) if x is not None else None)
                except:
                    pass
    return df

# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================
@st.cache_data(ttl=300)
def load_customer_360():
    return query_data("""
        SELECT customer_id, customer_sk, full_name_masked, age, gender,
               primary_branch_code, customer_segment, kyc_status,
               register_date, total_accounts, total_cards, total_loans,
               has_credit_card, has_savings, has_loan,
               total_deposit_balance, total_loan_outstanding,
               aum_total, aum_bucket,
               txn_count_30d, txn_amount_30d, last_txn_date, days_since_last_txn,
               primary_channel, interaction_count_90d,
               rfm_recency_score, rfm_frequency_score, rfm_monetary_score,
               rfm_segment, churn_flag, cross_sell_credit_card_flag, cob_dt
        FROM gold.mart_customer_360
    """)

@st.cache_data(ttl=300)
def load_rfm():
    return query_data("""
        SELECT customer_id, customer_sk, recency_days, frequency, monetary,
               r_score, f_score, m_score, rfm_score, rfm_segment, cob_dt
        FROM gold.rfm_segment
    """)

@st.cache_data(ttl=300)
def load_churn():
    return query_data("""
        SELECT customer_id, customer_sk, txn_cnt_30d, txn_cnt_90d,
               txn_amt_30d, txn_amt_90d, days_since_last_txn,
               churn_risk, is_churn_candidate, cob_dt
        FROM gold.churn_prediction
    """)

@st.cache_data(ttl=300)
def load_campaign():
    return query_data("""
        SELECT customer_id, customer_sk, rfm_segment, rfm_score,
               recency_days, frequency, monetary,
               churn_risk, is_churn_candidate, days_since_last_txn,
               customer_segment, aum_total, aum_bucket,
               primary_branch_code, primary_opportunity, no_credit_card,
               campaign_type, cob_dt
        FROM gold.campaign_target
    """)

@st.cache_data(ttl=300)
def load_cross_sell():
    return query_data("""
        SELECT customer_id, customer_sk, customer_segment,
               no_credit_card, no_debit_card, primary_opportunity, cob_dt
        FROM gold.cross_sell_segment
    """)

@st.cache_data(ttl=300)
def load_balance():
    return query_data("""
        SELECT customer_id, customer_sk, total_account_balance,
               avg_account_balance, aum_total, aum_bucket, cob_dt
        FROM gold.customer_balance_summary
    """)

@st.cache_data(ttl=300)
def load_transaction():
    return query_data("""
        SELECT customer_id, customer_sk, acct_txn_count_30d, acct_txn_amount_30d,
               acct_credit_count_30d, acct_debit_count_30d,
               card_txn_count_30d, card_txn_amount_30d,
               total_txn_count_30d, total_txn_amount_30d,
               last_txn_date, cob_dt
        FROM gold.customer_transaction_summary
    """)

@st.cache_data(ttl=300)
def load_product():
    return query_data("""
        SELECT customer_id, customer_sk, total_accounts, cnt_casa_active,
               cnt_td_active, total_cards, cnt_credit_cards, cnt_debit_cards,
               has_credit_card, has_savings, has_loan, cob_dt
        FROM gold.customer_product_summary
    """)

@st.cache_data(ttl=300)
def load_card():
    return query_data("""
        SELECT customer_id, customer_sk, total_cards, cnt_credit_active,
               cnt_debit_active, max_credit_limit,
               total_card_txn_count_30d, total_card_txn_amount_30d,
               avg_card_txn_amount_30d, distinct_merchant_categories,
               last_card_txn_date, cob_dt
        FROM gold.customer_card_summary
    """)

# =============================================================================
# SIDEBAR - FILTERS
# =============================================================================
st.sidebar.title("🏦 Banking Dashboard")
st.sidebar.markdown("---")

# Global Filters
st.sidebar.header("🔍 Global Filters")

# Customer Segment Filter
df_temp = load_customer_360()
all_segments = sorted(df_temp['customer_segment'].unique())
selected_segments = st.sidebar.multiselect(
    "Customer Segment",
    options=all_segments,
    default=all_segments
)

# AUM Bucket Filter
all_aum_buckets = sorted(df_temp['aum_bucket'].unique())
selected_aum_buckets = st.sidebar.multiselect(
    "AUM Bucket",
    options=all_aum_buckets,
    default=all_aum_buckets
)

# RFM Segment Filter
df_temp_rfm = load_rfm()
all_rfm_segments = sorted(df_temp_rfm['rfm_segment'].unique())
selected_rfm_segments = st.sidebar.multiselect(
    "RFM Segment",
    options=all_rfm_segments,
    default=all_rfm_segments
)

# Churn Risk Filter
df_temp_churn = load_churn()
all_churn_risks = sorted(df_temp_churn['churn_risk'].unique())
selected_churn_risks = st.sidebar.multiselect(
    "Churn Risk",
    options=all_churn_risks,
    default=all_churn_risks
)

# Min AUM Filter
min_aum = st.sidebar.number_input(
    "Min AUM (₫)",
    min_value=0,
    max_value=10000000000,
    value=0,
    step=100000000
)

# Refresh Button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.info("""
**Data Source:** Trino → Iceberg → Gold
**Refresh:** 5 min cache
**dbt:** 12 semantic models
""")

# =============================================================================
# MAIN CONTENT - MULTI-PAGE
# =============================================================================
page = st.selectbox(
    "📌 Navigation",
    ["📊 Overview", "👥 Customer 360", "📈 RFM Analysis", "⚠️ Churn Risk",
     "🎯 Campaign Target", "💰 Balance & AUM", "💳 Card Analytics",
     "📊 Transaction Analytics", "📋 Raw Data", "ℹ️ About"]
)

# =============================================================================
# PAGE: OVERVIEW
# =============================================================================
if page == "📊 Overview":
    st.title("📊 Banking Customer 360 — Overview")
    st.markdown("---")

    # Filter data
    try:
        df = load_customer_360()
        df = df[df['customer_segment'].isin(selected_segments)]
        df = df[df['aum_bucket'].isin(selected_aum_buckets)]
        df = df[df['aum_total'] >= min_aum]

        # KPI Cards
        col1, col2, col3, col4, col5 = st.columns(5)

        total_customers = len(df)
        total_aum = df['aum_total'].sum()
        avg_aum = df['aum_total'].mean()
        active_customers = len(df[df['churn_flag'] == 0])
        avg_age = df['age'].mean()

        with col1:
            st.metric("👥 Total Customers", f"{total_customers:,}")
        with col2:
            if total_aum >= 1e9:
                st.metric("💰 Total AUM", f"₫{total_aum/1e9:,.1f}B")
            else:
                st.metric("💰 Total AUM", f"₫{total_aum/1e6:,.1f}M")
        with col3:
            if avg_aum >= 1e9:
                st.metric("📊 Avg AUM", f"₫{avg_aum/1e9:,.1f}B")
            else:
                st.metric("📊 Avg AUM", f"₫{avg_aum/1e6:,.1f}M")
        with col4:
            st.metric("✅ Active Customers", f"{active_customers:,}")
        with col5:
            st.metric("👤 Avg Age", f"{avg_age:.1f} years")
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

    st.markdown("---")

    # Charts Row 1
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("📊 Customer Segment")
        seg_counts = df['customer_segment'].value_counts().reset_index()
        seg_counts.columns = ['segment', 'count']
        fig = px.pie(seg_counts, values='count', names='segment',
                     color_discrete_sequence=px.colors.qualitative.Set2,
                     hole=0.4)
        fig.update_traces(textposition='inside', textinfo='percent+label',
                         textfont_size=12, textfont_color='white')
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
            margin=dict(t=30, b=30, l=20, r=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("💰 AUM Distribution")
        aum_counts = df['aum_bucket'].value_counts().reset_index()
        aum_counts.columns = ['bucket', 'count']
        fig = px.bar(aum_counts, x='bucket', y='count',
                     color='bucket', color_discrete_sequence=px.colors.qualitative.Vivid,
                     text='count')
        fig.update_traces(textposition='outside', textfont_size=12)
        fig.update_layout(
            showlegend=False,
            xaxis_title="AUM Bucket",
            yaxis_title="Count",
            margin=dict(t=30, b=30, l=20, r=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        st.subheader("👤 Gender Distribution")
        gender_counts = df['gender'].value_counts().reset_index()
        gender_counts.columns = ['gender', 'count']
        fig = px.pie(gender_counts, values='count', names='gender',
                     color_discrete_sequence=['#3498db', '#e74c3c'],
                     hole=0.4)
        fig.update_traces(textposition='inside', textinfo='percent+label',
                         textfont_size=12, textfont_color='white')
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
            margin=dict(t=30, b=30, l=20, r=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    # Charts Row 2
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("📈 RFM Segmentation")
        df_rfm = load_rfm()
        df_rfm = df_rfm[df_rfm['rfm_segment'].isin(selected_rfm_segments)]
        rfm_counts = df_rfm['rfm_segment'].value_counts().reset_index()
        rfm_counts.columns = ['segment', 'count']
        fig = px.bar(rfm_counts, x='segment', y='count',
                     color='segment', color_discrete_sequence=px.colors.qualitative.Set3,
                     text='count')
        fig.update_traces(textposition='outside', textfont_size=10)
        fig.update_layout(
            xaxis_title="RFM Segment",
            yaxis_title="Count",
            showlegend=False,
            margin=dict(t=30, b=30, l=20, r=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("⚠️ Churn Risk Distribution")
        df_churn = load_churn()
        df_churn = df_churn[df_churn['churn_risk'].isin(selected_churn_risks)]
        churn_counts = df_churn['churn_risk'].value_counts().reset_index()
        churn_counts.columns = ['risk', 'count']
        colors = {'High': '#e74c3c', 'Medium': '#f39c12', 'Low': '#3498db', 'Active': '#2ecc71'}
        fig = px.pie(churn_counts, values='count', names='risk',
                     color='risk', color_discrete_map=colors,
                     hole=0.4)
        fig.update_traces(textposition='inside', textinfo='percent+label',
                         textfont_size=12, textfont_color='white')
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
            margin=dict(t=30, b=30, l=20, r=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    # Charts Row 3
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("🎯 Campaign Target")
        df_campaign = load_campaign()
        campaign_counts = df_campaign['campaign_type'].value_counts().reset_index()
        campaign_counts.columns = ['type', 'count']
        fig = px.pie(campaign_counts, values='count', names='type',
                     color_discrete_sequence=px.colors.qualitative.Bold,
                     hole=0.4)
        fig.update_traces(textposition='inside', textinfo='percent+label',
                         textfont_size=12, textfont_color='white')
        fig.update_layout(
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5),
            margin=dict(t=30, b=30, l=20, r=20)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("💼 Cross-Sell Opportunities")
        df_cross = load_cross_sell()
        cross_counts = df_cross['primary_opportunity'].value_counts().reset_index()
        cross_counts.columns = ['opportunity', 'count']
        fig = px.bar(cross_counts, x='opportunity', y='count',
                     color='opportunity', color_discrete_sequence=px.colors.qualitative.Pastel,
                     text='count')
        fig.update_traces(textposition='outside', textfont_size=12)
        fig.update_layout(
            xaxis_title="Opportunity",
            yaxis_title="Count",
            showlegend=False,
            margin=dict(t=30, b=30, l=20, r=20)
        )
        st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# PAGE: CUSTOMER 360
# =============================================================================
elif page == "👥 Customer 360":
    st.title("👥 Customer 360 Profile")
    st.markdown("---")

    try:
        df = load_customer_360()
        df = df[df['customer_segment'].isin(selected_segments)]
        df = df[df['aum_bucket'].isin(selected_aum_buckets)]
        df = df[df['aum_total'] >= min_aum]

        # Search customer
        search_id = st.text_input("🔍 Search Customer ID", "")
        if search_id:
            df = df[df['customer_id'].astype(str).str.contains(search_id)]

        # Display customer list
        st.subheader(f"Customer List ({len(df)} customers)")

        if len(df) == 0:
            st.warning("No customers found matching the filters.")
        else:
            # Pagination
            page_size = 50
            total_pages = (len(df) - 1) // page_size + 1
            page_num = st.number_input("Page", min_value=1, max_value=total_pages, value=1)
            start_idx = (page_num - 1) * page_size
            end_idx = min(start_idx + page_size, len(df))

            # Display dataframe
            display_df = df.iloc[start_idx:end_idx][[
                'customer_id', 'full_name_masked', 'customer_segment', 'gender',
                'age', 'aum_total', 'aum_bucket', 'rfm_segment', 'churn_flag'
            ]].copy()

            # Format aum_total for display
            display_df['aum_total'] = display_df['aum_total'].apply(
                lambda x: f"₫{x:,.0f}" if pd.notnull(x) else "N/A"
            )

            # Map churn_flag to readable text
            display_df['churn_flag'] = display_df['churn_flag'].map(
                {0: '✅ Active', 1: '⚠️ At Risk'}
            ).fillna('Unknown')

            st.dataframe(display_df, use_container_width=True, height=400)

        # Customer Detail
        if search_id:
            st.markdown("---")
            st.subheader("Customer Detail")

            customer = df[df['customer_id'].astype(str) == search_id]
            if not customer.empty:
                cust = customer.iloc[0]

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Customer ID", cust['customer_id'])
                    st.metric("Segment", cust['customer_segment'])
                    aum_val = cust['aum_total']
                    if aum_val >= 1e9:
                        st.metric("AUM", f"₫{aum_val/1e9:,.1f}B")
                    else:
                        st.metric("AUM", f"₫{aum_val/1e6:,.1f}M")
                with col2:
                    st.metric("Age", f"{cust['age']} years")
                    st.metric("Gender", cust['gender'])
                    st.metric("AUM Bucket", cust['aum_bucket'])
                with col3:
                    st.metric("RFM Segment", cust['rfm_segment'])
                    st.metric("Churn Status", "✅ Active" if cust['churn_flag'] == 0 else "⚠️ At Risk")
                    st.metric("Days Since Last Txn", f"{cust['days_since_last_txn']} days")
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")

# =============================================================================
# PAGE: RFM ANALYSIS
# =============================================================================
elif page == "📈 RFM Analysis":
    st.title("📈 RFM Segmentation Analysis")
    st.markdown("---")

    df_rfm = load_rfm()
    df_rfm = df_rfm[df_rfm['rfm_segment'].isin(selected_rfm_segments)]

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers", f"{len(df_rfm):,}")
    col2.metric("Avg Recency", f"{df_rfm['recency_days'].mean():.1f} days")
    col3.metric("Avg Frequency", f"{df_rfm['frequency'].mean():.1f}")
    col4.metric("Avg Monetary", f"₫{df_rfm['monetary'].mean():,.0f}")

    st.markdown("---")

    # RFM Distribution Charts
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Recency Distribution")
        fig = px.histogram(df_rfm, x='recency_days', nbins=30,
                           color_discrete_sequence=['#3498db'])
        fig.update_layout(xaxis_title="Recency (days)", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Frequency Distribution")
        fig = px.histogram(df_rfm, x='frequency', nbins=30,
                           color_discrete_sequence=['#2ecc71'])
        fig.update_layout(xaxis_title="Frequency", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

    with col3:
        st.subheader("Monetary Distribution")
        fig = px.histogram(df_rfm, x='monetary', nbins=30,
                           color_discrete_sequence=['#e74c3c'])
        fig.update_layout(xaxis_title="Monetary (₫)", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

    # RFM Segment Analysis
    st.markdown("---")
    st.subheader("RFM Segment Analysis")

    rfm_summary = df_rfm.groupby('rfm_segment').agg({
        'customer_id': 'count',
        'recency_days': 'mean',
        'frequency': 'mean',
        'monetary': 'mean'
    }).rename(columns={'customer_id': 'count'}).reset_index()

    col1, col2 = st.columns(2)

    with col1:
        fig = px.bar(rfm_summary, x='rfm_segment', y='count',
                     color='rfm_segment', color_discrete_sequence=px.colors.qualitative.Set3)
        fig.update_layout(title="Customer Count by RFM Segment")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = px.scatter(rfm_summary, x='frequency', y='monetary',
                         size='count', color='rfm_segment',
                         color_discrete_sequence=px.colors.qualitative.Set3)
        fig.update_layout(title="Frequency vs Monetary by RFM Segment")
        st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# PAGE: CHURN RISK
# =============================================================================
elif page == "⚠️ Churn Risk":
    st.title("⚠️ Churn Risk Analysis")
    st.markdown("---")

    df_churn = load_churn()
    df_churn = df_churn[df_churn['churn_risk'].isin(selected_churn_risks)]

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers", f"{len(df_churn):,}")
    col2.metric("High Risk", f"{len(df_churn[df_churn['churn_risk'] == 'High']):,}")
    col3.metric("Medium Risk", f"{len(df_churn[df_churn['churn_risk'] == 'Medium']):,}")
    col4.metric("Active", f"{len(df_churn[df_churn['churn_risk'] == 'Active']):,}")

    st.markdown("---")

    # Churn Risk Distribution
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Churn Risk Distribution")
        churn_counts = df_churn['churn_risk'].value_counts().reset_index()
        churn_counts.columns = ['risk', 'count']
        colors = {'High': '#e74c3c', 'Medium': '#f39c12', 'Low': '#3498db', 'Active': '#2ecc71'}
        fig = px.pie(churn_counts, values='count', names='risk',
                     color='risk', color_discrete_map=colors)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Days Since Last Transaction by Churn Risk")
        fig = px.box(df_churn, x='churn_risk', y='days_since_last_txn',
                     color='churn_risk', color_discrete_map=colors)
        fig.update_layout(xaxis_title="Churn Risk", yaxis_title="Days Since Last Txn")
        st.plotly_chart(fig, use_container_width=True)

    # Churn Risk Details
    st.markdown("---")
    st.subheader("Churn Risk Details")

    high_risk = df_churn[df_churn['churn_risk'] == 'High'].sort_values('days_since_last_txn', ascending=False)
    st.dataframe(
        high_risk[['customer_id', 'txn_cnt_30d', 'txn_cnt_90d', 'days_since_last_txn', 'churn_risk']],
        use_container_width=True
    )

# =============================================================================
# PAGE: CAMPAIGN TARGET
# =============================================================================
elif page == "🎯 Campaign Target":
    st.title("🎯 Campaign Target Analysis")
    st.markdown("---")

    df_campaign = load_campaign()

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Targets", f"{len(df_campaign):,}")
    col2.metric("Retention", f"{len(df_campaign[df_campaign['campaign_type'] == 'Retention']):,}")
    col3.metric("Cross-Sell", f"{len(df_campaign[df_campaign['campaign_type'].str.contains('Cross_Sell')]):,}")
    col4.metric("Upsell", f"{len(df_campaign[df_campaign['campaign_type'] == 'Upsell']):,}")

    st.markdown("---")

    # Campaign Distribution
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Campaign Type Distribution")
        campaign_counts = df_campaign['campaign_type'].value_counts().reset_index()
        campaign_counts.columns = ['type', 'count']
        fig = px.pie(campaign_counts, values='count', names='type',
                     color_discrete_sequence=px.colors.qualitative.Bold)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Campaign by Customer Segment")
        campaign_segment = df_campaign.groupby(['campaign_type', 'customer_segment']).size().reset_index(name='count')
        fig = px.bar(campaign_segment, x='campaign_type', y='count',
                     color='customer_segment', barmode='group',
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(xaxis_title="Campaign Type", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

    # Campaign Details
    st.markdown("---")
    st.subheader("Campaign Target Details")
    st.dataframe(
        df_campaign[['customer_id', 'customer_segment', 'rfm_segment', 'churn_risk',
                     'primary_opportunity', 'campaign_type', 'aum_total']],
        use_container_width=True
    )

# =============================================================================
# PAGE: BALANCE & AUM
# =============================================================================
elif page == "💰 Balance & AUM":
    st.title("💰 Balance & AUM Analysis")
    st.markdown("---")

    df_balance = load_balance()
    df_balance = df_balance[df_balance['aum_bucket'].isin(selected_aum_buckets)]
    df_balance = df_balance[df_balance['aum_total'] >= min_aum]

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers", f"{len(df_balance):,}")
    col2.metric("Total AUM", f"₫{df_balance['aum_total'].sum():,.0f}")
    col3.metric("Avg Balance", f"₫{df_balance['total_account_balance'].mean():,.0f}")
    col4.metric("Avg AUM Bucket", df_balance['aum_bucket'].mode()[0])

    st.markdown("---")

    # AUM Distribution
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("AUM Distribution by Bucket")
        aum_summary = df_balance.groupby('aum_bucket').agg({
            'customer_id': 'count',
            'aum_total': 'sum'
        }).rename(columns={'customer_id': 'count'}).reset_index()
        fig = px.bar(aum_summary, x='aum_bucket', y='aum_total',
                     color='aum_bucket', color_discrete_sequence=px.colors.qualitative.Vivid)
        fig.update_layout(xaxis_title="AUM Bucket", yaxis_title="Total AUM (₫)")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("AUM Distribution")
        fig = px.histogram(df_balance, x='aum_total', nbins=50,
                           color_discrete_sequence=['#3498db'])
        fig.update_layout(xaxis_title="AUM (₫)", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

    # Balance Details
    st.markdown("---")
    st.subheader("Balance Details")
    st.dataframe(
        df_balance[['customer_id', 'total_account_balance', 'avg_account_balance',
                    'aum_total', 'aum_bucket']],
        use_container_width=True
    )

# =============================================================================
# PAGE: CARD ANALYTICS
# =============================================================================
elif page == "💳 Card Analytics":
    st.title("💳 Card Analytics")
    st.markdown("---")

    df_card = load_card()

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers", f"{len(df_card):,}")
    col2.metric("Avg Cards", f"{df_card['total_cards'].mean():.1f}")
    col3.metric("Total Card Txns", f"{df_card['total_card_txn_count_30d'].sum():,}")
    col4.metric("Avg Card Txn Amount", f"₫{df_card['avg_card_txn_amount_30d'].mean():,.0f}")

    st.markdown("---")

    # Card Distribution
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Card Distribution")
        card_counts = df_card['total_cards'].value_counts().head(10).reset_index()
        card_counts.columns = ['cards', 'count']
        fig = px.bar(card_counts, x='cards', y='count',
                     color_discrete_sequence=['#9b59b6'])
        fig.update_layout(xaxis_title="Number of Cards", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Card Transaction Volume")
        fig = px.histogram(df_card, x='total_card_txn_amount_30d', nbins=30,
                           color_discrete_sequence=['#e67e22'])
        fig.update_layout(xaxis_title="Card Transaction Amount (₫)", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

    # Card Details
    st.markdown("---")
    st.subheader("Card Details")
    st.dataframe(
        df_card[['customer_id', 'total_cards', 'cnt_credit_active', 'cnt_debit_active',
                'total_card_txn_count_30d', 'total_card_txn_amount_30d']],
        use_container_width=True
    )

# =============================================================================
# PAGE: TRANSACTION ANALYTICS
# =============================================================================
elif page == "📊 Transaction Analytics":
    st.title("📊 Transaction Analytics")
    st.markdown("---")

    df_txn = load_transaction()

    # KPI Cards
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Customers", f"{len(df_txn):,}")
    col2.metric("Avg Txn Count 30d", f"{df_txn['total_txn_count_30d'].mean():.1f}")
    col3.metric("Total Txn Amount", f"₫{df_txn['total_txn_amount_30d'].sum():,.0f}")
    col4.metric("Avg Txn Amount", f"₫{df_txn['total_txn_amount_30d'].mean():,.0f}")

    st.markdown("---")

    # Transaction Distribution
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Transaction Count Distribution")
        fig = px.histogram(df_txn, x='total_txn_count_30d', nbins=30,
                           color_discrete_sequence=['#1abc9c'])
        fig.update_layout(xaxis_title="Transaction Count (30d)", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Transaction Amount Distribution")
        fig = px.histogram(df_txn, x='total_txn_amount_30d', nbins=30,
                           color_discrete_sequence=['#e74c3c'])
        fig.update_layout(xaxis_title="Transaction Amount (₫)", yaxis_title="Count")
        st.plotly_chart(fig, use_container_width=True)

    # Transaction Details
    st.markdown("---")
    st.subheader("Transaction Details")
    st.dataframe(
        df_txn[['customer_id', 'total_txn_count_30d', 'total_txn_amount_30d',
               'acct_txn_count_30d', 'card_txn_count_30d', 'last_txn_date']],
        use_container_width=True
    )

# =============================================================================
# PAGE: RAW DATA
# =============================================================================
elif page == "📋 Raw Data":
    st.title("📋 Raw Data Explorer")
    st.markdown("---")

    # Select table
    table_options = [
        "mart_customer_360",
        "rfm_segment",
        "churn_prediction",
        "campaign_target",
        "cross_sell_segment",
        "customer_balance_summary",
        "customer_transaction_summary",
        "customer_product_summary",
        "customer_card_summary"
    ]

    selected_table = st.selectbox("Select Table", table_options)

    # Load data
    @st.cache_data(ttl=300)
    def load_table_data(table_name):
        return query_data(f"SELECT * FROM gold.{table_name}")

    df = load_table_data(selected_table)

    # Display info
    st.subheader(f"Table: {selected_table}")
    col1, col2, col3 = st.columns(3)
    col1.metric("Rows", f"{len(df):,}")
    col2.metric("Columns", f"{len(df.columns)}")
    col3.metric("Memory", f"{df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")

    # Filters
    st.subheader("Filters")
    col1, col2 = st.columns(2)

    with col1:
        # Auto-detect filterable columns
        filter_cols = [col for col in df.columns if df[col].nunique() < 50]
        if filter_cols:
            filter_col = st.selectbox("Filter Column", filter_cols)
            filter_values = st.multiselect(
                "Filter Values",
                options=df[filter_col].unique(),
                default=df[filter_col].unique()[:5]
            )
            if filter_values:
                df = df[df[filter_col].isin(filter_values)]

    with col2:
        # Search
        search_term = st.text_input("Search in all columns", "")
        if search_term:
            mask = df.astype(str).apply(lambda x: x.str.contains(search_term, case=False)).any(axis=1)
            df = df[mask]

    # Display data
    st.subheader(f"Data ({len(df)} rows)")
    st.dataframe(df, use_container_width=True, height=400)

    # Export
    st.markdown("---")
    st.subheader("Export Data")

    col1, col2, col3 = st.columns(3)

    with col1:
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"{selected_table}.csv",
            mime="text/csv"
        )

    with col2:
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, engine='openpyxl')
        st.download_button(
            label="📥 Download Excel",
            data=excel_buffer.getvalue(),
            file_name=f"{selected_table}.xlsx",
            mime="application/vnd.ms-excel"
        )

    with col3:
        json = df.to_json(orient='records', indent=2)
        st.download_button(
            label="📥 Download JSON",
            data=json,
            file_name=f"{selected_table}.json",
            mime="application/json"
        )

# =============================================================================
# PAGE: ABOUT
# =============================================================================
elif page == "ℹ️ About":
    st.title("ℹ️ About Banking Data Platform")
    st.markdown("---")

    st.subheader("Architecture")
    st.markdown("""
    ```
    Data Generator → PostgreSQL → Bronze → Silver → Gold → Streamlit
                                        ↓
                                   CDC (Debezium)
                                        ↓
                                       Kafka
                                                    → dbt → Trino
    ```
    """)

    st.subheader("Tech Stack")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
        **Data Layer**
        - PostgreSQL 15
        - Apache Spark 3.5.3
        - Apache Iceberg 1.6
        - MinIO (S3)
        """)

    with col2:
        st.markdown("""
        **Processing Layer**
        - Trino 443
        - dbt-core 1.12.0
        - Apache Airflow 2.10.0
        - Debezium + Kafka
        """)

    with col3:
        st.markdown("""
        **Serving Layer**
        - Streamlit (Dashboard)
        - OpenMetadata (Catalog)
        - Superset (Optional)
        """)

    st.subheader("Data Layers")
    st.markdown("""
    | Layer | Tables | Description |
    |-------|--------|-------------|
    | Bronze | 16 | Raw data from PostgreSQL |
    | Silver | 13 | Cleaned, deduplicated (8 dims + 5 facts) |
    | Gold | 19 | Analytics-ready marts (10 history + 9 current) |
    """)

    st.subheader("dbt Semantic Layer")
    st.markdown("""
    - **12 semantic models** (ephemeral on Gold)
    - **52 dbt tests** (ALL PASS)
    - **4 exposures** (Superset, Power BI, Notebook, AI)
    """)

    st.subheader("Data Quality")
    st.markdown("""
    - **24 history contracts** + **9 current-serving contracts**
    - **8 DQ check types** (anomaly, freshness, schema drift, etc.)
    - **5 quarantine tables** for violations
    """)

# =============================================================================
# FOOTER
# =============================================================================
st.markdown("---")
st.markdown("""
**Banking Data Platform** | Built with Streamlit + Trino + Iceberg + dbt
- **Data Layer:** Bronze → Silver → Gold
- **Query Engine:** Trino
- **Table Format:** Apache Iceberg
- **Orchestration:** Airflow
- **Semantic Layer:** dbt
""")
