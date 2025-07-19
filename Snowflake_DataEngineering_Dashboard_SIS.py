# Import python packages

# This is a personal project and is not affiliated with, endorsed, or sponsored by Snowflake Inc. in any way. All trademarks and registered trademarks are the property of their respective owners.
# This code is provided on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
# While this code is read-only and is not intended to modify any data, it may contain bugs or logical errors. 
# The author makes no guarantee as to the accuracy, completeness, or reliability of the information returned by the code.
# In no event shall the author be liable for any claims or damages resulting from business decisions or actions taken based on potentially incorrect information provided by this software.
# You are solely responsible for thoroughly testing and validating the accuracy of the results before use. Use at your own risk.

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta
import snowflake.snowpark as snowpark
import os

# Page configuration
st.set_page_config(
    page_title="Data Ingestion Metrics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 5px solid #1f77b4;
    }
    .kpi-container {
        background-color: white;
        padding: 10px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }
    .trend-up {
        color: #28a745;
    }
    .trend-down {
        color: #dc3545;
    }
    .trend-neutral {
        color: #6c757d;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def init_snowflake_connection():
        session = get_active_session()
        return session


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data_from_snowflake():
    """Load data from Snowflake using Snowpark"""
    session = init_snowflake_connection()
    
    if session is None :
        return generate_sample_data()
    
    try:
        # MODIFIED QUERY: Dynamically filters for the last 3 months of data.
        query_1 = '''
            WITH copy_aggr AS (
                -- gives one row per copy job vs. row per file
                SELECT
                    last_load_time, status, table_catalog_name, pipe_schema_name, table_name,
                    SUM(row_count) AS row_count,
                    SUM(row_parsed) AS row_parsed,
                    SUM(file_size) AS file_size,
                    COUNT(file_name) AS file_count
                FROM snowflake.account_usage.copy_history
                GROUP BY ALL
            ),
            IngestHistory AS (
                SELECT
                    ch.last_load_time AS load_time, ch.row_count, ch.row_parsed, ch.file_size,
                    ch.file_count, ch.status, ch.table_catalog_name, ch.pipe_schema_name, ch.table_name,
                    qh.query_id, qh.warehouse_name, qh.warehouse_size,
                    qah.credits_attributed_compute AS CreditsUsed
                FROM copy_aggr AS ch
                LEFT JOIN snowflake.account_usage.query_history AS qh
                    ON CONTAINS(qh.query_text, ch.table_name)
                    AND qh.query_type = 'COPY'
                    AND qh.start_time = ch.last_load_time
                JOIN snowflake.account_usage.query_attribution_history AS qah
                    ON qh.query_id = qah.query_id
            )
            SELECT 
                DATE_TRUNC('day', load_time) AS IngestDay,
                WAREHOUSE_NAME,
                WAREHOUSE_SIZE,
                SUM(row_count) AS TotalRows,
                SUM(file_count) AS fileCount,
                SUM(file_size) / (1024*1024*1024) AS TotalGB,
                SUM(CreditsUsed) AS TotalCredits
            FROM IngestHistory
            WHERE IngestDay >= DATEADD(week, -4, CURRENT_DATE()) -- Filters for last 3 months
            GROUP BY ALL
            ORDER BY IngestDay DESC
        '''
        df_snowpark = session.sql(query_1)
        
        # Convert to Pandas DataFrame
        df = df_snowpark.to_pandas()
        
        # Ensure proper data types
        df['INGESTDAY'] = pd.to_datetime(df['INGESTDAY'])
        df['TOTALROWS'] = pd.to_numeric(df['TOTALROWS'])
        df['TOTALGB'] = pd.to_numeric(df['TOTALGB'])
        df['TOTALCREDITS'] = pd.to_numeric(df['TOTALCREDITS'])
        
        return df
        
        
    except Exception as e:
        st.error(f"Error loading data from Snowflake: {str(e)}")
        return generate_sample_data()

def generate_sample_data():
    """Generate sample data for demonstration"""
    np.random.seed(42)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=90) # Generate 90 days of sample data
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    warehouses = ['COMPUTE_WH', 'LOAD_WH', 'ANALYTICS_WH', 'ETL_WH']
    warehouse_sizes = ['SMALL', 'MEDIUM', 'LARGE', 'X-LARGE']
    data = []
    for date in date_range:
        for warehouse in warehouses:
            base_rows = np.random.randint(100000, 5000000)*10
            base_gb = base_rows / (np.random.randint(20000, 120000)*np.random.randint(5, 25))
            size_multiplier = {'SMALL': 1, 'MEDIUM': 2, 'LARGE': 4, 'X-LARGE': 8}
            warehouse_size = np.random.choice(warehouse_sizes)
            credits = max(0.1, (base_gb * 0.1 * size_multiplier[warehouse_size]) + np.random.normal(0, 0.5))/100
            data.append({
                'INGESTDAY': date, 'WAREHOUSE_NAME': warehouse, 'WAREHOUSE_SIZE': warehouse_size,
                'TOTALROWS': int(base_rows), 'TOTALGB': round(base_gb, 2), 'TOTALCREDITS': round(credits, 3)
            })
    return pd.DataFrame(data)

def calculate_kpis(df):
    """Calculate KPI metrics"""
    df['CREDITS_PER_MILLION_ROWS'] = (df['TOTALCREDITS'] / df['TOTALROWS']) * 1000000
    df['CREDITS_PER_GB'] = df['TOTALCREDITS'] / df['TOTALGB']
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    current_date = df['INGESTDAY'].max()
    current_period = df[df['INGESTDAY'] >= (current_date - timedelta(days=7))]
    previous_period = df[(df['INGESTDAY'] >= (current_date - timedelta(days=14))) & (df['INGESTDAY'] < (current_date - timedelta(days=7)))]
    
    current_credits_per_million = current_period['CREDITS_PER_MILLION_ROWS'].mean()
    previous_credits_per_million = previous_period['CREDITS_PER_MILLION_ROWS'].mean()
    current_credits_per_gb = current_period['CREDITS_PER_GB'].mean()
    previous_credits_per_gb = previous_period['CREDITS_PER_GB'].mean()
    
    trend_credits_per_million = ((current_credits_per_million - previous_credits_per_million) / previous_credits_per_million * 100) if previous_credits_per_million else 0
    trend_credits_per_gb = ((current_credits_per_gb - previous_credits_per_gb) / previous_credits_per_gb * 100) if previous_credits_per_gb else 0
    
    return {
        'current_credits_per_million': current_credits_per_million, 'trend_credits_per_million': trend_credits_per_million,
        'current_credits_per_gb': current_credits_per_gb, 'trend_credits_per_gb': trend_credits_per_gb
    }

def create_kpi_card(title, value, trend, unit=""):
    """Create a KPI card with trend indicator"""
    trend_color = "trend-down" if trend < 0 else "trend-up"
    trend_icon = "↘️" if trend < 0 else "↗️"
    if trend == 0: trend_color, trend_icon = "trend-neutral", "➡️"
    
    return f"""
    <div class="kpi-container">
        <h3 style="margin: 0; color: #1f77b4;">{title}</h3>
        <h1 style="margin: 10px 0; color: #333;">{value:.3f}{unit}</h1>
        <p class="{trend_color}" style="margin: 0; font-size: 16px;">
            {trend_icon} {abs(trend):.1f}% vs last week
        </p>
    </div>
    """

def create_trend_chart(df, metric_col, title):
    """Create trend line chart"""

    daily_trend = df.groupby('INGESTDAY')[metric_col].sum().reset_index()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily_trend['INGESTDAY'], y=daily_trend[metric_col], mode='lines+markers', name=title, line=dict(color='#1f77b4', width=3), marker=dict(size=6)))
    fig.update_layout(
        legend=dict(
            orientation="h",      # horizontal legend
            yanchor="bottom",
            y=1.02,               # a bit above the plot
            xanchor="center",
            x=0.5
        )
    )    
    temp_df = daily_trend.dropna(subset=[metric_col])
    if len(temp_df) > 1:
        x_numeric = pd.to_numeric(temp_df['INGESTDAY'])
        z = np.polyfit(x_numeric, temp_df[metric_col], 1)
        p = np.poly1d(z)
        fig.add_trace(go.Scatter(x=temp_df['INGESTDAY'], y=p(x_numeric), mode='lines', name='Trend', line=dict(color='red', width=2, dash='dash')))
        
    fig.update_layout(title=title, xaxis_title="Date", yaxis_title=metric_col.replace('_', ' ').title(), hovermode='x unified', showlegend=True, height=400)
    return fig

def create_warehouse_comparison(df):
    """Create warehouse comparison chart"""
    warehouse_metrics = df.groupby('WAREHOUSE_NAME').agg({'CREDITS_PER_MILLION_ROWS': 'mean', 'CREDITS_PER_GB': 'mean', 'TOTALCREDITS': 'sum'}).reset_index()
    fig = make_subplots(rows=1, cols=2, subplot_titles=('Credits per Million Rows', 'Credits per GB'))
    fig.add_trace(go.Bar(x=warehouse_metrics['WAREHOUSE_NAME'], y=warehouse_metrics['CREDITS_PER_MILLION_ROWS'], name='Credits/Million Rows', marker_color='lightblue'), row=1, col=1)
    fig.add_trace(go.Bar(x=warehouse_metrics['WAREHOUSE_NAME'], y=warehouse_metrics['CREDITS_PER_GB'], name='Credits/GB', marker_color='lightcoral'), row=1, col=2)
    fig.update_layout(title_text="Warehouse Performance Comparison", showlegend=False, height=400)
    return fig

def main():
    """Main dashboard function"""
    st.title("📊 Snowflake Data Ingestion Metrics Dashboard")
    st.markdown("Monitor your data ingestion performance and costs")
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    with st.spinner("Loading data from Snowflake..."):
        df = load_data_from_snowflake()
    
    if df.empty:
        st.error("No data available for the selected period.")
        return
    
    df['CREDITS_PER_MILLION_ROWS'] = (df['TOTALCREDITS'] / df['TOTALROWS']) * 1000000
    df['CREDITS_PER_GB'] = df['TOTALCREDITS'] / df['TOTALGB']
    
    # MODIFIED: Date Slider
    all_dates = sorted(df['INGESTDAY'].dt.date.unique())
    date_range = st.sidebar.select_slider(
        "Select Date Range",
        options=all_dates,
        value=(all_dates[0], all_dates[-1]),
        format_func=lambda date: date.strftime('%b %d, %Y')
    )
    
    # MODIFIED: Warehouse selection with Select/Unselect All
    st.sidebar.markdown("---")
    st.sidebar.write("Filter Warehouses")
    all_warehouses = list(df['WAREHOUSE_NAME'].unique())
    
    # Use session state to manage selections
    if 'warehouse_selection' not in st.session_state:
        st.session_state.warehouse_selection = all_warehouses
        
    btn_col1, btn_col2 = st.sidebar.columns(2)
    if btn_col1.button("Select All", use_container_width=True):
        st.session_state.warehouse_selection = all_warehouses
    if btn_col2.button("Unselect All", use_container_width=True):
        st.session_state.warehouse_selection = []
        
    warehouses = st.sidebar.multiselect(
        "Select Warehouses",
        options=all_warehouses,
        default=st.session_state.warehouse_selection,
        label_visibility="collapsed"
    )
    st.session_state.warehouse_selection = warehouses # Update state with current selection

    # Filter data based on sidebar selections
    if len(date_range) == 2:
        df_filtered = df[
            (df['INGESTDAY'].dt.date >= date_range[0]) &
            (df['INGESTDAY'].dt.date <= date_range[1]) &
            (df['WAREHOUSE_NAME'].isin(warehouses))
        ]
    else:
        df_filtered = df[df['WAREHOUSE_NAME'].isin(warehouses)]

    if df_filtered.empty:
        st.warning("No data matches your current filter selection.")
        return

    # --- Dashboard Layout ---
    kpis = calculate_kpis(df_filtered)
    
    st.header("📈 Key Performance Indicators")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown(create_kpi_card("Credits per Million Rows", kpis['current_credits_per_million'], kpis['trend_credits_per_million']), unsafe_allow_html=True)
    with col2:
        st.markdown(create_kpi_card("Credits per GB Ingested", kpis['current_credits_per_gb'], kpis['trend_credits_per_gb']), unsafe_allow_html=True)

    st.markdown('<br><br>', unsafe_allow_html=True)

    st.header("📋 Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)
    total_rows_all = df['TOTALROWS'].sum()
    total_gb_all = df['TOTALGB'].sum()
    total_credits_all = df['TOTALCREDITS'].sum()
    
    col1.metric("Total Rows Ingested", f"{df_filtered['TOTALROWS'].sum():,.0f}", f"{((df_filtered['TOTALROWS'].sum() / total_rows_all) * 100):.1f}% of Total")
    col2.metric("Total GB Ingested", f"{df_filtered['TOTALGB'].sum():,.0f}", f"{((df_filtered['TOTALGB'].sum() / total_gb_all) * 100):.1f}% of Total")
    col3.metric("Total Credits Used", f"{df_filtered['TOTALCREDITS'].sum():,.2f}", f"{((df_filtered['TOTALCREDITS'].sum() / total_credits_all) * 100):.1f}% of Total")
    avg_efficiency = df_filtered['TOTALROWS'].sum() / df_filtered['TOTALCREDITS'].sum() if df_filtered['TOTALCREDITS'].sum() else 0
    col4.metric("Rows per Credit", f"{avg_efficiency:,.0f}", "Efficiency Metric")
    
    st.markdown('<br><br>', unsafe_allow_html=True)

    
    st.header("📊 Trend Analysis")
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(create_trend_chart(df_filtered, 'CREDITS_PER_MILLION_ROWS', 'Credits per Million Rows Trend'), use_container_width=True)
    with col2:
        st.plotly_chart(create_trend_chart(df_filtered, 'CREDITS_PER_GB', 'Credits per GB Trend'), use_container_width=True)
    
    col3, col4 = st.columns(2)
    with col3:
        st.plotly_chart(create_trend_chart(df_filtered, 'TOTALROWS', 'Number of Rows per Day Trend'), use_container_width=True)
    with col4:
        st.plotly_chart(create_trend_chart(df_filtered, 'TOTALGB', 'Number of GB per Day Trend'), use_container_width=True)

    st.header("🏭 Warehouse Performance Comparison")
    st.plotly_chart(create_warehouse_comparison(df_filtered), use_container_width=True)
    

    with st.expander("📋 View Raw Data"):
        st.dataframe(df_filtered.sort_values('INGESTDAY', ascending=False), use_container_width=True)
    
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

if __name__ == "__main__":
    main()
