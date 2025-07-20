# This is a personal project and is not affiliated with, endorsed, or sponsored by Snowflake Inc. in any way. All trademarks and registered trademarks are the property of their respective owners.
# This code is provided on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or implied.
# While this code is read-only and is not intended to modify any data, it may contain bugs or logical errors. 
# The author makes no guarantee as to the accuracy, completeness, or reliability of the information returned by the code.
# In no event shall the author be liable for any claims or damages resulting from business decisions or actions taken based on potentially incorrect information provided by this software.
# You are solely responsible for thoroughly testing and validating the accuracy of the results before use. Use at your own risk.


# Import python packages
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
        color: #dc3545; /* Red for increase in cost - bad */
    }
    .trend-down {
        color: #28a745; /* Green for decrease in cost - good */
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
def load_data_from_snowflake(period_days):
    """Load data from Snowflake using a dynamic period in the WHERE clause."""
    session = init_snowflake_connection()
    
    if session is None or 1==1:
        # Pass period_days to generate sufficient sample data
        return generate_sample_data(period_days)
    
    try:
        # Fetch data for the selected period plus the prior period for comparison
        lookback_days = period_days * 2

        query_1 = f'''
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
            WHERE IngestDay >= DATEADD(day, -{lookback_days}, CURRENT_DATE()) -- DYNAMIC PERIOD FILTER
            GROUP BY ALL
            ORDER BY IngestDay DESC
        '''
        df_snowpark = session.sql(query_1)
        
        # Convert to Pandas DataFrame
        df = df_snowpark.to_pandas()
        
        # Ensure proper data types and consistent column names
        df.columns = [col.upper() for col in df.columns]
        df['INGESTDAY'] = pd.to_datetime(df['INGESTDAY'])
        df['TOTALROWS'] = pd.to_numeric(df['TOTALROWS'])
        df['TOTALGB'] = pd.to_numeric(df['TOTALGB'])
        df['TOTALCREDITS'] = pd.to_numeric(df['TOTALCREDITS'])
        
        return df
        
    except Exception as e:
        st.error(f"Error loading data from Snowflake: {str(e)}")
        return generate_sample_data(period_days)

def generate_sample_data(period_days):
    """Generate sample data for demonstration"""
    st.warning("Could not connect to Snowflake. Generating sample data.", icon="⚠️")
    np.random.seed(42)
    end_date = datetime.now().date()
    # Generate enough data for the selected period and the prior one
    start_date = end_date - timedelta(days=(period_days * 2)) 
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    warehouses = ['COMPUTE_WH', 'LOAD_WH', 'ANALYTICS_WH', 'ETL_WH']
    warehouse_sizes = ['SMALL', 'MEDIUM', 'LARGE', 'X-LARGE']
    data = []
    for date in date_range:
        for warehouse in warehouses:
            # Simulate some days with no activity for a warehouse
            if np.random.rand() > 0.8:
                continue
            base_rows = np.random.randint(100000, 5000000) * 10
            base_gb = base_rows / (np.random.randint(20000, 120000) * np.random.randint(5, 25))
            size_multiplier = {'SMALL': 1, 'MEDIUM': 2, 'LARGE': 4, 'X-LARGE': 8}
            warehouse_size = np.random.choice(warehouse_sizes)
            credits = max(0.1, (base_gb * 0.1 * size_multiplier[warehouse_size]) + np.random.normal(0, 0.5)) / 10
            data.append({
                'INGESTDAY': date, 'WAREHOUSE_NAME': warehouse, 'WAREHOUSE_SIZE': warehouse_size,
                'TOTALROWS': int(base_rows), 'TOTALGB': round(base_gb, 2), 'TOTALCREDITS': round(credits, 3)
            })
    df = pd.DataFrame(data)
    df.columns = [col.upper() for col in df.columns]
    return df

def add_efficiency_metrics(df):
    """Helper function to calculate and add efficiency metric columns to a dataframe."""
    if df.empty:
        return df
    df_copy = df.copy()
    # Avoid division by zero issues
    df_copy['TOTALROWS'] = df_copy['TOTALROWS'].replace(0, np.nan)
    df_copy['TOTALGB'] = df_copy['TOTALGB'].replace(0, np.nan)
    
    df_copy['CREDITS_PER_MILLION_ROWS'] = (df_copy['TOTALCREDITS'] / df_copy['TOTALROWS']) * 1_000_000
    df_copy['CREDITS_PER_GB'] = df_copy['TOTALCREDITS'] / df_copy['TOTALGB']
    df_copy.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df_copy


def format_large_number(num):
    """Formats a large number with K, M, B, T suffixes."""
    if num is None or pd.isna(num):
        return "N/A"
    if abs(num) < 1000:
        return f"{num:,.0f}"
    
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    
    # Format to one decimal place and add the appropriate suffix
    return f"{num:.1f}{['', 'K', 'M', 'B', 'T'][magnitude]}"


def calculate_kpis(current_period_df, previous_period_df):
    """Calculate KPI metrics based on two dataframes, handling empty prior dataframe."""
    # Calculate current period metrics
    current_credits_per_million = current_period_df['CREDITS_PER_MILLION_ROWS'].mean()
    current_credits_per_gb = current_period_df['CREDITS_PER_GB'].mean()

    # Initialize previous period metrics as NaN (Not a Number)
    previous_credits_per_million = np.nan
    previous_credits_per_gb = np.nan

    # Only try to calculate previous metrics if the dataframe has data
    if not previous_period_df.empty:
        previous_credits_per_million = previous_period_df['CREDITS_PER_MILLION_ROWS'].mean()
        previous_credits_per_gb = previous_period_df['CREDITS_PER_GB'].mean()
    
    # Handle trend calculation if prior period data is missing or zero
    if pd.notna(previous_credits_per_million) and previous_credits_per_million > 0:
        trend_credits_per_million = ((current_credits_per_million - previous_credits_per_million) / previous_credits_per_million * 100)
    else:
        trend_credits_per_million = None # Indicate no data for comparison

    if pd.notna(previous_credits_per_gb) and previous_credits_per_gb > 0:
        trend_credits_per_gb = ((current_credits_per_gb - previous_credits_per_gb) / previous_credits_per_gb * 100)
    else:
        trend_credits_per_gb = None # Indicate no data for comparison
    
    return {
        'current_credits_per_million': current_credits_per_million if pd.notna(current_credits_per_million) else 0,
        'trend_credits_per_million': trend_credits_per_million,
        'current_credits_per_gb': current_credits_per_gb if pd.notna(current_credits_per_gb) else 0,
        'trend_credits_per_gb': trend_credits_per_gb
    }

def create_kpi_card(title, value, trend, period_days, unit="", value_format=".3f"):
    """Create a KPI card with trend indicator, handling None for trend and custom formatting."""
    
    if trend is None:
        # Create the HTML for the trend line as a clean, single-line string
        trend_html = '<p class="trend-neutral" style="margin: 0; font-size: 16px;">No prior period data</p>'
    else:
        # Determine color and icon
        trend_color = "trend-up" if trend > 0 else "trend-down"
        trend_icon = "↗️" if trend > 0 else "↘️"
        if trend == 0: 
            trend_color, trend_icon = "trend-neutral", "➡️"
        
        # Create the HTML for the trend line as a clean, single-line string
        trend_html = f'<p class="{trend_color}" style="margin: 0; font-size: 16px;">{trend_icon} {abs(trend):.1f}% vs prior {period_days} days</p>'
    
    # Construct the final HTML for the card
    return f"""
    <div class="kpi-container">
        <h3 style="margin: 0; color: #1f77b4;">{title}</h3>
        <h1 style="margin: 10px 0; color: #333;">{value:{value_format}}{unit}</h1>
        {trend_html}
    </div>
    """

def create_daily_ingestion_chart(df):
    """Creates a stacked subplot chart to display daily GB, Rows, and Credits."""
    daily_summary = df.groupby('INGESTDAY').agg({
        'TOTALGB': 'sum', 'TOTALROWS': 'sum', 'TOTALCREDITS': 'sum'
    }).reset_index()

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.1,
        subplot_titles=('Total GB Ingested', 'Total Rows Ingested', 'Total Credits Used')
    )
    fig.add_trace(go.Bar(x=daily_summary['INGESTDAY'], y=daily_summary['TOTALGB'], name='GB', marker_color='#28a745'), row=1, col=1)
    fig.add_trace(go.Scatter(x=daily_summary['INGESTDAY'], y=daily_summary['TOTALROWS'], name='Rows', mode='lines', line=dict(color='#dc3545')), row=2, col=1)
    fig.add_trace(go.Scatter(x=daily_summary['INGESTDAY'], y=daily_summary['TOTALCREDITS'], name='Credits', mode='lines', line=dict(color='#1f77b4')), row=3, col=1)

    fig.update_layout(title_text="Daily Ingestion Details", height=600, showlegend=False)
    fig.update_yaxes(title_text="GB", row=1, col=1)
    fig.update_yaxes(title_text="Rows", row=2, col=1)
    fig.update_yaxes(title_text="Credits", row=3, col=1)
    fig.update_xaxes(title_text="Date", row=3, col=1)
    return fig

def create_trend_chart(df, metric_col, title):
    """Create trend line chart for a daily aggregated metric."""
    if 'CREDITS_PER' in metric_col:
        daily_trend = df.groupby('INGESTDAY')[metric_col].mean().reset_index()
    else:
        daily_trend = df.groupby('INGESTDAY')[metric_col].sum().reset_index()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily_trend['INGESTDAY'], y=daily_trend[metric_col], mode='lines+markers',
        name=title, line=dict(color='#1f77b4', width=2), marker=dict(size=4)
    ))

    temp_df = daily_trend.dropna(subset=[metric_col])
    if len(temp_df) > 1:
        x_numeric = pd.to_numeric(temp_df['INGESTDAY'])
        z = np.polyfit(x_numeric, temp_df[metric_col], 1)
        p = np.poly1d(z)
        fig.add_trace(go.Scatter(
            x=temp_df['INGESTDAY'], y=p(x_numeric), mode='lines',
            name='Trend', line=dict(color='red', width=2, dash='dash')
        ))

    fig.update_layout(
        title=title, xaxis_title="Date", yaxis_title=metric_col.replace('_', ' ').title(),
        hovermode='x unified', showlegend=True, height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    return fig

def create_performance_barchart(df_current, df_previous, metric_col, agg_func, title, higher_is_better=False, value_format=',.2f'):
    """Creates a horizontal bar chart comparing warehouse performance with % change."""
    if df_current.empty:
        fig = go.Figure()
        fig.update_layout(
            title_text=f"{title}<br><sub>No data for current period</sub>", height=400,
            xaxis={"visible": False}, yaxis={"visible": False}
        )
        return fig

    current_metrics = df_current.groupby('WAREHOUSE_NAME').agg(Metric=(metric_col, agg_func)).reset_index()
    
    if not df_previous.empty:
        previous_metrics = df_previous.groupby('WAREHOUSE_NAME').agg(PreviousMetric=(metric_col, agg_func)).reset_index()
        comparison_df = pd.merge(current_metrics, previous_metrics, on='WAREHOUSE_NAME', how='left')
        comparison_df['PreviousMetric'].fillna(0, inplace=True)
        # Handle division by zero when previous value was 0
        comparison_df['Change'] = 100 * (comparison_df['Metric'] - comparison_df['PreviousMetric']) / comparison_df['PreviousMetric'].replace(0, np.nan)

    else:
        comparison_df = current_metrics
        comparison_df['Change'] = np.nan # Use NaN to indicate no prior data

    comparison_df = comparison_df.sort_values(by='Metric', ascending=True)

    def get_change_text_and_color(change):
        if pd.isna(change):
            return "N/A", "#6c757d"
        icon = "▲" if change > 0 else "▼"
        text = f"{icon} {abs(change):.1f}%"
        is_bad_change = (change > 0 and not higher_is_better) or (change < 0 and higher_is_better)
        color = "#dc3545" if is_bad_change else "#28a745"
        return text, color

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=comparison_df['WAREHOUSE_NAME'], x=comparison_df['Metric'], orientation='h',
        text=comparison_df['Metric'].apply(lambda x: f'{x:{value_format}}'),
        textposition='auto', insidetextanchor='end', marker_color='#1f77b4'
    ))

    for i, row in comparison_df.iterrows():
        change_text, color = get_change_text_and_color(row['Change'])
        fig.add_annotation(
            x=row['Metric'], y=row['WAREHOUSE_NAME'], xref='x', yref='y',
            text=change_text, showarrow=False, xanchor='left', xshift=5,
            font=dict(color=color, size=12)
        )

    fig.update_layout(
        title_text=title, xaxis_title=metric_col.replace('_', ' ').title(), yaxis_title=None,
        height=max(400, len(comparison_df) * 40 + 150),
        margin=dict(l=120, r=20, t=80, b=50),
        xaxis=dict(range=[0, comparison_df['Metric'].max() * 1.35])
    )
    return fig

def create_warehouse_comparison(df):
    """Original warehouse comparison chart function (now supplementary)."""
    df_with_metrics = add_efficiency_metrics(df.copy())
    if df_with_metrics.empty:
        fig = go.Figure()
        fig.update_layout(title_text="Original Warehouse Performance View<br><sub>No data for period</sub>", height=400, xaxis={"visible": False}, yaxis={"visible": False})
        return fig
        
    warehouse_metrics = df_with_metrics.groupby('WAREHOUSE_NAME').agg({
        'CREDITS_PER_MILLION_ROWS': 'mean', 'CREDITS_PER_GB': 'mean', 'TOTALCREDITS': 'sum'
    }).reset_index()

    fig = make_subplots(rows=1, cols=2, subplot_titles=('Avg. Credits per Million Rows', 'Avg. Credits per GB'))
    fig.add_trace(go.Bar(
        x=warehouse_metrics['WAREHOUSE_NAME'], y=warehouse_metrics['CREDITS_PER_MILLION_ROWS'],
        name='Credits/Million Rows', marker_color='lightblue'), row=1, col=1)
    fig.add_trace(go.Bar(
        x=warehouse_metrics['WAREHOUSE_NAME'], y=warehouse_metrics['CREDITS_PER_GB'],
        name='Credits/GB', marker_color='lightcoral'), row=1, col=2)
    fig.update_layout(title_text="Original Warehouse Performance View", showlegend=False, height=400)
    return fig


def main():
    """Main dashboard function"""
    st.title("📊 Data Ingestion Metrics Dashboard")
    st.markdown("Monitor your data ingestion performance and costs")
    
    st.sidebar.header("Filters")
    period_days = st.sidebar.selectbox(
        "Select Period",
        options=[7, 30, 90, 180],
        format_func=lambda x: f"Last {x} Days",
        index=1 
    )

    with st.spinner("Loading and processing data..."):
        # Pass the selected period to the data loading function
        df_master = load_data_from_snowflake(period_days)
    
    if df_master.empty:
        st.error("No data available to display.")
        return

    # The master dataframe is now already filtered by the query, 
    # but we still need to separate it into current and prior periods in pandas.
    max_date = df_master['INGESTDAY'].max()
    start_date = max_date - timedelta(days=period_days)
    previous_period_end_date = start_date - timedelta(days=1)
    previous_period_start_date = previous_period_end_date - timedelta(days=period_days)

    st.sidebar.markdown("---")
    st.sidebar.write("Filter Warehouses")
    all_warehouses = sorted(list(df_master['WAREHOUSE_NAME'].unique()))
    
    # This logic prevents errors by ensuring the session state only contains valid options.
    if 'warehouse_selection' not in st.session_state:
        # Initialize state if it's the very first run
        st.session_state.warehouse_selection = all_warehouses
    else:
        # On subsequent runs, filter the state to only include warehouses present in the new data
        st.session_state.warehouse_selection = [
            wh for wh in st.session_state.warehouse_selection if wh in all_warehouses
        ]
        
    btn_col1, btn_col2 = st.sidebar.columns(2)
    if btn_col1.button("Select All", use_container_width=True):
        st.session_state.warehouse_selection = all_warehouses
    if btn_col2.button("Unselect All", use_container_width=True):
        st.session_state.warehouse_selection = []
        
    # The default value is now guaranteed to be a subset of the options
    warehouses = st.sidebar.multiselect(
        "Select Warehouses", options=all_warehouses,
        default=st.session_state.warehouse_selection, label_visibility="collapsed"
    )
    st.session_state.warehouse_selection = warehouses

    df_filtered = df_master[
        (df_master['INGESTDAY'] > start_date) & (df_master['INGESTDAY'] <= max_date) &
        (df_master['WAREHOUSE_NAME'].isin(warehouses))
    ]
    df_previous_period = df_master[
        (df_master['INGESTDAY'] > previous_period_start_date) & (df_master['INGESTDAY'] <= previous_period_end_date) &
        (df_master['WAREHOUSE_NAME'].isin(warehouses))
    ]
    
    df_all_time = df_master[df_master['WAREHOUSE_NAME'].isin(warehouses)]

    if df_filtered.empty:
        st.warning("No data matches your current filter selection.")
        return

    # Calculate efficiency metrics *before* they are used.
    df_filtered = add_efficiency_metrics(df_filtered)
    df_previous_period = add_efficiency_metrics(df_previous_period)
    
    # --- Dashboard Layout ---
    st.header("📈 Key Performance Indicators")
    st.markdown("Key cost and efficiency metrics for the selected period.")
    
    # --- KPI Calculations ---
    avg_kpis = calculate_kpis(df_filtered.copy(), df_previous_period.copy())
    
    current_credits_total = df_filtered['TOTALCREDITS'].sum()
    previous_credits_total = df_previous_period['TOTALCREDITS'].sum()
    if previous_credits_total > 0:
        trend_credits_total = ((current_credits_total - previous_credits_total) / previous_credits_total * 100)
    else:
        trend_credits_total = None # No prior data for comparison

    # --- KPI Display ---
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(create_kpi_card(
            "Total Credits Used",
            current_credits_total,
            trend_credits_total,
            period_days,
            value_format=',.0f'
        ), unsafe_allow_html=True)
    with col2:
        st.markdown(create_kpi_card(
            "Avg Credits / Million Rows",
            avg_kpis['current_credits_per_million'],
            avg_kpis['trend_credits_per_million'],
            period_days
        ), unsafe_allow_html=True)
    with col3:
        st.markdown(create_kpi_card(
            "Avg Credits / GB Ingested",
            avg_kpis['current_credits_per_gb'],
            avg_kpis['trend_credits_per_gb'],
            period_days
        ), unsafe_allow_html=True)


    st.markdown('<br>', unsafe_allow_html=True)
    st.header("📋 Summary Statistics")
    
    # --- Summary Statistics Calculation with Error Handling ---
    current_rows = df_filtered['TOTALROWS'].sum()
    previous_rows = df_previous_period['TOTALROWS'].sum()
    total_rows_all = df_all_time['TOTALROWS'].sum()
    if previous_rows > 0:
        delta_rows_text = f"{((current_rows - previous_rows) / previous_rows * 100):.1f}%"
    else:
        delta_rows_text = "N/A"
    percent_of_total_rows = (current_rows / total_rows_all * 100) if total_rows_all > 0 else 0

    current_gb = df_filtered['TOTALGB'].sum()
    previous_gb = df_previous_period['TOTALGB'].sum()
    total_gb_all = df_all_time['TOTALGB'].sum()
    if previous_gb > 0:
        delta_gb_text = f"{((current_gb - previous_gb) / previous_gb * 100):.1f}%"
    else:
        delta_gb_text = "N/A"
    percent_of_total_gb = (current_gb / total_gb_all * 100) if total_gb_all > 0 else 0

    col1, col2, col3 = st.columns(3)
    # MODIFICATION: Using the new format_large_number function
    col1.metric("Total Rows Ingested", format_large_number(current_rows), delta=delta_rows_text, help=f"This is {percent_of_total_rows:.1f}% of total rows in the dataset.")
    col2.metric("Total GB Ingested", f"{current_gb:,.0f}", delta=delta_gb_text, help=f"This is {percent_of_total_gb:.1f}% of total GB in the dataset.")
    avg_efficiency = current_rows / current_credits_total if current_credits_total > 0 else 0
    # MODIFICATION: Using the new format_large_number function
    col3.metric("Rows per Credit", format_large_number(avg_efficiency), help="Overall efficiency for the period.")
    
    st.markdown("---")
    st.header("💾 Daily Ingestion Volume")
    st.plotly_chart(create_daily_ingestion_chart(df_filtered), use_container_width=True)
    
    st.markdown("---")
    st.header("📊 Trend Analysis")
    col1_trend, col2_trend = st.columns(2)
    with col1_trend:
        st.plotly_chart(create_trend_chart(df_filtered, 'CREDITS_PER_MILLION_ROWS', 'Credits per Million Rows Trend'), use_container_width=True)
    with col2_trend:
        st.plotly_chart(create_trend_chart(df_filtered, 'CREDITS_PER_GB', 'Credits per GB Trend'), use_container_width=True)
    
    col3_trend, col4_trend = st.columns(2)
    with col3_trend:
        st.plotly_chart(create_trend_chart(df_filtered, 'TOTALROWS', 'Number of Rows per Day Trend'), use_container_width=True)
    with col4_trend:
        st.plotly_chart(create_trend_chart(df_filtered, 'TOTALGB', 'Number of GB per Day Trend'), use_container_width=True)

    st.markdown("---")
    st.header("🏭 Warehouse Performance")
    st.markdown("Compare warehouse costs and efficiency against the prior period.")
    
    col1a, col2a, col3a = st.columns(3)
    with col1a:
        st.plotly_chart(create_performance_barchart(
            df_current=df_filtered, df_previous=df_previous_period,
            metric_col='TOTALCREDITS', agg_func='sum',
            title='Total Credits Used',
            higher_is_better=False, value_format=',.0f'
        ), use_container_width=True)
    with col2a:
        st.plotly_chart(create_performance_barchart(
            df_current=df_filtered, df_previous=df_previous_period,
            metric_col='CREDITS_PER_GB', agg_func='mean',
            title='Avg. Credits per GB',
            higher_is_better=False
        ), use_container_width=True)
    with col3a:
        st.plotly_chart(create_performance_barchart(
            df_current=df_filtered, df_previous=df_previous_period,
            metric_col='CREDITS_PER_MILLION_ROWS', agg_func='mean',
            title='Avg. Credits / Million Rows',
            higher_is_better=False
        ), use_container_width=True)

    # Original chart is kept to satisfy "do not remove" constraint
    with st.expander("Original Warehouse Comparison View"):
        st.plotly_chart(create_warehouse_comparison(df_filtered), use_container_width=True)

    with st.expander("📋 View Raw Data"):
        st.dataframe(df_filtered.sort_values('INGESTDAY', ascending=False), use_container_width=True)
    
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

if __name__ == "__main__":
    main()
