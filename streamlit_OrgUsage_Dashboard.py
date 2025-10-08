# Nick Akincilar - 10/8/2025 
# Use a role like AccountAdmin, OrgAdmin or a custom role that has access to SNOWFLAKE.ORGANIZATION_USAGE views

# STEPS TO DEPLOY
# 1. make sure you have the role with access to ORGANIZATION_USAGE views seelected at BOTTOM-RIGHT
# 2. Projects > Streamlit
# 3. Create a new Streamlit app
# 4. Paste this code by replace all the existing code
# 5. Click on PACKAGES button on top-left and search & add PLOTLY chart package.


import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
from snowflake.snowpark.context import get_active_session


# Page configuration
st.set_page_config(
    page_title="Snowflake Organization Usage Dashboard",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #29B5E8;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #29B5E8;
    }
    .filter-section {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
    }
    </style>
""", unsafe_allow_html=True)

# Initialize session
@st.cache_resource
def get_snowflake_session():
    """Get or create Snowflake session"""
    return get_active_session()

session = get_snowflake_session()

# Data loading function
@st.cache_data(ttl=3600)
def load_usage_data():
    """Load usage data from Snowflake"""
    query = """
    SELECT 
        ORGANIZATION_NAME,
        CONTRACT_NUMBER,
        ACCOUNT_NAME,
        ACCOUNT_LOCATOR,
        REGION,
        SERVICE_LEVEL,
        USAGE_DATE,
        USAGE_TYPE,
        USAGE,
        CURRENCY,
        USAGE_IN_CURRENCY,
        BALANCE_SOURCE,
        BILLING_TYPE,
        RATING_TYPE,
        SERVICE_TYPE,
        IS_ADJUSTMENT
    FROM SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY
    ORDER BY USAGE_DATE DESC
    """
    df = session.sql(query).to_pandas()
    df['USAGE_DATE'] = pd.to_datetime(df['USAGE_DATE'])
    return df

# Main header
st.markdown('<div class="main-header">❄️ Snowflake Organization Usage Dashboard</div>', unsafe_allow_html=True)

# Load data
try:
    with st.spinner('Loading usage data...'):
        df = load_usage_data()
    
    if df.empty:
        st.warning("No data available in the usage view.")
        st.stop()
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Date range filter
    min_date = df['USAGE_DATE'].min().date()
    #max_date = df['USAGE_DATE'].max().date()
    max_date = date.today() + timedelta(days=1)
    
    

    
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(max_date - timedelta(days=30), max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df[(df['USAGE_DATE'].dt.date >= start_date) & 
                        (df['USAGE_DATE'].dt.date <= end_date)]
    else:
        df_filtered = df
    
    # Account filter
    accounts = ['All'] + sorted(df_filtered['ACCOUNT_NAME'].unique().tolist())
    selected_account = st.sidebar.selectbox("Account", accounts)
    
    if selected_account != 'All':
        df_filtered = df_filtered[df_filtered['ACCOUNT_NAME'] == selected_account]
    
    # Service Type filter
    service_types = ['All'] + sorted(df_filtered['SERVICE_TYPE'].unique().tolist())
    selected_service_type = st.sidebar.multiselect(
        "Service Type",
        service_types,
        default=['All']
    )
    
    if 'All' not in selected_service_type:
        df_filtered = df_filtered[df_filtered['SERVICE_TYPE'].isin(selected_service_type)]
    
    # Region filter
    regions = ['All'] + sorted(df_filtered['REGION'].unique().tolist())
    selected_region = st.sidebar.selectbox("Region", regions)
    
    if selected_region != 'All':
        df_filtered = df_filtered[df_filtered['REGION'] == selected_region]
    
    # Service Level filter
    service_levels = ['All'] + sorted(df_filtered['SERVICE_LEVEL'].unique().tolist())
    selected_service_level = st.sidebar.selectbox("Service Level", service_levels)
    
    if selected_service_level != 'All':
        df_filtered = df_filtered[df_filtered['SERVICE_LEVEL'] == selected_service_level]
    
    # Exclude adjustments option
    exclude_adjustments = st.sidebar.checkbox("Exclude Adjustments", value=True)
    if exclude_adjustments:
        df_filtered = df_filtered[df_filtered['IS_ADJUSTMENT'] == False]
    
    # Key Metrics
    st.markdown("## 📊 Key Metrics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    total_cost = df_filtered['USAGE_IN_CURRENCY'].sum()
    unique_accounts = df_filtered['ACCOUNT_NAME'].nunique()
    total_usage = df_filtered['USAGE'].sum()
    avg_daily_cost = df_filtered.groupby('USAGE_DATE')['USAGE_IN_CURRENCY'].sum().mean()
    unique_regions = df_filtered['REGION'].nunique()
    
    with col1:
        st.metric(
            label="Total Cost",
            value=f"${total_cost:,.0f}",
            delta=None
        )
    
    with col2:
        st.metric(
            label="Accounts",
            value=f"{unique_accounts}"
        )
    
    with col3:
        st.metric(
            label="Total Credit Usage",
            value=f"{total_usage:,.0f}"
        )
    
    with col4:
        st.metric(
            label="Avg Daily Cost",
            value=f"${avg_daily_cost:,.0f}"
        )
    
    with col5:
        st.metric(
            label="Regions",
            value=f"{unique_regions}"
        )
    
    # Add some spacing
    st.markdown("---")
    
    # Visualizations in tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Trends", 
        "🏢 Accounts", 
        "🌍 Regions", 
        "⚙️ Service Types",
        "📋 Raw Data"
    ])
    
    with tab1:
        st.markdown("### Usage Trends Over Time")
        
        # Daily cost trend
        daily_cost = df_filtered.groupby('USAGE_DATE')['USAGE_IN_CURRENCY'].sum().reset_index()
        daily_cost.columns = ['Date', 'Cost']
        
        fig_daily = px.line(
            daily_cost,
            x='Date',
            y='Cost',
            title='Daily Cost Trend',
            labels={'Cost': 'Cost (USD)', 'Date': 'Date'}
        )
        fig_daily.update_traces(line_color='#29B5E8', line_width=2)
        fig_daily.update_layout(hovermode='x unified')
        st.plotly_chart(fig_daily, use_container_width=True)
        
        # Cost by Service Type over time
        col1, col2 = st.columns(2)
        
        with col1:
            service_trend = df_filtered.groupby(['USAGE_DATE', 'SERVICE_TYPE'])['USAGE_IN_CURRENCY'].sum().reset_index()
            fig_service = px.area(
                service_trend,
                x='USAGE_DATE',
                y='USAGE_IN_CURRENCY',
                color='SERVICE_TYPE',
                title='Cost by Service Type Over Time',
                labels={'USAGE_IN_CURRENCY': 'Cost (USD)', 'USAGE_DATE': 'Date'}
            )
            st.plotly_chart(fig_service, use_container_width=True)
        
        with col2:
            # Cost by Billing Type
            billing_trend = df_filtered.groupby(['USAGE_DATE', 'BILLING_TYPE'])['USAGE_IN_CURRENCY'].sum().reset_index()
            fig_billing = px.area(
                billing_trend,
                x='USAGE_DATE',
                y='USAGE_IN_CURRENCY',
                color='BILLING_TYPE',
                title='Cost by Billing Type Over Time',
                labels={'USAGE_IN_CURRENCY': 'Cost (USD)', 'USAGE_DATE': 'Date'}
            )
            st.plotly_chart(fig_billing, use_container_width=True)
    
    with tab2:
        st.markdown("### Account Analysis")
        
        # Top accounts by cost
        account_cost = df_filtered.groupby('ACCOUNT_NAME')['USAGE_IN_CURRENCY'].sum().reset_index()
        account_cost = account_cost.sort_values('USAGE_IN_CURRENCY', ascending=False).head(15)
        
        fig_accounts = px.bar(
            account_cost,
            x='USAGE_IN_CURRENCY',
            y='ACCOUNT_NAME',
            orientation='h',
            title='Top 15 Accounts by Cost',
            labels={'USAGE_IN_CURRENCY': 'Cost (USD)', 'ACCOUNT_NAME': 'Account'},
            color='USAGE_IN_CURRENCY',
            color_continuous_scale='Blues'
        )
        fig_accounts.update_layout(showlegend=False)
        st.plotly_chart(fig_accounts, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Account breakdown by service type
            account_service = df_filtered.groupby(['ACCOUNT_NAME', 'SERVICE_TYPE'])['USAGE_IN_CURRENCY'].sum().reset_index()
            top_accounts = account_cost.head(10)['ACCOUNT_NAME'].tolist()
            account_service = account_service[account_service['ACCOUNT_NAME'].isin(top_accounts)]
            
            fig_acc_service = px.bar(
                account_service,
                x='ACCOUNT_NAME',
                y='USAGE_IN_CURRENCY',
                color='SERVICE_TYPE',
                title='Top 10 Accounts - Service Type Breakdown',
                labels={'USAGE_IN_CURRENCY': 'Cost (USD)', 'ACCOUNT_NAME': 'Account'},
                barmode='stack'
            )
            fig_acc_service.update_xaxes(tickangle=-45)
            st.plotly_chart(fig_acc_service, use_container_width=True)
        
        with col2:
            # Account by service level
            service_level_cost = df_filtered.groupby('SERVICE_LEVEL')['USAGE_IN_CURRENCY'].sum().reset_index()
            fig_service_level = px.pie(
                service_level_cost,
                values='USAGE_IN_CURRENCY',
                names='SERVICE_LEVEL',
                title='Cost Distribution by Service Level',
                hole=0.4
            )
            st.plotly_chart(fig_service_level, use_container_width=True)
    
    with tab3:
        st.markdown("### Regional Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Cost by region
            region_cost = df_filtered.groupby('REGION')['USAGE_IN_CURRENCY'].sum().reset_index()
            region_cost = region_cost.sort_values('USAGE_IN_CURRENCY', ascending=False)
            
            fig_region = px.bar(
                region_cost,
                x='REGION',
                y='USAGE_IN_CURRENCY',
                title='Cost by Region',
                labels={'USAGE_IN_CURRENCY': 'Cost (USD)', 'REGION': 'Region'},
                color='USAGE_IN_CURRENCY',
                color_continuous_scale='Viridis'
            )
            fig_region.update_xaxes(tickangle=-45)
            fig_region.update_layout(showlegend=False)
            st.plotly_chart(fig_region, use_container_width=True)
        
        with col2:
            # Region pie chart
            fig_region_pie = px.pie(
                region_cost,
                values='USAGE_IN_CURRENCY',
                names='REGION',
                title='Regional Cost Distribution',
                hole=0.3
            )
            st.plotly_chart(fig_region_pie, use_container_width=True)
        
       # Regional trends over time
        st.markdown("#### Regional Cost Trends")
        region_trend = df_filtered.groupby(['USAGE_DATE', 'REGION'])['USAGE_IN_CURRENCY'].sum().reset_index()
        region_trend_pivot = region_trend.pivot(index='USAGE_DATE', columns='REGION', values='USAGE_IN_CURRENCY').fillna(0)
        st.line_chart(region_trend_pivot, use_container_width=True)
        
        # Region-Service Type heatmap
        region_service = df_filtered.groupby(['REGION', 'SERVICE_TYPE'])['USAGE_IN_CURRENCY'].sum().reset_index()
        pivot_data = region_service.pivot(index='REGION', columns='SERVICE_TYPE', values='USAGE_IN_CURRENCY').fillna(0)
        
        fig_heatmap = px.imshow(
            pivot_data,
            labels=dict(x="Service Type", y="Region", color="Cost (USD)"),
            title="Cost Heatmap: Region vs Service Type",
            color_continuous_scale='Blues',
            aspect='auto'
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)
    
    with tab4:
        st.markdown("### Service Type Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Cost by service type
            service_cost = df_filtered.groupby('SERVICE_TYPE')['USAGE_IN_CURRENCY'].sum().reset_index()
            service_cost = service_cost.sort_values('USAGE_IN_CURRENCY', ascending=False)
            
            fig_service_bar = px.bar(
                service_cost,
                x='SERVICE_TYPE',
                y='USAGE_IN_CURRENCY',
                title='Cost by Service Type',
                labels={'USAGE_IN_CURRENCY': 'Cost (USD)', 'SERVICE_TYPE': 'Service Type'},
                color='SERVICE_TYPE'
            )
            st.plotly_chart(fig_service_bar, use_container_width=True)
        
        with col2:
            # Usage type breakdown
            usage_type_cost = df_filtered.groupby('USAGE_TYPE')['USAGE_IN_CURRENCY'].sum().reset_index()
            usage_type_cost = usage_type_cost.sort_values('USAGE_IN_CURRENCY', ascending=False).head(10)
            
            fig_usage_type = px.bar(
                usage_type_cost,
                y='USAGE_TYPE',
                x='USAGE_IN_CURRENCY',
                orientation='h',
                title='Top 10 Usage Types by Cost',
                labels={'USAGE_IN_CURRENCY': 'Cost (USD)', 'USAGE_TYPE': 'Usage Type'},
                color='USAGE_IN_CURRENCY',
                color_continuous_scale='Oranges'
            )
            fig_usage_type.update_layout(showlegend=False)
            st.plotly_chart(fig_usage_type, use_container_width=True)
        
        # Service type trends
        service_trend_detailed = df_filtered.groupby(['USAGE_DATE', 'SERVICE_TYPE'])['USAGE_IN_CURRENCY'].sum().reset_index()
        fig_service_trend = px.line(
            service_trend_detailed,
            x='USAGE_DATE',
            y='USAGE_IN_CURRENCY',
            color='SERVICE_TYPE',
            title='Service Type Cost Trends',
            labels={'USAGE_IN_CURRENCY': 'Cost (USD)', 'USAGE_DATE': 'Date'}
        )
        st.plotly_chart(fig_service_trend, use_container_width=True)
        
        # Rating type breakdown
        col1, col2 = st.columns(2)
        
        with col1:
            rating_cost = df_filtered.groupby('RATING_TYPE')['USAGE_IN_CURRENCY'].sum().reset_index()
            fig_rating = px.pie(
                rating_cost,
                values='USAGE_IN_CURRENCY',
                names='RATING_TYPE',
                title='Cost by Rating Type',
                hole=0.4
            )
            st.plotly_chart(fig_rating, use_container_width=True)
        
        with col2:
            balance_cost = df_filtered.groupby('BALANCE_SOURCE')['USAGE_IN_CURRENCY'].sum().reset_index()
            fig_balance = px.pie(
                balance_cost,
                values='USAGE_IN_CURRENCY',
                names='BALANCE_SOURCE',
                title='Cost by Balance Source',
                hole=0.4
            )
            st.plotly_chart(fig_balance, use_container_width=True)
    
    with tab5:
        st.markdown("### Raw Data Explorer")
        
        # Summary statistics
        st.markdown("#### Summary Statistics")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Records", f"{len(df_filtered):,}")
        with col2:
            st.metric("Date Range", f"{df_filtered['USAGE_DATE'].min().date()} to {df_filtered['USAGE_DATE'].max().date()}")
        with col3:
            st.metric("Total Organizations", df_filtered['ORGANIZATION_NAME'].nunique())
        
        # Data table with search
        st.markdown("#### Detailed Usage Data")
        
        # Column selector
        all_columns = df_filtered.columns.tolist()
        default_columns = ['USAGE_DATE', 'ACCOUNT_NAME', 'SERVICE_TYPE', 'USAGE_TYPE', 
                          'USAGE', 'USAGE_IN_CURRENCY', 'REGION', 'SERVICE_LEVEL']
        
        selected_columns = st.multiselect(
            "Select columns to display",
            all_columns,
            default=default_columns
        )
        
        if selected_columns:
            display_df = df_filtered[selected_columns].copy()
        else:
            display_df = df_filtered.copy()
        
        # Sort options
        sort_column = st.selectbox("Sort by", display_df.columns.tolist())
        sort_order = st.radio("Sort order", ['Descending', 'Ascending'], horizontal=True)
        
        display_df = display_df.sort_values(
            by=sort_column,
            ascending=(sort_order == 'Ascending')
        )
        
        # Display paginated data
        rows_per_page = st.selectbox("Rows per page", [25, 50, 100, 500], index=1)
        total_rows = len(display_df)
        total_pages = (total_rows - 1) // rows_per_page + 1
        
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1)
        
        start_idx = (page - 1) * rows_per_page
        end_idx = min(start_idx + rows_per_page, total_rows)
        
        st.dataframe(
            display_df.iloc[start_idx:end_idx],
            use_container_width=True,
            height=400
        )
        
        st.caption(f"Showing rows {start_idx + 1} to {end_idx} of {total_rows}")
        
        # Download option
        csv = df_filtered.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Download Filtered Data as CSV",
            data=csv,
            file_name=f"snowflake_usage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    # Footer
    st.markdown("---")
    st.caption(f"Dashboard last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.caption("Data source: SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY")

except Exception as e:
    st.error(f"Error loading data: {str(e)}")
    st.info("Please ensure you have access to the SNOWFLAKE.ORGANIZATION_USAGE.USAGE_IN_CURRENCY_DAILY view and are running this in Snowflake Streamlit.")
    st.code(str(e))

