"""
BlockPulse Crypto Analytics Dashboard
A Streamlit app for visualizing cryptocurrency market data from the Gold layer
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="BlockPulse Crypto Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""", unsafe_allow_html=True)


# ============================================
# DATA LOADING FUNCTIONS
# ============================================

@st.cache_data(ttl=3600)
def load_data_from_local(file_path):
    """Load data from local parquet files (for demo/testing)"""
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

@st.cache_data(ttl=3600)
def load_data_from_azure(table_name):
    """Load data from Azure Data Lake (production) - handles partitioned data"""
    try:
        from azure.storage.blob import BlobServiceClient
        import io
        import pandas as pd
        
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        
        if not connection_string:
            st.error("❌ Azure connection string not found in .env file")
            return None
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("crypto-gold")
        
        # Search for parquet files in the table folder (including partitions)
        blob_path = f"gold/{table_name}"
        all_blobs = list(container_client.list_blobs(name_starts_with=blob_path))
        
        if not all_blobs:
            st.warning(f"⚠️ No data found for: {table_name}")
            return None
        
        # Find all actual parquet files (not metadata files)
        parquet_files = [
            blob for blob in all_blobs 
            if blob.name.endswith('.parquet') and blob.size > 100  # Ignore tiny metadata files
        ]
        
        if not parquet_files:
            st.warning(f"⚠️ No valid parquet files found in {blob_path}")
            return None
        
        st.info(f"📂 Found {len(parquet_files)} parquet file(s) for {table_name}")
        
        # Read all parquet files and combine
        dfs = []
        for idx, parquet_file in enumerate(parquet_files):
            try:
                blob_client = container_client.get_blob_client(parquet_file.name)
                download_stream = blob_client.download_blob()
                data = download_stream.readall()
                
                if len(data) == 0:
                    continue
                
                df = pd.read_parquet(io.BytesIO(data))
                dfs.append(df)
                
                # Show progress for first few files
                if idx < 3:
                    file_name = parquet_file.name.split('/')[-1][:50]
                    st.success(f"✅ Loaded {len(df)} records from ...{file_name}")
                    
            except Exception as e:
                st.warning(f"⚠️ Error reading file: {str(e)}")
                continue
        
        if not dfs:
            st.error(f"❌ Could not read any data from {table_name}")
            return None
        
        # Combine all dataframes
        final_df = pd.concat(dfs, ignore_index=True)
        
        # Remove any duplicate partition columns if they exist
        if 'date_key' in final_df.columns and final_df['date_key'].dtype == 'object':
            try:
                final_df['date_key'] = pd.to_numeric(final_df['date_key'], errors='coerce')
            except:
                pass
        
        st.success(f"✅ **Total loaded: {len(final_df)} records from {table_name}**")
        
        return final_df
        
    except Exception as e:
        st.error(f"❌ Error loading {table_name}: {str(e)}")
        import traceback
        st.code(traceback.format_exc())
        return None


def get_sample_data():
    """Generate sample data for demonstration"""
    st.warning("📌 Using sample data for demonstration. Connect to Azure for real data.")
    
    # Sample daily market summary
    dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
    daily_summary = pd.DataFrame({
        'silver_processing_date': dates,
        'total_cryptocurrencies': [250] * 30,
        'total_market_cap': [2.5e12 + i * 1e10 for i in range(30)],
        'total_volume': [1e11 + i * 5e9 for i in range(30)],
        'avg_price_change_pct': [(-5 + i * 0.3) for i in range(30)],
        'high_volatility_count': [15 + i for i in range(30)]
    })
    
    # Sample top performers
    top_performers = pd.DataFrame({
        'symbol': ['BTC', 'ETH', 'SOL', 'ADA', 'XRP', 'DOGE', 'DOT', 'AVAX', 'MATIC', 'LINK'],
        'name': ['Bitcoin', 'Ethereum', 'Solana', 'Cardano', 'Ripple', 
                 'Dogecoin', 'Polkadot', 'Avalanche', 'Polygon', 'Chainlink'],
        'price_change_percentage_24h': [15.5, 12.3, 25.7, -8.4, 10.2, -5.1, 8.9, 18.3, 6.5, -3.2],
        'current_price': [45000, 2800, 120, 0.58, 0.65, 0.08, 7.5, 38, 0.95, 15.2],
        'market_cap': [880e9, 340e9, 52e9, 20e9, 35e9, 11e9, 9e9, 14e9, 8e9, 7e9],
        'performer_type': ['Gainer'] * 5 + ['Loser'] * 5
    })
    
    # Sample volume leaders
    volume_leaders = pd.DataFrame({
        'symbol': ['BTC', 'ETH', 'USDT', 'BNB', 'SOL', 'XRP', 'USDC', 'ADA', 'DOGE', 'TRX'],
        'name': ['Bitcoin', 'Ethereum', 'Tether', 'BNB', 'Solana', 
                 'Ripple', 'USD Coin', 'Cardano', 'Dogecoin', 'TRON'],
        'total_volume': [45e9, 28e9, 65e9, 12e9, 8e9, 7e9, 6.5e9, 5e9, 4.2e9, 3.8e9],
        'market_cap': [880e9, 340e9, 95e9, 85e9, 52e9, 35e9, 32e9, 20e9, 11e9, 9e9],
        'volume_to_mcap_ratio': [0.051, 0.082, 0.684, 0.141, 0.154, 0.2, 0.203, 0.25, 0.382, 0.422]
    })
    
    # Sample category performance
    category_performance = pd.DataFrame({
        'market_cap_category': ['Large Cap', 'Mid Cap', 'Small Cap', 'Micro Cap'],
        'crypto_count': [25, 60, 100, 65],
        'total_market_cap': [1.8e12, 500e9, 150e9, 50e9],
        'avg_price_change_pct': [2.5, 3.8, -1.2, 5.5],
        'total_volume': [90e9, 25e9, 8e9, 2e9]
    })
    
    return daily_summary, top_performers, volume_leaders, category_performance


# ============================================
# VISUALIZATION FUNCTIONS
# ============================================

def plot_market_overview(daily_summary):
    """Create market overview metrics"""
    latest = daily_summary.iloc[-1]
    previous = daily_summary.iloc[-2] if len(daily_summary) > 1 else latest
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        market_cap_change = ((latest['total_market_cap'] - previous['total_market_cap']) 
                            / previous['total_market_cap'] * 100)
        st.metric(
            "Total Market Cap",
            f"${latest['total_market_cap']/1e12:.2f}T",
            f"{market_cap_change:+.2f}%"
        )
    
    with col2:
        volume_change = ((latest['total_volume'] - previous['total_volume']) 
                        / previous['total_volume'] * 100)
        st.metric(
            "24h Volume",
            f"${latest['total_volume']/1e9:.2f}B",
            f"{volume_change:+.2f}%"
        )
    
    with col3:
        st.metric(
            "Cryptocurrencies",
            f"{int(latest['total_cryptocurrencies'])}",
            "Tracked"
        )
    
    with col4:
        st.metric(
            "High Volatility Assets",
            f"{int(latest['high_volatility_count'])}",
            "Assets >10% change"
        )


def plot_market_cap_trend(daily_summary):
    """Plot market cap trend over time"""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=daily_summary['silver_processing_date'],
        y=daily_summary['total_market_cap'] / 1e12,
        mode='lines+markers',
        name='Market Cap',
        line=dict(color='#1f77b4', width=3),
        fill='tozeroy',
        fillcolor='rgba(31, 119, 180, 0.2)'
    ))
    
    fig.update_layout(
        title="Total Crypto Market Cap Trend (Last 30 Days)",
        xaxis_title="Date",
        yaxis_title="Market Cap (Trillion USD)",
        hovermode='x unified',
        height=400,
        template='plotly_white'
    )
    
    return fig


def plot_volume_analysis(daily_summary):
    """Plot volume and volatility analysis"""
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('24h Trading Volume', 'High Volatility Assets'),
        vertical_spacing=0.12,
        row_heights=[0.6, 0.4]
    )
    
    # Volume chart
    fig.add_trace(
        go.Bar(
            x=daily_summary['silver_processing_date'],
            y=daily_summary['total_volume'] / 1e9,
            name='Volume',
            marker_color='#2ca02c'
        ),
        row=1, col=1
    )
    
    # Volatility count
    fig.add_trace(
        go.Scatter(
            x=daily_summary['silver_processing_date'],
            y=daily_summary['high_volatility_count'],
            mode='lines+markers',
            name='High Volatility Count',
            line=dict(color='#d62728', width=2),
            marker=dict(size=6)
        ),
        row=2, col=1
    )
    
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Volume (Billion USD)", row=1, col=1)
    fig.update_yaxes(title_text="Asset Count", row=2, col=1)
    
    fig.update_layout(
        height=600,
        showlegend=False,
        template='plotly_white'
    )
    
    return fig


def plot_top_performers(top_performers):
    """Horizontal bar chart for top gainers and losers"""
    gainers = top_performers[top_performers['performer_type'] == 'Gainer'].nlargest(10, 'price_change_percentage_24h')
    losers = top_performers[top_performers['performer_type'] == 'Loser'].nsmallest(10, 'price_change_percentage_24h')
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Top 10 Gainers (24h)', 'Top 10 Losers (24h)'),
        horizontal_spacing=0.15
    )
    
    # Gainers
    fig.add_trace(
        go.Bar(
            y=gainers['symbol'],
            x=gainers['price_change_percentage_24h'],
            orientation='h',
            marker_color='#2ca02c',
            text=gainers['price_change_percentage_24h'].round(2).astype(str) + '%',
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Change: %{x:.2f}%<extra></extra>'
        ),
        row=1, col=1
    )
    
    # Losers
    fig.add_trace(
        go.Bar(
            y=losers['symbol'],
            x=losers['price_change_percentage_24h'],
            orientation='h',
            marker_color='#d62728',
            text=losers['price_change_percentage_24h'].round(2).astype(str) + '%',
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Change: %{x:.2f}%<extra></extra>'
        ),
        row=1, col=2
    )
    
    fig.update_xaxes(title_text="Price Change %", row=1, col=1)
    fig.update_xaxes(title_text="Price Change %", row=1, col=2)
    
    fig.update_layout(
        height=500,
        showlegend=False,
        template='plotly_white'
    )
    
    return fig


def plot_volume_leaders(volume_leaders):
    """Treemap for volume leaders"""
    fig = px.treemap(
        volume_leaders.head(15),
        path=[px.Constant("Crypto Market"), 'symbol'],
        values='total_volume',
        color='volume_to_mcap_ratio',
        hover_data=['name', 'total_volume'],
        color_continuous_scale='Blues',
        title='Top 15 Volume Leaders (Size = Trading Volume)'
    )
    
    fig.update_traces(
        textposition='middle center',
        textfont_size=14,
        hovertemplate='<b>%{label}</b><br>Volume: $%{value:,.0f}<extra></extra>'
    )
    
    fig.update_layout(
        height=500,
        template='plotly_white'
    )
    
    return fig


def plot_category_performance(category_performance):
    """Grouped bar chart for category performance"""
    fig = go.Figure()
    
    # Market cap bars
    fig.add_trace(go.Bar(
        name='Market Cap',
        x=category_performance['market_cap_category'],
        y=category_performance['total_market_cap'] / 1e9,
        marker_color='#1f77b4',
        yaxis='y',
        offsetgroup=1
    ))
    
    # Volume bars
    fig.add_trace(go.Bar(
        name='Volume',
        x=category_performance['market_cap_category'],
        y=category_performance['total_volume'] / 1e9,
        marker_color='#2ca02c',
        yaxis='y',
        offsetgroup=2
    ))
    
    fig.update_layout(
        title='Market Cap vs Volume by Category',
        xaxis_title='Category',
        yaxis_title='Billion USD',
        barmode='group',
        height=450,
        template='plotly_white',
        legend=dict(x=0.01, y=0.99)
    )
    
    return fig


def plot_category_distribution(category_performance):
    """Pie chart for crypto count distribution"""
    fig = px.pie(
        category_performance,
        values='crypto_count',
        names='market_cap_category',
        title='Distribution of Cryptocurrencies by Market Cap Category',
        color_discrete_sequence=px.colors.qualitative.Set3,
        hole=0.4
    )
    
    fig.update_traces(
        textposition='inside',
        textinfo='percent+label',
        hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
    )
    
    fig.update_layout(
        height=450,
        template='plotly_white'
    )
    
    return fig


# ============================================
# MAIN APP
# ============================================

def main():
    # Header
    st.markdown('<h1 class="main-header">📊 BlockPulse Crypto Analytics Dashboard</h1>', 
                unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.title("🚀 Navigation")
        
        page = st.radio(
            "Select View",
            ["📈 Market Overview", "🏆 Top Performers", "💰 Volume Analysis", "📊 Category Insights"]
        )
        
        st.divider()
        
        st.subheader("Data Source")
        data_source = st.radio(
            "Choose data source:",
            ["Sample Data", "Azure Data Lake"]
        )
        
        st.divider()
        
        st.markdown("### About")
        st.info(
            "This dashboard visualizes cryptocurrency market data "
            "from the BlockPulse data pipeline (Bronze → Silver → Gold layers)."
        )
        
        st.markdown("**Last Updated:** " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    

# Load data
    if data_source == "Sample Data":
        daily_summary, top_performers, volume_leaders, category_performance = get_sample_data()
    else:
        with st.spinner("🔌 Connecting to Azure Data Lake..."):
            try:
                # Load from Azure
                daily_summary = load_data_from_azure('agg_daily_market_summary')
                top_performers = load_data_from_azure('agg_top_performers')
                volume_leaders = load_data_from_azure('agg_volume_leaders')
                category_performance = load_data_from_azure('agg_category_performance')
                
                # Fallback to sample data if any failed
                if daily_summary is None or top_performers is None or volume_leaders is None or category_performance is None:
                    st.warning("⚠️ Some tables failed to load. Using sample data.")
                    daily_summary, top_performers, volume_leaders, category_performance = get_sample_data()
                else:
                    st.success("🎉 Successfully loaded all data from Azure!")
                    
            except Exception as e:
                st.error(f"Failed to load from Azure: {e}")
                st.warning("Falling back to sample data.")
                daily_summary, top_performers, volume_leaders, category_performance = get_sample_data()
    
    # Page routing
    if page == "📈 Market Overview":
        st.header("Market Overview")
        
        # Key metrics
        plot_market_overview(daily_summary)
        
        st.divider()
        
        # Market cap trend
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.plotly_chart(plot_market_cap_trend(daily_summary), use_container_width=True)
        
        with col2:
            st.subheader("Market Statistics")
            latest = daily_summary.iloc[-1]
            st.markdown(f"""
            **Current Metrics:**
            - **Market Cap:** ${latest['total_market_cap']/1e12:.2f}T
            - **24h Volume:** ${latest['total_volume']/1e9:.2f}B
            - **Avg Price Change:** {latest['avg_price_change_pct']:.2f}%
            - **Volatile Assets:** {int(latest['high_volatility_count'])}
            """)
        
        st.divider()
        
        # Volume analysis
        st.plotly_chart(plot_volume_analysis(daily_summary), use_container_width=True)
    
    elif page == "🏆 Top Performers":
        st.header("Top Performers (24h)")
        
        # Performance chart
        st.plotly_chart(plot_top_performers(top_performers), use_container_width=True)
        
        st.divider()
        
        # Detailed tables
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚀 Top Gainers")
            gainers = top_performers[top_performers['performer_type'] == 'Gainer'].nlargest(10, 'price_change_percentage_24h')
            st.dataframe(
                gainers[['symbol', 'name', 'price_change_percentage_24h', 'current_price']].round(2),
                hide_index=True,
                use_container_width=True
            )
        
        with col2:
            st.subheader("📉 Top Losers")
            losers = top_performers[top_performers['performer_type'] == 'Loser'].nsmallest(10, 'price_change_percentage_24h')
            st.dataframe(
                losers[['symbol', 'name', 'price_change_percentage_24h', 'current_price']].round(2),
                hide_index=True,
                use_container_width=True
            )
    
    elif page == "💰 Volume Analysis":
        st.header("Volume Leaders")
        
        # Treemap
        st.plotly_chart(plot_volume_leaders(volume_leaders), use_container_width=True)
        
        st.divider()
        
        # Volume leaders table
        st.subheader("Top 10 by Trading Volume")
        volume_display = volume_leaders.head(10)[['symbol', 'name', 'total_volume', 'market_cap', 'volume_to_mcap_ratio']]
        volume_display['total_volume'] = volume_display['total_volume'].apply(lambda x: f"${x/1e9:.2f}B")
        volume_display['market_cap'] = volume_display['market_cap'].apply(lambda x: f"${x/1e9:.2f}B")
        volume_display['volume_to_mcap_ratio'] = volume_display['volume_to_mcap_ratio'].apply(lambda x: f"{x:.3f}")
        
        st.dataframe(volume_display, hide_index=True, use_container_width=True)
    
    elif page == "📊 Category Insights":
        st.header("Category Performance Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.plotly_chart(plot_category_performance(category_performance), use_container_width=True)
        
        with col2:
            st.plotly_chart(plot_category_distribution(category_performance), use_container_width=True)
        
        st.divider()
        
        # Category details
        st.subheader("Category Breakdown")
        category_display = category_performance.copy()
        category_display['total_market_cap'] = category_display['total_market_cap'].apply(lambda x: f"${x/1e9:.2f}B")
        category_display['total_volume'] = category_display['total_volume'].apply(lambda x: f"${x/1e9:.2f}B")
        category_display['avg_price_change_pct'] = category_display['avg_price_change_pct'].apply(lambda x: f"{x:.2f}%")
        
        st.dataframe(category_display, hide_index=True, use_container_width=True)
    
    # Footer
    st.divider()
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>BlockPulse Crypto Analytics | Data from CoinGecko API via Azure Data Pipeline</p>
            <p>Built with Streamlit 🎈 | Powered by Azure Databricks & ADLS Gen2</p>
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
