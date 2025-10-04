# BlockPulse Crypto Data Pipeline

A production-ready data pipeline for extracting, transforming, and analyzing cryptocurrency market data from CoinGecko API using Azure Data Factory and Databricks.

## Architecture Overview

This pipeline implements a **Medallion Architecture** (Bronze → Silver → Gold) for cryptocurrency market data processing.

```
CoinGecko API
     ↓
Azure Data Factory (Orchestration)
     ↓
Bronze Layer (Raw Data)
     ↓
Silver Layer (Cleaned & Transformed)
     ↓
Gold Layer (Star Schema - Analytics Ready)
     ↓
Visualization/Analytics Tools
```

## Pipeline Components

### 1. Data Ingestion (ADF Copy Activity)
- **Source**: CoinGecko Markets API (`/api/v3/coins/markets`)
- **Parameters**: 
  - `vs_currency=usd`
  - `per_page=250` (maximum records per request)
- **Authentication**: API Key via custom header (`x-cg-demo-api-key`)
- **Output**: Raw JSON files in Azure Data Lake Storage Gen2

### 2. Bronze Layer (Raw Data Storage)
- **Format**: JSON
- **Storage Path**: `abfss://crypto-bronze@<storage-account>/year=YYYY/month=MM/day=DD/data.json`
- **Partitioning**: Date-based (year/month/day)
- **Purpose**: Immutable raw data archive

### 3. Silver Layer (Data Transformation)
**Notebook**: `bronze_to_silver.py`

**Transformations**:
- Data cleaning and validation
- Duplicate removal
- Null value handling
- Type conversions (timestamps, numeric fields)
- Derived metrics calculation:
  - Market cap categories (Large/Mid/Small/Micro Cap)
  - Volatility flags (>10% price change)
  - Volume-to-market-cap ratio
  - Distance from all-time high percentage

**Output**:
- **Format**: Parquet
- **Storage Path**: `abfss://crypto-silver@<storage-account>/silver/`
- **Partitioning**: `silver_processing_date`

**Data Quality Checks**:
- Record count validation
- Null value analysis
- Market cap distribution
- High volatility asset identification

### 4. Gold Layer (Analytics & Star Schema)
**Notebook**: `silver_to_gold.py`

**Data Model**: Star Schema

#### Dimension Tables
1. **dim_date**
   - Date key, year, month, day, quarter
   - Day of week, month name
   - Weekend flag

2. **dim_crypto**
   - Crypto ID, symbol, name
   - Market cap category and rank
   - Supply metrics (max, circulating, total)
   - All-time high/low data
   - SCD Type 2 implementation

#### Fact Table
**fact_market_snapshot**
- Daily snapshots of all cryptocurrencies
- Price metrics (current, high, low, changes)
- Market metrics (cap, volume, rank)
- Derived KPIs
- Foreign keys to dimension tables

#### Aggregated Tables (for Dashboards)
1. **agg_daily_market_summary**
   - Total market cap and volume
   - Average price and price changes
   - High volatility asset counts

2. **agg_category_performance**
   - Performance by market cap category
   - Volume and price change statistics

3. **agg_top_performers**
   - Top 10 gainers and losers daily

4. **agg_volume_leaders**
   - Top 20 cryptocurrencies by trading volume

## Technology Stack

- **Orchestration**: Azure Data Factory
- **Storage**: Azure Data Lake Storage Gen2
- **Processing**: Azure Databricks (Apache Spark)
- **Version Control**: GitHub
- **Language**: Python (PySpark)

## Azure Services Configuration

### Storage Account
- **Name**: `blockpulsest73150`
- **Containers**:
  - `crypto-bronze` - Raw data
  - `crypto-silver` - Cleaned data
  - `crypto-gold` - Analytics-ready data

### Databricks
- **Notebooks**:
  - `/Shared/crypto-pipeline/bronze_to_silver`
  - `/Shared/crypto-pipeline/silver_to_gold`
- **Authentication**: Service Principal with OAuth
- **Secret Scope**: `secret_scope1` (ADLS access key)

### Data Factory Pipeline
**Activities**:
1. Copy Data From CoinGecko (Copy Activity)
2. Wait (30 seconds buffer)
3. Bronze To Silver Layer (Databricks Notebook)
4. Silver To Gold Layer (Databricks Notebook)

## Data Schema

### CoinGecko API Response Fields
- `id`, `symbol`, `name` - Asset identifiers
- `current_price` - Current USD price
- `market_cap`, `market_cap_rank` - Market capitalization
- `total_volume` - 24h trading volume
- `high_24h`, `low_24h` - 24h price range
- `price_change_24h`, `price_change_percentage_24h` - Price changes
- `circulating_supply`, `total_supply`, `max_supply` - Supply metrics
- `ath`, `atl` - All-time high/low prices
- `last_updated` - Data timestamp

## Pipeline Execution Flow

1. **Daily Trigger** (Scheduled in ADF)
2. **Extract** 250 cryptocurrencies from CoinGecko API
3. **Load** raw JSON to Bronze layer (date-partitioned)
4. **Transform** Bronze → Silver (cleaning, validation, enrichment)
5. **Model** Silver → Gold (star schema creation)
6. **Validate** Data quality checks at each layer
7. **Complete** Ready for BI tool consumption

## Data Quality & Monitoring

### Bronze Layer
- File existence validation
- Record count verification

### Silver Layer
- Duplicate removal
- Null value analysis
- Market cap distribution checks
- Volatility detection

### Gold Layer
- Dimension table record counts
- Fact table completeness
- Aggregation accuracy verification

## Setup Instructions

### Prerequisites
- Azure subscription
- CoinGecko API key
- Azure Data Factory instance
- Azure Databricks workspace
- Azure Data Lake Storage Gen2

### Configuration Steps
1. Create storage containers (bronze, silver, gold)
2. Set up Databricks secret scope with ADLS key
3. Import notebooks to Databricks `/Shared/crypto-pipeline/`
4. Configure ADF linked services (REST API, Databricks, ADLS)
5. Create datasets with dynamic date expressions
6. Build and publish ADF pipeline
7. Schedule daily trigger

## Usage

### Manual Execution
Run the ADF pipeline manually from the Azure portal for testing.

### Scheduled Execution
Pipeline runs automatically via configured trigger (daily recommended).

### Querying Gold Layer
```python
# Example: Read daily market summary
df = spark.read.parquet("abfss://crypto-gold@<storage>/gold/agg_daily_market_summary")
df.show()

# Example: Top performers
df = spark.read.parquet("abfss://crypto-gold@<storage>/gold/agg_top_performers")
df.filter(col("performer_type") == "Gainer").show()
```

## Future Enhancements

- [ ] Incremental loading (append mode)
- [ ] Historical trend analysis (7-day, 30-day)
- [ ] Delta Lake implementation for ACID transactions
- [ ] Real-time streaming data ingestion
- [ ] Power BI dashboard integration
- [ ] Alert system for significant market movements
- [ ] Multi-currency support (EUR, GBP, etc.)
- [ ] Additional data sources (Binance, Coinbase APIs)

## Troubleshooting

### Common Issues

**Path Not Found Errors**
- Verify date expressions use `utcnow()` correctly
- Check folder structure matches expected partitioning

**Authentication Failures**
- Confirm service principal credentials in Databricks
- Verify secret scope contains correct ADLS key

**Duplicate Files in Bronze**
- Set Copy Activity "Copy behavior" to "Flatten hierarchy"
- Limit "Max concurrent connections" to 1

## License

This project is part of the BlockPulse cryptocurrency analytics platform.

## Contact

For questions or issues, please open a GitHub issue.