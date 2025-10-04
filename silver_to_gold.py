# Databricks notebook source
service_credential = dbutils.secrets.get(scope="secret_scope1",key="databricks-adls-key")

spark.conf.set("fs.azure.account.auth.type.blockpulsest73150.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.blockpulsest73150.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.blockpulsest73150.dfs.core.windows.net", "18369a82-189f-4462-aa50-8675fd0203e5")
spark.conf.set("fs.azure.account.oauth2.client.secret.blockpulsest73150.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.blockpulsest73150.dfs.core.windows.net", "https://login.microsoftonline.com/0c58acc0-bcd0-4d37-8c72-64c92bdac3cb/oauth2/token")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window  # Add this line
from datetime import datetime
import logging

# Storage account credentials
storage_account = "blockpulsest73150"
silver_container = "crypto-silver"
gold_container = "crypto-gold"
today = datetime.now()
current_year = today.year
current_month = today.month
current_day = today.day

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CryptoMarketData_SilverToGold") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Define paths
silver_path = f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/silver"
gold_path = f"abfss://{gold_container}@{storage_account}.dfs.core.windows.net/gold"

print(f"Reading from: {silver_path}")
print(f"Writing to: {gold_path}")


# ============================================
# DIMENSION TABLES
# ============================================

def create_date_dimension():
    """Create Date Dimension table"""
    logger.info("Creating Date Dimension")
    
    # Read Silver data
    df_silver = spark.read.parquet(silver_path)
    
    # Extract unique dates and create date dimension
    date_dim = df_silver.select("silver_processing_date").distinct()
    
    date_dim = date_dim.withColumn("date_key", date_format(col("silver_processing_date"), "yyyyMMdd").cast("int")) \
        .withColumn("year", year(col("silver_processing_date"))) \
        .withColumn("month", month(col("silver_processing_date"))) \
        .withColumn("day", dayofmonth(col("silver_processing_date"))) \
        .withColumn("quarter", quarter(col("silver_processing_date"))) \
        .withColumn("day_of_week", dayofweek(col("silver_processing_date"))) \
        .withColumn("day_name", date_format(col("silver_processing_date"), "EEEE")) \
        .withColumn("month_name", date_format(col("silver_processing_date"), "MMMM")) \
        .withColumn("is_weekend", when(dayofweek(col("silver_processing_date")).isin([1, 7]), True).otherwise(False))
    
    # Write to Gold layer
    date_dim.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/dim_date")
    
    logger.info(f"Date Dimension created with {date_dim.count()} records")
    return date_dim


def create_crypto_dimension():
    """Create Crypto Asset Dimension table (SCD Type 2)"""
    logger.info("Creating Crypto Asset Dimension")
    
    # Read Silver data
    df_silver = spark.read.parquet(silver_path)
    
    # Get the latest record for each crypto asset
    window_spec = Window.partitionBy("id").orderBy(col("last_updated_ts").desc())
    
    crypto_dim = df_silver.withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .select(
            col("id").alias("crypto_id"),
            col("symbol"),
            col("name"),
            col("image"),
            col("market_cap_category"),
            col("market_cap_rank"),
            col("max_supply"),
            col("circulating_supply"),
            col("total_supply"),
            col("ath"),
            col("ath_date_ts"),
            col("atl"),
            col("atl_date_ts"),
            current_timestamp().alias("dim_created_at"),
            lit(True).alias("is_current")
        )
    
    # Write to Gold layer
    crypto_dim.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/dim_crypto")
    
    logger.info(f"Crypto Dimension created with {crypto_dim.count()} records")
    return crypto_dim


# ============================================
# FACT TABLES
# ============================================

def create_market_snapshot_fact():
    """Create Crypto Market Snapshot Fact table"""
    logger.info("Creating Market Snapshot Fact table")
    
    # Read Silver data
    df_silver = spark.read.parquet(silver_path)
    
    # Create fact table with metrics
    fact_snapshot = df_silver.select(
        # Keys
        date_format(col("silver_processing_date"), "yyyyMMdd").cast("int").alias("date_key"),
        col("id").alias("crypto_id"),
        
        # Price metrics
        col("current_price"),
        col("high_24h"),
        col("low_24h"),
        col("price_change_24h"),
        col("price_change_percentage_24h"),
        
        # Market metrics
        col("market_cap"),
        col("market_cap_rank"),
        col("market_cap_change_24h"),
        col("market_cap_change_percentage_24h"),
        col("fully_diluted_valuation"),
        
        # Volume metrics
        col("total_volume"),
        col("volume_to_mcap_ratio"),
        
        # Supply metrics
        col("circulating_supply"),
        
        # Derived metrics
        col("is_high_volatility"),
        col("distance_from_ath_pct"),
        
        # ATH/ATL
        col("ath"),
        col("ath_change_percentage"),
        col("atl"),
        col("atl_change_percentage"),
        
        # Timestamps
        col("last_updated_ts"),
        col("silver_processing_timestamp").alias("fact_created_at")
    )
    
    # Write to Gold layer with partitioning
    fact_snapshot.write \
        .mode("overwrite") \
        .partitionBy("date_key") \
        .parquet(f"{gold_path}/fact_market_snapshot")
    
    logger.info(f"Market Snapshot Fact created with {fact_snapshot.count()} records")
    return fact_snapshot


# ============================================
# AGGREGATED TABLES FOR DASHBOARDS
# ============================================

def create_daily_market_summary():
    """Create daily aggregated market summary"""
    logger.info("Creating Daily Market Summary")
    
    # Read Silver data
    df_silver = spark.read.parquet(silver_path)
    
    # Aggregate by date
    daily_summary = df_silver.groupBy("silver_processing_date").agg(
        count("id").alias("total_cryptocurrencies"),
        sum("market_cap").alias("total_market_cap"),
        sum("total_volume").alias("total_volume"),
        avg("current_price").alias("avg_price"),
        avg("price_change_percentage_24h").alias("avg_price_change_pct"),
        max("market_cap").alias("max_market_cap"),
        min("market_cap").alias("min_market_cap"),
        countDistinct(when(col("is_high_volatility") == True, col("id"))).alias("high_volatility_count")
    )
    
    # Add date key
    daily_summary = daily_summary.withColumn(
        "date_key",
        date_format(col("silver_processing_date"), "yyyyMMdd").cast("int")
    )
    
    # Write to Gold layer
    daily_summary.write \
        .mode("overwrite") \
        .parquet(f"{gold_path}/agg_daily_market_summary")
    
    logger.info("Daily Market Summary created")
    return daily_summary


def create_category_performance():
    """Create performance metrics by market cap category"""
    logger.info("Creating Category Performance aggregation")
    
    # Read Silver data
    df_silver = spark.read.parquet(silver_path)
    
    # Aggregate by date and category
    category_perf = df_silver.groupBy("silver_processing_date", "market_cap_category").agg(
        count("id").alias("crypto_count"),
        sum("market_cap").alias("total_market_cap"),
        sum("total_volume").alias("total_volume"),
        avg("price_change_percentage_24h").alias("avg_price_change_pct"),
        avg("volume_to_mcap_ratio").alias("avg_volume_to_mcap_ratio"),
        max("price_change_percentage_24h").alias("max_price_change_pct"),
        min("price_change_percentage_24h").alias("min_price_change_pct")
    )
    
    # Add date key
    category_perf = category_perf.withColumn(
        "date_key",
        date_format(col("silver_processing_date"), "yyyyMMdd").cast("int")
    )
    
    # Write to Gold layer
    category_perf.write \
        .mode("overwrite") \
        .partitionBy("date_key") \
        .parquet(f"{gold_path}/agg_category_performance")
    
    logger.info("Category Performance aggregation created")
    return category_perf


def create_top_performers():
    """Create top performers table (top gainers and losers)"""
    logger.info("Creating Top Performers table")
    
    # Read Silver data
    df_silver = spark.read.parquet(silver_path)
    
    # Get top 10 gainers
    top_gainers = df_silver.orderBy(col("price_change_percentage_24h").desc()).limit(10) \
        .select(
            col("silver_processing_date"),
            col("id"),
            col("symbol"),
            col("name"),
            col("current_price"),
            col("price_change_percentage_24h"),
            col("market_cap"),
            col("market_cap_category"),
            lit("Gainer").alias("performer_type")
        )
    
    # Get top 10 losers
    top_losers = df_silver.orderBy(col("price_change_percentage_24h").asc()).limit(10) \
        .select(
            col("silver_processing_date"),
            col("id"),
            col("symbol"),
            col("name"),
            col("current_price"),
            col("price_change_percentage_24h"),
            col("market_cap"),
            col("market_cap_category"),
            lit("Loser").alias("performer_type")
        )
    
    # Union both
    top_performers = top_gainers.union(top_losers)
    
    # Add date key
    top_performers = top_performers.withColumn(
        "date_key",
        date_format(col("silver_processing_date"), "yyyyMMdd").cast("int")
    )
    
    # Write to Gold layer
    top_performers.write \
        .mode("overwrite") \
        .partitionBy("date_key") \
        .parquet(f"{gold_path}/agg_top_performers")
    
    logger.info("Top Performers table created")
    return top_performers


def create_volume_leaders():
    """Create volume leaders table"""
    logger.info("Creating Volume Leaders table")
    
    # Read Silver data
    df_silver = spark.read.parquet(silver_path)
    
    # Get top 20 by volume
    volume_leaders = df_silver.orderBy(col("total_volume").desc()).limit(20) \
        .select(
            col("silver_processing_date"),
            col("id"),
            col("symbol"),
            col("name"),
            col("total_volume"),
            col("market_cap"),
            col("volume_to_mcap_ratio"),
            col("current_price"),
            col("market_cap_category")
        )
    
    # Add date key and rank
    volume_leaders = volume_leaders.withColumn(
        "date_key",
        date_format(col("silver_processing_date"), "yyyyMMdd").cast("int")
    ).withColumn(
        "volume_rank",
        row_number().over(Window.orderBy(col("total_volume").desc()))
    )
    
    # Write to Gold layer
    volume_leaders.write \
        .mode("overwrite") \
        .partitionBy("date_key") \
        .parquet(f"{gold_path}/agg_volume_leaders")
    
    logger.info("Volume Leaders table created")
    return volume_leaders


# ============================================
# DATA QUALITY CHECKS
# ============================================

def run_gold_quality_checks():
    """Run quality checks on Gold layer tables"""
    logger.info("Running Gold layer quality checks")
    
    print("\n=== GOLD LAYER QUALITY REPORT ===\n")
    
    # Check dimensions
    dim_date = spark.read.parquet(f"{gold_path}/dim_date")
    dim_crypto = spark.read.parquet(f"{gold_path}/dim_crypto")
    
    print(f"Date Dimension: {dim_date.count()} records")
    print(f"Crypto Dimension: {dim_crypto.count()} records")
    
    # Check fact table
    fact_snapshot = spark.read.parquet(f"{gold_path}/fact_market_snapshot")
    print(f"\nMarket Snapshot Fact: {fact_snapshot.count()} records")
    
    # Check aggregations
    daily_summary = spark.read.parquet(f"{gold_path}/agg_daily_market_summary")
    category_perf = spark.read.parquet(f"{gold_path}/agg_category_performance")
    top_performers = spark.read.parquet(f"{gold_path}/agg_top_performers")
    volume_leaders = spark.read.parquet(f"{gold_path}/agg_volume_leaders")
    
    print(f"\nDaily Market Summary: {daily_summary.count()} records")
    print(f"Category Performance: {category_perf.count()} records")
    print(f"Top Performers: {top_performers.count()} records")
    print(f"Volume Leaders: {volume_leaders.count()} records")
    
    # Show sample from daily summary
    print("\nSample Daily Market Summary:")
    daily_summary.show(5, truncate=False)
    
    # Show category performance
    print("\nCategory Performance:")
    category_perf.show(truncate=False)
    
    # Show top performers
    print("\nTop Performers:")
    top_performers.select("symbol", "name", "price_change_percentage_24h", "performer_type").show(20, truncate=False)


# ============================================
# MAIN PIPELINE
# ============================================

def process_silver_to_gold():
    """Main pipeline to create Gold layer from Silver"""
    try:
        logger.info("Starting Silver to Gold processing pipeline")
        
        # Create dimension tables
        create_date_dimension()
        create_crypto_dimension()
        
        # Create fact table
        create_market_snapshot_fact()
        
        # Create aggregated tables
        create_daily_market_summary()
        create_category_performance()
        create_top_performers()
        create_volume_leaders()
        
        logger.info("Silver to Gold processing completed successfully")
        
        # Run quality checks
        run_gold_quality_checks()
        
        return True
        
    except Exception as e:
        logger.error(f"Error in Silver to Gold processing: {str(e)}")
        raise


# Execute the pipeline
try:
    process_silver_to_gold()
    print("\n✅ Gold layer created successfully!")
    
except Exception as e:
    logger.error(f"Pipeline failed: {str(e)}")
    raise