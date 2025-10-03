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
from datetime import datetime
import logging

# Storage account credentials
storage_account = "blockpulsest73150"
bronze_container = "crypto-bronze"
silver_container = "crypto-silver"
today = datetime.now()
current_year = today.year
current_month = today.month
current_day = today.day

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CryptoMarketData_BronzeToSilver") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Define paths
bronze_path = f"abfss://{bronze_container}@{storage_account}.dfs.core.windows.net/year={current_year}/month={current_month:02d}/day={current_day:02d}/data.json"
silver_path = f"abfss://{silver_container}@{storage_account}.dfs.core.windows.net/silver"

print(f"Reading from: {bronze_path}")
print(f"Writing to: {silver_path}")


# Read data from Bronze layer
def read_bronze_data():
    """Read raw JSON data from Bronze layer"""
    try:
        logger.info("Reading data from Bronze layer")
        
        # Read as a JSON array
        df = spark.read.option("multiline", "true") \
                      .option("mode", "PERMISSIVE") \
                      .json(bronze_path)
        
        record_count = df.count()
        logger.info(f"Read {record_count} records from Bronze layer")
        
        # Show schema to debug
        print("\nBronze data schema:")
        df.printSchema()
        
        return df
    
    except Exception as e:
        logger.error(f"Error reading Bronze data: {str(e)}")
        raise


# Clean and validate data
def clean_and_validate_data(df):
    """Clean and validate market data"""
    logger.info("Starting data cleaning and validation")
    
    # Remove duplicates based on id and last_updated
    df_clean = df.dropDuplicates(["id", "last_updated"])
    
    # Filter out records with null essential fields
    df_clean = df_clean.filter(
        col("id").isNotNull() &
        col("symbol").isNotNull() &
        col("name").isNotNull() &
        col("current_price").isNotNull()
    )
    
    # Convert timestamp strings to proper timestamp type
    df_clean = df_clean.withColumn(
        "last_updated_ts",
        to_timestamp(col("last_updated"))
    )
    
    df_clean = df_clean.withColumn(
        "ath_date_ts",
        to_timestamp(col("ath_date"))
    )
    
    df_clean = df_clean.withColumn(
        "atl_date_ts",
        to_timestamp(col("atl_date"))
    )
    
    # Add data quality flags
    df_clean = df_clean.withColumn(
        "has_market_cap",
        when(col("market_cap").isNotNull() & (col("market_cap") > 0), True).otherwise(False)
    )
    
    df_clean = df_clean.withColumn(
        "has_volume",
        when(col("total_volume").isNotNull() & (col("total_volume") > 0), True).otherwise(False)
    )
    
    # Add processing metadata
    df_clean = df_clean.withColumn("silver_processing_timestamp", current_timestamp())
    df_clean = df_clean.withColumn("silver_processing_date", current_date())
    
    logger.info(f"Cleaned data: {df_clean.count()} records")
    return df_clean


# Add derived metrics
def add_derived_metrics(df):
    """Add calculated metrics and business logic"""
    logger.info("Adding derived metrics")
    
    # Market cap categories
    df_enhanced = df.withColumn(
        "market_cap_category",
        when(col("market_cap") >= 10e9, "Large Cap")
        .when((col("market_cap") >= 1e9) & (col("market_cap") < 10e9), "Mid Cap")
        .when((col("market_cap") >= 100e6) & (col("market_cap") < 1e9), "Small Cap")
        .when(col("market_cap") < 100e6, "Micro Cap")
        .otherwise("Unknown")
    )
    
    # Price volatility indicators
    df_enhanced = df_enhanced.withColumn(
        "is_high_volatility",
        when(abs(col("price_change_percentage_24h")) > 10, True).otherwise(False)
    )
    
    # Volume to market cap ratio
    df_enhanced = df_enhanced.withColumn(
        "volume_to_mcap_ratio",
        when(col("market_cap") > 0, col("total_volume") / col("market_cap")).otherwise(None)
    )
    
    # Distance from ATH
    df_enhanced = df_enhanced.withColumn(
        "distance_from_ath_pct",
        when(col("ath") > 0, ((col("current_price") - col("ath")) / col("ath")) * 100).otherwise(None)
    )
    
    logger.info("Derived metrics added successfully")
    return df_enhanced


# Data Quality Checks
def run_data_quality_checks(df):
    """Run comprehensive data quality checks"""
    logger.info("Running data quality checks")
    
    print("\n=== Data Quality Report ===")
    
    # Basic statistics
    total_records = df.count()
    print(f"Total records processed: {total_records}")
    
    # Null value analysis
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    
    print("\nNull value counts:")
    for col_name, null_count in null_counts.items():
        if null_count > 0:
            print(f"  {col_name}: {null_count} ({null_count/total_records*100:.2f}%)")
    
    # Market cap distribution
    print("\nMarket Cap Category Distribution:")
    df.groupBy("market_cap_category").count().show()
    
    # High volatility assets
    high_vol_count = df.filter(col("is_high_volatility") == True).count()
    print(f"\nHigh volatility assets (>10% change): {high_vol_count} ({high_vol_count/total_records*100:.2f}%)")


# Main Bronze to Silver processing pipeline
def process_bronze_to_silver():
    """Main processing pipeline from Bronze to Silver layer"""
    try:
        logger.info("Starting Bronze to Silver processing pipeline")
        
        # Step 1: Read raw data from Bronze layer
        df_bronze = read_bronze_data()
        
        if df_bronze.count() == 0:
            logger.warning("No data found in Bronze layer")
            return None
        
        # Clean and validate data
        df_clean = clean_and_validate_data(df_bronze)
        
        # Add derived metrics
        df_silver = add_derived_metrics(df_clean)
        
        # Write to Silver layer
        logger.info("Writing data to Silver layer")
        df_silver.write \
            .mode("overwrite") \
            .partitionBy("silver_processing_date") \
            .parquet(silver_path)
        
        # Log processing summary
        record_count = df_silver.count()
        logger.info(f"Successfully processed {record_count} records to Silver layer")
        
        # Display sample of processed data
        print("\nSample Silver data:")
        df_silver.select(
            "id", "symbol", "name", "current_price", "market_cap",
            "market_cap_category", "is_high_volatility", "silver_processing_date"
        ).show(10, truncate=False)
        
        return df_silver
    
    except Exception as e:
        logger.error(f"Error in Bronze to Silver processing: {str(e)}")
        raise


# Execute the pipeline
try:
    processed_df = process_bronze_to_silver()
    
    if processed_df is not None:
        # Run quality checks
        run_data_quality_checks(processed_df)
        
        logger.info("Bronze to Silver processing pipeline completed successfully")
    
except Exception as e:
    logger.error(f"Pipeline failed: {str(e)}")
    raise