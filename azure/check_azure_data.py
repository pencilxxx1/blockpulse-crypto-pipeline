# Create diagnostic script
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

print("=" * 60)
print("CHECKING AZURE STORAGE STRUCTURE")
print("=" * 60)

# Check all containers
print("\n1. Available Containers:")
for container in blob_service_client.list_containers():
    print(f"   ✓ {container.name}")

# Check crypto-gold container
container_client = blob_service_client.get_container_client("crypto-gold")

print("\n2. Files in crypto-gold container:")
blobs = list(container_client.list_blobs())

if not blobs:
    print("   ⚠️  Container is EMPTY!")
    print("\n   → You need to run your Databricks notebooks first:")
    print("      1. bronze_to_silver.py")
    print("      2. silver_to_gold.py")
else:
    print(f"   Found {len(blobs)} files/folders\n")
    
    for blob in blobs[:20]:  # Show first 20
        size_kb = blob.size / 1024
        print(f"   📁 {blob.name}")
        print(f"      Size: {size_kb:.2f} KB")
        print(f"      Type: {blob.blob_type}")
        print()

# Check specifically for gold tables
print("\n3. Looking for Gold Layer Tables:")
gold_tables = [
    'agg_daily_market_summary',
    'agg_top_performers', 
    'agg_volume_leaders',
    'agg_category_performance'
]

for table in gold_tables:
    table_blobs = list(container_client.list_blobs(name_starts_with=f"gold/{table}"))
    if table_blobs:
        print(f"   ✓ {table}: {len(table_blobs)} files")
        for blob in table_blobs[:3]:
            print(f"      - {blob.name} ({blob.size / 1024:.2f} KB)")
    else:
        print(f"   ✗ {table}: NOT FOUND")

print("\n" + "=" * 60)
print("DIAGNOSTIC CHECK COMPLETE")
