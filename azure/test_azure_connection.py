from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
print(f"Connection string found: {connection_string[:50]}..." if connection_string else "❌ No connection string")

try:
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client("crypto-gold")
    
    print("\n✅ Connected to Azure successfully!")
    print("\nFiles in crypto-gold container:")
    
    blobs = container_client.list_blobs(name_starts_with="gold/")
    for blob in blobs:
        print(f"  - {blob.name} ({blob.size / 1024:.2f} KB)")
        
except Exception as e:
    print(f"\n❌ Error: {e}")