from azure.storage.blob import BlobServiceClient, generate_blob_sas, generate_container_sas, ContainerSasPermissions, BlobSasPermissions, __version__
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from pathlib import Path
import polars as pl
import os
import io

load_dotenv(Path(__file__).parent.parent.parent / ".env")

class BlobCon:
    def __init__(self):
        self.account_url = os.getenv("AZURE_BLOB_URL")
        self.account_key = os.getenv("AZURE_BLOB_KEY")
        self.account_name = os.getenv("AZURE_ACCOUNT_NAME")
        self.container_name= os.getenv("AZURE_CONTAINER_NAME")
        self.blob_service_client = BlobServiceClient(account_url=self.account_url, credential=self.account_key)

    def save_data_to_blob(self, file_path, data):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_path)
            print("\nUploading to Azure Storage as blob:\n\t" + file_path)

            # Upload the created file
            blob_client.upload_blob(data, overwrite=True)
            
        except Exception as ex:
            print('Exception writing to azure blob:')
            print(ex)

    def read_data_from_blob(self,file_path, as_text=True):
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_path)
        blob_data = blob_client.download_blob().readall()
        if as_text:
            return blob_data.decode('utf-8')
        return blob_data # gibt Bytes zurück

    def list_blob(self, blob_path):
        container_client = self.blob_service_client.get_container_client(self.container_name)
        return container_client.list_blobs(blob_path)
    
    def list_blob_filename(self, blob_path):
        file_list =[blob.get('name').split("/")[-1] for blob in self.list_blob(blob_path)] 
        return file_list

    def generate_sas_token(self):
        sas_token = generate_container_sas(
            account_name=self.account_name,
            container_name=self.container_name,
            #blob_name=None,
            account_key= self.account_key,
            permission=ContainerSasPermissions(read=True, list=True, delete=True),
            expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        )
        return sas_token
    
    def safe_as_parquet(self, df, save_path):
        buffer = io.BytesIO()
        df.write_parquet(buffer)
        buffer.seek(0)  # wichtig: zurück zum Anfang des Streams 
        self.save_data_to_blob(save_path ,buffer)
        