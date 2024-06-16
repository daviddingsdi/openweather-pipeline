import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def main(mytimer: func.TimerRequest) -> None:
    # Create a credential object using DefaultAzureCredential
    credential = DefaultAzureCredential()

    # Create a secret client using the credential and the key vault url
    key_vault_url = "https://databaesgeneric.vault.azure.net/"
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

    # Retrieve the API key and connection string from Azure Key Vault
    api_key = secret_client.get_secret("openweathermap-api").value

    # Define the URL of the public weather API
    cityname = "Sydney,au"
    api_url = f"http://api.openweathermap.org/data/2.5/weather?q={cityname}&appid={api_key}"

    # Send a GET request to the API
    response = requests.get(api_url)

    # Parse the response to JSON
    data = response.json()

    # Create a connection to Azure Blob Storage
    blob_key = secret_client.get_secret("databaseblobkey1").value
    blob_conn = f"DefaultEndpointsProtocol=https;AccountName=databaesblobstorage;AccountKey={blob_key}/xB+AStQrU3wQ==;EndpointSuffix=core.windows.net"

    blob_service_client = BlobServiceClient.from_connection_string(blob_conn)

    # Create a blob client for the specific blob in the container
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    blob_name = f"{cityname}_{current_datetime}.csv"
    blob_client = blob_service_client.get_blob_client("openweathermap", blob_name)

    # Upload the parsed data to the blob

    df = pd.json_normalize(data)
    df["datetime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    csv_data = df.to_csv(index=False)
    blob_client.upload_blob(csv_data, overwrite=True)

    # Retrieve the SQL Server connection string from Azure Key Vault
    azure_sql_username = "sdiadmin"
    azure_sql_key = secret_client.get_secret("azuresql-sdippl-pw").value
    sql_cs = f"mssql+pyodbc://sdiadmin:{azure_sql_key}@sdippl.database.windows.net:1433/demo_db?driver=ODBC+Driver+18+for+SQL+Server"
    # Create a SQLAlchemy engine
    engine = create_engine(sql_cs)

    # Write the DataFrame to the SQL database
    df.to_sql("fact_sydweather", engine, if_exists='append', index=False)