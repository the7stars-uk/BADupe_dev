import os
import datetime
import json
import google.auth
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud import secretmanager
from google.api_core import exceptions
import pandas as pd

# --- Configuration ---
# Note: GCP_PROJECT_ID is no longer hardcoded here. 
# It will be retrieved dynamically.

# IDs of your individual secrets in Google Secret Manager
GSC_SERVICE_ACCOUNT_KEY_SECRET_ID = 'gsc-service-account-key'
GSC_PROPERTY_SECRET_ID = 'gsc-property'
BQ_PROJECT_ID_SECRET_ID = 'bq-project-id'
BQ_DATASET_ID_SECRET_ID = 'bq-dataset-id'
BQ_ORGANIC_TABLE_ID_SECRET_ID = 'bq-table-id'
SECRET_VERSION = 'latest'
GSC_SCOPES = ['https://www.googleapis.com/auth/webmasters.readonly']

def get_current_project_id():
    """Attempts to retrieve the GCP Project ID from the environment."""
    print("Attempting to determine GCP Project ID from environment...")
    try:
        # google.auth.default() returns (credentials, project_id)
        _, project_id = google.auth.default()
        if project_id:
            print(f"Successfully determined GCP Project ID: {project_id}")
            return project_id
        else:
            print("Error: Could not determine GCP Project ID from environment configuration.")
            print("If running locally, ensure you have run 'gcloud auth application-default login' and set a default project.")
            return None
    except Exception as e:
        print(f"Error determining GCP Project ID: {e}")
        return None

def get_secret(client, project_id, secret_id, version_id):
    """Helper function to access a single secret's payload."""
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    try:
        response = client.access_secret_version(name=name)
        return response.payload.data.decode('UTF-8')
    except exceptions.NotFound:
        print(f"Error: Secret '{secret_id}' not found in project '{project_id}'.")
        return None
    except Exception as e:
        print(f"Error accessing secret '{secret_id}': {e}")
        return None

def get_all_configs(project_id):
    """Fetches all configuration values from individual secrets."""
    # Initialize the Secret Manager client using the detected project context
    try:
        client = secretmanager.SecretManagerServiceClient()
    except Exception as e:
         print(f"Error initializing Secret Manager client: {e}")
         return None

    config = {}
    print(f"Fetching secrets from project: {project_id}...")

    config['GSC_PROPERTY'] = get_secret(client, project_id, GSC_PROPERTY_SECRET_ID, SECRET_VERSION)
    config['BIGQUERY_PROJECT_ID'] = get_secret(client, project_id, BQ_PROJECT_ID_SECRET_ID, SECRET_VERSION)
    config['BIGQUERY_DATASET_ID'] = get_secret(client, project_id, BQ_DATASET_ID_SECRET_ID, SECRET_VERSION)
    config['BIGQUERY_TABLE_ID'] = get_secret(client, project_id, BQ_ORGANIC_TABLE_ID_SECRET_ID, SECRET_VERSION)
    config['GSC_SA_KEY_JSON_STR'] = get_secret(client, project_id, GSC_SERVICE_ACCOUNT_KEY_SECRET_ID, SECRET_VERSION)

    # Filter out None values to check if all succeeded
    successful_configs = {k: v for k, v in config.items() if v is not None}

    if len(successful_configs) != len(config):
        print("One or more secrets could not be retrieved. Please check logs above.")
        return None

    return config

def authenticate_gsc_with_service_account(config):
    """Authenticates with GSC API using a service account key from Secret Manager."""
    try:
        key_info = json.loads(config['GSC_SA_KEY_JSON_STR'])
        credentials = service_account.Credentials.from_service_account_info(
            key_info, scopes=GSC_SCOPES)
        print("Successfully authenticated with service account for GSC.")
        return build('searchconsole', 'v1', credentials=credentials)
    except json.JSONDecodeError:
        print("Error: The GSC service account secret is not valid JSON.")
        return None
    except Exception as e:
        print(f"Failed to authenticate with service account. Error: {e}")
        return None

def get_gsc_data(service, gsc_property):
    """Fetches organic clicks and impressions from Google Search Console."""
    # Fetch data for the last available full day (usually 2-3 days ago)
    # Adjusting explicitly to get the last 3 days to ensure data availability
    end_date = datetime.date.today() - datetime.timedelta(days=2)
    start_date = end_date - datetime.timedelta(days=2) # Get 3 days of data
    
    print(f"Fetching GSC data from {start_date} to {end_date}...")

    request = {
        'startDate': start_date.strftime('%Y-%m-%d'),
        'endDate': end_date.strftime('%Y-%m-%d'),
        'dimensions': ['date'],
        'type': 'web'
    }

    try:
        response = service.searchanalytics().query(
            siteUrl=gsc_property, body=request).execute()
        rows = response.get('rows', [])
        if not rows:
            print("Query executed successfully but returned no data for this date range.")
        return rows
    except Exception as e:
        print(f"An error occurred while fetching GSC data: {e}")
        return None

def load_data_to_bigquery(data, config):
    """Loads data into a BigQuery table using WRITE_TRUNCATE."""
    if not data:
        print("No data to load to BigQuery.")
        return

    try:
        bq_project_id = config.get('BIGQUERY_PROJECT_ID')
        # The BQ client will automatically pick up environment credentials
        client = bigquery.Client(project=bq_project_id)
        print(f"Successfully initialized BigQuery client for project {bq_project_id}.")
    except Exception as e:
        print(f"Failed to initialize BigQuery client. Error: {e}")
        return

    dataset_id = config.get('BIGQUERY_DATASET_ID')
    table_id = config.get('BIGQUERY_TABLE_ID')
    table_ref = client.dataset(dataset_id).table(table_id)

    # --- Step 1: Define Schema ---
    schema = [
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
    ]

    # --- Step 2: Prepare DataFrame ---
    df = pd.DataFrame(data)
    # GSC returns keys/clicks/impressions, 'keys' is the date dimension
    df.rename(columns={'keys': 'date'}, inplace=True) 
    
    # Extract the date string from the list GSC returns: ['YYYY-MM-DD'] -> 'YYYY-MM-DD'
    df['date'] = df['date'].apply(lambda x: x[0] if isinstance(x, list) and x else x)
    
    # Ensure correct types
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['clicks'] = df['clicks'].astype(int)
    df['impressions'] = df['impressions'].astype(int)
    
    # Select only necessary columns in correct order
    df = df[['date', 'clicks', 'impressions']]
    
    print(f"Prepared DataFrame with {len(df)} rows.")

    # --- Step 3: Configure and Run Load Job (TRUNCATE) ---
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE", # Overwrite existing table data
        create_disposition="CREATE_IF_NEEDED" # Create table if it doesn't exist
    )

    print(f"Starting load job to {dataset_id}.{table_id} (WRITE_TRUNCATE)...")
    try:
        job = client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        )
        job.result() # Wait for completion
        
        table = client.get_table(table_ref)
        print(f"Success. Table {dataset_id}.{table_id} now contains {table.num_rows} rows.")
        
    except Exception as e:
        print(f"An error occurred while loading data to BigQuery: {e}")
        if hasattr(e, 'errors'):
            print("Detailed errors:")
            for error in e.errors:
                print(error)

if __name__ == '__main__':
    print("--- Starting GSC to BigQuery Script ---")
    
    # 1. Determine the Project ID dynamically
    current_project_id = get_current_project_id()

    if current_project_id:
        # 2. Fetch configurations using the detected project ID
        config = get_all_configs(current_project_id)

        if config:
            print("Configuration successfully retrieved.")
            
            # 3. Authenticate with GSC
            gsc_service = authenticate_gsc_with_service_account(config)
            
            if gsc_service:
                # 4. Fetch Data
                gsc_property = config.get('GSC_PROPERTY')
                performance_data = get_gsc_data(gsc_service, gsc_property)
                
                # 5. Load to BigQuery (if data exists)
                if performance_data:
                    print(f"Successfully fetched {len(performance_data)} rows from GSC.")
                    load_data_to_bigquery(performance_data, config)
                else:
                    # Handle cases where GSC returns no data (e.g., data not ready yet)
                    # We still run load_data to truncate the table to empty if that's desired,
                    # or we can skip. Here we skip loading empty data.
                    print("No data returned from GSC. Skipping BigQuery load.")
        else:
            print("Failed to retrieve all required configurations.")
    else:
        print("Exiting because the GCP Project ID could not be determined.")

    print("--- Script finished ---")