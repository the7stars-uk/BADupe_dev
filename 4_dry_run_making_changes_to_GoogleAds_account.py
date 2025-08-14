import os
import google.ads.googleads.client
from google.cloud import bigquery
from google.cloud import secretmanager
# Note: json_format is no longer needed in this version as we don't process a response.

# --- Configuration ---
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
# IMPORTANT: Update this with your project, dataset, and table ID.
BIGQUERY_TABLE_ID = "your-gcp-project-id.google_ads_automation.keyword_updates" 

def access_secret(secret_id, project_id, version_id="latest"):
    """Fetches a secret from Google Cloud Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_keywords_from_bigquery():
    """Queries BigQuery to get the list of keywords to update."""
    print(f"Querying BigQuery table: {BIGQUERY_TABLE_ID}")
    bq_client = bigquery.Client()
    query = f"""
        SELECT
            ad_group_id,
            criterion_id,
            new_status
        FROM
            `{BIGQUERY_TABLE_ID}`
        WHERE DATE(processed_at) IS NULL OR DATE(processed_at) != CURRENT_DATE()
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    print(f"Found {results.total_rows} keywords to update.")
    return results

def update_google_ads_keywords(event, context):
    """
    Cloud Function entry point.
    [DRY RUN MODE] This version simulates changes but does not modify Google Ads.
    """
    print("Pipeline started: Fetching credentials from Secret Manager.")

    # Steps 1, 2, 3 and 4 are identical, as we still want to test this logic.
    # 1. Fetch Credentials
    try:
        developer_token = access_secret("google-ads-developer-token", GCP_PROJECT_ID)
        client_id = access_secret("google-ads-client-id", GCP_PROJECT_ID)
        client_secret = access_secret("google-ads-client-secret", GCP_PROJECT_ID)
        refresh_token = access_secret("google-ads-refresh-token", GCP_PROJECT_ID)
        customer_id = access_secret("google-ads-customer-id", GCP_PROJECT_ID)
    except Exception as e:
        print(f"FATAL: Could not fetch secrets. Error: {e}")
        return

    # 2. Initialize Google Ads API Client
    credentials = {
        "developer_token": developer_token,
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
        "use_proto_plus": True
    }
    
    try:
        google_ads_client = google.ads.googleads.client.GoogleAdsClient.load_from_dict(credentials)
        print("Google Ads client initialized successfully.")
    except Exception as e:
        print(f"FATAL: Could not initialize Google Ads client. Error: {e}")
        return

    # 3. Get Keywords from BigQuery
    keywords_to_update = list(get_keywords_from_bigquery())

    if not keywords_to_update:
        print("No keywords to update. Exiting.")
        return

    # 4. Build Mutation Operations
    ad_group_criterion_service = google_ads_client.get_service("AdGroupCriterionService")
    mutate_operations = []

    for row in keywords_to_update:
        # This loop remains the same
        ad_group_id = row.ad_group_id
        criterion_id = row.criterion_id
        new_status = row.new_status.upper()

        if new_status not in ["ENABLED", "PAUSED"]:
            print(f"Skipping invalid status '{new_status}' for criterion {criterion_id}.")
            continue

        operation = google_ads_client.get_type("MutateOperation")
        criterion = operation.ad_group_criterion_operation.update
        
        criterion.resource_name = ad_group_criterion_service.ad_group_criterion_path(
            customer_id, ad_group_id, criterion_id
        )
        criterion.status = google_ads_client.enums.AdGroupCriterionStatusEnum[new_status]
        
        google_ads_client.copy_from(
            operation.ad_group_criterion_operation.update_mask,
            google_ads_client.get_type("FieldMask", all_fields=True),
        )
        mutate_operations.append(operation)

    # --- KEY CHANGE: Replaced API call with a simulation message ---
    
    print("\n--- [DRY RUN MODE ENABLED] ---")
    
    if mutate_operations:
        print(f"[SUCCESS] Prepared {len(mutate_operations)} keyword update operations.")
        print("The script has successfully authenticated and prepared the following changes:")
        # You can add more detail here if you want, e.g., count enables vs. pauses
        enables = sum(1 for op in mutate_operations if op.ad_group_criterion_operation.update.status == google_ads_client.enums.AdGroupCriterionStatusEnum.ENABLED)
        pauses = len(mutate_operations) - enables
        print(f"  - Keywords to ENABLE: {enables}")
        print(f"  - Keywords to PAUSE:  {pauses}")
    else:
        print("[INFO] No valid operations were prepared. Nothing would have been sent.")
    
    print("\n[IMPORTANT] API call was SKIPPED. NO CHANGES WERE MADE to your Google Ads account.")
    print("--- [END OF DRY RUN] ---\n")