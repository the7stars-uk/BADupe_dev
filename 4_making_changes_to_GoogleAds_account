import os
import google.ads.googleads.client
from google.cloud import bigquery
from google.cloud import secretmanager
from google.protobuf import json_format

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
    # Query to select only rows that haven't been processed today
    # You can customize this logic to prevent re-processing.
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
    Triggered by Cloud Scheduler. Updates keyword statuses in Google Ads.
    """
    print("Pipeline started: Fetching credentials from Secret Manager.")

    # 1. Fetch Credentials from Secret Manager
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
    keywords_to_update = list(get_keywords_from_bigquery()) # Convert to list to get length

    if not keywords_to_update:
        print("No keywords to update. Exiting.")
        return

    # 4. Build and Execute Mutation Operations in Google Ads
    ad_group_criterion_service = google_ads_client.get_service("AdGroupCriterionService")
    mutate_operations = []

    for row in keywords_to_update:
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

    print(f"Sending {len(mutate_operations)} updates to Google Ads API.")
    
    # 5. Send the request with Partial Failure enabled and handle the response
    try:
        mutate_response = ad_group_criterion_service.mutate_ad_group_criteria(
            customer_id=customer_id,
            mutate_operations=mutate_operations,
            # --- KEY CHANGE: This allows the batch to succeed even if some operations fail ---
            partial_failure=True,
        )

        # --- NEW: Check for and log partial failure errors ---
        if mutate_response.partial_failure_error:
            # The 'partial_failure_error' field is a GoogleAdsFailure object.
            # It contains a list of errors, where each error provides details
            # about a failed operation.
            failure_details = json_format.MessageToDict(mutate_response.partial_failure_error._pb)
            print("Partial failure occurred. The following operations failed:")
            for error in failure_details.get("errors", []):
                error_index = error.get("location", {}).get("fieldPathElements", [{}])[0].get("index", "N/A")
                error_message = error.get("message", "Unknown error")
                print(f"\t- Operation at index {error_index}: '{error_message}'")

        # Log successful mutations
        successful_count = 0
        for result in mutate_response.results:
            # A successful result will have a resource_name. An empty result
            # corresponds to a failed operation.
            if result.resource_name:
                successful_count += 1
        
        print(f"Successfully processed {successful_count} out of {len(mutate_operations)} keywords.")

    except google.ads.googleads.errors.GoogleAdsException as ex:
        # This block will now only catch errors that cause the ENTIRE request to fail,
        # such as authentication issues, not individual operation failures.
        print(f'Request failed with status "{ex.error.code().name}" and includes the following errors:')
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')

    print("Pipeline finished.")