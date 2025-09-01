import os
import sys
import pandas as pd
from flask import Flask
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery
from google.cloud import secretmanager

app = Flask(__name__)

# --- Configuration ---
# GCP Project ID is automatically provided by the Cloud Run environment
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT")

# Control DRY_RUN mode with an environment variable for flexibility
DRY_RUN = os.environ.get("DRY_RUN", "True").lower() == "true"

def access_secret_version(secret_id, version_id="latest"):
    """
    Accesses a secret version from Google Cloud Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# --- Global Initialization ---
# Fetch individual secrets and build the configuration dictionary in memory.
try:
    print("Fetching configuration from Secret Manager...")
    
    # Build the Google Ads config dictionary directly
    ads_config = {
        "developer_token": access_secret_version("google-ads-developer-token"),
        "client_id": access_secret_version("google-ads-client-id"),
        "client_secret": access_secret_version("google-ads-client-secret"),
        "refresh_token": access_secret_version("google-ads-refresh-token"),
        "login_customer_id": access_secret_version("google-ads-customer-id"),
        "use_proto_plus": True  # This is a standard setting
    }
    print("Google Ads configuration fetched.")

    # Fetch BigQuery details
    bq_project_id = access_secret_version("bigquery_project_id")
    bq_dataset_id = access_secret_version("bigquery_dataset_id")
    bq_table_id = access_secret_version("bigquery_table_id")
    print("BigQuery configuration fetched.")

    print("Initializing Google Ads client...")
    googleads_client = GoogleAdsClient.load_from_dict(ads_config)
    print("Google Ads client initialized.")

except Exception as e:
    print(f"FATAL: A critical error occurred during initialization: {e}")
    # Set to None to prevent the app from running if initialization fails
    googleads_client = None
    bq_project_id = None
# --- End Global Initialization ---


def get_keyword_statuses_from_bigquery():
    """
    Fetches keyword data from the specified BigQuery table.
    """
    try:
        client = bigquery.Client(project=bq_project_id)
        query = f"""
            SELECT customer_id, adgroup_id, criterion_id, status
            FROM `{bq_project_id}.{bq_dataset_id}.{bq_table_id}`
        """
        print("Fetching keyword statuses from BigQuery...")
        df = client.query(query).to_dataframe()
        print(f"Found {len(df)} keywords to process.")
        return df
    except Exception as e:
        print(f"An error occurred while querying BigQuery: {e}")
        return None


def update_keyword_statuses_in_google_ads(customer_id, keywords_df):
    """
    Updates keyword statuses using the globally configured Google Ads client.
    """
    try:
        ad_group_criterion_service = googleads_client.get_service("AdGroupCriterionService")
        operations = []
        for index, row in keywords_df.iterrows():
            operation = googleads_client.get_type("AdGroupCriterionOperation")
            operation.update_mask.paths.append("status")

            criterion = operation.update
            criterion.resource_name = ad_group_criterion_service.ad_group_criterion_path(
                customer_id, row['adgroup_id'], row['criterion_id']
            )

            status_enum = googleads_client.get_type("AdGroupCriterionStatusEnum").AdGroupCriterionStatus
            new_status_str = row['status'].upper()

            if new_status_str == 'ENABLED':
                criterion.status = status_enum.ENABLED
            elif new_status_str == 'PAUSED':
                criterion.status = status_enum.PAUSED
            else:
                print(f"Skipping keyword {row['criterion_id']} with unknown status: {row['status']}")
                continue

            operations.append(operation)

        if not operations:
            print(f"No valid operations to perform for customer {customer_id}.")
            return

        if DRY_RUN:
            print(f"\n*** DRY RUN: Prepared {len(operations)} updates for customer {customer_id}. ***")
            if operations:
                print("--- Example Operation ---\n", operations[0], "-----------------------")
        else:
            print(f"--- LIVE MODE: Updating {len(operations)} keywords for customer {customer_id}... ---")
            response = ad_group_criterion_service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=operations
            )
            print(f"Successfully updated keywords for customer {customer_id}.")

    except GoogleAdsException as ex:
        print(f"GoogleAdsException for customer {customer_id}: {[e.message for e in ex.failure.errors]}")
    except Exception as e:
        print(f"An unexpected error occurred for customer {customer_id}: {e}")

@app.route("/", methods=["POST"])
def main_handler():
    """Main function triggered by an HTTP POST request."""
    if DRY_RUN:
        print("="*50, "\nSCRIPT IS RUNNING IN DRY RUN MODE.", "\nTo apply changes, set DRY_RUN environment variable to 'False'.", "\n"+"="*50)

    if not googleads_client or not bq_project_id:
        error_msg = "FATAL: Service is not configured. Check logs for initialization errors."
        print(error_msg)
        return error_msg, 500

    keywords_df = get_keyword_statuses_from_bigquery()

    if keywords_df is None or keywords_df.empty:
        msg = "No keyword data found in BigQuery or an error occurred."
        print(msg)
        return msg, 200

    for customer_id, group_df in keywords_df.groupby('customer_id'):
        customer_id_str = str(customer_id).replace("-", "")
        update_keyword_statuses_in_google_ads(customer_id_str, group_df)

    return "Processing complete.", 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))