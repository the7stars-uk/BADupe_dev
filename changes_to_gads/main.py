import os
import sys
import logging
import pandas as pd
from flask import Flask
from google.cloud import bigquery
from google.cloud import secretmanager
import google.cloud.logging # Import the Cloud Logging client library
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# --- Cloud Logging Setup ---
# This helper connects your logs to Cloud Logging.
# It's best practice to run this once when the service starts.
try:
    client = google.cloud.logging.Client()
    # Attaches the Cloud Logging handler to the root Python logger
    client.setup_logging(log_level=logging.INFO)
    logging.info("Cloud Logging handler successfully attached.")
except Exception as e:
    # If for some reason the client fails to initialize, fall back to basic logging.
    logging.basicConfig(level=logging.INFO)
    logging.critical(f"Could not attach Google Cloud Logging handler: {e}", exc_info=True)


app = Flask(__name__)

# --- Configuration ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT")
DRY_RUN = os.environ.get("DRY_RUN", "True").lower() == "true"

def access_secret_version(secret_id, version_id="latest"):
    """
    Accesses a secret version from Google Cloud Secret Manager.
    Logs errors if a secret cannot be accessed.
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception:
        # Log the error with specific details about which secret failed
        logging.critical(
            "Failed to access secret from Secret Manager.",
            exc_info=True,
            extra={'json_fields': {'secret_id': secret_id}}
        )
        # Re-raise the exception to halt initialization, as secrets are critical.
        raise

# --- Global Initialization ---
# Use a global try/except to catch any critical startup failures.
try:
    logging.info("Starting service initialization...")
    
    ads_config = {
        "developer_token": access_secret_version("google-ads-developer-token"),
        "client_id": access_secret_version("google-ads-client-id"),
        "client_secret": access_secret_version("google-ads-client-secret"),
        "refresh_token": access_secret_version("google-ads-refresh-token"),
        "login_customer_id": access_secret_version("google-ads-customer-id"),
        "use_proto_plus": True
    }
    logging.info("Google Ads configuration fetched successfully from Secret Manager.")

    bq_project_id = access_secret_version("bigquery_project_id")
    bq_dataset_id = access_secret_version("bigquery_dataset_id")
    bq_table_id = access_secret_version("bigquery_table_id")
    logging.info("BigQuery configuration fetched successfully from Secret Manager.")

    googleads_client = GoogleAdsClient.load_from_dict(ads_config)
    logging.info("Google Ads client initialized successfully.")

except Exception:
    # The specific error is already logged in access_secret_version.
    # We log a general critical failure here and set globals to None.
    logging.critical("FATAL: A critical error occurred during initialization. The service cannot operate.")
    googleads_client = None
    bq_project_id = None
# --- End Global Initialization ---


def get_keyword_statuses_from_bigquery():
    """
    Fetches keyword data from the specified BigQuery table with structured logging.
    """
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    try:
        client = bigquery.Client(project=bq_project_id)
        query = f"SELECT customer_id, adgroup_id, criterion_id, status FROM `{full_table_id}`"
        
        logging.info(
            "Fetching keyword statuses from BigQuery.",
            extra={'json_fields': {'table_id': full_table_id}}
        )
        df = client.query(query).to_dataframe()
        
        logging.info(
            f"Found {len(df)} keywords to process from BigQuery.",
            extra={'json_fields': {'record_count': len(df), 'table_id': full_table_id}}
        )
        return df
    except Exception:
        logging.error(
            "An error occurred while querying BigQuery.",
            exc_info=True, # Includes stack trace
            extra={'json_fields': {'table_id': full_table_id}}
        )
        return None


def update_keyword_statuses_in_google_ads(customer_id, keywords_df):
    """
    Updates keyword statuses using the Google Ads API with structured logging.
    """
    ad_group_criterion_service = googleads_client.get_service("AdGroupCriterionService")
    operations = []
    
    for _, row in keywords_df.iterrows():
        try:
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
                logging.warning(
                    "Skipping keyword with unknown status.",
                    extra={'json_fields': {
                        'customer_id': customer_id,
                        'criterion_id': row['criterion_id'],
                        'unknown_status': row['status']
                    }}
                )
                continue
            
            operations.append(operation)
        except Exception:
            logging.error(
                "Failed to build an operation for a keyword.",
                exc_info=True,
                extra={'json_fields': {'customer_id': customer_id, 'row_data': row.to_dict()}}
            )

    if not operations:
        logging.warning(
            "No valid operations to perform for customer.",
            extra={'json_fields': {'customer_id': customer_id}}
        )
        return

    log_context = {'customer_id': customer_id, 'operation_count': len(operations)}

    if DRY_RUN:
        logging.info(
            "*** DRY RUN: Prepared keyword updates. No changes will be made. ***",
            extra={'json_fields': log_context}
        )
        # Optional: Log an example operation for debugging
        logging.debug("Example operation:", extra={'json_fields': {'example_op': operations[0]}})
    else:
        try:
            logging.info(
                "--- LIVE MODE: Sending keyword updates to Google Ads API... ---",
                extra={'json_fields': log_context}
            )
            response = ad_group_criterion_service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=operations
            )
            logging.info(
                "Successfully updated keywords in Google Ads.",
                extra={'json_fields': {**log_context, 'results_count': len(response.results)}}
            )

        except GoogleAdsException as ex:
            # This logs the specific, structured errors from the Google Ads API
            logging.error(
                "GoogleAdsException occurred during keyword update.",
                extra={'json_fields': {
                    **log_context,
                    'google_ads_errors': [e.message for e in ex.failure.errors],
                    'request_id': ex.request_id
                }}
            )
        except Exception:
            logging.error(
                "An unexpected error occurred during Google Ads mutation.",
                exc_info=True,
                extra={'json_fields': log_context}
            )

@app.route("/", methods=["POST"])
def main_handler():
    """Main function triggered by an HTTP POST request."""
    logging.info(
        f"Processing request started. DRY_RUN is set to {DRY_RUN}.",
        extra={'json_fields': {'dry_run_mode': DRY_RUN}}
    )

    if not googleads_client or not bq_project_id:
        error_msg = "FATAL: Service is not configured. Check startup logs for initialization errors."
        logging.critical(error_msg)
        return error_msg, 500

    keywords_df = get_keyword_statuses_from_bigquery()

    if keywords_df is None:
        msg = "Processing halted due to an error while fetching from BigQuery."
        # The error is already logged in the function, so we just log the outcome here.
        logging.error(msg)
        # Return 500 because the process failed.
        return msg, 500
    
    if keywords_df.empty:
        msg = "No keyword data found in BigQuery to process."
        logging.info(msg)
        return msg, 200

    # Group by customer_id and process each group
    for customer_id, group_df in keywords_df.groupby('customer_id'):
        customer_id_str = str(customer_id).replace("-", "")
        update_keyword_statuses_in_google_ads(customer_id_str, group_df)

    logging.info("Processing complete.")
    return "Processing complete.", 200

# The following is used for local development.
# When deploying to Cloud Run, a Gunicorn server is used to run the 'app' object.
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))