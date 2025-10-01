import os
import sys
import logging
import pandas as pd
import datetime
import uuid # <-- Make sure this import is here
from flask import Flask, current_app
from google.cloud import bigquery
from google.cloud import secretmanager
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import atexit
import signal
import google.auth

# --- Cloud Logging Setup ---
cloud_handler = None  # Initialize to None

try:
    log_client = google.cloud.logging.Client()
    cloud_handler = CloudLoggingHandler(log_client, name="service-b")
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(cloud_handler)
    
    logging.info("Cloud Logging handler successfully attached manually.")
except Exception as e:
    logging.basicConfig(level=logging.INFO)
    logging.critical(f"Could not attach Google Cloud Logging handler: {e}", exc_info=True)

def graceful_shutdown():
    """Flushes the logging handler if it was successfully created."""
    if cloud_handler:
        print("Application is shutting down. Closing the Cloud Logging handler...")
        cloud_handler.close()
        print("Logs flushed and handler closed.")

def sigterm_handler(_signum, _frame):
    """Handler for the SIGTERM signal."""
    logging.warning("SIGTERM received, initiating graceful shutdown.")
    # The atexit hook will handle the rest, but you can add more logic here if needed.
    exit(0)

def access_secret_version(secret_id, project_id, version_id="latest"):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception:
        logging.critical(f"Failed to access secret: {secret_id}.", exc_info=True)
        raise

def get_keyword_statuses_from_bigquery(bq_client, bq_project_id, bq_dataset_id, bq_table_id, invocation_id):
    """Fetches keyword data, now including invocation_id in logs."""
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    query = f"SELECT customer_id, adgroup_id, criterion_id, status, keyword, change_reason FROM `{full_table_id}`"
    try:
        logging.info("Fetching keywords from BigQuery.", extra={'json_fields': {'table_id': full_table_id, 'invocation_id': invocation_id}})
        df = bq_client.query(query).to_dataframe()
        logging.info(f"Found {len(df)} keywords to process.", extra={'json_fields': {'record_count': len(df), 'invocation_id': invocation_id}})
        return df
    except Exception:
        # --- FIX: ADD INVOCATION ID TO ERROR LOGS ---
        logging.error("An error occurred while querying BigQuery.", exc_info=True, extra={'json_fields': {'table_id': full_table_id, 'invocation_id': invocation_id}})
        return None

def update_keyword_statuses_in_google_ads(customer_id, keywords_df, invocation_id):
    """This function already correctly uses the invocation_id to build its history logs."""
    googleads_client = current_app.googleads_client
    bq_client = current_app.bq_client
    DRY_RUN = current_app.config["DRY_RUN"]
    
    prepared_operations = []
    history_rows_to_log = []
    ad_group_criterion_service = googleads_client.get_service("AdGroupCriterionService")
    
    for _, row in keywords_df.iterrows():
        # This part is already correct from your original script. It tags every
        # history record with the invocation_id it received.
        history_log = {
            "invocation_id": invocation_id, "log_timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "customer_id": customer_id, "adgroup_id": row['adgroup_id'], "criterion_id": row['criterion_id'],
            "keyword_text": row['keyword'], "previous_status": row['status'], "new_status": row['status'].upper(),
            "action": "STATUS_UPDATE", "outcome": None, "change_reason": row.get('change_reason', 'N/A'), "details": None
        }
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
            continue
        prepared_operations.append(operation)
        history_rows_to_log.append(history_log)

    if not prepared_operations:
        logging.warning("No valid operations to perform for customer.", extra={'json_fields': {'customer_id': customer_id, 'invocation_id': invocation_id}})
        return

    if DRY_RUN:
        logging.info(f"*** DRY RUN: Prepared {len(prepared_operations)} updates. ***", extra={'json_fields': {'customer_id': customer_id, 'invocation_id': invocation_id}})
        for log_row in history_rows_to_log:
            log_row["outcome"] = "INFO"
            log_row["details"] = "Dry run, no changes made."

    # Optional: Log an example operation for debugging
    #logging.debug("Example operation:", extra={'json_fields': {'example_op': operations[0]}})
    else:
        try:
            logging.info(f"--- LIVE MODE: Updating {len(prepared_operations)} keywords... ---", extra={'json_fields': {'customer_id': customer_id, 'invocation_id': invocation_id}})
            ad_group_criterion_service.mutate_ad_group_criteria(customer_id=customer_id, operations=prepared_operations)
            for log_row in history_rows_to_log:
                log_row["outcome"] = "SUCCESS"
                log_row["details"] = "Keyword status updated successfully."
        except GoogleAdsException as ex:
            # --- FIX: ADD INVOCATION ID TO ERROR LOGS ---
            logging.error("GoogleAdsException occurred.", extra={'json_fields': {'customer_id': customer_id, 'invocation_id': invocation_id}})
            error_details = ", ".join([e.message for e in ex.failure.errors])
            for log_row in history_rows_to_log:
                log_row["outcome"] = "FAILURE"
                log_row["details"] = f"GoogleAdsException: {error_details} | Request ID: {ex.request_id}"

    log_change_to_bigquery(bq_client, history_rows_to_log)

def log_change_to_bigquery(bq_client, rows_to_insert):
    """Placeholder for logging history to BigQuery."""
    if rows_to_insert:
        # We don't need to add the ID here, because it's already in the rows.
        logging.info(f"Placeholder: Would log {len(rows_to_insert)} history rows to BQ.")

def create_app():
    """Application Factory Function"""
    app = Flask(__name__)

    # --- Configuration and Initialization now happens inside the factory ---
    try:
        logging.info("Starting service initialization...")
        try:
            _, GCP_PROJECT_ID = google.auth.default()
            logging.info(f"Successfully determined GCP Project ID: {GCP_PROJECT_ID}")
        except google.auth.exceptions.DefaultCredentialsError:
            logging.critical("Could not automatically determine GCP Project ID. "
                             "Please ensure the application is running in a GCP environment "
                             "or that the GOOGLE_APPLICATION_CREDENTIALS environment variable is set.")
            raise
        app.config["DRY_RUN"] = os.environ.get("DRY_RUN", "True").lower() == "true"
        
        ads_config = {
            "developer_token": access_secret_version("google-ads-developer-token", GCP_PROJECT_ID),
            "client_id": access_secret_version("google-ads-client-id", GCP_PROJECT_ID),
            "client_secret": access_secret_version("google-ads-client-secret", GCP_PROJECT_ID),
            "refresh_token": access_secret_version("google-ads-refresh-token", GCP_PROJECT_ID),
            "login_customer_id": access_secret_version("google-ads-customer-id", GCP_PROJECT_ID),
            "use_proto_plus": True
        }
        app.config["BQ_PROJECT_ID"] = access_secret_version("bigquery_project_id", GCP_PROJECT_ID)
        app.config["BQ_DATASET_ID"] = access_secret_version("bigquery_dataset_id", GCP_PROJECT_ID)
        app.config["BQ_TABLE_ID"] = access_secret_version("bigquery_table_id", GCP_PROJECT_ID)
        
        # Attach clients to the app object
        app.googleads_client = GoogleAdsClient.load_from_dict(ads_config)
        app.bq_client = bigquery.Client(project=app.config["BQ_PROJECT_ID"])
        app.config['SERVICE_INITIALIZED'] = True
        logging.info("Service initialized successfully.")
    except Exception as e:
        logging.critical(f"FATAL: A critical error occurred during initialization: {e}", exc_info=True)
        raise
        
    @app.route("/", methods=["POST"])
    def main_handler():
        """Main function that orchestrates the process."""
        invocation_id = str(uuid.uuid4())
        
        logging.info(
            f"Processing request started. DRY_RUN is set to {current_app.config['DRY_RUN']}.",
            extra={'json_fields': {'invocation_id': invocation_id}}
        )

        if not current_app.config.get('SERVICE_INITIALIZED'):
            error_msg = "FATAL: Service is not configured. Check startup logs for initialization errors."
            logging.critical(error_msg)
            return error_msg, 500

        # 2. The ID is PASSED DOWN to the next function.
        keywords_df = get_keyword_statuses_from_bigquery(
            current_app.bq_client, 
            current_app.config["BQ_PROJECT_ID"],
            current_app.config["BQ_DATASET_ID"],
            current_app.config["BQ_TABLE_ID"],
            invocation_id
        )

        if keywords_df is None:
            logging.error("Processing halted due to BQ error.", extra={'json_fields': {'invocation_id': invocation_id}})
            return "Processing halted due to an error while fetching from BigQuery.", 500
        if keywords_df.empty:
            logging.info("No keyword data found to process.", extra={'json_fields': {'invocation_id': invocation_id}})
            return "No keyword data found in BigQuery to process.", 200

        for customer_id, group_df in keywords_df.groupby('customer_id'):
            customer_id_str = str(customer_id).replace("-", "")
            update_keyword_statuses_in_google_ads(customer_id_str, group_df, invocation_id)

        logging.info("Processing complete.", extra={'json_fields': {'invocation_id': invocation_id}})
        return "Processing complete.", 200
        
    return app

app = create_app()
    
    # This part is now for local execution only
if __name__ == "__main__":
    atexit.register(graceful_shutdown)
    signal.signal(signal.SIGTERM, sigterm_handler)
    
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))