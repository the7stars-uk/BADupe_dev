import os
import logging
from flask import Flask
from google.cloud import bigquery
from google.cloud import secretmanager
import google.cloud.logging  
from google.cloud.logging_v2.handlers import CloudLoggingHandler
import requests
import json
import logic
import atexit
import signal
import google.auth

# ===================================================================
# 1. IMMEDIATE LOGGING SETUP
# Configure logging FIRST, so that any startup errors are captured.
# ===================================================================
cloud_handler = None
try:
    log_client = google.cloud.logging.Client()
    cloud_handler = CloudLoggingHandler(log_client, name="service-a-prod")
    
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(cloud_handler)
    
    # This will be the first message in Cloud Logging
    logging.info("Cloud Logging handler successfully attached.")
except Exception as e:
    # If cloud setup fails, fall back to basic console logging
    logging.basicConfig(level=logging.INFO)
    logging.critical(f"Could not attach Google Cloud Logging handler: {e}", exc_info=True)

# ===================================================================
# 2. DEFINE HELPER FUNCTIONS (Project ID and Secrets)
# ===================================================================
def get_gcp_project_id():
    """Gets the GCP Project ID from the application's credentials."""
    try:
        _, project_id = google.auth.default()
        if project_id:
            # This log will now go to Cloud Logging successfully
            logging.info(f"Successfully discovered GCP Project ID: {project_id}")
            return project_id
    except google.auth.exceptions.DefaultCredentialsError:
        logging.critical("Could not automatically determine GCP project.")
        return None

def access_secret_version(secret_id, version_id="latest"):
    """Accesses a secret version from Google Cloud Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        # This will use the 'project_id' variable defined below
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception:
        logging.critical(
            f"FATAL: Failed to access secret '{secret_id}' from Secret Manager.",
            exc_info=True,
        )
        raise

# ===================================================================
# 3. GLOBAL INITIALIZATION (Discover Project ID and Fetch Secrets)
# Now that logging is running, safely initialize the app's config.
# ===================================================================
project_id = get_gcp_project_id()
bq_project_id = None  # Initialize to None as a safeguard

try:
    if project_id:
        logging.info("Fetching configuration from Secret Manager...")
        bq_project_id = access_secret_version("bigquery_project_id")
        bq_dataset_id = access_secret_version("bigquery_dataset_id")
        bq_table_id = access_secret_version("bigquery_table_id")
        dataforseo_username = access_secret_version("dataforseo_username")
        dataforseo_password = access_secret_version("dataforseo_password")
        logging.info("Configuration fetched successfully.")
    else:
        # This will be a critical log in Cloud Logging if discovery fails
        logging.critical("Halting initialization because GCP Project ID could not be determined.")
except Exception:
    logging.critical("A critical error occurred during initialization. The service cannot operate.")
    # bq_project_id remains None, which will halt processing later

# ===================================================================
# 4. SETUP FLASK APP AND GRACEFUL SHUTDOWN
# ===================================================================
app = Flask(__name__)

def graceful_shutdown():
    """Flushes the logging handler if it was successfully created."""
    if cloud_handler:
        cloud_handler.close()
        print("Logs flushed and handler closed.") # This print is for local debugging

def sigterm_handler(_signum, _frame):
    """Handler for the SIGTERM signal."""
    logging.warning("SIGTERM received, initiating graceful shutdown.")
    exit(0)

atexit.register(graceful_shutdown)
signal.signal(signal.SIGTERM, sigterm_handler)

logging.info("Graceful shutdown hooks have been registered.")

def get_keywords_from_bigquery(client):
    """Fetches keywords from BigQuery with structured logging."""
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    query = f"SELECT keyword, status, domain_url FROM `{full_table_id}`"
    try:
        logging.info(
            "Fetching keywords from BigQuery.",
            extra={'json_fields': {'table_id': full_table_id}}
        )
        query_job = client.query(query)
        keywords = list(query_job)
        logging.info(
            f"Found {len(keywords)} keywords to process from BigQuery.",
            extra={'json_fields': {'record_count': len(keywords), 'table_id': full_table_id}}
        )
        return keywords
    except Exception:
        logging.error(
            "Error fetching data from BigQuery.",
            exc_info=True,
            extra={'json_fields': {'table_id': full_table_id, 'query': query}}
        )
        return None # Return None on failure

def get_serp_data(keyword, device_type="desktop"):
    """Pulls SERP data from the DataForSEO API with structured logging."""
    log_context = {'keyword': keyword, 'device': device_type}
    logging.info(f"Fetching SERP data for '{keyword}' ({device_type})...", extra={'json_fields': log_context})
    post_data = [{"language_code": "en", "location_code": 2826, "keyword": keyword, "device": device_type}]
    try:
        response = requests.post(
            "https://api.dataforseo.com/v3/serp/google/organic/live/advanced",
            auth=(dataforseo_username, dataforseo_password),
            json=post_data
        )
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException:
        logging.error(
            f"Error fetching SERP data for '{keyword}' ({device_type}).",
            exc_info=True, # Provides the full stack trace
            extra={'json_fields': log_context}
        )
        return None

def update_keyword_data(client, keyword, new_status, competitor_domains):
    """Updates the status and competitor domains of a keyword in the BigQuery table."""
    log_context = {'keyword': keyword, 'new_status': new_status, 'competitors': competitor_domains}
    full_table_id = f"{bq_project_id}.{bq_dataset_id}.{bq_table_id}"
    query = f"""
        UPDATE `{full_table_id}`
        SET status = @new_status, competitor_domains = @domains
        WHERE keyword = @keyword
    """
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("new_status", "STRING", new_status),
                bigquery.ArrayQueryParameter("domains", "STRING", competitor_domains),
                bigquery.ScalarQueryParameter("keyword", "STRING", keyword),
            ]
        )
        query_job = client.query(query, job_config=job_config)
        query_job.result() # Wait for the job to complete
        logging.info(
            f"Successfully updated data for '{keyword}'.",
            extra={'json_fields': {**log_context, 'rows_affected': query_job.num_dml_affected_rows}}
        )
    except Exception:
        logging.error(
            f"Error updating data for '{keyword}'.",
            exc_info=True,
            extra={'json_fields': {**log_context, 'table_id': full_table_id, 'query': query}}
        )

def extract_competitor_domains(serp_data, my_domain):
    """
    Extracts the domains of paid ads and organic results ranking above my_domain.
    (No changes in this function)
    """
    competitor_domains = set()
    my_domain_rank = float('inf')

    if not serp_data or 'tasks' not in serp_data or not serp_data['tasks'] or 'result' not in serp_data['tasks'][0] or not serp_data['tasks'][0]['result'] or 'items' not in serp_data['tasks'][0]['result'][0]:
        logging.warning("SERP data is missing expected structure ('tasks'[0]['result'][0]['items']).")
        return []

    items = serp_data['tasks'][0]['result'][0]['items']

    for item in items:
        if item.get('type') == 'organic' and item.get('domain') == my_domain:
            my_domain_rank = item.get('rank_absolute', float('inf'))
            break

    for item in items:
        item_type = item.get('type')
        domain = item.get('domain')

        if not domain or domain == my_domain:
            continue

        if item_type == 'paid':
            competitor_domains.add(domain)

        if item_type == 'organic' and item.get('rank_absolute', float('inf')) < my_domain_rank:
            competitor_domains.add(domain)

    return list(competitor_domains)

@app.route("/", methods=["POST"])
def main():
    """Main function to run the keyword status update process."""
    logging.info("Processing request started.")
    if not bq_project_id:
        error_msg = "FATAL: Service is not configured. Check startup logs for initialization errors."
        logging.critical(error_msg)
        return error_msg, 500
        
    try:
        bigquery_client = bigquery.Client(project=bq_project_id)
    except Exception:
        logging.critical("Failed to initialize BigQuery client.", exc_info=True)
        return "Error initializing BigQuery client", 500

    keywords_data = get_keywords_from_bigquery(bigquery_client)

    if keywords_data is None: # Check for failure from the function
        return "Processing halted due to an error while fetching from BigQuery.", 500
    
    if not keywords_data: # Check for empty (but successful) result
        logging.info("No keywords found in the BigQuery table to process.")
        return "No keywords found", 200

    for row in keywords_data:
        keyword = row['keyword']
        current_status = row['status']
        domain_url = row['domain_url']
        # This context will be added to all logs for this specific keyword's processing
        keyword_context = {'keyword': keyword, 'domain_url': domain_url, 'current_status': current_status}
        
        logging.info(f"Processing keyword: '{keyword}'", extra={'json_fields': keyword_context})

        desktop_serp_data = get_serp_data(keyword, device_type="desktop")
        mobile_serp_data = get_serp_data(keyword, device_type="mobile")

        desktop_has_competitor_ad = logic.check_for_competitor_ads(desktop_serp_data, domain_url)
        mobile_has_competitor_ad = logic.check_for_competitor_ads(mobile_serp_data, domain_url)
        desktop_is_ranked_one = logic.is_domain_ranked_number_one(desktop_serp_data, domain_url)
        mobile_is_ranked_one = logic.is_domain_ranked_number_one(mobile_serp_data, domain_url)

        # Log the analysis results for traceability
        analysis_results = {
            'desktop_competitor_ad': desktop_has_competitor_ad,
            'mobile_competitor_ad': mobile_has_competitor_ad,
            'desktop_ranked_one': desktop_is_ranked_one,
            'mobile_ranked_one': mobile_is_ranked_one
        }
        logging.info("SERP analysis complete.", extra={'json_fields': {**keyword_context, **analysis_results}})

        # Determine the new status based on the logic
        new_status = current_status
        if desktop_has_competitor_ad or mobile_has_competitor_ad:
            new_status = 'ENABLED'
        elif desktop_is_ranked_one and mobile_is_ranked_one:
            new_status = 'PAUSED'
        
        # Log the final decision
        logging.info(
            f"Final decision for '{keyword}': Set status to '{new_status}'.",
            extra={'json_fields': {**keyword_context, 'new_status': new_status}}
        )

        if new_status:
            # 1. Extract competitors from both desktop and mobile SERP data
            desktop_competitors = extract_competitor_domains(desktop_serp_data, domain_url)
            mobile_competitors = extract_competitor_domains(mobile_serp_data, domain_url)

            # 2. Combine the lists and remove duplicates using a set
            all_competitors = list(set(desktop_competitors + mobile_competitors))

            # 3. Pass the RESULT (the list) to the update function.
            update_keyword_data(bigquery_client, keyword, new_status, all_competitors)
            
            # 4. Log the combined list for debugging/traceability
            logging.info(
                f"Found {len(all_competitors)} unique competitor domains for '{keyword}'.",
                extra={'json_fields': {**keyword_context, 'competitors': all_competitors}}
            )

    logging.info("Processing complete.")
    return "Processing complete", 200

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))