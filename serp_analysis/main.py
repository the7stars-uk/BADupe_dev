import os
from flask import Flask
from google.cloud import bigquery
from google.cloud import secretmanager
import requests
import json

app = Flask(__name__)

# --- Configuration ---
# Google Cloud Project ID from environment variable (set by Cloud Run)
project_id = os.environ.get("GCP_PROJECT")

def access_secret_version(secret_id, version_id="latest"):
    """
    Accesses a secret version from Google Cloud Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Fetch configurations from Secret Manager
try:
    bq_project_id = access_secret_version("bigquery_project_id")
    bq_dataset_id = access_secret_version("bigquery_dataset_id")
    bq_table_id = access_secret_version("bigquery_table_id")
    dataforseo_username = access_secret_version("dataforseo_username")
    dataforseo_password = access_secret_version("dataforseo_password")
except Exception as e:
    print(f"Error fetching secrets from Secret Manager: {e}")
    # Handle the error appropriately, maybe exit or raise an exception
    # For now, we'll print and let it fail later
    bq_project_id = None 
    
def get_keywords_from_bigquery(client):
    """Fetches keywords and their details from a BigQuery table."""
    query = f"""
        SELECT keyword, status, domain_url
        FROM `{bq_project_id}.{bq_dataset_id}.{bq_table_id}`
    """
    try:
        query_job = client.query(query)
        return list(query_job)
    except Exception as e:
        print(f"Error fetching data from BigQuery: {e}")
        return []

def get_serp_data(keyword, device_type="desktop"):
    """
    Pulls comprehensive SERP data from the DataForSEO API for a specific device type.
    """
    print(f"Fetching {device_type} SERP data for '{keyword}'...")
    post_data = [{
        "language_code": "en",
        "location_code": 2826, # UK
        "keyword": keyword,
        "device": device_type
    }]
    try:
        response = requests.post(
            "https://api.dataforseo.com/v3/serp/google/organic/live/advanced",
            auth=(dataforseo_username, dataforseo_password),
            json=post_data
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {device_type} SERP data for '{keyword}': {e}")
        return None

def check_for_competitor_ads(serp_data, domain_url):
    """Analyzes SERP data to find non-matching paid ad domains."""
    if not serp_data or 'tasks' not in serp_data or not serp_data['tasks'] or 'result' not in serp_data['tasks'][0] or not serp_data['tasks'][0]['result']:
        return False

    for item in serp_data['tasks'][0]['result'][0].get('items', []):
        if item.get('type') == 'ads' and 'ads' in item:
            for ad in item['ads']:
                if 'domain' in ad and ad['domain'] != domain_url:
                    return True
    return False

def is_domain_ranked_number_one(serp_data, domain_url):
    """Checks if the specified domain is ranked #1 in organic results."""
    if not serp_data or 'tasks' not in serp_data or not serp_data['tasks'] or 'result' not in serp_data['tasks'][0] or not serp_data['tasks'][0]['result']:
        return False

    for item in serp_data['tasks'][0]['result'][0].get('items', []):
        if item.get('type') == 'organic' and item.get('rank_absolute') == 1:
            if item.get('domain') == domain_url:
                return True
            else:
                return False
    return False

def update_keyword_status(client, keyword, new_status):
    """Updates the status of a keyword in the BigQuery table."""
    query = f"""
        UPDATE `{bq_project_id}.{bq_dataset_id}.{bq_table_id}`
        SET status = @new_status
        WHERE keyword = @keyword
    """
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("new_status", "STRING", new_status),
                bigquery.ScalarQueryParameter("keyword", "STRING", keyword),
            ]
        )
        query_job = client.query(query, job_config=job_config)
        query_job.result()
        print(f"Successfully updated status for '{keyword}' to '{new_status}'")
    except Exception as e:
        print(f"Error updating status for '{keyword}': {e}")

@app.route("/", methods=["POST"])
def main():
    """Main function to run the keyword status update process."""
    if not bq_project_id:
        return "Error: BigQuery project ID not configured from Secret Manager.", 500
        
    try:
        # The BigQuery client will use the Cloud Run service account's credentials
        bigquery_client = bigquery.Client(project=bq_project_id)
    except Exception as e:
        print(f"Failed to initialize BigQuery client: {e}")
        return "Error initializing BigQuery client", 500

    keywords_data = get_keywords_from_bigquery(bigquery_client)

    if not keywords_data:
        print("No keywords found in the BigQuery table.")
        return "No keywords found", 200

    for row in keywords_data:
        keyword = row['keyword']
        current_status = row['status']
        domain_url = row['domain_url']
        new_status = None

        print(f"\nProcessing keyword: '{keyword}' with domain '{domain_url}'")

        desktop_serp_data = get_serp_data(keyword, device_type="desktop")
        mobile_serp_data = get_serp_data(keyword, device_type="mobile")

        desktop_has_competitor_ad = check_for_competitor_ads(desktop_serp_data, domain_url) if desktop_serp_data else False
        mobile_has_competitor_ad = check_for_competitor_ads(mobile_serp_data, domain_url) if mobile_serp_data else False

        desktop_is_ranked_one = is_domain_ranked_number_one(desktop_serp_data, domain_url) if desktop_serp_data else False
        mobile_is_ranked_one = is_domain_ranked_number_one(mobile_serp_data, domain_url) if mobile_serp_data else False
        
        print(f"-> Desktop Analysis: Competitor Ad = {desktop_has_competitor_ad}, Ranked #1 = {desktop_is_ranked_one}")
        print(f"-> Mobile Analysis: Competitor Ad = {mobile_has_competitor_ad}, Ranked #1 = {mobile_is_ranked_one}")

        if desktop_has_competitor_ad or mobile_has_competitor_ad:
            print("-> Final Decision: Competitor ad found on at least one device. Setting status to 'ENABLED'.")
            new_status = 'ENABLED'
        else:
            print("-> No competitor ads found on either device. Checking organic ranking...")
            if desktop_is_ranked_one and mobile_is_ranked_one:
                print("-> Final Decision: Ranked #1 on both devices. Setting status to 'PAUSED'.")
                new_status = 'PAUSED'
            else:
                print("-> Final Decision: Not ranked #1 on at least one device. Setting status to 'ENABLED'.")
                new_status = 'ENABLED'

        if new_status and new_status != current_status:
            update_keyword_status(bigquery_client, keyword, new_status)
        elif new_status:
            print(f"-> Status for '{keyword}' remains '{current_status}'. No update needed.")

    return "Processing complete", 200

if __name__ == "__main__":
    # PORT is automatically set by Cloud Run.
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))