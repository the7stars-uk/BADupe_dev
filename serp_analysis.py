import os
from google.cloud import bigquery
import requests
import json

# --- Configuration ---
# Google Cloud BigQuery Configuration
project_id = "xxx"
dataset_id = "xxx"
table_id = "xxx"

# DataForSEO API Configuration
dataforseo_username = "xxx"
dataforseo_password = "xxx"

def get_keywords_from_bigquery(client):
    """Fetches keywords and their details from a BigQuery table."""
    query = f"""
        SELECT keyword, status, domain_url
        FROM `{project_id}.{dataset_id}.{table_id}`
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
    Using the 'organic' endpoint is more efficient as it includes ads data.
    """
    print(f"Fetching {device_type} SERP data for '{keyword}'...")
    post_data = [{
        "language_code": "en",
        "location_code": 2826, # UK
        "keyword": keyword,
        "device": device_type  # Specify "desktop" or "mobile"
    }]
    try:
        response = requests.post(
            "https://api.dataforseo.com/v3/serp/google/organic/live/advanced",
            auth=(dataforseo_username, dataforseo_password),
            json=post_data
        )
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {device_type} SERP data for '{keyword}': {e}")
        return None

def get_serp_data_mobile(keyword, device_type="mobile"):
    """
    Pulls comprehensive SERP data (organic and paid) from the DataForSEO API.
    Using the 'organic' endpoint is more efficient as it includes ads data.
    """
    post_data = [{
        "language_code": "en",
        "location_code": 2826, # UK
        "keyword": keyword,
        "device": device_type  # Can be "desktop" or "mobile"
    }]
    try:
        response = requests.post(
            "https://api.dataforseo.com/v3/serp/google/organic/live/advanced",
            auth=(dataforseo_username, dataforseo_password),
            json=post_data
        )
        response.raise_for_status()  # Raise an exception for bad status codes
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
                    # Found a competitor ad
                    return True
    return False

def is_domain_ranked_number_one(serp_data, domain_url):
    """Checks if the specified domain is ranked #1 in organic results."""
    if not serp_data or 'tasks' not in serp_data or not serp_data['tasks'] or 'result' not in serp_data['tasks'][0] or not serp_data['tasks'][0]['result']:
        return False

    for item in serp_data['tasks'][0]['result'][0].get('items', []):
        # We only care about the very first organic result
        if item.get('type') == 'organic' and item.get('rank_absolute') == 1:
            if item.get('domain') == domain_url:
                # The domain is ranked #1
                return True
            else:
                # Another domain is #1, so ours is not
                return False
    return False

def update_keyword_status(client, keyword, new_status):
    """Updates the status of a keyword in the BigQuery table."""
    query = f"""
        UPDATE `{project_id}.{dataset_id}.{table_id}`
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
        query_job.result()  # Wait for the job to complete
        print(f"Successfully updated status for '{keyword}' to '{new_status}'")
    except Exception as e:
        print(f"Error updating status for '{keyword}': {e}")

def main():
    """Main function to run the keyword status update process."""
    # Initialize BigQuery client
    try:
        bigquery_client = bigquery.Client(project=project_id)
    except Exception as e:
        print(f"Failed to initialize BigQuery client: {e}")
        return

    keywords_data = get_keywords_from_bigquery(bigquery_client)

    if not keywords_data:
        print("No keywords found in the BigQuery table.")
        return

    for row in keywords_data:
        keyword = row['keyword']
        current_status = row['status']
        domain_url = row['domain_url']
        new_status = None

        print(f"\nProcessing keyword: '{keyword}' with domain '{domain_url}'")

        # --- Step 1: Fetch SERP data for both desktop and mobile ---
        desktop_serp_data = get_serp_data(keyword, device_type="desktop")
        mobile_serp_data = get_serp_data(keyword, device_type="mobile")

        # --- Step 2: Analyze results for each device ---
        # Default to False if data is missing to ensure we don't pause by mistake
        desktop_has_competitor_ad = check_for_competitor_ads(desktop_serp_data, domain_url) if desktop_serp_data else False
        mobile_has_competitor_ad = check_for_competitor_ads(mobile_serp_data, domain_url) if mobile_serp_data else False

        desktop_is_ranked_one = is_domain_ranked_number_one(desktop_serp_data, domain_url) if desktop_serp_data else False
        mobile_is_ranked_one = is_domain_ranked_number_one(mobile_serp_data, domain_url) if mobile_serp_data else False
        
        print(f"-> Desktop Analysis: Competitor Ad = {desktop_has_competitor_ad}, Ranked #1 = {desktop_is_ranked_one}")
        print(f"-> Mobile Analysis: Competitor Ad = {mobile_has_competitor_ad}, Ranked #1 = {mobile_is_ranked_one}")

        # --- Step 3: Apply the combined logic ---
        
        # PRIMARY CONDITION: Check for competitor ads first.
        # If an ad exists on EITHER desktop OR mobile, enable the keyword.
        if desktop_has_competitor_ad or mobile_has_competitor_ad:
            print("-> Final Decision: Competitor ad found on at least one device. Setting status to 'ENABLED'.")
            new_status = 'ENABLED'
        else:
            # SECONDARY CONDITION: Only runs if NO competitor ads were found on either device.
            # Check organic ranking. We must be #1 on BOTH devices to pause.
            print("-> No competitor ads found on either device. Checking organic ranking...")
            if desktop_is_ranked_one and mobile_is_ranked_one:
                print("-> Final Decision: Ranked #1 on both devices. Setting status to 'PAUSED'.")
                new_status = 'PAUSED'
            else:
                print("-> Final Decision: Not ranked #1 on at least one device. Setting status to 'ENABLED'.")
                new_status = 'ENABLED'

        # --- Step 4: Update BigQuery if the status has changed ---
        if new_status and new_status != current_status:
            update_keyword_status(bigquery_client, keyword, new_status)
        elif new_status:
            print(f"-> Status for '{keyword}' remains '{current_status}'. No update needed.")

if __name__ == "__main__":
    main()