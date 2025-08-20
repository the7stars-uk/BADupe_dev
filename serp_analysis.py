import os
from google.cloud import bigquery
import requests
import json

# --- Configuration ---
# Google Cloud BigQuery Configuration
project_id = "organic-xxx-saver"
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

def get_serp_data(keyword):
    """
    Pulls comprehensive SERP data (organic and paid) from the DataForSEO API.
    Using the 'organic' endpoint is more efficient as it includes ads data.
    """
    post_data = [{
        "language_code": "en",
        "location_code": 2826, # UK
        "keyword": keyword
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
        print(f"Error fetching SERP data for '{keyword}': {e}")
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

        print(f"Processing keyword: '{keyword}' with domain '{domain_url}'")

        serp_data = get_serp_data(keyword)

        if serp_data:
            # 1. Primary Condition: Check for competitor paid ads
            has_competitor_ad = check_for_competitor_ads(serp_data, domain_url)
            
            if has_competitor_ad:
                print(f"-> Found competitor ad. Setting status to 'enabled'.")
                new_status = 'ENABLED'
            else:
                # 2. Secondary Condition: No competitor ads found, check organic ranking
                print("-> No competitor ads found. Checking organic ranking...")
                is_ranked_one = is_domain_ranked_number_one(serp_data, domain_url)
                
                if is_ranked_one:
                    print(f"-> Domain is ranked #1 organically. Setting status to 'paused'.")
                    new_status = 'PAUSED'
                else:
                    print(f"-> Domain is NOT ranked #1 organically. Setting status to 'enabled'.")
                    new_status = 'ENABLED'

            # Update BigQuery only if the status has changed
            if new_status and new_status != current_status:
                update_keyword_status(bigquery_client, keyword, new_status)
            elif new_status:
                print(f"-> Status for '{keyword}' remains '{current_status}'. No update needed.")

if __name__ == "__main__":
    main()