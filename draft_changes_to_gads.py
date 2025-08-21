import argparse
import sys
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery
from pathlib import Path

# Your Google Cloud Project ID where the BigQuery table resides.
BIGQUERY_PROJECT_ID = "organic-keyword-saver"
# Your BigQuery Dataset ID.
BIGQUERY_DATASET_ID = "belladuke_dataset"
# Your BigQuery Table ID.
BIGQUERY_TABLE_ID = "gads_keywords"


def get_keywords_from_bigquery():
    """
    Queries BigQuery to get a dictionary of keywords and their desired statuses.

    Returns:
        A dictionary mapping criterion_id (as string) to desired status.
    """
    print("Fetching keyword statuses from BigQuery...")
    try:
        client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        query = f"""
            SELECT
                CAST(criterion_id AS STRING) as criterion_id,
                status
            FROM
                `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TABLE_ID}`
            WHERE
                status IN ('ENABLED', 'PAUSED')
        """
        query_job = client.query(query)
        rows = query_job.result()
        
        desired_statuses = {row.criterion_id: row.status for row in rows}
        print(f"Found {len(desired_statuses)} keywords in BigQuery table.")
        return desired_statuses
    except Exception as e:
        print(f"An error occurred while querying BigQuery: {e}")
        sys.exit(1)


def get_keywords_from_google_ads(client, customer_id):
    """
    Queries the Google Ads API to get all keywords and their current statuses.

    Args:
        client: An initialized GoogleAdsClient.
        customer_id: The ID of the Google Ads account to query.

    Returns:
        A dictionary mapping criterion_id to a tuple of (resource_name, current_status).
    """
    print("Fetching current keyword statuses from Google Ads...")
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT
            ad_group_criterion.resource_name,
            ad_group_criterion.criterion_id,
            ad_group_criterion.status
        FROM ad_group_criterion
        WHERE ad_group_criterion.type = 'KEYWORD'
    """
    
    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    
    account_keywords = {}
    for batch in stream:
        for row in batch.results:
            criterion = row.ad_group_criterion
            account_keywords[str(criterion.criterion_id)] = (
                criterion.resource_name,
                criterion.status.name,
            )
            
    print(f"Found {len(account_keywords)} keywords in Google Ads account.")
    return account_keywords


def report_on_planned_changes(desired_statuses, account_keywords):
    """
    Compares BigQuery statuses with Google Ads statuses and reports on discrepancies.

    Args:
        desired_statuses: A dictionary of {criterion_id: status} from BigQuery.
        account_keywords: A dictionary from Google Ads.

    Returns:
        The total count of keywords that need to be updated.
    """
    update_count = 0
    
    for criterion_id, desired_status in desired_statuses.items():
        if criterion_id in account_keywords:
            resource_name, current_status = account_keywords[criterion_id]
            
            # Check if the status from BigQuery is different from the one in the account
            if desired_status != current_status:
                print(
                    f"  - Change Planned for ID {criterion_id}: "
                    f"Current status is '{current_status}', should be '{desired_status}'"
                )
                update_count += 1
        else:
            print(f"  - Warning: Keyword with ID {criterion_id} from BigQuery was not found in the account.")
            
    return update_count


def main(client, customer_id):
    """
    Main function to orchestrate the synchronization preview.
    This function will only report on changes and will NOT modify the account.
    """
    # 1. Get desired states from BigQuery
    desired_statuses = get_keywords_from_bigquery()
    if not desired_statuses:
        print("No keywords found in BigQuery. Exiting.")
        return

    # 2. Get current states from Google Ads
    account_keywords = get_keywords_from_google_ads(client, customer_id)
    if not account_keywords:
        print("No keywords found in the Google Ads account. Exiting.")
        return

    # 3. Compare statuses and report on the changes that would be made
    print("\n--- DRY RUN MODE: Analyzing potential changes ---")
    
    total_changes_needed = report_on_planned_changes(desired_statuses, account_keywords)

    # 4. Display the final summary
    if total_changes_needed == 0:
        print("\nResult: All keyword statuses are already in sync. No changes needed.")
    else:
        print(f"\nResult: Found {total_changes_needed} keywords that need status updates.")

    print("\nScript finished. No changes were made to the Google Ads account.")


if __name__ == "__main__":
    # Ensure you've created a google-ads.yaml file in your home directory
    googleads_client = GoogleAdsClient.load_from_storage()

    parser = argparse.ArgumentParser(
        description="Previews keyword status changes between BigQuery and Google Ads."
    )
    parser.add_argument(
        "-c",
        "--customer_id",
        type=str,
        required=True,
        help="The Google Ads customer ID.",
    )
    args = parser.parse_args()

    main(googleads_client, args.customer_id)