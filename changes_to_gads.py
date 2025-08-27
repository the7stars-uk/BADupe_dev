import pandas as pd
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery
import sys
import yaml
# No special imports are needed for this final version.

# --- Configuration ---
# All configuration is now loaded from the YAML file.
GOOGLE_ADS_YAML_PATH = r"C:\Users\AmyChan\google-ads.yaml"

# --- Test Mode Switch ---
DRY_RUN = True


def get_keyword_statuses_from_bigquery(bq_project_id, bq_dataset_id, bq_table_id):
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


def update_keyword_statuses_in_google_ads(googleads_client, customer_id, keywords_df):
    """
    Updates keyword statuses using a pre-configured Google Ads client.
    """
    try:
        ad_group_criterion_service = googleads_client.get_service("AdGroupCriterionService")

        operations = []
        for index, row in keywords_df.iterrows():
            ad_group_criterion_operation = googleads_client.get_type("AdGroupCriterionOperation")

            # =======================================================================
            # --- THE FINAL, ROBUST FIX ---
            # We directly modify the update_mask that is already part of the operation.
            # This avoids the buggy get_type("FieldMask") and the problematic
            # protobuf_helpers.field_mask() workaround.
            # This is the most direct and stable method.
            # =======================================================================
            ad_group_criterion_operation.update_mask.paths.append("status")
            # =======================================================================

            # Now, we configure the 'update' part of the operation
            ad_group_criterion = ad_group_criterion_operation.update
            ad_group_criterion.resource_name = ad_group_criterion_service.ad_group_criterion_path(
                customer_id, row['adgroup_id'], row['criterion_id']
            )

            status_enum = googleads_client.get_type("AdGroupCriterionStatusEnum").AdGroupCriterionStatus
            new_status_str = row['status'].upper()

            if new_status_str == 'ENABLED':
                ad_group_criterion.status = status_enum.ENABLED
            elif new_status_str == 'PAUSED':
                ad_group_criterion.status = status_enum.PAUSED
            else:
                print(f"Skipping keyword {row['criterion_id']} with unknown status: {row['status']}")
                continue

            operations.append(ad_group_criterion_operation)

        if not operations:
            print(f"No valid operations to perform for customer {customer_id}.")
            return

        if DRY_RUN:
            print(f"\n*** DRY RUN: Successfully prepared {len(operations)} updates for customer {customer_id}. ***")
            if operations:
                print("--- Example Operation ---")
                print(operations[0])
                print("-----------------------")
        else:
            print(f"--- LIVE MODE: Updating {len(operations)} keywords for customer {customer_id}... ---")
            response = ad_group_criterion_service.mutate_ad_group_criteria(
                customer_id=customer_id, operations=operations
            )
            print(f"Successfully updated keywords for customer {customer_id}.")

    except GoogleAdsException as ex:
        print(f"A GoogleAdsException occurred for customer {customer_id}:")
        for error in ex.failure.errors:
            print(f"\t- Error: '{error.message}'")
    except Exception as e:
        print(f"An unexpected error occurred for customer {customer_id}: {e}")


def main():
    """Main function to run the keyword status update process."""
    if DRY_RUN:
        print("=" * 50)
        print("SCRIPT IS RUNNING IN TEST (DRY RUN) MODE.")
        print("To apply changes, set the DRY_RUN variable to False.")
        print("=" * 50)

    # Manually load the entire configuration from the YAML file.
    try:
        print(f"Loading configuration from: {GOOGLE_ADS_YAML_PATH}")
        with open(GOOGLE_ADS_YAML_PATH, "r") as f:
            config = yaml.safe_load(f)
        print("Configuration file loaded successfully.")
    except FileNotFoundError:
        print(f"FATAL: Configuration file not found at '{GOOGLE_ADS_YAML_PATH}'")
        return
    except Exception as e:
        print(f"FATAL: Could not read or parse the YAML configuration file. Error: {e}")
        return

    # Initialize the Ads client using the config dictionary.
    try:
        print("Initializing Google Ads client from the configuration dictionary...")
        googleads_client = GoogleAdsClient.load_from_dict(config)
        print("Client initialized successfully.")
    except Exception as e:
        print(f"FATAL: Could not initialize Google Ads client from config. Error: {e}")
        return

    # Get BQ details from the config dictionary.
    try:
        bq_details = config['bigquery_details']
        bq_project_id = bq_details['project_id']
        bq_dataset_id = bq_details['dataset_id']
        bq_table_id = bq_details['table_id']
    except KeyError:
        print("FATAL: 'bigquery_details' section is missing or incomplete in the YAML file.")
        return

    # Pass the BQ details to the function that needs them.
    keywords_df = get_keyword_statuses_from_bigquery(
        bq_project_id, bq_dataset_id, bq_table_id
    )

    if keywords_df is not None and not keywords_df.empty:
        for customer_id, group_df in keywords_df.groupby('customer_id'):
            customer_id_str = str(customer_id).replace("-", "")
            update_keyword_statuses_in_google_ads(googleads_client, customer_id_str, group_df)


if __name__ == "__main__":
    main()