import requests
import json
import time

# -- Step 0: Set Up Your Credentials --
# IMPORTANT: Replace these with your actual login and API key from your DataForSEO dashboard.
# For better security, use environment variables instead of hardcoding them.
D4S_LOGIN = "xxx"
D4S_API_KEY = "xxx"

# ==============================================================================
# -- Step 1: POST the Task to DataForSEO (Place Your Coffee Order) --
# ==============================================================================
print("--- Step 1: Posting a new task ---")

# Define the task you want to perform
post_data = [
    {
        "language_code": "en",
        "location_code": 2826,  # Location code for UK
        "keyword": "bella and duke",
        "tag": "belladuke_badupe" # Optional: for your own reference
    }
]

try:
    # Make the POST request to create the task
    response_post = requests.post(
        "https://api.dataforseo.com/v3/serp/google/organic/task_post",
        auth=(D4S_LOGIN, D4S_API_KEY),
        json=post_data
    )
    response_post.raise_for_status()  # Raise an exception for bad status codes

except requests.exceptions.RequestException as e:
    print(f"POST request failed: {e}")
    # If the request fails, we can't continue, so exit the script
    exit()

# Extract the task_id from the successful response
response_post_json = response_post.json()

# Check if the task was created successfully
if response_post_json.get("tasks_error", 1) > 0:
    print("Error creating task:")
    print(json.dumps(response_post_json, indent=4))
    exit()

# Get the task ID from the first task in the response list
# This is your "order number"
task_id = response_post_json["tasks"][0]["id"]
print(f"✅ Task successfully posted! Your Task ID is: {task_id}\n")


# ==============================================================================
# -- Step 2: Wait for the Task to Complete (Wait for Your Coffee) --
# ==============================================================================
print("--- Step 2: Waiting for the task to be completed by the server ---")
# In a real application, you might use a more sophisticated method like checking
# the task status periodically or using a postback URL.
# For this example, we will just wait for 30 seconds.
WAIT_TIME_SECONDS = 30
print(f"Waiting for {WAIT_TIME_SECONDS} seconds...\n")
time.sleep(WAIT_TIME_SECONDS)


# ==============================================================================
# -- Step 3: GET the Results (Collect Your Order) --
# ==============================================================================
print("--- Step 3: Retrieving the results for your task ---")

# Construct the URL to get the results for our specific task_id
# We use the '/advanced/' endpoint to get the detailed, structured data
get_url = f"https://api.dataforseo.com/v3/serp/google/organic/task_get/advanced/{task_id}"

try:
    # Make the GET request to fetch the results
    response_get = requests.get(
        get_url,
        auth=(D4S_LOGIN, D4S_API_KEY)
    )
    response_get.raise_for_status()

except requests.exceptions.RequestException as e:
    print(f"GET request failed: {e}")
    exit()

# Load the JSON from the response
response_get_json = response_get.json()

# Check if the task finished successfully
if response_get_json["tasks"][0]["status_code"] != 20000:
    print("Task did not complete successfully:")
    print(json.dumps(response_get_json, indent=4))
    exit()

print("✅ Results successfully retrieved!\n")

# Extract the list of all SERP items (paid, organic, etc.)
# The actual results are nested inside the 'result' list
serp_items = response_get_json["tasks"][0]["result"][0]["items"]


# ==============================================================================
# -- Step 4: Filter the Results to Find Only Paid Ads --
# ==============================================================================
print("--- Step 4: Filtering results to find only paid ads ---")

paid_ads = []
for item in serp_items:
    # This is the key part: we check the 'type' of each item
    if item.get("type") == "paid":
        paid_ads.append(item)

# Finally, print the filtered list of paid ads
if paid_ads:
    print(f"✅ Found {len(paid_ads)} paid ad(s)!")
    print("--- Paid Ad Results ---")
    print(json.dumps(paid_ads, indent=4))
else:
    print("ℹ️ No paid ads were found in the results for this keyword.")