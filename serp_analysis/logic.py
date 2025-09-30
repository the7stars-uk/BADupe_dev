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
            # If we find the #1 rank, we check if it's our domain
            if item.get('domain') == domain_url:
                return True # Found it! We can stop and return True.

    # If we get through the whole loop without finding our domain at rank #1,
    # then we can safely return False.
    return False