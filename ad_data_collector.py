import json

import requests

import config


def get_next_url(fb_response):
    """
    Returns url to next page of ad results

    :param fb_response: response from facebook ad library api
    :return: url to next page of results or None if url not found
    """

    next_url = fb_response.get('paging')
    if next_url:
        next_url = next_url.get('next')

    return next_url


access_token = config.user_access_token_extended_expiry
url = "https://graph.facebook.com/v4.0/ads_archive"
search_fields = ', '.join(config.search_fields)
params = {
    'search_terms': "''",
    'ad_type': 'POLITICAL_AND_ISSUE_ADS',
    'ad_reached_countries': 'GB',
    'access_token': access_token,
    'fields': search_fields,
    'ad_active_status': 'ALL',
    'limit': 10,
}

# todo: use paging['next'] url to keep getting more data until it's not present
# or blank
# useful post
# http://rpubs.com/zoowalk/FB_EP2019


res = requests.get(url=url, params=params)
response = res.json()
next_page_url = get_next_url(response)

while next_page_url:



print(json.dumps(response, indent=4, default=str))
import pdb; pdb.set_trace()



