import json

import requests

import config

# Use the graph api to get a new token
#       https://developers.facebook.com/tools/explorer
# That token will expire in about an hour so use the debugger to extend it and
# up to two months.
#       https://developers.facebook.com/tools/debug/accesstoken
# helpful post on getting access tokens:
#       https://medium.com/@DrGabrielA81/python-how-making-facebook-api-calls-using-facebook-sdk-ea18bec973c8#6fdb
access_token = config.user_access_token_extended_expiry

search_fields = [
    'ad_creation_time',
    'ad_delivery_start_time',
    'ad_delivery_stop_time',
    'ad_creative_body',
    'page_id',
    'page_name',
    'currency',
    'spend',
    'demographic_distribution',
    'funding_entity',
    'impressions',
    'region_distribution',
]
search_fields = ', '.join(search_fields)

data = {
    'search_terms': "''",
    'ad_type': 'POLITICAL_AND_ISSUE_ADS',
    'ad_reached_countries': 'GB',
    'access_token': access_token,
    'fields': search_fields,
    'ad_active_status': 'ALL',
    'limit': 10,
}

url = "https://graph.facebook.com/v4.0/ads_archive"

# todo: use paging['next'] url to keep getting more data until it's not present
# or blank
# useful post
# http://rpubs.com/zoowalk/FB_EP2019
res = requests.get(url=url, params=data)
response = res.json()
print(json.dumps(response, indent=4, default=str))
import pdb; pdb.set_trace()



