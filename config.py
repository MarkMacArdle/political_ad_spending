search_fields = [
    'ad_creation_time',
    'ad_delivery_start_time',
    'ad_delivery_stop_time',
    'ad_creative_body',
    'ad_creative_link_title',
    'ad_creative_link_caption',
    'ad_creative_link_description',
    'page_id',
    'page_name',
    'currency',
    'spend',  # returns range in dict
    'demographic_distribution',  # returns list of dicts
    'funding_entity',
    'impressions',  # returns range in dict
    'region_distribution',  # returns list of dicts
]

output_folder_name = 'ad_data'

# After you've registered as a developer and set up an app use the graph api
# to get a new token:
#       https://developers.facebook.com/tools/explorer
# That token will expire in about an hour so use the debugger to extend it and
# up to two months:
#       https://developers.facebook.com/tools/debug/accesstoken
# Helpful post on getting access tokens:
#       https://medium.com/@DrGabrielA81/python-how-making-facebook-api-calls-using-facebook-sdk-ea18bec973c8#6fdb
access_token = 'EAAPaWGvcZCGoBACBBEBgtNCXZCRXSC4MeCLedRvMWnOrEtVxowSbQLnNnaXwY87sHnxhX6GZCPf38uAVxcjq0IClvhFgOGdpeRnZCYtHAp0TuXRqC7gKYwrYbMZAnRfpHbJtOx2yZCvu4ZCcN0ZB9n8t7kvNCpeVGkPcm0vTZCKfuFwZDZD'
