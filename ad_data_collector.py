import json
import logging
import os

import requests

from config import access_token, output_folder_name, search_fields


def get_next_url(fb_response):
    """
    Returns url to next page of ad results

    :param fb_response: response from facebook ad library api
    :return: url to next page of results or None if url not found
    """

    next_url = fb_response.get('paging')
    if next_url:
        return next_url.get('next')


def create_folder_if_needed(folder_name):
    """Creates folder if it doesn't exist already"""

    if not os.path.exists(folder_name):
        os.mkdir(folder_name)
        logging.info(f'Created folder: {folder_name}')


# So info logs are printed when run locally
logging.basicConfig(level=logging.INFO)

# max value from docs is 5000 but found you get an error saying you've asked
# for too much data when you try that.
results_per_page = 1000

search_fields = ', '.join(search_fields)
params = {
    'search_terms': "''",
    'ad_type': 'POLITICAL_AND_ISSUE_ADS',
    'ad_reached_countries': 'GB',
    'access_token': access_token,
    'fields': search_fields,
    'ad_active_status': 'ALL',
    'limit': results_per_page,
}
url = "https://graph.facebook.com/v4.0/ads_archive"

counter = 1
total_ads = 0
while True:
    logging.info(
        f'Starting loop {counter}, getting next {results_per_page} results'
    )
    response = requests.get(url=url, params=params).json()
    data = response['data']
    fname = f'{counter:06}.json'
    logging.info(f'Found {len(data)} ads, saving to {fname}')

    create_folder_if_needed(output_folder_name)
    with open(f'{output_folder_name}/{fname}', mode='w') as outfile:
        json.dump(data, outfile)

    params = None
    counter += 1
    total_ads += len(data)
    logging.info(f'{total_ads} saved so far')

    url = get_next_url(response)
    if not url:
        break

logging.info('No more pages found for results. Ending now.')
