import logging
import os

import jsonlines
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


def encode_ads(ad_data, counter):
    """Encode ads into a standard format that suits BigQuery"""

    datetime_fields = [
        'ad_creation_time',
        'ad_delivery_start_time',
        'ad_delivery_stop_time',
    ]

    for ad in ad_data:
        # Facebook doesn't provide a unique ID for ads so add one
        ad['id'] = counter
        counter += 1

        for field in search_fields:
            # If a field isn't present add it with a null value
            if field not in ad.keys():
                ad[field] = None

            # Timestamps start in a format like '2019-10-09T16:36:35+0000'
            # BigQuery wants a format like '2019-10-09 16:36:35'
            if field in datetime_fields and ad[field] is not None:
                ad[field] = ad[field].replace('T', ' ').split('+')[0]

    return ad_data, counter


def get_and_save_ad_data():
    """
    Call facebook api for political ads and save them in newline
    delimited json files in batches of 1000.
    """

    # So info logs are printed when run locally
    logging.basicConfig(level=logging.INFO)

    # max value from docs is 5000 but found you get an error saying you've
    # asked for too much data when you try that.
    results_per_page = 1000

    search_fields_joined = ', '.join(search_fields)
    params = {
        'search_terms': "''",
        'ad_type': 'POLITICAL_AND_ISSUE_ADS',
        'ad_reached_countries': 'GB',
        'access_token': access_token,
        'fields': search_fields_joined,
        'ad_active_status': 'ALL',
        'limit': results_per_page,
    }
    url = "https://graph.facebook.com/v4.0/ads_archive"

    loop_counter = 1
    ad_counter = 1
    while True:
        logging.info(
            f'Starting loop {loop_counter}, getting next {results_per_page} results'
        )
        response = requests.get(url=url, params=params).json()
        data, ad_counter = encode_ads(response['data'], ad_counter)

        create_folder_if_needed(output_folder_name)
        fname = f'{loop_counter:06}.jsonl'
        logging.info(f'Found {len(data)} ads, saving to {fname}')

        # write in json lines format that is accepted by BigQuery
        with jsonlines.open(f'{output_folder_name}/{fname}', 'w') as writer:
            writer.write_all(data)

        params = None
        loop_counter += 1
        logging.info(f'{ad_counter} saved so far')

        url = get_next_url(response)
        if not url or loop_counter > 2:
            break

    logging.info('No more pages found for results. Ending now.')
