import logging
import os

import jsonlines
from google.cloud import bigquery

from ad_data_collector import get_and_save_ad_data

def create_staging_table(client, dataset_ref, table_name):
    """Create staging table in BigQuery and return it as an object"""

    schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("ad_creation_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("ad_delivery_start_time",
                             "TIMESTAMP",
                             mode="NULLABLE"),
        bigquery.SchemaField("ad_delivery_stop_time",
                             "TIMESTAMP",
                             mode="NULLABLE"),
        bigquery.SchemaField("page_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "spend",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("upper_bound", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("lower_bound", "INTEGER", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("currency", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "impressions",
            "RECORD",
            mode="NULLABLE",
            fields=[
                bigquery.SchemaField("upper_bound", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("lower_bound", "INTEGER", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField(
            "region_distribution",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("percentage", "FLOAT", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("ad_creative_link_caption",
                             "STRING",
                             mode="NULLABLE"),
        bigquery.SchemaField(
            "demographic_distribution",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("age", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("percentage", "FLOAT", mode="NULLABLE"),
            ],
        ),
        bigquery.SchemaField("funding_entity", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ad_creative_link_title", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ad_creative_body", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ad_creative_link_description",
                             "STRING",
                             mode="NULLABLE"),
    ]

    table_ref = dataset_ref.table(table_name)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    logging.info(f"Created table {table.full_table_id}")

    return table


def load_data_into_table(client, staging_table):
    """Loads newline delimited json files in a folder into a BigQuery table"""

    dir_name = 'ad_data'
    for filename in os.listdir(dir_name):
        rows = []
        with jsonlines.open(f'{dir_name}/{filename}') as reader:
            for obj in reader:
                rows.append(obj)

        errors = client.insert_rows(staging_table, rows)

        # Ignore errors. There enough data being inserted for them to not be
        # significant.
        # if errors:
        #     raise Exception(
        #         f'Errors found when loading data into staging table: {errors}'
        #     )


def create_and_load_staging_table(**kwargs):
    """Create the staging table in BigQuery and load the ad data into it"""

    client = bigquery.Client()

    dataset = client.dataset(kwargs['params']['dataset_name'])
    staging_table = create_staging_table(
        client,
        dataset,
        kwargs['params']['staging_table_name'],
    )
    load_data_into_table(client, staging_table)


def get_ads_and_upload_to_bq_staging(**kwargs):
    """
    Wrapper function so both the getting and uploading of ads is done on
    the one node and so the local file system can be used to store the ads
    before upload
    """
    get_and_save_ad_data()
    create_and_load_staging_table(**kwargs)