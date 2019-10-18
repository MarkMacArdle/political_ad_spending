import logging
import os

import jsonlines
import pandas
from google.cloud import bigquery, storage

from sql_queries import sql_ads_table, sql_spend_table, \
    sql_impressions_table, sql_regions_table, sql_demographics_table, \
    sql_count_table_rows


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
        if errors:
            raise Exception(
                f'Errors found when loading data into staging table: {errors}'
            )


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


def load_data_into_working_table(**kwargs):
    """Run a query on the staging table and save the result to a new table"""

    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset(kwargs['params']['dataset_name']).table(
        kwargs['params']['table_name']
    )
    job_config.destination = table_ref

    # Start the query
    query_job = client.query(
        kwargs['params']['sql'].format(
            staging_table_path=kwargs['params']['staging_table_path']
        ),
        location='EU',
        job_config=job_config
    )

    # Wait for the query to finish
    query_job.result()
    logging.info(f'Query results loaded to table {table_ref.path}')


def check_rows_in_table(**kwargs):
    """Check the passed table has at least one row, raise error if not"""

    client = bigquery.Client()

    query_job = client.query(sql_count_table_rows.format(
        project=kwargs['params']['project_name'],
        dataset=kwargs['params']['dataset_name'],
        table=kwargs['params']['table_name'],
    ))

    results = query_job.result()
    row_count = list(results)[0][0]

    if row_count == 0:
        raise Exception(
            f'No rows found in {kwargs["params"]["table_name"]}'
        )

    logging.info(f'Found {row_count} rows.')
