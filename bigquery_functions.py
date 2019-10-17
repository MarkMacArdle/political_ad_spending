import logging
import os

import jsonlines
from google.cloud import bigquery

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
            logging.error(errors)


def create_and_load_staging_table(**kwargs):
    """Create the staging table in BigQuery and load the ad data into it"""

    client = bigquery.Client()
    dataset = client.dataset(kwargs['dataset_name'])
    staging_table = create_staging_table(
        client,
        dataset,
        kwargs['staging_table_name'],
    )
    load_data_into_table(client, staging_table)


def load_data_into_working_tables(**kwargs):
    client = bigquery.Client()
    staging_table = client.get_table(
        f'{kwargs["dataset_name"]}.{kwargs["staging_table_name"]}'
    )

    load_data_into_table(client, staging_table)

    tables_and_load_queries = {
        'ads': sql_ads_table,
        'spends': sql_spend_table,
        'demographics': sql_demographics_table,
        'impressions': sql_impressions_table,
        'regions': sql_regions_table,
    }

    for table_name, sql in tables_and_load_queries:
        job_config = bigquery.QueryJobConfig()
        table_ref = client.dataset(kwargs["dataset_name"]).table(table_name)
        job_config.destination = table_ref

        # Start the query
        query_job = client.query(
            sql.format(kwargs["staging_table_path"]),
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
        id_col=kwargs['id_col'],
        project=kwargs['project_name'],
        dataset=kwargs['dataset_name'],
        table=kwargs['table_name'],
    ))

    results = query_job.result()
    row_count = list(results)[0][0]

    if row_count == 0:
        raise Exception(f'No rows found in table {kwargs["table_name"]}')
