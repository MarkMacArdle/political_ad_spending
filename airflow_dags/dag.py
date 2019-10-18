from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from ad_data_collector import get_and_save_ad_data
from bigquery_functions import create_and_load_staging_table, \
    check_rows_in_table, load_data_into_working_table

from sql_queries import sql_ads_table, sql_spend_table, \
    sql_demographics_table, sql_demographics_impressions_spend_table, \
    sql_impressions_table, sql_regions_table, \
    sql_regions_impressions_spend_table


project_name = 'bbm-data-lake-prod'
dataset_name = 'markm_udacity_project3'
staging_table_name = 'staging_table'
staging_table_path = f'{project_name}.{dataset_name}.{staging_table_name}'
tables_and_load_queries = {
    'ads': sql_ads_table,
    'spends': sql_spend_table,
    'demographics': sql_demographics_table,
    'demographics_impressions_spend_joined':
        sql_demographics_impressions_spend_table,
    'impressions': sql_impressions_table,
    'regions': sql_regions_table,
    'regions_impressions_spend_joined': sql_regions_impressions_spend_table,
}

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 17),
}

with DAG(dag_id='facebook_political_ads_to_bigquery_v1',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    create_dataset = BashOperator(
        task_id='create_dataset',
        bash_command=f'bq --location=EU mk {dataset_name}',
    )

    get_facebook_ads = PythonOperator(
        task_id='get_facebook_ads_task',
        python_callable=get_and_save_ad_data,
    )

    load_staging_table = PythonOperator(
        task_id='load_to_staging_table_task',
        python_callable=create_and_load_staging_table,
        provide_context=True,
        params={
            'dataset_name': dataset_name,
            'staging_table_name': staging_table_name,
        },
    )

    check_staging_table = PythonOperator(
        task_id='check_rows_in_staging_table',
        python_callable=check_rows_in_table,
        provide_context=True,
        params={
            'project_name': project_name,
            'dataset_name': dataset_name,
            'table_name': staging_table_name,
        },
    )

    create_dataset >> get_facebook_ads >> load_staging_table
    load_staging_table >> check_staging_table

    for table_name, sql in tables_and_load_queries.items():
        create_table = PythonOperator(
            task_id=f'{table_name}_table_create',
            python_callable=load_data_into_working_table,
            provide_context=True,
            params={
                'sql': sql,
                'dataset_name': dataset_name,
                'table_name': table_name,
                'staging_table_path': staging_table_path,
            }
        )

        check_table = PythonOperator(
            task_id=f'check_rows_in_{table_name}_table',
            python_callable=check_rows_in_table,
            provide_context=True,
            params={
                'project_name': project_name,
                'dataset_name': dataset_name,
                'table_name': table_name,
            },
        )

        check_staging_table >> create_table >> check_table
