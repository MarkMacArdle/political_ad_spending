from datetime import datetime

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from ad_data_collector import get_and_save_ad_data


project_name = 'bbm-data-lake-prod'
dataset_name = 'markm_udacity_project'
staging_table_name = 'staging_table'
staging_table_path = f'{project_name}.{dataset_name}.{staging_table_name}'


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 17),
}

with DAG(dag_id='facebook_political_ads_to_bigquery_v1',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False) as dag:

    get_facebook_ads_task = PythonOperator(
        task_id='get_facebook_ads_task',
        python_callable=get_and_save_ad_data,
    )


