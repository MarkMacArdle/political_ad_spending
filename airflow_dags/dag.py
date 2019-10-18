from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_check_operator import \
    BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.bigquery_table_delete_operator import \
    BigQueryTableDeleteOperator
from airflow.contrib.operators.gcs_to_bq import \
    GoogleCloudStorageToBigQueryOperator

from bigquery_functions import get_ads_and_upload_to_bq_staging
from sql_queries import sql_ads_table, sql_spend_table, \
    sql_demographics_table, \
    sql_impressions_table, sql_regions_table, sql_transform_ons_table, \
    sql_count_table_rows, sql_transform_guardian_table


project_name = 'bbm-data-lake-prod'
dataset_name = 'markm_facebook_political_ad_spending'
staging_table_name = 'staging_table'
staging_table_path = f'{project_name}.{dataset_name}.{staging_table_name}'

ads_table_name = 'ads'
spends_table_name = 'spends'
demograph_table_name = 'demographics'
impressions_table_name = 'impressions'
regions_table_name = 'regions'
ons_uk_pops_table_name = 'ons_uk_population_estimates_from_2016_for_2019'
ons_processed_table_name = 'uk_population_by_age_groups'
guardian_fb_users_table_name = 'facebook_users_guardian_2018'
guardian_processed_table_name = 'facebook_users_by_age_groups'

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2019, 10, 17),
}

dag = DAG(
    dag_id='facebook_political_ads_to_bigquery_v1',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

create_dataset = BashOperator(
    task_id='create_dataset',
    bash_command=f'bq --location=EU mk {dataset_name}',
    dag=dag,
)

get_ads_and_upload_to_staging = PythonOperator(
    task_id='get_ads_and_upload_to_staging_table',
    python_callable=get_ads_and_upload_to_bq_staging,
    provide_context=True,
    params={
        'dataset_name': dataset_name,
        'staging_table_name': staging_table_name,
    },
    dag=dag,
)

check_staging_table = BigQueryCheckOperator(
    task_id='check_rows_in_staging_table',
    sql=sql_count_table_rows.format(
        project=project_name,
        dataset=dataset_name,
        table=staging_table_name,
    ),
    use_legacy_sql=False,
    dag=dag,
)

create_ads_table = BigQueryOperator(
    task_id='ads_table_create',
    sql=sql_ads_table.format(staging_table_path=staging_table_path),
    use_legacy_sql=False,
    destination_dataset_table=(
        f'{project_name}.{dataset_name}.{ads_table_name}'
    ),
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

check_ads = BigQueryCheckOperator(
    task_id=f'check_ads',
    sql=sql_count_table_rows.format(project=project_name,
                                    dataset=dataset_name,
                                    table=ads_table_name),
    use_legacy_sql=False,
    dag=dag,
)

create_spends_table = BigQueryOperator(
    task_id='spends_table_create',
    sql=sql_spend_table.format(staging_table_path=staging_table_path),
    use_legacy_sql=False,
    destination_dataset_table=(
        f'{project_name}.{dataset_name}.{spends_table_name}'
    ),
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

check_spends = BigQueryCheckOperator(
    task_id=f'check_spends',
    sql=sql_count_table_rows.format(project=project_name,
                                    dataset=dataset_name,
                                    table=spends_table_name),
    use_legacy_sql=False,
    dag=dag,
)

create_demograph_table = BigQueryOperator(
    task_id='demograph_table_create',
    sql=sql_demographics_table.format(staging_table_path=staging_table_path),
    use_legacy_sql=False,
    destination_dataset_table=(
        f'{project_name}.{dataset_name}.{demograph_table_name}'
    ),
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

check_demograph = BigQueryCheckOperator(
    task_id=f'check_demograph',
    sql=sql_count_table_rows.format(project=project_name,
                                    dataset=dataset_name,
                                    table=demograph_table_name),
    use_legacy_sql=False,
    dag=dag,
)

create_impressions_table = BigQueryOperator(
    task_id='impressions_table_create',
    sql=sql_impressions_table.format(staging_table_path=staging_table_path),
    use_legacy_sql=False,
    destination_dataset_table=(
        f'{project_name}.{dataset_name}.{impressions_table_name}'
    ),
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

check_impressions = BigQueryCheckOperator(
    task_id=f'check_impressions',
    sql=sql_count_table_rows.format(project=project_name,
                                    dataset=dataset_name,
                                    table=impressions_table_name),
    use_legacy_sql=False,
    dag=dag,
)

create_regions_table = BigQueryOperator(
    task_id='regions_table_create',
    sql=sql_regions_table.format(staging_table_path=staging_table_path),
    use_legacy_sql=False,
    destination_dataset_table=(
        f'{project_name}.{dataset_name}.{regions_table_name}'
    ),
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)

check_regions = BigQueryCheckOperator(
    task_id=f'check_regions',
    sql=sql_count_table_rows.format(project=project_name,
                                    dataset=dataset_name,
                                    table=regions_table_name),
    use_legacy_sql=False,
    dag=dag,
)

delete_staging = BigQueryTableDeleteOperator(
    task_id='delete_staging',
    deletion_dataset_table=(
        f'{project_name}.{dataset_name}.{staging_table_name}'
    ),
    dag=dag
)

load_guardian_fb_users_csv = GoogleCloudStorageToBigQueryOperator(
    task_id='load_guardian_fb_users_csv',
    bucket='europe-west2-data-warehouse-6d0163ac-bucket',
    source_objects=['dags/guardian_facebook_users_2018.csv'],
    skip_leading_rows=1,
    destination_project_dataset_table=(
        f'{dataset_name}.{guardian_fb_users_table_name}'
    ),
    schema_fields=[
        {'name': 'age_group', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'population', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

load_ons_uk_populations_csv = GoogleCloudStorageToBigQueryOperator(
    task_id='load_ons_uk_populations_csv',
    bucket='europe-west2-data-warehouse-6d0163ac-bucket',
    source_objects=[
        'dags/ons_uk_population_estimates_from_2016_for_2019.csv'
    ],
    skip_leading_rows=1,
    destination_project_dataset_table=(
        f'{dataset_name}.{ons_uk_pops_table_name}'
    ),
    schema_fields=[
        {'name': 'gender', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'age_group', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'population', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

transform_guardian = BigQueryOperator(
    task_id='transform_guardian',
    sql=sql_transform_guardian_table.format(
        project=project_name,
        dataset=dataset_name,
        table=guardian_fb_users_table_name,
    ),
    use_legacy_sql=False,
    destination_dataset_table=(
        f'{project_name}.{dataset_name}.{guardian_processed_table_name}'
    ),
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)
transform_ons = BigQueryOperator(
    task_id='transform_ons',
    sql=sql_transform_ons_table.format(
        project=project_name,
        dataset=dataset_name,
        table=ons_uk_pops_table_name,
    ),
    use_legacy_sql=False,
    destination_dataset_table=(
        f'{project_name}.{dataset_name}.{ons_processed_table_name}'
    ),
    write_disposition='WRITE_TRUNCATE',
    dag=dag
)

check_guardian = BigQueryCheckOperator(
    task_id='check_transformed_guardian',
    sql=sql_count_table_rows.format(
        project=project_name,
        dataset=dataset_name,
        table=guardian_processed_table_name,
    ),
    use_legacy_sql=False,
    dag=dag
)

check_ons = BigQueryCheckOperator(
    task_id='check_transformed_ons',
    sql=sql_count_table_rows.format(
        project=project_name,
        dataset=dataset_name,
        table=ons_processed_table_name,
    ),
    use_legacy_sql=False,
    dag=dag
)

delete_original_guardian = BigQueryTableDeleteOperator(
    task_id='delete_original_guardian',
    deletion_dataset_table=(
        f'{project_name}.{dataset_name}.{guardian_fb_users_table_name}'
    ),
    dag=dag
)

delete_original_ons = BigQueryTableDeleteOperator(
    task_id='delete_original_ons_table',
    deletion_dataset_table=(
        f'{project_name}.{dataset_name}.{ons_uk_pops_table_name}'
    ),
    dag=dag
)


create_dataset >> load_guardian_fb_users_csv
create_dataset >> load_ons_uk_populations_csv
create_dataset >> get_ads_and_upload_to_staging >> check_staging_table

check_staging_table >> create_ads_table >> check_ads
check_staging_table >> create_spends_table >> check_spends
check_staging_table >> create_demograph_table >> check_demograph
check_staging_table >> create_impressions_table >> check_impressions
check_staging_table >> create_regions_table >> check_regions

check_ads >> delete_staging
check_spends >> delete_staging
check_demograph >> delete_staging
check_impressions >> delete_staging
check_regions >> delete_staging

load_guardian_fb_users_csv >> transform_guardian >> check_guardian
check_guardian >> delete_original_guardian

load_ons_uk_populations_csv >> transform_ons >> check_ons
check_ons >> delete_original_ons
