from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from airflow.utils.dates import days_ago
from sqlalchemy.orm.base import DEFAULT_MANAGER_ATTR
from starburst_plugin.operators.starburst import StarburstOperator
from starburst_plugin.operators.starburst_s3 import Starburst_S3Operator

from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.models import Variable





# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020,4,13)
}

# [START dag]
with DAG(
    'Starburst_s3',
    default_args=default_args,
    description='Downloading Tables from Starburst via s3',
    schedule_interval=timedelta(days=1),
    concurrency=5,
    template_searchpath='/home/airflow/dags/include/',
    start_date=days_ago(2)
) as dag:
    

    starburst_query = Starburst_S3Operator(
                task_id = 'starburst_query',
                sql="{{var.value.sql}}", 
                s3_location="{{var.value.s3_location}}",
                dag=dag);
    

    decompress = BashOperator(
        task_id='decompress',
        bash_command="gunzip -cr {{var.value.QUERY_DESTINATION}}/{{ti.xcom_pull(task_ids='starburst_query',key='prefix')}} > {{var.value.QUERY_DESTINATION}}/query-{{ti.xcom_pull(task_ids='starburst_query',key='uid')}}",
        dag = dag
        )

    drop_temp_table = StarburstOperator(
                    task_id = 'drop_temp_table',
                    sql='DROP TABLE hive.default.temp',
                    dag=dag)

    drop_s3_folder = S3DeleteObjectsOperator(
                    task_id='drop_s3_folder',
                    bucket = "{{ti.xcom_pull(task_ids='starburst_query',key='bucket')}}",
                    prefix = "{{ti.xcom_pull(task_ids='starburst_query',key='prefix')}}",
                    aws_conn_id='aws_default')
    
    starburst_query >> decompress
    starburst_query >> drop_temp_table
    starburst_query >> drop_s3_folder