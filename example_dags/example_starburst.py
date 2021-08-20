from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy.orm.base import DEFAULT_MANAGER_ATTR
from starburst_plugin.operators.starburst import StarburstOperator

# These args will get passed on to each operator
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020,4,13)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# [START dag]
with DAG(
    'Starburst_Example_DAGs',
    default_args=default_args,
    description='Querying Starburst Enterprise',
    schedule_interval=timedelta(days=1),
    template_searchpath='/home/airflow/dags/include/',
    start_date=days_ago(2)
) as dag:

    # [START howto_operator_starburst]
    # Query Starburst using Jinja templated file store in the include folder
    starburst_template_file_query = StarburstOperator(
        task_id='starburst_template_file_query',
        sql='include/query.sql',
        autocommit=True,
        parameters=None,
        xcom_push=True,
        params={'catalog':'tpch'},
        dag=dag
    )

    #Execute a single query to Starburst
    starburst_sql_query = StarburstOperator(
        task_id='starburst_sql_query',
        sql='show schemas from tpch;',
        autocommit=True,
        parameters=None,
        xcom_push=True,
        dag=dag
    )
  
    #Execute a series of queries to Starburst
    starburst_sql_query_list = StarburstOperator(
        task_id='starburst_sql_query_list',
        sql="show catalogs;show schemas from tpch;",
        autocommit=True,
        parameters=None,
        xcom_push=False,
        dag=dag
    )

    # [END howto_operator_starburst

    # [END dag]

    
