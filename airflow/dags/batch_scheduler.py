# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta


schedule_interval = timedelta(days=1)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 4), # yyyy, mm, dd
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2020, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': callback_function,
    # 'on_success_callback': callback_function,
    # 'on_retry_callback': callback_function,
    # 'trigger_rule': u'all_success'
}

# [START instantiate_dag]
dag = DAG (
    dag_id = 'dws_batch_scheduler',
    default_args = default_args,
    description = 'DAG for the Spark Batch Job',
    schedule_interval = schedule_interval
)
# [END instantiate_dag]


# Step 1. Extract data from the GDELT website and load to AWS S3
dws_extract_job = BashOperator (
    task_id = "GDELT_to_S3_task",
    bash_command = "$HOME/data-warehouse-solution/ingestion/run_download.sh --schedule",
    dag = dag
)


# Step 2. Preprocess the data and load it into the database
dws_spark_job = BashOperator (
    task_id = 'etl_spark_task',
    depends_on_past = False,
    bash_command = "$HOME/data-warehouse-solution/spark/run_spark.sh --schedule",
    dag = dag
)


# [START documentation]
dag.doc_md = __doc__

dws_extract_job.doc_md = """\
#### Task Documentation
'dws_extract_job' task extracts GDELT data from the website once per day
and saves the extracted data on AWS S3, unzips it there for further processing
"""
# [END documentation]

# set dependencies
dws_extract_job >> dws_spark_job

