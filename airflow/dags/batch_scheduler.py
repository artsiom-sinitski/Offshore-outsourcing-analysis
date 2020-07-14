# airflow related
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta


schedule_interval = timedelta(minutes=20)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    #datetime(2020, 6, 4), # yyyy, mm, dd
    'start_date': datetime.now() - schedule_interval, 
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

# Step 1:
# Extract raw data from the GDELT website and load into AWS S3 object storage
raw_data_extraction_job = BashOperator (
    task_id = "raw_data_extraction_task",
    bash_command = "$HOME/data-warehouse-solution/ingestion/run_download.sh --schedule",
    dag = dag
)

# Step 2:
# Retrieve raw data from S3, preprocess and load it into central storage
centralstorage_etl_job = BashOperator (
    task_id = 'centralstorage_etl_task',
    depends_on_past = False,
    bash_command = "$HOME/data-warehouse-solution/spark/run_centralstorage_etl.sh --schedule",
    dag = dag
)

# Step 3:
# Retrieve data from central storage, preprocess and load it into Protests Datamart database.
datamart_etl_job = BashOperator (
    task_id = 'datamart_etl_task',
    depends_on_past = False,
    bash_command = "$HOME/data-warehouse-solution/spark/run_datamart_etl.sh --schedule",
    dag = dag
)

# [START documentation]
dag.doc_md = __doc__

raw_data_extraction_job.doc_md = """\
#### Task Documentation
'raw_data_extraction_job' task extracts GDELT data from the website once per day
and saves the extracted data in AWS S3, unzips it there for further processing.
"""
centralstorage_etl_job.doc_md = """\
#### Task Documentation
'centralstorage_etl_job' task gets the GDELT raw date from AWS S3 storage,
enforces the GDELT schema onto the raw date and moves the data to Central Storage.
"""
datamart_etl_job.doc_md = """\
#### Task Documentation
'datamart_etl_job' task gets the GDELT event and mention data from
the central storage, and populates Datamart tables according to the 'star' schema.
"""
# [END documentation]

# set task dependencies
raw_data_extraction_job >> centralstorage_etl_job >> datamart_etl_job

