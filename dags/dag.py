from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.models import Variable
from helpers import SqlQueries

from subdags import load_dim_table_subdag

# Define set of default arguments
default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2021, 3, 5, 2, 0, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'retries_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry':False
}

# Instantiate a DAG object
dag = DAG('Udacity-DEND_Sparkify-Project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

# Begin execution
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Task - Stage events log to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    redshift_schema="public",
    destination_table="staging_events",
    s3_bucket=Variable.get('s3_bucket'),
    s3_key=Variable.get('s3_key_log'),
    format_as_json=Variable.get('s3_log_jsonpath')
)

# Task - Stage songs data to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    region="us-west-2",
    redshift_schema="public",
    destination_table="staging_songs",
    s3_bucket=Variable.get('s3_bucket'),
    s3_key=Variable.get('s3_key_songs')
)

# Task - Append data to songplays table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    load_type="append",
    redshift_schema="public",
    destination_table="songplays"
)

# Task - Truncate dimension tables then insert data
dims_insert_task = SubDagOperator(
    task_id='load_dimension_tables',
    subdag=load_dim_table_subdag(
        parent_dag_name='Udacity-DEND_Sparkify-Project',
        child_dag_name='load_dimension_tables',
        redshift_conn_id="redshift",
        load_type="truncate_insert",
        redshift_schema='public',
        default_args=default_args
    ),
    dag=dag,
)

# Task - Perform row count and null value checks against fact and dimension tables
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift"
)

# Stop execution
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Order of task dependencies
start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> dims_insert_task
dims_insert_task >> run_quality_checks
run_quality_checks >> end_operator