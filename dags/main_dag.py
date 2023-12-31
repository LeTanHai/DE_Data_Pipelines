from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries
from helpers.data_quality_check import DataQualityCheck
from subdag_load_dim_table import load_dimension_table_subdag

S3_BUCKET = 'proj3-bucket'
S3_SONG_KEY = 'song-data'
S3_LOG_KEY = 'log-data/{execution_date.year}/{execution_date.month}'
LOG_JSON_PATH = f's3://{S3_BUCKET}/log_json_path.json'
REGION = 'us-west-2'
AWS_CREDENTIALS_ID = 'aws_credentials'
REDSHIFT_CONN_ID = 'redshift'
DAG_ID = 'proj3-dag'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date':datetime(2018, 11, 1),
    'email_on_retry': False,
    'catchup': False,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table='staging_events',
    s3_bucket=S3_BUCKET,
    s3_key=S3_LOG_KEY,
    region=REGION,
    truncate=True,
    data_format=f"JSON '{LOG_JSON_PATH}'",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs_table',
    dag=dag,
    redshift_conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table='staging_songs',
    s3_bucket=S3_BUCKET,
    s3_key=S3_SONG_KEY,
    region=REGION,
    truncate=True,
    data_format="JSON 'auto'",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    sql=SqlQueries.songplay_table_insert,
    table='songplays',
    truncate=True,
)

load_dimension_table = SubDagOperator(
    subdag=load_dimension_table_subdag(
        parent_dag_name=DAG_ID,
        task_id='Load_dim_table_subdag',
        default_args=default_args,
        postgres_conn_id=REDSHIFT_CONN_ID,
        sql_queries=[
            SqlQueries.user_table_insert,
            SqlQueries.song_table_insert,
            SqlQueries.artist_table_insert,
            SqlQueries.time_table_insert,
        ],
        tables=['users', 'songs', 'artists', 'time'],
        truncate_flags=[True]*4,
    ),
    dag=dag,
    task_id='Load_dim_table_subdag',
)

tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    postgres_conn_id=REDSHIFT_CONN_ID,
    tests=[DataQualityCheck.test(table) for table in tables],
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_dimension_table
load_dimension_table >> run_quality_checks
run_quality_checks >> end_operator