import os
import pendulum
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from SQL.sql_statements import *

start_date = datetime(2018, 11, 1)
end_date = datetime(2018, 11, 30)

s3_bucket = "udacity-airflow-bucket"
events_s3_path = "log-data"
songs_s3_path = "song-data/A/A/"
log_json_file = 'log_json_path.json'

default_args = {
    'owner': 'Nhannt70',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'catchup': False,
    'email_on_retry': False
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def main_dag():
    """
    Create dag to load and transform data in Redshift
    """
    start_operator = DummyOperator(task_id='begin_execution')

    create_redshift_tables = PostgresOperator(
        task_id = 'create_tables',
        postgres_conn_id = "redshift",
        sql = 'SQL/DDL/create_tables.sql'
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket=s3_bucket,
        s3_key=events_s3_path,
        log_json_file=log_json_file
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id = 'stage_songs',
        table = "staging_songs",
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        s3_bucket = s3_bucket,
        s3_key = songs_s3_path
    )

    load_songplays_table = LoadFactOperator(
        task_id = 'load_songplays_fact_table',
        redshift_conn_id = "redshift",
        sql_query = songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id = 'load_user_dim_table',
        redshift_conn_id = "redshift",
        sql_query = user_table_insert,
        table = 'users',
        truncate = True
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id = 'load_song_dim_table',
        redshift_conn_id = "redshift",
        sql_query = song_table_insert,
        table = 'songs',
        truncate = True
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='load_artist_dim_table',
        redshift_conn_id="redshift",
        sql_query = artist_table_insert,
        table = 'artists',
        truncate = True
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id = 'load_time_dim_table',
        redshift_conn_id = "redshift",
        sql_query = time_table_insert,
        table = 'time',
        truncate = True
    )

    run_quality_checks = DataQualityOperator(
        task_id = 'data_quality_check',
        redshift_conn_id = "redshift",
        tables = ["songplays", "users", "songs", "artists", "time"]
    )

    end_operator = DummyOperator(task_id='end_execution')

    start_operator >> create_redshift_tables >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> \
    run_quality_checks >> end_operator


final_dag = main_dag()
