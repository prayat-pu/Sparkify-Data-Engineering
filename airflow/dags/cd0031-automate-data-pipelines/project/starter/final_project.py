from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from final_project_operators.has_rows import HasRowsOperator
from final_project_operators.drop_all_tables_operators import DropALLTablesRedShift
from udacity.common import final_project_sql_statements
from airflow.models import Variable
from airflow.secrets.metastore import MetastoreBackend
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta



default_args = {
    'owner': 'udacity',
    'depends_on_past':False,
    'retries':3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry':False,
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():

    sql_query = final_project_sql_statements.SqlQueries()

    start_operator = DummyOperator(task_id='Begin_execution')

    s3_bucket = Variable.get("s3_bucket")
    log_data = Variable.get("log_data")
    log_json_path = Variable.get("log_json_path")
    song_data = Variable.get("song_data")


    # delete previous table
    drop_previous_table_task = DropALLTablesRedShift(
        task_id='drop_all_previous_tables',
        redshift_conn_id="redshift",
        table_list=["staging_events","staging_songs","songplays","users","songs","artists","time"]
        )

    # create table if not exist task
    #------------------------------------------------------------------#
    # create staging_events table if not exist task


    create_stating_events_table_task=PostgresOperator(
        task_id='create_staging_events_table',
        postgres_conn_id="redshift",
        sql=sql_query.staging_events_table_create,
    )

    # create staging_songs table if not exist task
    create_staging_songs_table_task=PostgresOperator(
       task_id="create_staging_songs_table",
        postgres_conn_id="redshift",
        sql=sql_query.staging_songs_table_create,
    )

    # create songplays table if not exist task
    create_songplay_table_task=PostgresOperator(
       task_id="create_songplay_table",
        postgres_conn_id="redshift",
        sql=sql_query.songplay_table_create
    )

    # create users table if not exist task
    create_users_table_task=PostgresOperator(
       task_id="create_user_table",
        postgres_conn_id="redshift",
        sql=sql_query.user_table_create
    )

    # create songs table if not exist task
    create_songs_table_task=PostgresOperator(
       task_id="create_songs_table",
        postgres_conn_id="redshift",
        sql=sql_query.song_table_create
    )

    # create artists table if not exist task
    create_artists_table_task=PostgresOperator(
       task_id="create_artists_table",
        postgres_conn_id="redshift",
        sql=sql_query.artist_table_create
    )

    # create time table if not exist task
    create_time_table_task=PostgresOperator(
       task_id="create_time_table",
        postgres_conn_id="redshift",
        sql=sql_query.time_table_create
    )

    #------------------------------------------------------------------#

    # run copy from staging are to redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket=s3_bucket,
        s3_key=log_data,
        json_path=log_json_path,
        sql=sql_query.staging_events_copy,
        operation='append-only'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket=s3_bucket,
        s3_key=song_data,
        sql=sql_query.staging_songs_copy,
        operation='append-only'
    )


    check_staging_events = HasRowsOperator(
        task_id="check_staging_events_data",
        redshift_conn_id="redshift",
        table="staging_events"
    )

    check_staging_songs = HasRowsOperator(
        task_id="check_staging_songs_data",
        redshift_conn_id="redshift",
        table="staging_songs"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="songplays",
        sql=sql_query.songplay_table_insert,
        operation='append-only'
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users",
        sql=sql_query.user_table_insert,
        operation='append-only'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        sql=sql_query.song_table_insert,
        operation='append-only'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artists",
        sql=sql_query.artist_table_insert,
        operation='append-only'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time",
        sql=sql_query.time_table_insert,
        operation='append-only'
    )

    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE user_id is null", 'expected_result': 0, 'table':'users'},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE song_id is null", 'expected_result': 0, 'table': 'songs'},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE songplay_id is null", 'expected_result': 0, 'table':'songplays'},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artist_id is null", 'expected_result': 0, 'table':'artists'},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0, 'table':'time'},
    ]

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        table_list=dq_checks,
    )

    # run create staging and data warehouse area tasks
    start_operator >> drop_previous_table_task >> create_stating_events_table_task >> stage_events_to_redshift >> check_staging_events 
    
    drop_previous_table_task >> create_staging_songs_table_task >> stage_songs_to_redshift >> check_staging_songs

    drop_previous_table_task >> create_songplay_table_task
    
    drop_previous_table_task >> create_users_table_task

    drop_previous_table_task >> create_songs_table_task

    drop_previous_table_task >> create_artists_table_task

    drop_previous_table_task >> create_time_table_task

    # load data in to fact table
    check_staging_songs >> load_songplays_table

    check_staging_events >> load_songplays_table

    # load data into dimension table
    load_songplays_table >> load_user_dimension_table

    load_songplays_table >> load_song_dimension_table

    load_songplays_table >> load_artist_dimension_table

    load_songplays_table >> load_time_dimension_table


    # run quality test check
    load_user_dimension_table >> run_quality_checks

    load_song_dimension_table >> run_quality_checks

    load_artist_dimension_table >> run_quality_checks

    load_time_dimension_table >> run_quality_checks





final_project_dag = final_project()