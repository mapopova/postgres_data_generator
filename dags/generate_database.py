import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="database_generator",
    start_date=datetime.datetime(2023, 3, 3),
    schedule="@once",
    catchup=False,
) as dag:

    generate_ids = PostgresOperator(
        task_id="generate_ids",
        postgres_conn_id="postgres_local",
        sql="sql/gen_ids.sql"
    )
    
    generate_relationships = PostgresOperator(
        task_id="generate_relationships",
        postgres_conn_id="postgres_local",
        sql="sql/gen_relationships.sql"
    )
    
    generate_catalogs = BashOperator(
    	task_id="generate_catalogs",
    	bash_command='''~/pgfutter --pw 'postgres' --schema 'public' csv ~/projects/postgres_data_generator/states.csv
'''
    )
    
    fill_with_data = PostgresOperator(
        task_id="fill_with_data",
        postgres_conn_id="postgres_local",
        sql="sql/fill_with_data.sql"
    )
    
    generate_ids >> generate_relationships
    generate_relationships >> fill_with_data
    generate_catalogs >> fill_with_data
