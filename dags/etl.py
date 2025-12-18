from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json


## Define the DAG
with DAG(
    dag_id= 'nasa_apod_etl',
    start_date= days_ago(1),
    schedule= '@daily',
    catchup= False,

) as dag:
    
    ## Step 1: Create the table if it doesn't exist

    @task
    def create_table():
        ## Intialize Postgres hook
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

        ## Create SQL table query
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE
            media_type VARCHAR(50)
        );"""

        ## Execute the create table query
        postgres_hook.run(create_table_query)

    ## Step 2: Extract the NASA API Data(APOD)-Astronomy Picture of the Day[Extract pipeline]
    ## Sample API url: https://api.nasa.gov/planetary/apod?api_key=DEMO_KEY
    extract_apod_data = SimpleHttpOperator(
        task_id='extract_apod_data',
        http_conn_id='nasa_api',## Connection ID created in Airflow UI
        endpoint='planetary/apod',## API endpoint
        method='GET',
        data= {'api_key': "{{conn.nasa_api.extra_dejson.api_key}}" },## API key from connection extra field
        response_filter=lambda response: response.json(), ## Convert response to JSON
    )
    ## Step 3: Transform the data 

    ## Step 4: Load the data into Postgres [Load pipeline]

    ## Step 5: Verify the data load by querying the Postgres table
    
    ## Step 6: Define task dependencies  

