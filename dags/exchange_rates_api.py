from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import XCom
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.http.sensors.http import HttpSensor
# from airflow.providers.http.operators.http import SimpleHttpOperator  # used in previous dag versions

import json, requests



# INPUTS

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}



# FUNCTIONS

def fetch_exchange_rate_data_func():  # Function of the fetch_exchange_rate_data_task
    response = requests.get('https://api.exchangerate-api.com/v4/latest/EUR')
    if response.status_code == 200:
        return response.json()  # is pullable w/o specifying key


def transform_data_func(**kwargs):  # Function of the transform_data_task
    ti = kwargs['ti']
    fetched_data = ti.xcom_pull(task_ids='fetch_exchange_rate_data')

    if fetched_data:
        try:
            # Extracts data
            exchange_date = fetched_data.get('date', 'unknown_date')
            usd_rate = fetched_data['rates'].get('USD', 'N/A')
            gbp_rate = fetched_data['rates'].get('GBP', 'N/A')

            # Creates a transformed dictionary
            transformed_data = {
                'date': exchange_date,
                'USD': usd_rate,
                'GBP': gbp_rate
            }

            # Pushes the data to XCom
            ti.xcom_push(key='transformed_data', value=transformed_data)

        except (json.JSONDecodeError, KeyError, TypeError):
            raise ValueError("Error during data transformation")
    else:
        raise ValueError("No data fetched from previous task")


def load_into_snowflake_func(**kwargs):  # Function of the load_into_snowflake_task
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data_task', key='transformed_data')

    # this sql is prone to duplicates, fixed in exchange_rates_api_decorated.py
    formatted_sql = f"""INSERT INTO SHOWCASE.RAW.XCHANGE_RATES (date, usd, gbp)
                        VALUES ('{transformed_data["date"]}', {transformed_data["USD"]}, {transformed_data["GBP"]});"""
    
    load_into_snowflake = SnowflakeOperator(
        task_id='load_into_snowflake',
        snowflake_conn_id='snowflake_conn',
        sql=formatted_sql,
        autocommit=True,
        dag=dag  # Pass the DAG context if needed
    )
    load_into_snowflake.execute(context=kwargs)  # Need to trigger the execution, since it's a nested task, not referenced by '<<'/'>>', skipped by scheduler


def clear_xcom_entries(**kwargs):
    ti = kwargs['ti']
    execution_date = kwargs['execution_date']
    task_instances = ti.get_dagrun().get_task_instances()
    task_ids = [task_instance.task_id for task_instance in task_instances]
    
    for task_id in task_ids:
        XCom.clear(
            dag_id=kwargs['dag'].dag_id,
            task_id=task_id,
            execution_date=execution_date,
        )



# DAGS

# Initialize the DAG
with DAG(
    'exchange_rates_api',
    default_args=default_args,
    description='An exchange rate data pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 8, 28),
    catchup=False,  # for the sake of simplicity, production pipelines should have this set to True
) as dag:

    # Task to check if the API is available
    api_available = HttpSensor(
        task_id='api_available',
        http_conn_id='exchange_rate_api_conn',
        endpoint='v4/latest/EUR',
        timeout=10,
        poke_interval=5,
        mode='poke'
    )

    # Task to fetch exchange rate data
    fetch_exchange_rate_data = PythonOperator(
        task_id='fetch_exchange_rate_data',
        python_callable=fetch_exchange_rate_data_func,
        provide_context=True,
    )

    # Task to transform the data
    # Is separated from fetch, but for large historical updates it might be better to combine them
    # To pass less data between tasks in XCom
    transform_data_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data_func,
        provide_context=True,
    )

    # Task to load data into Snowflake
    load_into_snowflake_task = PythonOperator(
        task_id='load_into_snowflake_task',
        python_callable=load_into_snowflake_func,
        provide_context=True,
    )

    clear_xcom = PythonOperator(
        task_id='cleanup_xcom_task',
        python_callable=clear_xcom_entries,
        provide_context=True,
        dag=dag,
    )

# Define task dependencies
    api_available >> fetch_exchange_rate_data >> transform_data_task >> load_into_snowflake_task >> clear_xcom.as_teardown() 
    # as_teardown() might be not the best solution if we want to keep XComs for debugging 
