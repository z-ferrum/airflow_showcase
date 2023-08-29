from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task, dag
from airflow.models import XCom, DagRun, TaskInstance
from datetime import datetime, timedelta

import json, requests


# INPUTS

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
}


# DAGS

@dag(
    default_args=default_args,
    description='An exchange rate data pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 8, 28),
    catchup=False,
)
def exchange_rates_api_decorated():

    @task
    def fetch_data():
        response = requests.get('https://api.exchangerate-api.com/v4/latest/EUR')
        if response.status_code != 200:
            raise Exception('API not available')
        return response.json()

    @task
    def transform_data(fetched_data: dict):
        try:
            exchange_date = fetched_data.get('date', 'unknown_date')
            usd_rate = fetched_data['rates'].get('USD', 'N/A')
            gbp_rate = fetched_data['rates'].get('GBP', 'N/A')
            return {
                'date': exchange_date,
                'USD': usd_rate,
                'GBP': gbp_rate,
            }
        except (json.JSONDecodeError, KeyError, TypeError):
            raise ValueError("Error during data transformation")

    @task
    def load_into_snowflake(transformed_data: dict):
        formatted_sql = f"""INSERT INTO SHOWCASE.RAW.XCHANGE_RATES (date, usd, gbp)
                            VALUES ('{transformed_data["date"]}', {transformed_data["USD"]}, {transformed_data["GBP"]});"""

        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        snowflake_hook.run(formatted_sql, autocommit=True)

    @task
    def clear_xcom_entries():
        dag_id_to_clear = 'exchange_rates_api_decorated'
        dug_run = DagRun.find(dag_id=dag_id_to_clear, state='running')
        
        if dug_run:

            task_instances = dug_run[0].get_task_instances()
            
            for task_instance in task_instances:
                XCom.clear(
                    dag_id=dag_id_to_clear,
                    task_id=task_instance.task_id,
                    execution_date=task_instance.execution_date
                )

    
    #   DAG EXECUTION   ######################

    fetch_data_op = fetch_data()
    transformed_data_op = transform_data(fetch_data_op)
    load_into_snowflake_op = load_into_snowflake(transformed_data_op)
    clear_xcom_entries_op = clear_xcom_entries()

    fetch_data_op >> transformed_data_op >> load_into_snowflake_op >> clear_xcom_entries_op.as_teardown()


# DAG CALL

exchange_rate_dag = exchange_rates_api_decorated()