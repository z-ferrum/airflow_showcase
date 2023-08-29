# A script to programmatically add a snowflake connection to airflow w/o UI
# yet still have access to SnowflakeOperator in DAGs

from airflow import models
from airflow.settings import Session
from configparser import ConfigParser

def create_snowflake_connection():  # Could be part of a larger script that creates all connections at once
    
    config = ConfigParser()  # not to expose credentials in the code
    config.read('config.ini')
    snowflake_login = config['snowflake']['login']
    snowflake_password = config['snowflake']['password']

    new_conn = models.Connection(
        conn_id='my_snowflake_conn_TEST1',
        conn_type='snowflake',
        host='ZBKUXJE-HE85214.snowflakecomputing.com',
        login=snowflake_login,
        password=snowflake_password,
        schema='RAW',
        extra='{"account": "ZBKUXJE-HE85214", \
              "warehouse": "COMPUTE_WH",      \
               "database": "SHOWCASE",        \
                   "role": "ACCOUNTADMIN"}'
    )

    session = Session()

    if not (session.query(models.Connection).filter(models.Connection.conn_id == new_conn.conn_id).first()):  # check if connection already exists
                                                                                                              # Can be run multiple times without creating duplicate entries.
        session.add(new_conn)
        session.commit()
        print(f"Connection {new_conn.conn_id} added.")
    else:
        print(f"Connection {new_conn.conn_id} already exists.")

if __name__ == "__main__":  # to be able to import functions from is file w/o running them when importing
    create_snowflake_connection()