# A script to programmatically add a snowflake connection to airflow w/o UI
# yet still have access to SnowflakeOperator in DAGs

from airflow import models
from airflow.settings import Session
from configparser import ConfigParser

def create_snowflake_connection():
    
    config = ConfigParser()
    config.read('config.ini')
    snowflake_login = config['snowflake']['login']
    snowflake_password = config['snowflake']['password']

    new_conn = models.Connection(
        conn_id='my_snowflake_conn_2',
        conn_type='snowflake',
        host='ZBKUXJE-HE85214.snowflakecomputing.com',
        login=snowflake_login,
        # login='frake',
        password=snowflake_password,
        # database='SHOWCASE',
        schema='RAW',
        extra='{"account": "ZBKUXJE-HE85214", \
              "warehouse": "COMPUTE_WH",      \
               "database": "SHOWCASE",        \
                   "role": "Admin"}'
    )

    session = Session()

    if not (session.query(models.Connection).filter(models.Connection.conn_id == new_conn.conn_id).first()):
        session.add(new_conn)
        session.commit()
        print(f"Connection {new_conn.conn_id} added.")
    else:
        print(f"Connection {new_conn.conn_id} already exists.")

if __name__ == "__main__":
    create_snowflake_connection()