# A script to programmatically add an exchange rate api connection to airflow w/o UI

from airflow.models import Connection
from airflow import settings
from airflow.utils.db import create_session

# Create a new connection object
new_conn = Connection(
    conn_id='exchange_rate_api_conn',
    conn_type='http',
    host='https://api.exchangerate-api.com',
)

# Use a session to add the new connection to the database
with create_session() as session:  # Session management is handled by Airflow's create_session, 
                                   # reducing the chances of session-related issues
    session.add(new_conn)          # If the connection exists, it will throw and error of DB unique contraint violation for conn_id
    session.commit()
