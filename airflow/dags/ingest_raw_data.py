from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os
import logging
from psycopg2 import OperationalError
from typing import Final

sql_folder: Final[str] = os.path.join(os.path.dirname(__file__), 'sql')

call_file_path: Final[str] = "/opt/airflow/data/raw_data/call_dataset.csv"
clv_file_path: Final[str] = "/opt/airflow/data/raw_data/clv_dataset.csv"
customer_file_path: Final[str] = "/opt/airflow/data/raw_data/customer_dataset.csv"
lead_file_path: Final[str] = "/opt/airflow/data/raw_data/lead_dataset.csv"
marketing_costs_file_path: Final[str] = "/opt/airflow/data/raw_data/marketing_costs_dataset.csv"
sales_costs_file_path: Final[str] = "/opt/airflow/data/raw_data/sales_cost_dataset.csv"

# Define your default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'ingest_raw_data',
    default_args=default_args,
    description='Ingest raw data into company_dw',
    schedule_interval=None,  # Set to None for manual trigger
)

def run_psql(sql_query: str):
    hook = PostgresHook(postgres_conn_id='company_dw')
    
    try:
        # Establish connection and create cursor
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Execute SQL query
        logging.info(f"Executing the following query: {sql_query}")
        cursor.execute(sql_query)
        conn.commit()

    except OperationalError as e:
        # Handle database connection errors or other operational issues
        logging.error(f"Error executing SQL query: {e}")
        conn.rollback()  # In case of an error, rollback the transaction

    except Exception as e:
        # Handle any other exceptions
        logging.error(f"An unexpected error occurred: {e}")
        conn.rollback()  # Rollback transaction on any other error

    finally:
        # Ensure that cursor and connection are closed
        logging.info("Success")
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def create_schema_if_not_exists():
    run_psql("CREATE SCHEMA IF NOT EXISTS raw;")


def delete_existing_table(table_name):
    run_psql(f"DROP TABLE IF EXISTS raw.{table_name} CASCADE;")

def ingest_csv_to_postgres(raw_file_path, table_name, table_sql_create, table_sql_copy):
    """
    Upload a raw csv file into a warehouse table
    """
    # Check the existence of the file first to see
    # if it makes sense to even begin the transaction
    if not os.path.exists(raw_file_path):
        logging.error(f"CSV file {raw_file_path} does not exist!")
        raise FileNotFoundError(f"CSV file {raw_file_path} not found.")
    
    run_psql(f"DROP TABLE IF EXISTS raw.{table_name} CASCADE;")
    try:
        with open(table_sql_create, 'r') as file:
            create_table_query = file.read()
        logging.info(f"Create table query from file: {table_sql_create}")

    except FileNotFoundError:
        logging.error(f"Create SQL not found: {table_sql_create}")
        raise
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    run_psql(create_table_query)


    try:
        # Read the content of the SQL file
        with open(table_sql_copy, 'r') as file:
            copy_sql = file.read()

        # Log the SQL content for debugging
        logging.info(f"COPY SQL loaded from file: {table_sql_copy}")

    except FileNotFoundError:
        logging.error(f"COPY SQL file not found: {table_sql_copy}")
        raise
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


    hook = PostgresHook(postgres_conn_id='company_dw')
    try:
        # Establish connection and create cursor
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Open the CSV file and load data into the table
        with open(raw_file_path, 'r') as f:
            # Execute the COPY command
            cursor.copy_expert(sql=copy_sql, file=f)
            conn.commit()        
            logging.info(f"Successfully uploaded data from {raw_file_path} to raw.{table_name}.")

    except OperationalError as e:
        # Handle database connection errors or other operational issues
        logging.error(f"Error executing SQL query: {e}")
        conn.rollback()  # In case of an error, rollback the transaction
        raise

    except Exception as e:
        # Handle any other exceptions
        logging.error(f"An unexpected error occurred: {e}")
        conn.rollback()  # Rollback transaction on any other error
        raise

    finally:
        # Ensure that cursor and connection are closed
        logging.info("Success")
        if cursor:
            cursor.close()
        if conn:
            conn.close()



with dag:
    create_schema = PythonOperator(
        task_id='create_schema',
        python_callable=create_schema_if_not_exists
    )

    delete_call = PythonOperator(
        task_id='delete_call',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'call'},
    )

    delete_clv = PythonOperator(
        task_id='delete_clv',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'clv'},
    )

    delete_customer = PythonOperator(
        task_id='delete_customer',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'customer'},
    )

    delete_lead = PythonOperator(
        task_id='delete_lead',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'lead'},
    )

    delete_marketing_cost = PythonOperator(
        task_id='delete_marketing_costs',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'marketing_costs'},
    )

    delete_sales_cost = PythonOperator(
        task_id='delete_sales_costs',
        python_callable=delete_existing_table,
        op_kwargs={'table_name': 'sales_cost'},
    )



    ingest_call = PythonOperator(
        task_id='ingest_call',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'raw_file_path': call_file_path, 'table_name': 'call', 
                   'table_sql_create': f'{sql_folder}/create_call.sql', 
                   'table_sql_copy': f'{sql_folder}/copy_call.sql'},
    )

    ingest_clv = PythonOperator(
        task_id='ingest_clv',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'raw_file_path': clv_file_path, 'table_name': 'clv', 
                   'table_sql_create': f'{sql_folder}/create_clv.sql', 
                   'table_sql_copy': f'{sql_folder}/copy_clv.sql'},
    )

    ingest_customer = PythonOperator(
        task_id='ingest_customer',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'raw_file_path': customer_file_path, 'table_name': 'customer', 
                   'table_sql_create': f'{sql_folder}/create_customer.sql', 
                   'table_sql_copy': f'{sql_folder}/copy_customer.sql'},
    )

    ingest_lead = PythonOperator(
        task_id='ingest_lead',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'raw_file_path': lead_file_path, 'table_name': 'lead', 
                   'table_sql_create': f'{sql_folder}/create_lead.sql', 
                   'table_sql_copy': f'{sql_folder}/copy_lead.sql'},
    )

    ingest_marketing_costs = PythonOperator(
        task_id='ingest_marketing_costs',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'raw_file_path': marketing_costs_file_path, 'table_name': 'marketing_costs', 
                   'table_sql_create': f'{sql_folder}/create_marketing_costs.sql', 
                   'table_sql_copy': f'{sql_folder}/copy_marketing_costs.sql'},
    )

    ingest_sales_costs = PythonOperator(
        task_id='ingest_sales_costs',
        python_callable=ingest_csv_to_postgres,
        op_kwargs={'raw_file_path': sales_costs_file_path, 'table_name': 'sales_costs', 
                   'table_sql_create': f'{sql_folder}/create_sales_costs.sql', 
                   'table_sql_copy': f'{sql_folder}/copy_sales_costs.sql'},
    )

    create_schema >> [delete_clv, delete_customer, delete_lead, delete_marketing_cost, delete_sales_cost]
    delete_call >> ingest_call
    delete_clv >> ingest_clv
    delete_customer >> ingest_customer
    delete_lead >> ingest_lead
    delete_marketing_cost >> ingest_marketing_costs
    delete_sales_cost >> ingest_sales_costs