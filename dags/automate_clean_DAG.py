'''
=================================================
This program is designed to automate the transformation and loading of data from PostgreSQL to ElasticSearch. 
The dataset used pertains to fraud data in multinational financial group during the year 2020.
=================================================
'''


from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine

from elasticsearch import Elasticsearch 

# postgres credential
db_name = 'airflow'
user_name = 'airflow'
password = 'airflow'
host='postgres'

# connection to postgres 
postgres_url = f'postgresql+psycopg2://{user_name}:{password}@{host}/{db_name}'
engine = create_engine(postgres_url)
conn = engine.connect()

def load_to_postgres():
    '''
    Load CSV data into a PostgreSQL database.

    This function reads data from a CSV file located at '/opt/airflow/data/{dataset_name}.csv' 
    and loads it into a PostgreSQL database table named '{table}'. 
    The table is replaced if it already exists.
    
    step: 
    1. Read the CSV file into a pandas DataFrame.
    2. Use the DataFrame's `to_sql` method to write the data to the '{table}'
       table in the PostgreSQL database.
    '''
    df = pd.read_csv('/opt/airflow/data/fraud_data.csv')
    df.to_sql('fraud_data_table', conn, index=False, if_exists='replace')

def fetch_from_postgres():
    '''
    Fetch data from a PostgreSQL database and save it to a CSV file.

    This function executes a SQL query to select all data from the '{table}' 
    table in the PostgreSQL database, reads the data into a pandas DataFrame, 
    and then writes the DataFrame to a CSV file located at '/opt/airflow/dags/{dataset_name_raw}.csv'.
    
    Steps:
    1. Execute a SQL query to fetch all records from the '{table}' table.
    2. Load the query result into a pandas DataFrame.
    3. Save the DataFrame to a CSV file with the specified path, using a comma as the separator and without including the DataFrame index.

    '''
    df = pd.read_sql_query('SELECT * FROM fraud_data_table', conn)
    df.to_csv('/opt/airflow/dags/fraud_data_raw.csv', sep=',', index=False)
    
def clean_raw_data():
    '''
    Clean raw data from a CSV file:
    
    Steps:
    1. Remove duplicate rows.
    2. Drop rows with missing values.
    3. Rename columns to lowercase with underscores instead of spaces.
    4. Convert 'date' column to datetime format ('%d-%b-%y').
    5. Strip currency symbol (£) from 'amount' column and convert to float.
    6. Replace 'gender' codes ('M' and 'F') with full names ('Male' and 'Female').
    7. Convert 'age' column values to integers by flooring.
    8. Replace 'fraud' codes (0 and 1) with labels ('Legitimate' and 'Fraud').
    '''
    # Read the raw CSV data into a DataFrame
    df = pd.read_csv('/opt/airflow/dags/fraud_data_raw.csv')

    df.drop_duplicates(inplace=True)

    df.dropna(inplace=True)

    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    
    df['date'] = pd.to_datetime(df['date'], format='%d-%b-%y')
    
    df['amount'] = df['amount'].str.replace('£', '').astype(float)
    
    df['gender'] = df['gender'].replace({'M': 'Male', 'F': 'Female'})
    
    df['age'] = np.floor(df['age']).astype(int)
    
    df['bank'] = df['bank'].replace('Barlcays' , 'Barclays')
    
    df['fraud'] = df['fraud'].replace({0: 'Legitimate', 1: 'Fraud'})
    
    
    df.to_csv('/opt/airflow/dags/fraud_data_clean.csv', sep=',', index=False)
    
def upload_to_elasticsearch():
    '''
    Upload data from a CSV file to Elasticsearch.

    This function connects to an Elasticsearch instance running at 'http://elasticsearch:9200/',
    reads data from '/opt/airflow/data/{dataset_name_clean}.csv' into a pandas DataFrame,
    and iterates through each row to index it into the 'fraud_data' index in Elasticsearch.

    Each row is converted to a dictionary (doc) and indexed with a unique id (i+1, where i is the row index).
    '''
    es = Elasticsearch('http://elasticsearch:9200/')    
    df = pd.read_csv('/opt/airflow/dags/fraud_data_clean.csv')
    
    for i, r in df.iterrows():
        doc = r.to_dict()
        res = es.index(index='fraud_data', id=i+1, body=doc)
        print(f'Response from Elasticsearch: {res}')
    
    
    
# define default arguments that will be inherited by all tasks in the DAG
default_args = {
    'owner': 'devi_nirfana',
    'start_date': dt.datetime(2024, 6, 18)
}

# define the DAG with a specific schedule and description.
with DAG('Automate_cleaning', 
        default_args=default_args,
        description='Automate_cleaning',
        schedule_interval='30 6 * * *'  # Schedule to run every day at 06:30 
        ) as dag:
    
    # load raw data into PostgreSQL using a Python function
    load_raw = PythonOperator(task_id='load_raw_data', 
                              python_callable = load_to_postgres)
    
    # fetch raw data from PostgreSQL and save it to a CSV file using a Python function.
    fetch_raw = PythonOperator(task_id='fetch_raw_data', 
                               python_callable = fetch_from_postgres)
    
    # clean raw data and save it to a CSV file using a Python function.
    clean_data = PythonOperator(task_id='clean_raw_data', 
                               python_callable = clean_raw_data)
    
    # upload clean data to elasticsearch using a Python function.
    upload_data = PythonOperator(task_id='upload_to_elasticsearch', 
                               python_callable = upload_to_elasticsearch)
    
# define the task dependencies
load_raw >> fetch_raw >> clean_data >> upload_data