'''
==========================
Milestone 3

Nama    : Betara Candra
Batch   : SBY 002

This program was created to automate the transformation and load of data from PostgreSQL to ElasticSearch as well as cleaning the dataset.
The dataset used is a dataset regarding car compare of horse power. 
==========================
'''

from elasticsearch import Elasticsearch 
import psycopg2 as db

import datetime as dt
from datetime import timedelta
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.operators.python import PythonOperator



def getdata():
    '''
    This function is use to get data from postgres where postgres load from csv
    '''
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"            # Make connection from postgre
    conn=db.connect(conn_string)

    df=pd.read_sql("select * from table_m3", conn)                                              # Read data 
                                            
    df.to_csv('/opt/airflow/data/P2M3_betara_candra_data_raw.csv')                              # Save raw file to folder data
    print("-------Data Saved------")

def clean():
    '''
    This function is use to clean data from dataset, and will save into clean_cars.csv
    '''
    df=pd.read_csv('/opt/airflow/data/P2M3_betara_candra_data_raw.csv')                         # Read dataset
    df.rename({'Unnamed: 0' :  'id'}, axis = 1, inplace= True)                                  # Change index to id, for gives unique in each data
    df.columns = df.columns.str.replace('.','_')                                                # Change string '.' to '_'
    df.columns = df.columns.str.replace(' ','_',)
    if df.isnull().sum().sum() > 0 :                                                            # Check missing value
        df.dropna(inplace=True)                                                                 # Drop missing value
    if df.duplicated().sum() > 0:                                                               # Check duplicated of data 
        df.drop_duplicates(inplace=True)                                                        # if there is duplicate it will be drop
    df.columns=[x.lower() for x in df.columns]                                                  # Change all name of columns to lower
   
    df.to_csv('/opt/airflow/data/P2M3_betara_candra_data_clean.csv', index= False)              # Save raw file

def posttoelastic (): 
    '''
    This function is use to push clean dataset to elastic
    '''
    df = pd.read_csv('/opt/airflow/data/P2M3_betara_candra_data_clean.csv')                      # Read clean dataset

    es = Elasticsearch("Elasticsearch")                                                          # Define elasticsearh                                                                                  # Test connection with ElasticSearch
    es.ping()
    for i,r in df.iterrows():
        doc = r.to_json()
        res = es.index(index='cleaning_dag',body=doc)

default_args = {
    'owner': 'candra',  
    'start_date': dt.datetime(2024, 1, 14, 6, 30, 0) - dt.timedelta(hours=7),                   # Start time to start data to elastic  
     

}


with DAG('cleaning_data',
         default_args=default_args,
         schedule_interval= '30 06 * * *',                                                      # Set Interval in 06.30
         catchup = False                                                                        # Set catchup to start DAG where you post late in airflow     
         ) as dag:
    # Define Task 1 Get function getdata
    FetchfromPostgresql = PythonOperator(task_id='getdata',
                                 python_callable=getdata)
    # Define Task 2 Get function cleandata
    DataCleaning = PythonOperator(task_id='clean',
                                 python_callable=clean)
    # Define Task 3 Get function posttoelastic
    PosttoElasticsearch = PythonOperator(task_id='posttoelastic',
                                  python_callable=posttoelastic)
    
    FetchfromPostgresql >> DataCleaning >> PosttoElasticsearch
    

