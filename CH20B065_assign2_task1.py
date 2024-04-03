from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


import os
import random
import requests
from bs4 import BeautifulSoup
from zipfile import ZipFile


## Task 1
# Function to read the data from the website
def reading_data(year,num_files,**context):
    url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/'

    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    csv_links = [a['href'] for a in soup.find_all('a', href=True) if a['href'].endswith('.csv')]

    files = random.sample(csv_links,num_files)

    # Saving the data on the local machine
    for file in files:
        newurl = os.path.join(url,file)
        res = requests.get(newurl)
        open(file,'wb').write(res.content)

    # Zipping the files
     
    with ZipFile('/root/airflow/DAGS/data.zip','w') as zip:
        for file in files:
            zip.write(file)


# DAG for task 1
dag1 = DAG(
    dag_id= 'getting_data',
    schedule_interval='@daily',
    default_args={
            'owner': 'first_task',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2024, 1, 1),
        },
    catchup=False
)

read_data = PythonOperator(
        task_id='get_data',
        python_callable=reading_data,
        provide_context=True,
        op_kwargs={'year':2020,'num_files':3,'name':'first_task'},
        dag=dag1
    )

read_data

