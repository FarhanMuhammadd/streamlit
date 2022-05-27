from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from modules.sheets import *
from modules.utility import *

args = {
    'owner': 'airflow',
    #'start_date': airflow.utils.dates.days_ago(2),
    'provide_context': True
}

dag = DAG('api_data_to_google_sheet', description='Scrapping data',
          schedule_interval='15 * * * *',
          start_date=datetime(2022, 5, 19), catchup=False, default_args = args)

get_api_data = PythonOperator(task_id='get_api_data',
                              python_callable=get_all_state_data, 
                              dag=dag)

transform_api_data = PythonOperator(task_id='transform_api_data', python_callable=transform_data , dag=dag)


upload_data_to_sheet = PythonOperator(task_id='upload_data_to_sheet', python_callable=upload_data_to_sheet , dag=dag)


get_api_data >> transform_api_data >> upload_data_to_sheet
