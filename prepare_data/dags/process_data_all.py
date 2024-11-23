from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from jobs.process_data import process_data
from jobs.create_tables import process


POSTGRES_CONFIG = Variable.get("postgres_connection", deserialize_json=True)
run_date = """{{ (dag_run.execution_date + macros.timedelta(hours=7)).strftime('%Y-%m-%d')}}"""


default_args = {
    'owner': 'lethuyduong2000@gmail.com',
    'start_date': datetime(2024, 10, 1),
}


with DAG(
    dag_id = 'process_data_all',
    default_args=default_args,
    schedule_interval='30 17 * * *',
    catchup=False, 
) as dag:

    preprocess_data = PythonOperator(
        task_id='preprocess_data',
        python_callable=process_data, 
        op_kwargs = {
            'run_date': run_date, 
            'POSTGRES_CONFIG': POSTGRES_CONFIG       
        }
    )

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=process, 
        op_kwargs = {
            'POSTGRES_CONFIG': POSTGRES_CONFIG       
        }
    )

    preprocess_data >> create_table
