from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from data_processing import process_data_from_files
from data_loading import load_data_to_database
from api.database import get_db  

def collect_data(**kwargs):
        # Coleta e processa os dados
        products_data, carts_data, customers_data = process_data_from_files()
        
        # Passa os dados via XCom para a próxima tarefa
        kwargs['ti'].xcom_push(key='products_data', value=products_data)
        kwargs['ti'].xcom_push(key='carts_data', value=carts_data)
        kwargs['ti'].xcom_push(key='customers_data', value=customers_data)

def load_data(**kwargs):
    # Pega os dados passados pela tarefa anterior via XCom
    ti = kwargs['ti']
    products_data = ti.xcom_pull(task_ids='collect_data', key='products_data')
    carts_data = ti.xcom_pull(task_ids='collect_data', key='carts_data')
    customers_data = ti.xcom_pull(task_ids='collect_data', key='customers_data')

    # Obtém a sessão do banco de dados
    db = next(get_db())  # Obtém a sessão

    # Carrega os dados no banco
    load_data_to_database(products_data, carts_data, customers_data, db)

    # Tarefa para coletar e processar os dados
    collect_data_task = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data,
        provide_context=True,
    )

    # Tarefa para carregar dados no banco de dados
    load_data_task = PythonOperator(
        task_id='load_data_to_database',
        python_callable=load_data,
        provide_context=True,
    )

    # Definindo a ordem das tarefas na DAG
    collect_data_task >> load_data_task