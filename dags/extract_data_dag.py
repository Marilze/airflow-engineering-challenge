from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import os
import yaml

# Função para extrair dados da API
def fetch_api_data(endpoint, file_name):
    url = f"http://localhost:8000{endpoint}?skip=0&limit=50"
    headers = {"Authorization": f"Bearer {get_auth_token()}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(f"/opt/airflow/data/{file_name}.json", "w") as f:
            f.write(response.text)
    else:
        raise Exception(f"Erro ao buscar {file_name}: {response.status_code}")


# Autenticação na API
def get_auth_token():
    response = requests.post("http://localhost:8000/token", data={"username": "admin", "password": "admin"})
    if response.status_code == 200:
        return response.json()["access_token"]
    raise Exception("Falha na autenticação")

# Lendo configuração YAML
with open("/opt/airflow/dags/config/endpoints.yaml", "r") as file:
    config = yaml.safe_load(file)


def create_tasks():
    tasks = {}
    for key, value in config.items():
        tasks[key] = PythonOperator(
            task_id=f"fetch_{key}",
            python_callable=fetch_api_data,
            op_args=[value["endpoint"], value["file_name"]],
            dag=dag
        )
    return tasks


# Definição da DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "extract_data_from_api",
    default_args=default_args,
    description="Pipeline para extrair dados da API",
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

# Criando tasks dinamicamente
tasks = create_tasks()

# Ordem de execução
tasks["products"] >> tasks["carts"] >> tasks["customers"]
