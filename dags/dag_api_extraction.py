import os
import json
import time
import requests
import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from requests.exceptions import RequestException

# Função para carregar o arquivo YAML
def load_api_config():
    with open('api_endpoints.yaml', 'r') as file:
        return yaml.safe_load(file)

# Funções auxiliares
def get_auth_token():
    """Obtém o token de autenticação"""
    url = 'http://api:8000/token'
    payload = {'username': 'admin', 'password': 'admin'}
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()['access_token']

def refresh_token(refresh_token):
    """Renova o token expirado"""
    url = 'http://api:8000/refresh-token'
    payload = {'refresh_token': refresh_token}
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()['access_token']

def handle_api_error(func):
    """Função de retry com backoff exponencial para erros 500"""
    def wrapper(*args, **kwargs):
        retries = 5
        delay = 2  # Delay inicial
        for i in range(retries):
            try:
                return func(*args, **kwargs)
            except RequestException as e:
                if e.response and e.response.status_code == 500:
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    raise
    return wrapper

@handle_api_error
def fetch_data_from_endpoint(url, token, endpoint, skip=0, limit=50):
    """Obtém dados paginados de um endpoint"""
    headers = {'Authorization': f'Bearer {token}'}
    params = {'skip': skip, 'limit': limit}
    response = requests.get(url + endpoint, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

def save_data_to_file(endpoint_name, data):
    """Salva os dados coletados em um arquivo JSON"""
    # Obtém a data e hora atual para criar os diretórios e o nome do arquivo
    today = datetime.today().strftime('%Y-%m-%d')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Define o diretório de destino
    directory = f'/path/to/local_storage/raw/{endpoint_name}/{today}/'
    
    # Cria o diretório se não existir
    os.makedirs(directory, exist_ok=True)
    
    # Define o caminho completo do arquivo
    file_path = os.path.join(directory, f'{endpoint_name}_{timestamp}.json')
    
    # Salva os dados no arquivo JSON
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    
    print(f"Data saved to {file_path}")

def process_endpoint(endpoint_config):
    """Processa um endpoint e coleta todos os dados paginados"""
    token = get_auth_token()  # Token de autenticação
    url = 'http://api:8000/'  # URL da API
    all_data = []
    skip = 0
    limit = endpoint_config['limit']
    
    while True:
        data = fetch_data_from_endpoint(url, token, endpoint_config['endpoint'], skip, limit)
        all_data.extend(data['items'])
        if len(data['items']) < limit:
            break
        skip += limit

    # Salvar os dados coletados no arquivo JSON
    save_data_to_file(endpoint_config['file_name'], all_data)
    
    return all_data

def collect_data_from_config(**kwargs):
    api_config = load_api_config()
    tasks = []
    
    for endpoint, config in api_config.items():
        tasks.append(
            PythonOperator(
                task_id=f'collect_{config["file_name"]}',
                python_callable=process_endpoint,
                op_args=[config],  # Passando a configuração do endpoint
                dag=dag,
            )
        )
    
    return tasks

# DAG Airflow
with DAG(
    'data_collection',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval='@daily',  # Executa uma vez por dia
    catchup=False,
) as dag:

    # Tarefa para coletar dados de todos os endpoints definidos no YAML
    collect_data_task = PythonOperator(
        task_id='collect_data',
        python_callable=collect_data_from_config,
    )