import os
import json
import time
import requests
#import yaml
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from requests.exceptions import RequestException


# Função para carregar o arquivo YAML
# def load_api_config():
#     base_dir = os.path.dirname(os.path.abspath(__file__))
#     configuration_file_path = os.path.join(base_dir, 'config', 'api_endpoints.yaml')
#     with open(configuration_file_path, encoding='utf-8') as yaml_file:
#         return yaml.safe_load(yaml_file)


# Configuração dos endpoints diretamente no script
CONFIG = {
    "products": {
        "endpoint": "/api/v1/products",
        "file_name": "products",
        "limit": 50,
        "table_name": "stage.products"
    },
    "carts": {
        "endpoint": "/api/v1/carts",
        "file_name": "carts",
        "limit": 50,
        "table_name": "stage.carts"
    },
    "customers": {
        "endpoint": "/api/v1/customer",
        "file_name": "customers",
        "limit": 50,
        "table_name": "stage.customers"
    }
}

API_URL = os.getenv("API_URL", "http://api:8000")
USERNAME = "admin"
PASSWORD = "admin"


def get_auth_token():
    """Obtém o token de autenticação."""
    response = requests.post(f"{API_URL}/token", data={"username": USERNAME, "password": PASSWORD})
    response.raise_for_status()
    tokens = response.json()
    
    return tokens.get("access_token"), tokens.get("refresh_token")

def refresh_token(refresh_token):
    """Renova o token expirado."""
    response = requests.post(f"{API_URL}/refresh-token", data={"refresh_token": refresh_token})
    response.raise_for_status()
    
    return response.json()["access_token"]

def handle_api_error(func):
    """Função de retry com backoff exponencial para erros 500."""
    def wrapper(*args, **kwargs):
        retries = 5
        delay = 2  # Delay inicial
        last_valid_data = []
        
        for attempt in range(retries):
            try:
                data = func(*args, **kwargs)
                if data:
                    last_valid_data = data  # Salva o último dado válido
                return data  # Retorna os dados se a chamada for bem-sucedida
            except RequestException as e:
                if e.response and e.response.status_code == 500:
                    print(f"Tentativa {attempt + 1}: Erro 500. Retentando em {delay} segundos...")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    break  # Se não for erro 500, interrompe o loop
            
        print("ERRO: Todas as tentativas falharam. Retornando os últimos dados válidos.")
        return last_valid_data  # Retorna os dados válidos antes do erro
    return wrapper


@handle_api_error
def fetch_data_from_endpoint(url, token, endpoint, skip=0, limit=50):
    """Obtém dados paginados de um endpoint"""
    headers = {'Authorization': f'Bearer {token}'}
    params = {'skip': skip, 'limit': limit}
    
    try:
        response = requests.get(url + endpoint, headers=headers, params=params)
        if response.status_code == 401:
            print("Token expirado, tentando renovar...")
            raise Exception("TOKEN_EXPIRED")
        
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, dict) and 'items' in data:
            return data['items']
        elif isinstance(data, list):
            return data
        else:
            print(f"Formato inesperado de resposta: {data}")
            return []
    
    except (RequestException, ValueError) as e:
        print(f"Erro na requisição para {endpoint}: {e}")
        return []  # Retorna lista vazia para evitar erro


def extract_date(record, endpoint):
    """Extrai a data do registro individual com base no endpoint."""
    date_key = {
        "products": "created_at",
        "customers": "created_at",
        "carts": "sale_date"
    }.get(endpoint, None)

    if not date_key:
        return None
    
    date_str = record.get(date_key, None)

    if not date_str:
        return None

    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        return None


def save_data_to_file(endpoint_name, data):
    if not isinstance(data, list) or len(data) == 0:
        print(f"WARNING: Nenhum dado para {endpoint_name}, ignorando salvamento.")
        return

    for record in data:
        date_obj = extract_date(record, endpoint_name)

        if not date_obj:
            print(f"WARNING: Não foi possível extrair a data para {endpoint_name}, salvando em 'unknown_date'.")
            folder_path = f"local_storage/raw/{endpoint_name}/unknown_date"
            filename = f"{endpoint_name}_unknown.json"
        else:
            date_folder = date_obj.strftime("%Y-%m-%d")
            timestamp = date_obj.strftime("%Y%m%d_%H%M%S")
            
            folder_path = f"local_storage/raw/{endpoint_name}/{date_folder}"
            filename = f"{endpoint_name}_{timestamp}.json"

        os.makedirs(folder_path, exist_ok=True)
        
        file_path = os.path.join(folder_path, filename)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=4)

        print(f"Data saved: {file_path}")


def process_endpoint(endpoint_config):
    """Processa um endpoint e coleta todos os dados paginados."""
    token, refresh = get_auth_token()  
    all_data = []
    skip = 0
    limit = endpoint_config['limit']
    
    while True:
        try:
            data = fetch_data_from_endpoint(API_URL, token, endpoint_config['endpoint'], skip, limit)
        except Exception as e:
            if str(e) == "TOKEN_EXPIRED":
                token = refresh_token(refresh)
                continue
            else:
                print(f"Erro fatal ao processar {endpoint_config['endpoint']}: {e}")
                break  # Sai do loop para evitar repetição infinita
        
        if data:
            all_data.extend(data)
        
        if len(data) < limit:  # Última página
            break  

        skip += limit  # Avança para a próxima página

    if all_data:
        save_data_to_file(endpoint_config['file_name'], all_data)
    
    return all_data


def create_tasks():
    """Cria tarefas para cada endpoint."""
    tasks = []
    for endpoint, config in CONFIG.items():
        task = PythonOperator(
            task_id=f"collect_{config['file_name']}",
            python_callable=process_endpoint,
            op_args=[config],
            dag=dag,
        )
        tasks.append(task)
    
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
    tags=['data', 'extraction']
) as dag:
    
    task_list = create_tasks()
    for task in task_list:
        task
