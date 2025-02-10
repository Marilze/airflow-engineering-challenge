import os
import json
import glob

def process_data_from_files():
    # Caminho base onde os arquivos JSON estão localizados
    local_storage_path = '/local_storage/raw/'

    # Função auxiliar para ler os arquivos JSON de uma pasta específica
    def read_json_files(directory):
        # Encontrar todos os arquivos JSON dentro da pasta especificada
        files = glob.glob(os.path.join(directory, '**/*.json'), recursive=True)
        
        data = []
        for file in files:
            try:
                with open(file, 'r') as f:
                    content = f.read().strip()
                    if content:
                        data.append(json.load(f))
                    else:
                        print(f"⚠️ Arquivo vazio: {file}")
            except json.JSONDecodeError as e:
                print(f"❌ Erro ao decodificar JSON em {file}: {e}")
            except Exception as e:
                print(f"❌ Erro ao ler {file}: {e}")
        
        return data

    # Lendo os dados de cada tipo (products, carts, customers)
    products_data = read_json_files(os.path.join(local_storage_path, 'products'))
    carts_data = read_json_files(os.path.join(local_storage_path, 'carts'))
    customers_data = read_json_files(os.path.join(local_storage_path, 'customers'))
    
     # Exibir a quantidade de arquivos lidos
    print(f"📦 Arquivos processados: Products({len(products_data)}), Carts({len(carts_data)}), Customers({len(customers_data)})")
    
    # Verifica se há dados carregados
    if not products_data or not carts_data or not customers_data:
        raise ValueError("Não há dados suficientes nos arquivos JSON.")

    # Retornando os dados para a próxima tarefa
    return products_data, carts_data, customers_data