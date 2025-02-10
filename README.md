# Teste Técnico – Engenharia de Dados com Airflow

## O que foi feito

1. **Melhorias na API**
- Capturar erros 500 e enviar mensagens amigáveis;
- Implementação do Refresh token para Renovar o Acesso


2. **Criação das Dags**
- dag_api_extration - Consumo dos dados da API (lidando com paginação, autenticação e falhas)
- Armazenamento dos dados na Camada Raw (local_storage)
- dag_data_pipeline - Extração dos dados em formato json da camada raw e envio dos dados para camada stage e trusted
- Automatização das pipelines no airflow

3. **Ajustes no docker-compose.yml**
- Foram necessárias uma série de ajustes no docker-compose para conseguir rodar o projeto sem erros
- Inclusão da network para controlar as chamadas dos containers
- Inclusão de mais variáveis de ambientes para que as dags funcionar corretamente

**Observação:** Tentei de tudo para carregar o arquivo de confifuração api_endpoints.yaml na dag_api_extraction.py como não tive sucesso coloquei a configuração direto no script, mas mantive o código comentado caso se queira usar essa opção.