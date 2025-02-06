from sqlalchemy.orm import Session
from api.models import TbProduct, TbCarts, TbCustomer, TbLogistics  # Importar os modelos das tabelas Stage

# Função para carregar dados na camada Stage
def load_stage_data(products_data, carts_data, customers_data, db: Session):
    # Carregar os dados na tabela de produtos (Stage)
    for product in products_data:
        db.add(TbProduct(**product))  # Certifique-se de que os dados estejam em formato de dicionário
    
    # Carregar os dados na tabela de carrinhos (Stage)
    for cart in carts_data:
        db.add(TbCarts(**cart))  # Certifique-se de que os dados estejam em formato de dicionário
    
    # Carregar os dados na tabela de clientes (Stage)
    for customer in customers_data:
        db.add(TbCustomer(**customer))  # Certifique-se de que os dados estejam em formato de dicionário
    
    # Commit dos dados para as tabelas Stage
    db.commit()

# Função para carregar dados na camada Trusted
def load_trusted_data(db: Session):
    # Carregar os dados agregados para a camada Trusted
    
    # Exemplo para preencher a tabela trusted.product_sales_daily
    db.execute("""
        INSERT INTO trusted.product_sales_daily (date, category, total_sales, avg_ticket, num_transactions)
        SELECT 
            sale_date AS date,
            category,
            SUM(total_amount) AS total_sales,
            AVG(total_amount) AS avg_ticket,
            COUNT(*) AS num_transactions
        FROM 
            stage.products p
        JOIN 
            stage.carts c ON c.product_id = p.id
        GROUP BY 
            sale_date, category;
    """)
    db.commit()

# Função que chama as duas camadas de carga de dados
def load_data_to_database(products_data, carts_data, customers_data, db: Session):
    
    # Carregar dados nas tabelas de Stage
    load_stage_data(products_data, carts_data, customers_data, db)
    
    # Carregar dados na camada Trusted após a Stage estar completa
    load_trusted_data(db)