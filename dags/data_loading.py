from sqlalchemy.orm import Session
from api.models import TbProduct, TbCarts, TbCustomer  # Importar os modelos das tabelas Stage

# Função para carregar dados na camada Stage
def load_stage_data(products_data, carts_data, customers_data, db: Session):
    def insert_data(data, model):
        for item in data:
            if isinstance(item, dict):
                db.add(model(**item))
            else:
                print(f"❌ Dados inválidos para {model.__tablename__}: {item}")

    insert_data(products_data, TbProduct)
    insert_data(carts_data, TbCarts)
    insert_data(customers_data, TbCustomer)
    
    db.commit()

# Função para carregar dados na camada Trusted
def load_trusted_data(db: Session):
    # Carregar os dados agregados para a camada Trusted
    db.execute("ANALYZE stage.products; ANALYZE stage.carts;")
    
    db.execute("""
        INSERT INTO trusted.product_sales_daily (date, category, total_sales, avg_ticket, num_transactions)
        SELECT 
            COALESCE(c.sale_date, NOW()::DATE) AS date,  -- Garante que não tenha valores nulos
            p.category,
            COALESCE(SUM(c.total_amount), 0) AS total_sales,
            COALESCE(AVG(c.total_amount), 0) AS avg_ticket,
            COUNT(c.id) AS num_transactions
        FROM 
            stage.products p
        LEFT JOIN 
            stage.carts c ON c.product_id = p.id
        GROUP BY 
            COALESCE(c.sale_date, NOW()::DATE), p.category;
    """)
    db.commit()

# Função que chama as duas camadas de carga de dados
def load_data_to_database(products_data, carts_data, customers_data, db: Session):
    
    # Carregar dados nas tabelas de Stage
    load_stage_data(products_data, carts_data, customers_data, db)
    
    # Carregar dados na camada Trusted após a Stage estar completa
    load_trusted_data(db)