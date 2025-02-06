from sqlalchemy import Column, Integer, String, Float, DateTime, func
from database import Base

class StageProduct(Base):
    __tablename__ = "stage_products"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    price = Column(Float)
    category = Column(String(100))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

class StageSale(Base):
    __tablename__ = "stage_carts"
    
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer)
    customer_id = Column(Integer)
    sale_date = Column(DateTime)
    total_amount = Column(Float)
    quantity = Column(Integer)
    created_at = Column(DateTime, default=func.now())