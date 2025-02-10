from sqlalchemy import Column, Integer, String, Float, Date, func
from database import Base

class TrustedProductSalesDaily(Base):
    __tablename__ = "trusted_product_sales_daily"
    
    date = Column(Date, primary_key=True)
    category = Column(String(100), primary_key=True)
    total_sales = Column(Float)
    avg_ticket = Column(Float)
    num_transactions = Column(Integer)