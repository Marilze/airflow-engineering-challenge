from sqlalchemy import Column, Integer, String, Float, DateTime, func, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

class TbCustomer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True)
    full_name = Column(String(255))
    email = Column(String(255))
    phone = Column(String(20))
    address = Column(String(255))
    city = Column(String(100))
    created_at = Column(DateTime, default=func.now())


class TbLogistics(Base):
    __tablename__ = "logistics"
    id = Column(Integer, primary_key=True)
    company_name = Column(String(255))
    service_type = Column(String(100))
    contact_phone = Column(String(20))
    origin_warehouse = Column(String(255))
    created_at = Column(DateTime, default=func.now())


class TbProduct(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    name = Column(String(255))
    category = Column(String(100))
    price = Column(Float)
    created_at = Column(DateTime, default=func.now())


class TbCarts(Base):
    __tablename__ = "sales"
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer)
    logistic_id = Column(Integer)
    sale_date = Column(DateTime)
    status = Column(String(50))
    total_amount = Column(Float)
    items = Column(JSONB)
    shipping_info = Column(JSONB)
    payment_info = Column(JSONB)
    created_at = Column(DateTime, default=func.now())


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True)
    password = Column(String(255))  