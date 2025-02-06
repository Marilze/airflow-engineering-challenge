from sqlalchemy import Column, Integer, String, Float, DateTime, func, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

class TbCustomer(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, index=True)
    full_name = Column(String(255), nullable=False)
    email = Column(String(255), unique=True, nullable=False, index=True)
    phone = Column(String(20), nullable=True)
    address = Column(String(255), nullable=True)
    city = Column(String(100), nullable=True)
    created_at = Column(DateTime, default=func.now())

    carts = relationship("TbCarts", back_populates="customer")


class TbLogistics(Base):
    __tablename__ = "logistics"
    id = Column(Integer, primary_key=True, index=True)
    company_name = Column(String(255), nullable=False)
    service_type = Column(String(100), nullable=True)
    contact_phone = Column(String(20), nullable=True)
    origin_warehouse = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=func.now())

    carts = relationship("TbCarts", back_populates="logistic")


class TbProduct(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    category = Column(String(100), nullable=False)
    price = Column(Float, nullable=False)
    created_at = Column(DateTime, default=func.now())


class TbCarts(Base):
    __tablename__ = "carts"  # Nome ajustado para refletir a classe
    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    logistic_id = Column(Integer, ForeignKey("logistics.id"), nullable=True)
    sale_date = Column(DateTime, default=func.now(), nullable=False)
    status = Column(String(50), nullable=False, default="pending")
    total_amount = Column(Float, nullable=False)
    items = Column(JSONB, nullable=False)  # Lista de produtos vendidos
    shipping_info = Column(JSONB, nullable=True)  # Endereço de entrega
    payment_info = Column(JSONB, nullable=True)  # Dados de pagamento
    created_at = Column(DateTime, default=func.now())

    customer = relationship("TbCustomer", back_populates="carts")
    logistic = relationship("TbLogistics", back_populates="carts")


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True)
    password = Column(String(255)) 