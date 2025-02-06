from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.security import OAuth2PasswordRequestForm
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import List
import models, database, auth
from fastapi.middleware.cors import CORSMiddleware
import random
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError

app = FastAPI(title="Fake API", version="1.0.0")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Middleware para simular erros 500 aleatórios
@app.middleware("http")
async def simulate_random_errors(request: Request, call_next):
    # Ignora endpoints de autenticação para não atrapalhar o login
    if not request.url.path.startswith(("/token", "/refresh-token", "/docs", "/openapi.json", "/redoc")):
        # 10% de chance de erro
        if random.random() < 0.1:
            return JSONResponse(
                status_code=500,
                content={
                    "detail": "Internal Server Error",
                    "message": "Oops! Something went wrong on our end. This is a simulated error for testing purposes."
                }
            )
    response = await call_next(request)
    return response

# Criar as tabelas
models.Base.metadata.create_all(bind=database.engine)

# Endpoints de autenticação
@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if form_data.username != "admin" or form_data.password != "admin":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    access_token = auth.create_access_token(
        data={"sub": form_data.username},
        expires_delta=timedelta(minutes=auth.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    refresh_token = auth.create_refresh_token(data={"sub": form_data.username})
    
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "token_type": "bearer"
    }

@app.post("/refresh-token")
async def refresh_token(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid refresh token",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, auth.SECRET_KEY, algorithms=[auth.ALGORITHM])
        username: str = payload.get("sub")
        exp: int = payload.get("exp")
        if username is None or exp is None:
            raise credentials_exception
        if datetime.utcnow().timestamp() > exp:
            raise credentials_exception
    except JWTError:
        raise credentials_exception
    
    new_access_token = auth.create_access_token(
        data={"sub": username},
        expires_delta=timedelta(minutes=auth.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    
    return {"access_token": new_access_token, "token_type": "bearer"}

@app.get("/api/v1/carts")
async def get_carts(
    skip: int = 0,
    limit: int = 50,
    db: Session = Depends(database.get_db),
    current_user: str = Depends(auth.get_current_user)
):
    try:
        carts = db.query(models.TbCarts).offset(skip).limit(min(limit, 50)).all()
        return carts
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="Database error: unable to retrieve products"
        )

@app.get("/api/v1/customer")
async def get_customers(
    skip: int = 0,
    limit: int = 50,
    db: Session = Depends(database.get_db),
    current_user: str = Depends(auth.get_current_user)
):
    try:
        customers = db.query(models.TbCustomer).offset(skip).limit(min(limit, 50)).all()
        return customers
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="Database error: unable to retrieve custumers"
        )

@app.get("/api/v1/logistict")
async def get_logistics(
    skip: int = 0,
    limit: int = 50,
    db: Session = Depends(database.get_db),
    current_user: str = Depends(auth.get_current_user)
):
    try:
        logistics = db.query(models.TbLogistics).offset(skip).limit(min(limit, 50)).all()
        return logistics
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail="Database error: unable to retrieve logistics"
        )
