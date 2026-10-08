import os
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from dotenv import load_dotenv
from contextlib import asynccontextmanager

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

class AlertResponse(BaseModel):
    alert_id: int
    transaction_id: str
    account_id: str
    customer_id: str
    amount: float
    risk_score: float
    risk_level: str
    fraud_reason: str
    status: str
    operator_notes: Optional[str] = None
    event_time: datetime
    created_at: datetime
    updated_at: datetime

class ActionRequest(BaseModel):
    status: str
    operator_notes: Optional[str] = None

class CustomerRiskProfile(BaseModel):
    customer_id: str
    full_name: Optional[str]
    email: Optional[str]
    risk_profile: Optional[str]
    valid_from: datetime
    valid_to: Optional[datetime]
    is_current: Optional[bool]

# Database connection pool reference
db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    ssl_context = False if DB_SSLMODE == "disable" else True
    db_pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        ssl=ssl_context
    )
    yield
    await db_pool.close()

app = FastAPI(title="Fraud Detection Operational API", lifespan=lifespan)

@app.get("/api/v1/alerts", response_model=List[AlertResponse])
async def get_recent_alerts(status: str = "PENDING", limit: int = 50):
    """
    Consulta transações suspeitas recentes, filtradas por status (ex: PENDING).
    Retorna ordenado pelo maior risco de fraude (risk_score DESC) e data mais recente.
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database connection pool is not initialized")
        
    query = """
        SELECT alert_id, transaction_id, account_id, customer_id, amount,
               risk_score, risk_level, fraud_reason, status, operator_notes,
               event_time, created_at, updated_at
        FROM operational.operational_alerts
        WHERE status = $1
        ORDER BY risk_score DESC, event_time DESC
        LIMIT $2
    """
    
    async with db_pool.acquire() as conn:
        records = await conn.fetch(query, status, limit)
        
    return [dict(record) for record in records]

@app.get("/api/v1/alerts/{transaction_id}", response_model=AlertResponse)
async def get_alert_detail(transaction_id: str):
    """
    Retorna os dados do evento e justificativa do score de uma transação específica.
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database connection pool is not initialized")
        
    query = """
        SELECT alert_id, transaction_id, account_id, customer_id, amount,
               risk_score, risk_level, fraud_reason, status, operator_notes,
               event_time, created_at, updated_at
        FROM operational.operational_alerts
        WHERE transaction_id = $1
    """
    
    async with db_pool.acquire() as conn:
        record = await conn.fetchrow(query, transaction_id)
        
    if not record:
        raise HTTPException(status_code=404, detail="Alert not found")
        
    return dict(record)

@app.post("/api/v1/alerts/{transaction_id}/action", response_model=AlertResponse)
async def triage_alert(transaction_id: str, action: ActionRequest):
    """
    Realiza a triagem operacional de um alerta, alterando seu status para APPROVED ou BLOCKED.
    """
    if action.status not in ["APPROVED", "BLOCKED"]:
        raise HTTPException(status_code=400, detail="Invalid status. Must be APPROVED or BLOCKED")
        
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database connection pool is not initialized")
        
    query_update = """
        UPDATE operational.operational_alerts
        SET status = $1, operator_notes = $2, updated_at = CURRENT_TIMESTAMP
        WHERE transaction_id = $3
        RETURNING alert_id, transaction_id, account_id, customer_id, amount,
                  risk_score, risk_level, fraud_reason, status, operator_notes,
                  event_time, created_at, updated_at
    """
    
    async with db_pool.acquire() as conn:
        record = await conn.fetchrow(query_update, action.status, action.operator_notes, transaction_id)
        
    if not record:
        raise HTTPException(status_code=404, detail="Alert not found")
        
    return dict(record)

@app.get("/api/v1/analytics/customers/{customer_id}/risk-profile", response_model=List[CustomerRiskProfile])
async def get_customer_risk_profile(customer_id: str):
    """
    Retorna o histórico do perfil de risco (SCD 2) de um cliente da camada analítica (Gold).
    """
    if not db_pool:
        raise HTTPException(status_code=500, detail="Database connection pool is not initialized")
        
    query = """
        SELECT customer_id, full_name, email, risk_profile,
               dbt_valid_from AS valid_from, dbt_valid_to AS valid_to,
               (dbt_valid_to IS NULL) AS is_current
        FROM gold.dim_customer
        WHERE customer_id = $1
        ORDER BY dbt_valid_from DESC
    """
    
    async with db_pool.acquire() as conn:
        records = await conn.fetch(query, customer_id)
        
    if not records:
        raise HTTPException(status_code=404, detail="Customer not found in analytics layer")
        
    return [dict(record) for record in records]
