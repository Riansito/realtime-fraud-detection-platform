import os
from contextlib import asynccontextmanager
from datetime import datetime

import asyncpg
import structlog
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ]
)
logger = structlog.get_logger("api-main")

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
    operator_notes: str | None = None
    event_time: datetime
    created_at: datetime
    updated_at: datetime


class ActionRequest(BaseModel):
    status: str
    operator_notes: str | None = None


class CustomerRiskProfile(BaseModel):
    customer_id: str
    full_name: str | None
    email: str | None
    risk_profile: str | None
    valid_from: datetime
    valid_to: datetime | None
    is_current: bool | None


class FraudMetrics(BaseModel):
    total_transactions: int
    total_suspected_fraud: int
    fraud_rate_percentage: float
    total_fraud_amount: float


# Database connection pool reference
db_pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    ssl_context = DB_SSLMODE != "disable"
    db_pool = await asyncpg.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        ssl=ssl_context,
    )
    logger.info("db_pool_initialized")
    yield
    await db_pool.close()
    logger.info("db_pool_closed")


app = FastAPI(title="Fraud Detection Operational API", lifespan=lifespan)

# Mount static files for the UI
app.mount("/ui", StaticFiles(directory="api/static", html=True), name="static")


@app.get("/api/v1/alerts", response_model=list[AlertResponse])
async def get_recent_alerts(status: str = "PENDING", limit: int = 50):
    """
    Consulta transações suspeitas recentes, filtradas por status (ex: PENDING).
    Retorna ordenado pelo maior risco de fraude (risk_score DESC) e data mais recente.
    """
    if not db_pool:
        raise HTTPException(
            status_code=500, detail="Database connection pool is not initialized"
        )

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
        raise HTTPException(
            status_code=500, detail="Database connection pool is not initialized"
        )

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
        raise HTTPException(
            status_code=400, detail="Invalid status. Must be APPROVED or BLOCKED"
        )

    if not db_pool:
        raise HTTPException(
            status_code=500, detail="Database connection pool is not initialized"
        )

    query_update = """
        UPDATE operational.operational_alerts
        SET status = $1, operator_notes = $2, updated_at = CURRENT_TIMESTAMP
        WHERE transaction_id = $3
        RETURNING alert_id, transaction_id, account_id, customer_id, amount,
                  risk_score, risk_level, fraud_reason, status, operator_notes,
                  event_time, created_at, updated_at
    """

    async with db_pool.acquire() as conn:
        record = await conn.fetchrow(
            query_update, action.status, action.operator_notes, transaction_id
        )

    if not record:
        raise HTTPException(status_code=404, detail="Alert not found")

    return dict(record)


@app.get(
    "/api/v1/analytics/customers/{customer_id}/risk-profile",
    response_model=list[CustomerRiskProfile],
)
async def get_customer_risk_profile(customer_id: str):
    """
    Retorna o histórico do perfil de risco (SCD 2) de um cliente da camada analítica (Gold).
    """
    if not db_pool:
        raise HTTPException(
            status_code=500, detail="Database connection pool is not initialized"
        )

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
        raise HTTPException(
            status_code=404, detail="Customer not found in analytics layer"
        )

    return [dict(record) for record in records]


@app.get("/api/v1/analytics/metrics/fraud-rate", response_model=FraudMetrics)
async def get_fraud_metrics():
    """
    Calcula e retorna as métricas globais agregadas da tabela fato (Gold).
    """
    if not db_pool:
        raise HTTPException(
            status_code=500, detail="Database connection pool is not initialized"
        )

    query = """
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(*) FILTER (WHERE is_fraud_suspect = TRUE) as total_suspected_fraud,
            COALESCE(SUM(amount) FILTER (WHERE is_fraud_suspect = TRUE), 0) as total_fraud_amount
        FROM gold.fact_transactions
    """

    async with db_pool.acquire() as conn:
        record = await conn.fetchrow(query)

    total_transactions = record["total_transactions"] or 0
    total_suspected = record["total_suspected_fraud"] or 0
    total_amount = float(record["total_fraud_amount"] or 0)

    fraud_rate = (
        (total_suspected / total_transactions * 100.0)
        if total_transactions > 0
        else 0.0
    )

    return FraudMetrics(
        total_transactions=total_transactions,
        total_suspected_fraud=total_suspected,
        fraud_rate_percentage=round(fraud_rate, 2),
        total_fraud_amount=total_amount,
    )
