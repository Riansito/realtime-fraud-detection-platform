CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS operational;

-- dim_customer (SCD Tipo 2)
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_sk VARCHAR(64) PRIMARY KEY, -- MD5(customer_id || dbt_valid_from)
    customer_id VARCHAR(50) NOT NULL,
    full_name VARCHAR(150),
    email VARCHAR(150),
    phone VARCHAR(30),
    risk_profile VARCHAR(20),
    city VARCHAR(100),
    state VARCHAR(50),
    dbt_valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    dbt_valid_to TIMESTAMP WITH TIME ZONE,
    dbt_is_current BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_lookup ON gold.dim_customer (customer_id, dbt_is_current);

-- dim_account (SCD Tipo 2)
CREATE TABLE IF NOT EXISTS gold.dim_account (
    account_sk VARCHAR(64) PRIMARY KEY, -- MD5(account_id || dbt_valid_from)
    account_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    account_type VARCHAR(30),
    credit_limit NUMERIC(12, 2),
    account_status VARCHAR(20),
    dbt_valid_from TIMESTAMP WITH TIME ZONE NOT NULL,
    dbt_valid_to TIMESTAMP WITH TIME ZONE,
    dbt_is_current BOOLEAN NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_account_lookup ON gold.dim_account (account_id, dbt_is_current);

-- dim_merchant
CREATE TABLE IF NOT EXISTS gold.dim_merchant (
    merchant_sk VARCHAR(64) PRIMARY KEY, -- MD5(merchant_id)
    merchant_id VARCHAR(50) NOT NULL UNIQUE,
    merchant_name VARCHAR(150),
    category VARCHAR(100),
    country VARCHAR(10),
    created_at TIMESTAMP WITH TIME ZONE
);

-- dim_date
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_sk INT PRIMARY KEY, -- Formato YYYYMMDD
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- fact_transactions
CREATE TABLE IF NOT EXISTS gold.fact_transactions (
    transaction_sk VARCHAR(64) PRIMARY KEY, -- MD5(transaction_id)
    transaction_id VARCHAR(50) NOT NULL,
    customer_sk VARCHAR(64) REFERENCES gold.dim_customer(customer_sk),
    account_sk VARCHAR(64) REFERENCES gold.dim_account(account_sk),
    merchant_sk VARCHAR(64) REFERENCES gold.dim_merchant(merchant_sk),
    date_sk INT REFERENCES gold.dim_date(date_sk),
    amount NUMERIC(12, 2) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    payment_method VARCHAR(30) NOT NULL,
    risk_score NUMERIC(5, 4) NOT NULL,
    is_fraud_suspect BOOLEAN NOT NULL,
    is_blocked_flag BOOLEAN DEFAULT FALSE,
    fraud_reason VARCHAR(255),
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_trans_time ON gold.fact_transactions (event_time);
CREATE INDEX IF NOT EXISTS idx_fact_trans_fraud ON gold.fact_transactions (is_fraud_suspect);

-- Operational Schema
CREATE TABLE IF NOT EXISTS operational.operational_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL UNIQUE,
    account_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    risk_score NUMERIC(5, 4) NOT NULL,
    risk_level VARCHAR(20) NOT NULL, -- HIGH, CRITICAL
    fraud_reason VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'PENDING', -- PENDING, APPROVED, BLOCKED
    operator_notes TEXT,
    event_time TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_status ON operational.operational_alerts (status, risk_score DESC);
