{{ config(
    materialized='table',
    schema='gold',
    unique_key='transaction_sk'
) }}

with transactions as (
    -- Append JDBC não é idempotente: um micro-batch reprocessado após falha
    -- pode duplicar linhas no Silver. Mantemos a primeira ocorrência.
    select distinct on (transaction_id) *
    from {{ source('silver', 'transactions') }}
    order by transaction_id, processed_at
),

-- SCD2: a primeira versão de cada entidade vale desde sempre, senão eventos
-- anteriores ao primeiro `dbt snapshot` ficariam sem dimensão.
dim_customer as (
    select
        *,
        case
            when row_number() over (partition by customer_id order by dbt_valid_from) = 1
                then '1900-01-01'::timestamptz
            else dbt_valid_from
        end as valid_from
    from {{ ref('dim_customer') }}
),

dim_account as (
    select
        *,
        case
            when row_number() over (partition by account_id order by dbt_valid_from) = 1
                then '1900-01-01'::timestamptz
            else dbt_valid_from
        end as valid_from
    from {{ ref('dim_account') }}
),

dim_merchant as (
    select * from {{ ref('dim_merchant') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
)

select
    md5(t.transaction_id) as transaction_sk,
    t.transaction_id,
    
    -- Ligar dim_customer SCD Tipo 2 (ativo no momento do evento)
    c.customer_sk,
    
    -- Ligar dim_account SCD Tipo 2 (ativo no momento do evento)
    a.account_sk,
    
    -- Ligar dim_merchant (SCD Tipo 1)
    m.merchant_sk,
    
    -- Ligar dim_date
    d.date_sk,
    
    cast(t.amount as numeric(12, 2)) as amount,
    t.currency,
    t.payment_method,
    cast(t.risk_score as numeric(5, 4)) as risk_score,
    t.is_fraud_suspect,
    false as is_blocked_flag,
    t.fraud_reason,
    cast(t.event_time as timestamp with time zone) as event_time,
    current_timestamp as inserted_at

from transactions t

left join dim_customer c
    on t.customer_id = c.customer_id
    and cast(t.event_time as timestamp with time zone) >= c.valid_from
    and (cast(t.event_time as timestamp with time zone) < c.dbt_valid_to or c.dbt_valid_to is null)

left join dim_account a
    on t.account_id = a.account_id
    and cast(t.event_time as timestamp with time zone) >= a.valid_from
    and (cast(t.event_time as timestamp with time zone) < a.dbt_valid_to or a.dbt_valid_to is null)

left join dim_merchant m
    on t.merchant_id = m.merchant_id

left join dim_date d
    on cast(to_char(t.event_time, 'YYYYMMDD') as int) = d.date_sk
