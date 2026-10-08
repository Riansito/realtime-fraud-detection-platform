{{ config(
    materialized='table',
    schema='gold',
    unique_key='merchant_sk'
) }}

select
    md5(merchant_id) as merchant_sk,
    merchant_id,
    merchant_name,
    category,
    country,
    cast(created_at as timestamp) as created_at
from {{ ref('raw_merchants') }}
