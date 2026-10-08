{% snapshot dim_customer %}

{{
    config(
      target_schema='gold',
      unique_key='customer_id',
      strategy='check',
      check_cols=['risk_profile', 'city', 'state', 'phone', 'email']
    )
}}

select
    -- Geramos a surrogate key compondo o ID com o timestamp de extração para rastreabilidade
    md5(customer_id || current_timestamp::varchar) as customer_sk,
    customer_id,
    full_name,
    email,
    phone,
    risk_profile,
    city,
    state
from {{ ref('raw_customers') }}

{% endsnapshot %}
