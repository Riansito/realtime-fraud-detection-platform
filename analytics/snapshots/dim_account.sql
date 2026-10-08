{% snapshot dim_account %}

{{
    config(
      target_schema='gold',
      unique_key='account_id',
      strategy='check',
      check_cols=['credit_limit', 'account_status']
    )
}}

select
    -- Geramos a surrogate key compondo o ID da conta com o timestamp
    md5(account_id || current_timestamp::varchar) as account_sk,
    account_id,
    customer_id,
    account_type,
    credit_limit,
    account_status
from {{ ref('raw_accounts') }}

{% endsnapshot %}
