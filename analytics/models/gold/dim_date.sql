{{ config(
    materialized='table',
    schema='gold',
    unique_key='date_sk'
) }}

with date_spine as (
    select
        generate_series(
            '2020-01-01'::date,
            '2030-12-31'::date,
            '1 day'::interval
        )::date as date_day
)

select
    cast(to_char(date_day, 'YYYYMMDD') as int) as date_sk,
    date_day as full_date,
    cast(extract(day from date_day) as int) as day,
    cast(extract(month from date_day) as int) as month,
    cast(extract(year from date_day) as int) as year,
    cast(extract(dow from date_day) as int) as day_of_week,
    trim(to_char(date_day, 'Day')) as day_name,
    case
        when extract(dow from date_day) in (0, 6) then true
        else false
    end as is_weekend
from date_spine
