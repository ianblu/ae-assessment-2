{{ config(
    materialized="incremental",
    unique_key=["currency","rate_date"],
) }}

with currency_rates as (
    select * from {{ ref('stg_currency_rates') }}
)
select currency
,      rate_date
,      exchange_rate_to_gbp
from currency_rates
