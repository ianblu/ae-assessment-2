{{ config(
    materialized="incremental",
    unique_key=["currency_sk"],
) }}


select cr.ROWID as currency_sk
,      cr.currency
,      d.date_key
,      cr.exchange_rate_to_gbp
from {{ ref('int_currency_rates') }} cr
inner join {{ ref('dim_date') }} d on (cr.rate_date = d.date_actual)

