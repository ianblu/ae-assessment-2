with source as (
    select * from {{ ref('currency_rates') }}
),

src_currency_rates as (
    select
        currency,
        date(rate_date) as rate_date,
        cast(exchange_rate_to_gbp as real) as exchange_rate_to_gbp
    from source
)
select * from src_currency_rates