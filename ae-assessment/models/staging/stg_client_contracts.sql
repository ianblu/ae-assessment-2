with source as (
    select * from {{ ref('client_contracts') }}
),

src_client_contracts as (
    select
        client_id,
        date(contract_start_date) as contract_start_date,
        cast(contract_duration_months as int) as contract_duration_months,
        cast(spend_threshold as real) as spend_threshold,
        cast(discounted_fee_margin as real) as discounted_fee_margin
    from source
)
select * from src_client_contracts

