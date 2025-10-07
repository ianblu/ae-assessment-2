with source as (
    select * from {{ ref('transactions') }}
),

src_transactions as (
    select
        transaction_id,
        client_id,
        cast(transaction_amount as real) as transaction_amount,
        transaction_type,
        date(transaction_date) as transaction_date,
        cast(platform_fee_margin as real) as platform_fee_margin,
        currency,
        linked_transaction_id
    from source
)
select * from src_transactions