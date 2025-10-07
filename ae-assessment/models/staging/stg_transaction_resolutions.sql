with source as (
    select * from {{ ref('transaction_resolutions') }}
),

src_transaction_resolutions as (
    select
        transaction_id,
        resolution_status,
        date(substr(resolution_date,7,4)||'-'||
             substr(resolution_date,4,2)||'-'||
             substr(resolution_date,1,2)) as resolution_date
    from source
)
select * from src_transaction_resolutions