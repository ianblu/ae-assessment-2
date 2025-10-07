{{ config(
    materialized="incremental",
    unique_key="client_sk",
) }}

select ROWID as client_sk
,      client_id
,      contract_start_date
,      contract_end_date
,      spend_threshold
,      discounted_fee_margin
from {{ ref('int_client_contracts') }}