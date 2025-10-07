{{ config(
    materialized="incremental",
    unique_key="client_id",
) }}

with client_thresholds as (
    select * from {{ ref('stg_client_contracts') }}
)
, known_clients as (
    select distinct client_id from {{ ref('stg_transactions') }}
)

, source as (
select coalesce(ct.client_id,kc.client_id) as client_id
,      ct.contract_start_date
,      date(ct.contract_start_date, '+'||ct.contract_duration_months||' months') as contract_end_date
,      ct.spend_threshold
,      ct.discounted_fee_margin
from client_thresholds ct
right outer join known_clients kc on (ct.client_id = kc.client_id)
)
select client_id
,      contract_start_date
,      contract_end_date
,      spend_threshold
,      discounted_fee_margin
from source