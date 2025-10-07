{{ config(
    materialized="incremental",
    unique_key=["transaction_id", "transaction_date"],
) }}


with transaction_resolutionss as (
select * from  {{ ref('stg_transaction_resolutions') }}
)
, transactions as (
select st.transaction_id
, st.transaction_date
, st.client_id
, st.transaction_amount
, st.transaction_type
, st.platform_fee_margin
, st.currency
from {{ ref('stg_transactions') }}  st
)
, resolved_chargebacks as (
select transaction_id
, resolution_status
, resolution_date
from transaction_resolutions
where resolution_status = 'resolved'
)
, client_contracts as (
select client_id
, contract_start_date
, contract_end_date
, spend_threshold
, discounted_fee_margin
from  {{ ref('int_client_contracts') }}
)
, currency_rates as (
select currency
, rate_date
, exchange_rate_to_gbp
from {{ ref ('int_currency_rates') }}
)
, transactions_over_time as (
select ts.transaction_id
, ts.client_id
, coalesce(rc.resolution_date, ts.transaction_date) as transaction_date
, ts.transaction_type
, ts.currency
, sum(ts.transaction_amount) over(partition by ts.client_id order by coalesce(rc.resolution_date, ts.transaction_date), ts.transaction_id) as fv_running_total
, sum(ts.transaction_amount* (case when ts.transaction_type = 'payment' then 1 when rc.transaction_id is not null then 1 else -1 end)) over(partition by ts.client_id order by coalesce(rc.resolution_date, ts.transaction_date), ts.transaction_id) as adjusted_fv_running_total
, case when ts.transaction_type = 'payment' then 1 when rc.transaction_id is not null then 1 else -1 end as pos_neg
, ts.transaction_amount* (case when ts.transaction_type = 'payment' then 1 when rc.transaction_id is not null then 1 else -1 end) as adj_amount
, ts.transaction_amount
, ts.platform_fee_margin
from   transactions ts
left join resolved_chargebacks rc on (ts.transaction_id = rc.transaction_id)
)
, transactions_with_charges_and_currency as (
select tot.transaction_id
, tot.client_id
, cc.spend_threshold
, tot.transaction_date -- there is a problem here although I am not sure why as all downstream sources have had dates formatted
, tot.transaction_type
, tot.transaction_amount
, tot.adj_amount as accounting_transaction_amount
, (tot.transaction_amount/100)*coalesce(cc.discounted_fee_margin, platform_fee_margin) as transaction_charge
, (tot.adj_amount/100)*coalesce(cc.discounted_fee_margin, platform_fee_margin) as accounting_transaction_charge
, tot.currency
, coalesce(cr.exchange_rate_to_gbp, (select cr2.exchange_rate_to_gbp from (select cr1.exchange_rate_to_gbp from currency_rates cr1 where cr1.currency = tot.currency and cr1.rate_date <= tot.transaction_date order by cr1.rate_date desc) cr2 limit 1)) as currency_rate
, case when cr.exchange_rate_to_gbp then 'No' else 'Yes' end as estimated
from   transactions_over_time tot
left join currency_rates cr on (tot.currency = cr.currency and tot.transaction_date = cr.rate_date)
left join client_contracts cc on (tot.client_id = cc.client_id and tot.transaction_date between cc.contract_start_date and cc.contract_end_date and tot.fv_running_total >= cc.spend_threshold)
)

select transaction_id
,      client_id
,      spend_threshold
,      transaction_date
,      transaction_type
,      transaction_amount
,      currency
,      currency_rate
,      (transaction_amount*currency_rate) as transaction_amount_gbp
,      (accounting_transaction_amount*currency_rate) as accounting_transaction_amount_gbp
,      (transaction_charge*currency_rate) as transaction_charge_gbp
,      (accounting_transaction_charge*currency_rate) as accounting_transaction_charge_gpb
,      estimated as estimated_currency_rate
from   transactions_with_charges_and_currency

