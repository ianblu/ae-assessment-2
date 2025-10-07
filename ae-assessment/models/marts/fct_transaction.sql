{{ config(
    materialized="incremental",
    unique_key=["transaction_sk"],
) }}


select tf.ROWID as transaction_sk
,      tf.transaction_id
,      c.client_sk
,      d.date_key
,      tf.transaction_type
,      tf.transaction_amount
,      tf.transaction_amount_gbp
,      tf.accounting_transaction_amount_gbp
,      tf.transaction_charge_gbp
,      tf.accounting_transaction_charge_gpb
,      tf.currency
,      cr.currency_sk
,      tf.estimated_currency_rate
from   {{ ref('int_transaction_financials') }} tf
inner join {{ ref('dim_client') }} c on (tf.client_id = c.client_id)
inner join {{ ref('dim_date') }} d on (d.date_actual = tf.transaction_date)
left join {{ ref('fct_currency_rate') }} cr on (cr.currency = tf.currency and cr.date_key = d.date_key)
