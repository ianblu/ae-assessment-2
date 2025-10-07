{{
    config(
        materialized='table',
        unique_key=['month_key', 'client_sk'],
    )
}}

WITH monthly_transactions AS (
    -- 1. Aggregate core metrics by client and month (using date_key)
    SELECT
        t.client_sk,
        CAST(SUBSTR(CAST(t.date_key AS TEXT), 1, 6) AS INTEGER) AS month_key,
        
        -- Monthly Aggregate Metrics
        SUM(t.transaction_amount_gbp) AS gross_merchandise_value_gbp,
        SUM(t.transaction_charge_gbp) AS gross_transaction_charge_gbp,
        SUM(t.accounting_transaction_charge_gpb) AS net_recognized_revenue_gbp
        
    FROM 
        {{ ref('fct_transaction') }} t
    
    GROUP BY
        t.client_sk,
        month_key
),

monthly_cumulative AS (
    -- 2. Calculate the Cumulative Spend (GMV) by client up to that month
    SELECT
        mt.client_sk,
        mt.month_key,
        mt.gross_merchandise_value_gbp,
        mt.gross_transaction_charge_gbp,
        mt.net_recognized_revenue_gbp,

        -- Window function to get cumulative GMV for threshold tracking
        SUM(mt.gross_merchandise_value_gbp) OVER (
            PARTITION BY mt.client_sk
            ORDER BY mt.month_key
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_gmv_gbp
        
    FROM 
        monthly_transactions mt
)

SELECT
    m.month_key,
    m.client_sk,
    c.client_id,
    
    m.gross_merchandise_value_gbp AS total_gmv_gbp,
    m.net_recognized_revenue_gbp AS revenue_recognized_gbp,
    
    c.spend_threshold,
    m.cumulative_gmv_gbp,
    
    c.discounted_fee_margin AS discount_rate_applied,
    
    CASE
        WHEN m.cumulative_gmv_gbp >= c.spend_threshold THEN 'Applied'
        ELSE 'Not Applied'
    END AS discount_application_status
    
FROM
    monthly_cumulative m
INNER JOIN 
    {{ ref('dim_client') }} c
    ON m.client_sk = c.client_sk
ORDER BY
    m.client_sk,
    m.month_key