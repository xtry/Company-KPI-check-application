{{
  config(
    materialized = 'table'
  )
}}

WITH total_revenue AS (
  SELECT
    SUM(c.contract_length * clv.avg_clv) AS total_revenue
  FROM
    {{ ref('stg_customer') }} c
  JOIN
    {{ ref('stg_clv') }} clv ON c.contract_length = clv.contract_length 
),
total_costs AS (
  SELECT
    SUM(m.marketing_costs) + SUM(s.sales_costs) + SUM(s.trial_costs) AS total_costs
  FROM
    {{ ref('stg_marketing_costs') }} m
  LEFT JOIN
    {{ ref('stg_sales_costs') }} s ON m.marketing_costs_date = s.sales_costs_date
),
profitability AS (
  SELECT
    (tr.total_revenue - tc.total_costs) / tc.total_costs AS overall_profitability
  FROM
    total_revenue tr,
    total_costs tc
)
SELECT
  cast(overall_profitability as decimal(10,4)),
  CURRENT_TIMESTAMP AS load_ts
FROM
  profitability
