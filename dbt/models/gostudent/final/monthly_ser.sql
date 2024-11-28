{{
  config(
    materialized = 'table'
  )
}}

WITH total_costs AS (
  SELECT
    TO_CHAR(c.customer_date, 'YYYY-MM') AS customer_month,
    SUM(m.marketing_costs) AS total_marketing_costs,
    SUM(s.sales_costs) AS total_sales_costs,
    SUM(s.trial_costs) AS total_trial_costs
  FROM
    {{ ref('stg_customer') }} c
  LEFT JOIN
    {{ ref('stg_marketing_costs') }} m ON TO_CHAR(c.customer_date, 'YYYY-MM') = m.marketing_costs_date
  LEFT JOIN
    {{ ref('stg_sales_costs') }} s ON TO_CHAR(c.customer_date, 'YYYY-MM') = s.sales_costs_date
  GROUP BY
    customer_month
),
customer_revenue AS (
  SELECT
    TO_CHAR(c.customer_date, 'YYYY-MM') AS customer_month,
    SUM(clv.avg_clv) AS total_revenue
  FROM
    {{ ref('stg_customer') }} c
  LEFT JOIN
    {{ ref('stg_clv') }} clv ON TO_CHAR(c.contract_length, 'YYYY-MM') = TO_CHAR(clv.contract_length, 'YYYY-MM')
  GROUP BY
    customer_month
)
SELECT
  tc.customer_month,
  customer_revenue.total_revenue / 
            (SUM(tc.total_marketing_costs + tc.total_sales_costs + tc.total_trial_costs)) as ser,
  CURRENT_TIMESTAMP AS load_ts
FROM
  total_costs tc
JOIN
  customer_revenue customer_revenue ON tc.customer_month = customer_revenue.customer_month
GROUP BY
  tc.customer_month, customer_revenue.total_revenue