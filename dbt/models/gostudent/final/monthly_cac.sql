{{
  config(
    materialized = 'table'
  )
}}

WITH total_costs AS (
  SELECT
    c.contact_id,
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
    c.contact_id, customer_month
),
customer_count AS (
  SELECT
    TO_CHAR(customer_date, 'YYYY-MM') AS customer_month,
    COUNT(DISTINCT contact_id) AS customer_count
  FROM
    {{ ref('stg_customer') }} c
  GROUP BY
    customer_month
)
SELECT
  tc.customer_month,
  (SUM(tc.total_marketing_costs + tc.total_sales_costs + tc.total_trial_costs) / cc.customer_count) AS cac,
  CURRENT_TIMESTAMP AS load_ts
FROM
  total_costs tc
JOIN
  customer_count cc ON tc.customer_month = cc.customer_month
GROUP BY
  tc.customer_month, cc.customer_count