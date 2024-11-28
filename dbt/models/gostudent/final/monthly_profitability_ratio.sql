{{
  config(
    materialized = 'table'
  )
}}

WITH revenue_by_source_and_month AS (
  SELECT
    l.marketing_source,
    TO_CHAR(c.customer_date, 'YYYY-MM') AS customer_month,
    SUM(clv.avg_clv) AS total_revenue
  FROM
    {{ ref('stg_customer') }} c
  JOIN
    {{ ref('stg_lead') }} l ON c.contact_id = l.contact_id
  LEFT JOIN
    {{ ref('stg_clv') }} clv ON TO_CHAR(c.contract_length, 'YYYY-MM') = TO_CHAR(clv.contract_length, 'YYYY-MM')
  GROUP BY
    l.marketing_source, customer_month
),
marketing_costs_by_source_and_month AS (
  SELECT
    m.marketing_source,
    m.marketing_costs_date AS marketing_month,
    SUM(m.marketing_costs) AS total_marketing_costs
  FROM
    {{ ref('stg_marketing_costs') }} m
  GROUP BY
    m.marketing_source, marketing_month
)
SELECT
  rbs.marketing_source,
  rbs.customer_month,
  cast(rbs.total_revenue as decimal(15,2)) as total_revenue,
  cast(mcs.total_marketing_costs as decimal(15,2)) as total_marketing_costs,
  cast(rbs.total_revenue / mcs.total_marketing_costs as decimal(5,2)) AS profitability_ratio,
  CURRENT_TIMESTAMP AS load_ts
FROM
  revenue_by_source_and_month rbs
LEFT JOIN
  marketing_costs_by_source_and_month mcs ON rbs.marketing_source = mcs.marketing_source
                                          AND rbs.customer_month = mcs.marketing_month
ORDER BY
  rbs.customer_month DESC, profitability_ratio DESC
