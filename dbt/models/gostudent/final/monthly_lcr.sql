{{
  config(
    materialized = 'table'
  )
}}

WITH lead_conversion_data AS (
  SELECT
    TO_CHAR(l.create_date, 'YYYY-MM') AS lead_month,  
    l.marketing_source, 
    l.known_city,  
    l.message_length, 
    COUNT(DISTINCT l.contact_id) AS total_leads,
    COUNT(DISTINCT c.contact_id) AS converted_leads, 
    CASE
      WHEN COUNT(DISTINCT l.contact_id) > 0 THEN
        COUNT(DISTINCT c.contact_id) * 1.0 / COUNT(DISTINCT l.contact_id) 
      ELSE 0
    END AS conversion_rate
  FROM
    {{ ref('stg_lead') }} l
  LEFT JOIN
    {{ ref('stg_customer') }} c ON l.contact_id = c.contact_id
  GROUP BY
    TO_CHAR(l.create_date, 'YYYY-MM'), l.marketing_source, l.known_city, l.message_length
),
monthly_conversion_trends AS (
  SELECT
    lead_month,
    AVG(conversion_rate) AS avg_conversion_rate,  
    COUNT(DISTINCT marketing_source) AS source_count, 
    COUNT(DISTINCT known_city) AS known_city_count
  FROM
    lead_conversion_data
  GROUP BY
    lead_month
)
SELECT
  lead_month,
  cast(avg_conversion_rate as decimal(10,4)),
  source_count,
  known_city_count,
  CURRENT_TIMESTAMP AS load_ts
FROM
  monthly_conversion_trends
ORDER BY
  lead_month DESC
