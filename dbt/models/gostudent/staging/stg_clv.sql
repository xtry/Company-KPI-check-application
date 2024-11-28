with cleaned as (

    select
        contract_length AS contract_length,
        replace(trim(avg_clv), ',', '')::decimal(10,2) AS avg_clv
    from
            {{ source('raw', 'clv') }}
)

SELECT
    contract_length,
    avg_clv
FROM cleaned