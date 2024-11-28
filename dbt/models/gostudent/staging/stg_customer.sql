with cleaned as (

    select
        customer_id,
        contact_id,
        customer_date,
        contract_length    
    from
            {{ source('raw', 'customer') }}
)

SELECT DISTINCT ON (contact_id)
    customer_id,
    contact_id,
    customer_date,
    contract_length
FROM cleaned

