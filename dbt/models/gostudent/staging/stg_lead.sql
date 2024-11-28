with cleaned as (

    select
        lead_id,
        contact_id,
        marketing_source,
        create_date,
        known_city,
        message_length,
        test_flag    
    from
            {{ source('raw', 'lead') }}
)

SELECT DISTINCT ON (contact_id)
    lead_id,
    contact_id,
    marketing_source,
    create_date,
    known_city,
    message_length,
    test_flag
FROM cleaned

