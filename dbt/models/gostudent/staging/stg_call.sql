with cleaned as (

    select
        call_id AS call_id,
        contact_id AS contact_id,
        (trial_booked <> 0) :: boolean AS trial_booked,
        CAST(trial_date AS DATE) AS trial_date, 
        CAST(call_attempts AS INT) AS call_attempts,
        CAST(total_call_duration AS DECIMAL(10, 2)) AS total_call_duration,
        CAST(calls_30 AS INT) AS calls_30
    from
            {{ source('raw', 'call') }}
)

SELECT DISTINCT ON (contact_id)
    call_id,
    contact_id,
    trial_booked,
    trial_date,
    call_attempts,
    total_call_duration,
    calls_30
FROM cleaned