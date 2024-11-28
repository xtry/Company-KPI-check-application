

with 
    "call" as (
        select * 
        from {{ ref('stg_call') }}
    ),
    customer as (
        select *
        from {{ ref('stg_customer') }}
    ), 
    lead as (
        select * 
        from {{ ref('stg_lead') }}
    )

select 
    --c.call_id,
    c.contact_id as contact_id,
    c.trial_booked as trial_booked,
    c.trial_date as trial_date,
    c.call_attempts as call_attempts,
    c.total_call_duration as total_call_duration,
    c.calls_30 as calls_30,

    cu.customer_id as customer_id,
    cu.customer_date as customer_date,
    cu.contract_length as contract_length,

    l.marketing_source as marketing_source,
    l.create_date as create_date,
    l.known_city as known_city,
    l.message_length as message_length,
    l.test_flag as test_flag  

from lead l
left join "call" c on l.contact_id = c.contact_id 
left join customer cu on c.contact_id = cu.contact_id