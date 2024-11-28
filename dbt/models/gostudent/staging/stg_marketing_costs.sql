with cleaned as (

    select
        marketing_costs_date,
        marketing_source,
        marketing_costs
    from
            {{ source('raw', 'marketing_costs') }}
)

select * from cleaned

