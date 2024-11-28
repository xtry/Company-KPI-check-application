with cleaned as (

    select
        sales_costs_date,
        sales_costs,
        trial_costs
    from
            {{ source('raw', 'sales_costs') }}
)

select * from cleaned

