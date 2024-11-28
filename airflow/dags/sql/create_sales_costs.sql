CREATE TABLE IF NOT EXISTS raw.sales_costs (
    sales_costs_date varchar(7),
    sales_costs numeric(15,2),
    trial_costs integer,
    load_ts timestamp default current_timestamp
);
