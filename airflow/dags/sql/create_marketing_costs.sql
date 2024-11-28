CREATE TABLE IF NOT EXISTS raw.marketing_costs (
    marketing_costs_date varchar(7),
    marketing_source varchar(64),
    marketing_costs numeric(15,5),
    load_ts timestamp default current_timestamp
);
