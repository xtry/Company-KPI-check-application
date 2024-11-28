CREATE TABLE IF NOT EXISTS raw.customer (
    customer_id integer,
    contact_id integer,
    customer_date date,
    contract_length integer,
    load_ts timestamp default current_timestamp
);
