CREATE TABLE IF NOT EXISTS raw.clv (
    contract_length integer,
    avg_clv varchar(64),
    load_ts timestamp default current_timestamp
);