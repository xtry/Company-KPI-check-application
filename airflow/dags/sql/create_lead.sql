CREATE TABLE IF NOT EXISTS raw.lead (
    lead_id integer,
    contact_id integer,
    marketing_source varchar(64),
    create_date date,
    known_city boolean,
    message_length integer,
    test_flag boolean,
    load_ts timestamp default current_timestamp
);
