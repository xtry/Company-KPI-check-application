CREATE TABLE IF NOT EXISTS raw.call (
    call_id integer,
    contact_id integer,
    trial_booked float,
    trial_date date,
    call_attempts float,
    total_call_duration float,
    calls_30 float,
    load_ts timestamp default current_timestamp
);