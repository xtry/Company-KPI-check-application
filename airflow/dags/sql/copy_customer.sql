COPY raw.customer (customer_id, contact_id, customer_date, contract_length)
                    FROM stdin WITH CSV HEADER DELIMITER AS ',';

