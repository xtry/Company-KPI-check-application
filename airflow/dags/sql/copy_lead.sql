COPY raw.lead (lead_id, contact_id, marketing_source, create_date, known_city, message_length, test_flag)
                    FROM stdin WITH CSV HEADER DELIMITER AS ',';

