COPY raw.call (call_id, contact_id, trial_booked, trial_date, call_attempts, total_call_duration, calls_30)
                    FROM stdin WITH CSV HEADER DELIMITER AS ';';