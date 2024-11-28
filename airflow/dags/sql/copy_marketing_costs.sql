COPY raw.marketing_costs (marketing_costs_date, marketing_source, marketing_costs)
                    FROM stdin WITH CSV HEADER DELIMITER AS ',';