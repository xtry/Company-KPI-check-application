COPY raw.sales_costs (sales_costs_date, sales_costs, trial_costs)
                    FROM stdin WITH CSV HEADER DELIMITER AS ',';

