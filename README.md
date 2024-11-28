# Introduction
I used an open-source solution to leverage code reusability, plus applied multiple fixes to it.
I build my solution on top of airflow(for etl/elt), dbt, postgres and superset(for visualisation). I find airflow and superset convenient & straightforward to use, and the rest were mandatory for the task.

# Running the project
* Run `init.sh` with sudo privilege to set up folder permissions. It also deletes the company_dw's data kept on the disk. Please check before running.
* Build and run the docker-compose
  * `docker-compose build`
  * `docker-compose up`
  * `docker-compose down` (optional)
  * `docker-compose down --volumes --remove-orphans` (optional)
* It takes some time for the system to come alive.
  * We can check the docker containers and their statuses with `docker ps`. Handy commands to look around (optional):
    * `docker logs containername`
    * `docker exec -it containername bash`
* We can check `localhost:8080` and look for the airflow webinterface to come up. Once it does, all the airflow services should be running.
  * creds: `admin`/`admin`
* Superset runs on `localhost:8088`
  * creds: `admin`/`admin`

# Credentials
The credentials are stored in the project's `.env` file.

# Services
The `docker-compose` kicks off these services:
* `gostudent_airflow_1`: airflow's webinterface, I run the DBT docs visualisation from here later.
* `gostudent_airflow-worker`
* `gostudent_airflow-scheduler`
* `gostudent_superset`: superset's service
* `gostudent_redis_1`: message broker for airflow
* `gostudent_company_dw_1`: will be reachable with the hostname company_dw, this is the warehouse where we'll store the result of the analytics
* `gostudent_airflowdb_1`: airflow pipeline's statuses etc
* `gostudent_superset_db_1`: superset's statuses etc
* `gostudent_redis_1`: message broker that queues task for workers to execute

# Running the workflow
* `ingest_raw_data`: pure python script as desired, I load in the raw data
* `run_dbt_init_tasks`: this initializes the dbt env and populates also the schema models (`target/manifest.json`), should be run only once. Populates dbt docs.
  * Caveat: if `target/manifest.json` doesn't yet exists then airflow failes to load the next dag. After succeeding with `run_dbt_init_tasks`, the next dag will be loaded and can run normally.
* `run_dbt_model_gostudent`: reads the dbt model and creates run/test tasks accordingly. Populates dbt docs.
  * Caveat: when adding new models, I found it useful to reload the raw data and then run the `run_dbt_model_gostudent` again.

![image](https://github.com/user-attachments/assets/090d978c-c403-4f48-b454-00c416ee1745)

# DBT docs visualisation (macros, schemas)
* Login to the webserver with cli, likely with the name `gostudent_airflow_1`
* Enter the dbt folder `cd dbt` (alternatively, dbt env params can be set up)
* Run `dbt docs serve --host 0.0.0.0 --port 5555` as I opened this port for seamless access

![image](https://github.com/user-attachments/assets/a270f621-f130-4fc1-a078-cb85517c374a)

# Superset dashboard visualisation
In superset it's possible to import the dashboard that I created. It consits of 6 panels displaying the requested aggregated data that can support marketing team or decision makers to choose the path for the company. 
* Go to `Dashboards`, click import icon (right hand side)
* Specify the zip file that can be found in the project folder superset/assets. This includes both the `company_dw` connection details + the dashboard.
  * Alternatively, if the import was not working it's possible to set up the database connection. Use `company_dw` for hostname and db, port `5432` and `postgre`/`postgres`

![image](https://github.com/user-attachments/assets/5d9ba01c-5b87-470c-b236-0f6be427058c)

# The equations for aggregated data
$$
CAC = \frac{ Total \ marketing \ Costs \ + \ Total \ Sales \ Costs \ + \ Total \ Trial \ Costs }{ New \ customers \ acquired } \text{\quad per monthly basis }
$$

$$
SER = \frac{ Total \ Revenue \ (new \ customer \ sales)}{Total \ Marketing \ Costs \ + \ Total \ Sales \ Costs \ + \ Total \ Trial \ Costs \} \text{\quad per monthly basis}
$$

$$
Profitability = \frac{Total \ Marketing \ Costs}{ Total \ Revenue} \text{\quad per\ monthly\ or \ aggregate \ or \ overall \ - \ three \ separate \ queries}
$$

$$
LCR = \frac{The \ number \ of \ converted \ leads}{Total \ number \ of \ leads \ within \ timeframe } \text{\quad per \ monthly \ basis}
$$

# Architecture diagram
The whole flow looks like this oversimplified:
`csv files ---> ingest_raw_data --->  run_dbt_init_tasks ---> run_dbt_model_gostudent ---> equation analytics + superset dashboards`

For more details, I enclose the workflow graphs
* ingest_raw_data
![image](https://github.com/user-attachments/assets/a742332c-ecd1-4361-aff0-97c5b35dbf5b)

* run_dbt_model_gostudent
![image](https://github.com/user-attachments/assets/6b55ead8-cb12-4e36-9625-31f512543231)


# Improvement ideas
* SSL for the web interfaces (nginx)
* Dockerize the dbt part as well
