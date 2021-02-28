# CovidBCN
Project to export data from different APIs to get a general overview of the Covid outbreak, particularly on the Catalonian Area

# File Structure

* /config -> airflow.cfg file to be loaded into the Docker container.
* /dags -> Our Airflow code in Python to perform the data loadings.
* /docker-compose.yml -> File used by the Makefile script
* /Makefile -> Script to automate the docker build and docker up actions.
* /requirements.txt -> Extra libraries needed by Docker to use our code.
* /variables.json -> Variables used inside the code.

# How to
To run the project we need to have [Docker](https://www.docker.com/products/docker-desktop) installed.

After downloading that report, we can change the password of the connection `postgres_default` to `airflow` and so be able to connect to the PostgreSQL database inside the container.

