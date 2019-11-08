# TPC - DI - Benchmark artefacts for Data Fusion

## Install Airflow:

1. Clone Repo [Airflow Docker](https://github.com/puckel/docker-airflow)
2. Build a docker image from
    ```sh
    cd docker-airflow
    docker build --rm --build-arg AIRFLOW_DEPS="gcp,dask" -t puckel/docker-airflow .
    ```