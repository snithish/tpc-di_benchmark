# TPC - DI - Benchmark artefacts for Cloud Composer and BigQuery

## Install Airflow:

### Create Airflow Docker Image
1. Clone Repo [Airflow Docker](https://github.com/puckel/docker-airflow)
2. Build a docker image from
    ```shell script
    cd docker-airflow
    docker build --rm --build-arg AIRFLOW_DEPS="gcp" -t tpc-di/benchmark-airflow .
    ```
3. Ensure image is created by executing
    ```shell script
    docker images
    ```

### Start Airflow
1. Run the following command to start Apache Airflow
    ```shell script
    docker-compose up -d
    ```
2. Navigate to [Airflow Web UI](http://localhost:8080/)
![Airflow Home Page](resources/AirflowHomePage.png)

### Create a Service Account in GCP for Airflow to use
1. Navigate to GCP [BigQuery API](https://console.cloud.google.com/apis/api/bigquery-json.googleapis.com/overview) and enable it if not already enabled
2. Navigate to the [Credential Page](https://console.cloud.google.com/apis/api/bigquery-json.googleapis.com/credentials)
![GCP Credential Page](resources/GCPCredentialPage.png)
3. Click on `Create Credential` button and choose `Service Account`
![GCP Credential Create](resources/GCPCredentialCreatePage.png)
4. Fill in `Service Account Name` and `ID`
5. Click on `Create`
6. Grant `BigQuery Admin` and `Storage Admin` roles and `Continue`
![GCP Assign Roles](resources/GCPCredentialSelectRole.png)
7. Click on `Create Key` and choose `JSON` key
![GCP Create Key](resources/GCPCredentialCreateJson.png)
8. Download generated JSON key and keep it safe

### Create connections in Airflow Admin UI
1. Navigate to Admin -> Connections [Connections Page](http://localhost:8080/admin/connection/)
![Airflow Connections Page](resources/AirflowConnectionsPage.png)
2. Find `bigquery_default` connection or equivalent and edit to get the edit window
![BigQuery Connection Edit Page](resources/AirflowBigQueryEdit.png)
3. Fill in the contents of `keyfile.json` from GCP obtained in earlier step and save connection
4. Repeat the above steps to setup connection for `google_cloud_default`
