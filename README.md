
# Stock Price Predictor

## Description

IS3107 Final Project

## Repository Structure


### airflow_docker (note that Docker is no longer used as Airflow is hosted on Google Cloud Composer)

- **Purpose**: Contains the Docker Compose file for setting up the Airflow environment and will store all the Directed Acyclic Graph (DAG) scripts and Python functions for ETL and machine learning.
- **Contents**:
  - `docker-compose.yaml`: Docker Compose file to set up Airflow services.
  - `dags/`: Directory for storing Airflow DAG scripts.
- **Setup**:
    1. Make sure directory is set to airflow_docker
        ```cd airflow_docker```
    2. Initialize the environment with the custom Dockerfile
        ```bash
        mkdir -p ./dags ./logs ./plugins ./config
        echo -e "AIRFLOW_UID=$(id -u)" > .env
        ```
    3. Start the Airflow Environment (1st time only)
        ```docker compose build```
    4. Start all services
        ```docker compose up```
    5. Access Airflow UI at `http://localhost:8080/`

    Default credentials   
    Username: airflow  
    Password: airflow  

### notebooks (to update)

### real_time_pipeline

- **Purpose**: This folder sets up a real-time data pipeline that fetches stock market data from the Yahoo Finance API and imports it into BigQuery.
The pipeline leverages several Google Cloud services:
  - Cloud Run
  - Cloud Scheduler
  - Pub/Sub
  - Dataflow
  - BigQuery

- **Contents**: 
  - `Dockerfile`: Configuration file to containerize the Python script for deployment on Google Cloud Run.
  - `requirements.txt`: List of Python packages required to run the script.
  - `real_time_streaming.py`: Python script that fetches data from the Yahoo Finance API and publishes it as a message to Pub/Sub.

- **Setup**:
  1. Pub/Sub Setup:
     Create a Pub/Sub topic to publish the messages containing the stock data.
  2. BigQuery Setup:
     Set up a BigQuery table to receive the streamed data.
  3. Deploy Cloud Run Service:
     Deploy the real_time_streaming.py script to Google Cloud Run using the provided Dockerfile and requirements.txt.
  4. Schedule the Job:
     Use Cloud Scheduler to trigger the Cloud Run service every minute to ensure real-time data fetching and publishing.
