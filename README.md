
# Stock Price Predictor

## Description

IS3107 Final Project

## Repository Structure

### airflow_docker

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
