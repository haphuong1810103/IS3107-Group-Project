1. Fetch the docker-compose.yaml file for Airflow 2.10.5

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.5/docker-compose.yaml'
```

2. Initialize the environment with the docker-compose.yaml file

```bash
mkdir -p ./dags ./logs ./plugins ./configdock
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

3. Make sure docker compose is installed

```bash
docker compose version
```

4. Make sure docker daemon is running

```bash
docker info
```

3. Start the Airflow environment

```bash
docker compose up airflow-init
```

4. Start all the services

```bash
docker compose up
```
