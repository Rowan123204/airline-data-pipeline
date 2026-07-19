
from spark import spark

# Absolute paths to the raw data files inside the Docker container.
# The project directory is mounted at /opt/airflow/project/ in docker-compose.yaml
DATA_PATH = "/opt/airflow/project/data/raw"

def extract():

    flights  = spark.read.csv(f"{DATA_PATH}/flights.csv",  header=True, inferSchema=False).limit(1000)
    airports = spark.read.csv(f"{DATA_PATH}/airports.csv", header=True, inferSchema=False)
    airlines = spark.read.csv(f"{DATA_PATH}/airlines.csv", header=True, inferSchema=False)

    return flights, airports, airlines