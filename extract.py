
from spark import spark

def extract():

 flights = spark.read.csv("raw_data/flights.csv", header=True, inferSchema=False).limit(1000)
 airports = spark.read.csv("raw_data/airports.csv", header=True, inferSchema=False)
 airlines = spark.read.csv("raw_data/airlines.csv", header=True, inferSchema=False)
 return flights, airports, airlines 