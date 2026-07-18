
from spark import spark

def extract():

 flights = spark.read.csv("../data/raw/flights.csv", header=True, inferSchema=False).limit(1000)
 airports = spark.read.csv("../data/raw/airports.csv", header=True, inferSchema=False)
 airlines = spark.read.csv("../data/raw/airlines.csv", header=True, inferSchema=False)
 return flights, airports, airlines 