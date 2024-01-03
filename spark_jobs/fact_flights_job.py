# import pyspark
from pyspark.sql import SparkSession
# from pyspark.sql import functions as F


spark = SparkSession.builder \
    .appName('Flights') \
    .getOrCreate()
  
spark.conf.set('temporaryGcsBucket', 'dataproc-temp-europe-west6-828225226997-fckhkym8')

#Load data from BigQuery.
airports_bq = spark.read.format('bigquery') \
  .option('table', f'resolute-choir-403411.flights.AirPorts') \
  .load()
airports_bq.createOrReplaceTempView('airports')

flights_bq = spark.read.format('bigquery') \
  .option('table', f'resolute-choir-403411.flights.Flights2') \
  .load()
flights_bq.createOrReplaceTempView('flights')

# Make Joining And Transformation "If Needed"
fact_flights = spark.sql(
    """
    SELECT 
        flights.Flight as flight ,
        origin.airport as origin_airport,
        origin.iata as origin_iata ,
        origin.Country as origin_country , 
        origin.City as origin_city, 
        origin.State as origin_state,
        dest.airport as dest_airport,
        dest.iata as dest_iata,
        dest.Country as dest_country,
        dest.City as dest_city,
        dest.State as dest_state,
        flights.Stops as stops ,
        flights.Price as price

    FROM airports as origin
    join flights as flights
        on origin.iata = flights.Orgin
    join airports as dest
        on dest.iata = flights.Dept
    """
    )

fact_flights.write.format('bigquery') \
  .option('table',  f'resolute-choir-403411.flights.fact_flights') \
  .save()

