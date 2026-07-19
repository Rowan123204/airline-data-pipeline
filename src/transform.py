from pyspark.sql.functions import col, sum, when

def transform(flights, airports, airlines):

    # ==========================================
    # Data Quality Check
    # ==========================================
    def data_quality_report(df, dataset_name):
        print(f"\n{'='*50}")
        print(f" {dataset_name.upper()}")
        print(f"{'='*50}")

        total_rows = df.count()
        print(f"total_rows: {total_rows}")

        duplicates = total_rows - df.dropDuplicates().count()
        print(f"Duplicates: {duplicates}")

        print("\n🔹 Nulls")
        nulls_df = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])
        nulls_df.show(vertical=True)

    def clean_data(df):
        df_cleaned = df.dropDuplicates()
        df_cleaned = df_cleaned.dropna()
        return df_cleaned


    data_quality_report(airports, "Airports")
    data_quality_report(airlines, "Airlines")
    data_quality_report(flights, "Flights")

    clean_airports = clean_data(airports)
    clean_airlines = clean_data(airlines)
    clean_flights = clean_data(flights)

    # ==========================================
    # Transformations
    # ==========================================

    # 1. Casting
    transformed_flights = clean_flights.withColumn(
        "DEPARTURE_DELAY", col("DEPARTURE_DELAY").cast("integer")
    )

    # 2. Feature Engineering
    transformed_flights = transformed_flights.withColumn(
        "IS_DELAYED",
        when(col("DEPARTURE_DELAY") > 0, 1).otherwise(0)
    )

    # 3. Joins
    # Problem: both flights and airlines tables have a column named AIRLINE
    # Solution: Rename AIRLINE column in airlines table to AIRLINE_NAME before joining
    clean_airlines = clean_airlines.withColumnRenamed("AIRLINE", "AIRLINE_NAME")

    # Join airlines table
    final_df = transformed_flights.join(
        clean_airlines,
        transformed_flights["AIRLINE"] == clean_airlines["IATA_CODE"],
        "left"
    ).drop(clean_airlines["IATA_CODE"])

    # Hidden problem: airports table has many columns (like CITY and STATE). Joining it twice will duplicate columns.
    # Parquet format rejects saving if there are duplicate column names!
    # Solution: Select only IATA_CODE and AIRPORT from airports table before joining and ignore the rest.
    clean_airports = clean_airports.select("IATA_CODE", "AIRPORT")
    
    # Join origin airport
    final_df = final_df.join(
        clean_airports,
        final_df["ORIGIN_AIRPORT"] == clean_airports["IATA_CODE"],
        "left"
    ).withColumnRenamed("AIRPORT", "ORIGIN_AIRPORT_NAME").drop(clean_airports["IATA_CODE"])

    
    final_df = final_df.join(
        clean_airports,
        final_df["DESTINATION_AIRPORT"] == clean_airports["IATA_CODE"],
        "left"
    ).withColumnRenamed("AIRPORT", "DESTINATION_AIRPORT_NAME").drop(clean_airports["IATA_CODE"])

    final_df.select("AIRLINE", "AIRLINE_NAME", "DEPARTURE_DELAY", "IS_DELAYED").show(10)
    
    return final_df 