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
    # المشكلة: جدول flights وجدول airlines الاتنين فيهم عمود اسمه AIRLINE
    # الحل: نغير اسم عمود AIRLINE في جدول الشركات لـ AIRLINE_NAME قبل الدمج
    clean_airlines = clean_airlines.withColumnRenamed("AIRLINE", "AIRLINE_NAME")

    # دمج جدول الشركات
    final_df = transformed_flights.join(
        clean_airlines,
        transformed_flights["AIRLINE"] == clean_airlines["IATA_CODE"],
        "left"
    ).drop(clean_airlines["IATA_CODE"])

    # مشكلة خفية جديدة: جدول المطارات فيه أعمدة كتير (زي CITY و STATE). لما ندمجه مرتين، الأعمدة دي هتتكرر.
    # صيغة Parquet بترفض الحفظ لو في عمودين ليهم نفس الاسم!
    # الحل: نختار فقط كود المطار واسمه من جدول المطارات قبل الدمج ونتجاهل باقي الأعمدة.
    clean_airports = clean_airports.select("IATA_CODE", "AIRPORT")
    
    # دمج مطار الإقلاع
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