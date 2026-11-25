from pipeline import Pipeline

pipeline = Pipeline()

def bronze(category: str = None):
    print("="*60)
    print("-"*3, " Bronze Layer ", "-"*3)
    print("="*60)
    print("Initializing spark.")
    pipeline.create_spark_session()
    categories = ["competitions", "matches", "events"]

    if category is None:
        for category in categories:
            print(f"Ingesting data for {category}.")
            df = pipeline.ingest_data(category)
            if category != "events":
                df.cache()

            if category == "matches":
                print(f"Fetching weatherdata from Meteostat.")
                weather_df = pipeline.fetch_weather_data(df)
                pipeline.save_compressed_data(weather_df, "weather")
                print(f"Saved data to Parquet for weather with size {weather_df.count()}.")
        
            pipeline.save_compressed_data(df, category)
            print(f"Finished saving data to Parquet for {category} with size {df.count()}.")
            df.unpersist()

    elif category not in categories:
        print("="*60)
        print("Please provide a valid category from these choices [competitions, matches, events].")
        print("="*60)

    else:
        print(f"Ingesting data for {category}.")
        df = pipeline.ingest_data(category)
        if category != "events":
            df.cache()
            
        if category == "matches":
            print(f"Fetching weatherdata from Meteostat.")
            weather_df = pipeline.fetch_weather_data(df)
            pipeline.save_compressed_data(weather_df, "weather")
            print(f"Saved data to Parquet for weather with size {weather_df.count()}.")
    
        pipeline.save_compressed_data(df, category)
        print(f"Finished saving data to Parquet for {category} with size {df.count()}.")
        df.unpersist()

bronze("events")