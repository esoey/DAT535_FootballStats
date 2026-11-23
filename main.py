from pipeline import Pipeline

pipeline = Pipeline()

pipeline.create_spark_session()

df = pipeline.ingest_data("competitions")
pipeline.save_compressed_data(df, "competitions")