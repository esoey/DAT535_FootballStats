from pipeline import Pipeline

pipeline = Pipeline()

pipeline.create_spark_session()

categories = ["comptetitions", "matches", "events"]

for category in categories:
    df = pipeline.ingest_data(category)
    pipeline.save_compressed_data(df, category)
