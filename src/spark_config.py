from pyspark.sql import SparkSession

def initialize_spark():
    """
    Gets and initializes the spark session.
    
    Return
    ------
        spark: SparkSession
            The entry point to programming Spark with the Dataset and DataFrame API.
    """
    spark = SparkSession.builder \
    .appName("DAT535-Project-FootballStats") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"Spark Version: {spark.version}")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    print(f"Spark UI: {spark.sparkContext.uiWebUrl}")

    return spark