"""Some docstring about document"""
#region Imports
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-amd64"
import sys

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from src.spark_config import initialize_spark
    from pyspark.sql import DataFrame 
    from pyspark.sql.functions import (
        col, lit, when, count, sum as spark_sum, avg,
        min as spark_min, max as spark_max, round as spark_round,
        desc, asc, to_timestamp, to_date, hour, dayofweek,
        lower, upper, trim, concat, coalesce, explode, split,
        countDistinct, collect_list, first, input_file_name,
        regexp_extract, current_timestamp
    )

except ImportError as e:
    logger.error(f"PySpark import failed: {e}")
    logger.error("Please install PySpark: pip install pyspark")
    sys.exit(1)

try:
    import meteostat as ms
except ImportError as e:
    logger.error(f"Meteostat weather data import failed: {e}")
    logger.error("Please install Meteostat: pip install meteostat")
    sys.exit(1)
try:
    import pandas as pd
except ImportError as e:
    logger.error(f"Pandas import failed: {e}")
    logger.error("Please install Pandas: pip install pandas")
    sys.exit(1)

from pathlib import Path
from datetime import datetime, date, timedelta

from src.helper_functions import get_coordinates, get_paths, get_schema
#endregion

PROJECT_ROOT = Path(__file__).resolve().parent

class Pipeline:
    def __init__(self, data_dir: str = None, shared_dir: str = None):
        """Initialize the pipeline with output directories."""
        if data_dir is None:
            temp_dir = get_paths()
        else:
            temp_dir = get_paths(data_dir)
    
        if shared_dir is None:
            shared_dir = PROJECT_ROOT

        self.competitions_dir = temp_dir["COMPETITIONS"]
        self.matches_dir = temp_dir["MATCHES"]
        self.events_dir = temp_dir["EVENTS"]
        self.bronze_dir = f"{shared_dir}/data/bronze"
        self.silver_dir = f"{shared_dir}/data/silver"
        self.gold_dir = f"{shared_dir}/data/gold"


    def create_spark_session(self):
        """Create and configure Spark session."""
        logger.info("Creating Spark session...")

        self.spark = initialize_spark()
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    #region Bronze Layer
    def ingest_data(self, category: str):
        """a"""
        logger.info(f"Fetching data for {category}.")

        schema = get_schema(category)
        match category:
            case "competitions":
                df = self.spark.read.schema(schema).json(self.competitions_dir, multiLine=True, recursiveFileLookup=True)
            case "matches":
                df = self.spark.read.schema(schema).json(self.matches_dir, multiLine=True, recursiveFileLookup=True)
            case "events":
                df = self.spark.read.schema(schema).json(self.events_dir, multiLine=True, recursiveFileLookup=True)
                df = df.withColumn("match_id", regexp_extract(input_file_name(), r"/(\d+)\.json$", 1).cast("long"))
            case _:
                raise ValueError(f"Unknown category: {category}")
        
        df = df.withColumn("ingest_date", to_date(current_timestamp()))
        df = df.withColumn("ingest_time", current_timestamp())

        logger.info(f"Dataframe successfully created.")
        return df


    def fetch_weather_data(self, match_df: DataFrame) -> DataFrame:
        """
        Fetches weather data from the nearest weather station for the different stadiums.
        
        Args:
            match_df (DataFrame): Statsbomb dataframe containing matches

        Returns:
            (Dataframe): Returns the weather DataFrame.
        """
        logger.info("Fetching weather data from Meteostat API.")
        distinct_df = match_df.select(col("match_id"), col("stadium.name").alias("stadium"), col("stadium.country.name").alias("country"), col("match_date"))
        
        weather = []

        for row in distinct_df.collect():
            try:
                coordinates = get_coordinates(row["stadium"], row["country"])
            except KeyError as e:
                logger.info(f"No stadium found for {row["stadium"]} in {row["country"]}.")
                continue
            # if coordinates is None:
            #     continue

            lat, long = coordinates

            POINT = ms.Point(lat, long)
            station = ms.stations.nearby(POINT, limit=1).index[0]

            match_date =  datetime.strptime(row["match_date"], "%Y-%m-%d")
            start = match_date + timedelta(hours=12)
            end = match_date + timedelta(hours=24)

            w = ms.hourly(station, start, end).fetch()
            if w is None: # Meaning the weather station has no data for that time-slot
                # TODO: Figure out if we should log it or quarantine it?
                continue

            w["match_id"] = row["match_id"]
            weather.append(w)

        weather = pd.concat(weather)
        weather_df = self.spark.createDataFrame(weather)
        weather_df = weather_df.withColumn("ingest_date", to_date(current_timestamp()))
        weather_df = weather_df.withColumn("ingest_time", current_timestamp())

        logger.info(f"Finished fetching weather data. \n - Created DataFrame with size {weather_df.count()}.")
        return weather_df


    def save_compressed_data(self, df: DataFrame, category: str):
        """Saves dataframe to parquet with the right path."""
        logger.info(f"Creating Parquet files for {category}.")

        match category:
            case "competitions":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/statsbomb/competitions", partitionBy="ingest_date")
            case "matches":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/statsbomb/matches", partitionBy="ingest_date")
            case "events":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/statsbomb/events", partitionBy="ingest_date")
            case "weather":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/meteostat", partitionBy="ingest_date")
            case "quarantine":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/quarantine", partitionBy="ingest_date")

        logger.info(f"Successfully created Parquet-files for {category}.")
    #endregion

    #region Silver Layer
    #endregion

