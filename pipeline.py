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
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        DoubleType, TimestampType, BooleanType, LongType, ArrayType
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

PROJECT_ROOT = Path(__file__).resolve().parent

class Pipeline:
    def __init__(self, data_dir: str = None, shared_dir: str = None):
        """Initialize the pipeline with output directories."""
        if data_dir is None:
            temp_dir = self.get_paths()
        else:
            temp_dir = self.get_paths(data_dir)
    
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


    def ingest_data(self, category: str):
        """a"""
        logger.info(f"Fetching data for {category}.")

        schema = self.get_schema(category)
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
        # TODO: Expand the helper-function of get_coordinates so it matches more stadiums
        distinct_df = match_df.select(["match_id", "stadium.name", "match_date"])
        weather = []

        for row in distinct_df.collect():
            try:
                lat, long = self.get_coordinates(row["name"])
            except KeyError:
                # TODO: Do we need any handling more than to skip?
                continue

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


    def get_paths(self, path: str | Path = Path(__file__).parent.parent / "open-data"):
        """
        Retrieves the different paths for the statsbomb data.

        Args:
            path (str): Path to the statsbomb folder (defaults to a folder-structure above the repo).

        Returns:
            Dictionary with specific paths.
        """
        return {
            "COMPETITIONS": f"{path}/data/competitions.json",
            "MATCHES": f"{path}/data/matches",
            "EVENTS": f"{path}/data/events"
        }


    def get_coordinates(self, stadium: str):
        """
        A function that gives you coordinates from all the stadiums participating in Laliga from Statsbomb per November, 2026.

        Args:
            stadium: Name of the stadium.

        Returns:
            Tuple with latitude and longitude.
        """
        stadium_coordinates = {
            "Abanca-Balaídos" : (42.211766, -8.739666),
            "Bernabéu" : (40.453044, -3.688197),
            "Campos de Sport de El Sardinero" : (43.476304, -3.79331),
            "Coliseum Alfonso Pérez" : (40.325725, -3.714933),
            "Estadi Municipal de Montilivi" : (41.961166, 2.828428),
            "Estadi Olímpic Lluís Companys" : (41.36472, 2.155645),
            "Estadio Abanca-Riazor" : (43.368716, -8.41751),
            "Estadio Alfredo Di Stéfano" : (40.47697, -3.614285),
            "Estadio Benito Villamarín" : (37.356406, -5.981739),
            "Estadio Carlos Belmonte" : (38.981082, -1.852144),
            "Estadio Ciudad de Valencia" : (39.494643, -0.363862),
            "Estadio El Alcoraz" : (42.13185, -0.42472),
            "Estadio El Sadar" : (42.7966625, -1.6371311),
            "Estadio Enrique Roca de Murcia" : (38.0422241, -1.1447324),
            "Estadio Heliodoro Rodríguez Lopéz" : (28.4632428, -16.260548),
            "Estadio José Rico Pérez" : (38.3573459, -0.4925974), 
            "Estadio La Rosaleda" : (36.73426080783186, -4.426581486641816), 
            "Estadio Manuel Martínez Valero" : (38.2670445, -0.6633051), 
            "Estadio Municipal El Molinón" : (43.536142696913366, -5.63743608626228), 
            "Estadio Municipal José Zorrilla" : (41.6444, -4.7611), 
            "Estadio Municipal de Butarque" : (40.340562, -3.760643), 
            "Estadio Municipal de Chapín" : (36.689343, -6.120523), 
            "Estadio Municipal de Ipurúa" : (43.1817, -2.4758), 
            "Estadio Nuevo Arcángel" : (37.87219, -4.764581), 
            "Estadio Nuevo Colombino" : (37.246663, -6.954204), 
            "Estadio Nuevo Los Cármenes" : (37.152891, -3.595894),
            "Estadio Nuevo Mirandilla" : (36.502604, -6.273017), 
            "Estadio Ramón Sánchez Pizjuán" : (37.384088, -5.970631), 
            "Estadio San Mamés" : (43.2642, -2.9494), 
            "Estadio Vicente Calderón" : (40.4017, -3.7206), # finnes ikke lengre
            "Estadio de Gran Canaria" : (28.100234, -15.456725), 
            "Estadio de Mendizorroza" : (42.8372, -2.6881), 
            "Estadio de Mestalla" : (39.4747, -0.3583), 
            "Estadio de Vallecas" : (40.391757, -3.658737), 
            "Estadio de la Cerámica" : (39.944208, -0.103405), 
            "Estadio de la Romareda" : (41.643034, -0.901014), 
            "Estádio Cívitas Metropolitano" : (40.4361, -3.5995), 
            "Los Pajaritos" : (41.7544, -2.4678), 
            "Nou Estadi Costa Daurada" : (41.12702, 1.272865), 
            "Power Horse Stadium – Estadio de los Juegos Mediterráneos" : (36.84, -2.4353), 
            "Reale Arena" : (43.30139, -1.973569), 
            "San Mamés Barria" : (43.264204, -2.949391), 
            "Spotify Camp Nou" : (41.380881, 2.122818), 
            "Stage Front Stadium" : (41.347847, 2.075603), 
            "Visit Mallorca Estadi" : (39.5900, 2.6300), 
        }
        return stadium_coordinates[stadium]


    def get_schema(self, category: str):
        """Get the specific schema for the different ingestions."""
        match category:
            case "competitions":
                return StructType([
                    StructField("competition_gender", StringType()),
                    StructField("competition_id", LongType()), 
                    StructField("competition_international", BooleanType()),
                    StructField("competition_name", StringType()),
                    StructField("competition_youth", BooleanType()),
                    StructField("country_name", StringType()),
                    StructField("match_available", StringType()),
                    StructField("match_available_360", StringType()),
                    StructField("match_updated", StringType()),
                    StructField("match_updated_360", StringType()),
                    StructField("season_id", LongType()),
                    StructField("season_name", StringType())
                ])
            case "matches": 
                return StructType([
                    StructField("away_score", LongType()),
                    StructField("away_team", StructType([
                        StructField("away_team_gender", StringType()),
                        StructField("away_team_group", StringType()),
                        StructField("away_team_id", LongType()),
                        StructField("away_team_name", StringType()),
                        StructField("country", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("managers", ArrayType(StructType([
                            StructField("country", StructType([
                                StructField("id", LongType()),
                                StructField("name", StringType()),
                            ])),
                            StructField("dob", StringType()),
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                            StructField("nickname", StringType()),
                        ]))),
                    ])),
                    StructField("competition", StructType([
                        StructField("competition_id", LongType()),
                        StructField("competition_name", StringType()),
                        StructField("country_name", StringType()),
                    ])),
                    StructField("competition_stage", StructType([
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ])),
                    StructField("home_score", LongType()),
                    StructField("home_team", StructType([
                        StructField("country", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType())
                        ])),
                        StructField("home_team_gender", StringType()),
                        StructField("home_team_group", StringType()),
                        StructField("home_team_id", LongType()),
                        StructField("home_team_name", StringType()),
                        StructField("managers", ArrayType(StructType([
                            StructField("country", StructType([
                                StructField("id", LongType()),
                                StructField("name", StringType()),
                            ])),
                            StructField("dob", StringType()),
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                            StructField("nickname", StringType()),
                        ]))),
                    ])),
                    StructField("kick_off", StringType()),
                    StructField("last_updated", StringType()),
                    StructField("last_updated_360", StringType()),
                    StructField("match_date", StringType()),
                    StructField("match_id", LongType()),
                    StructField("match_status", StringType()),
                    StructField("match_status_360", StringType()),
                    StructField("match_week", LongType()),
                    StructField("metadata", StructType([
                        StructField("data_version", StringType()),
                        StructField("shot_fidelity_version", StringType()),
                        StructField("xy_fidelity_version", StringType()),
                    ])),
                    StructField("referee", StructType([
                        StructField("country", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ])),
                    StructField("season", StructType([
                        StructField("season_id", LongType()),
                        StructField("season_name", StringType()),
                    ])),
                    StructField("stadium", StructType([
                        StructField("country", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ])),
                ])
            case "events":
                return StructType([
                    StructField("50_50", StructType([
                        StructField("outcome", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                    ])),
                    StructField("ball_receipt", StructType([
                        StructField("outcome", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                    ])),
                    StructField("ball_recovery", StructType([
                        StructField("recovery_failure", BooleanType()),
                    ])),
                    StructField("block", StructType([
                        StructField("deflection", BooleanType()),
                        StructField("offensive", BooleanType()),
                    ])),
                    StructField("carry", StructType([
                        StructField("end_location", ArrayType(DoubleType())),
                    ])),
                    StructField("clearance", StructType([
                        StructField("aerial_won", BooleanType()),
                        StructField("body_part", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("head", BooleanType()),
                        StructField("left_foot", BooleanType()),
                        StructField("other", BooleanType()),
                        StructField("right_foot", BooleanType()),
                    ])),
                    StructField("counterpress", BooleanType()),
                    StructField("dribble", StructType([
                        StructField("nutmeg", BooleanType()),
                        StructField("outcome", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("overrun", BooleanType()),
                    ])),
                    StructField("duel", StructType([
                        StructField("outcome", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("type", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                    ])),
                    StructField("duration", DoubleType()),
                    StructField("foul_committed", StructType([
                        StructField("advantage", BooleanType()),
                        StructField("card", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("offensive", BooleanType()),
                        StructField("type", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                    ])),
                    StructField("foul_won", StructType([
                        StructField("advantage", BooleanType()),
                        StructField("defensive", BooleanType()),
                    ])),
                    StructField("goalkeeper", StructType([
                        StructField("body_part", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("end_location", ArrayType(DoubleType())),
                        StructField("outcome", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("position", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("technique", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("type", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                    ])),
                    StructField("id", StringType()),
                    StructField("index", LongType()),
                    StructField("interception", StructType([
                        StructField("outcome", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                    ])),
                    StructField("location", ArrayType(DoubleType())),
                    StructField("minute", LongType()),
                    StructField("off_camera", BooleanType()),
                    StructField("out", BooleanType()),
                    StructField("pass", StructType([
                        StructField("aerial_won", BooleanType()),
                        StructField("angle", DoubleType()),
                        StructField("assisted_shot_id", StringType()),
                        StructField("body_part", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("cross", BooleanType()),
                        StructField("cut_back", BooleanType()),
                        StructField("deflected", BooleanType()),
                        StructField("end_location", ArrayType(DoubleType())),
                        StructField("goal_assist", BooleanType()),
                        StructField("height", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("inswinging", BooleanType()),
                        StructField("length", DoubleType()),
                        StructField("miscommunication", BooleanType()),
                        StructField("no_touch", BooleanType()),
                        StructField("outcome", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("outswinging", BooleanType()),
                        StructField("recipient", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("shot_assist", BooleanType()),
                        StructField("switch", BooleanType()),
                        StructField("technique", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("through_ball", BooleanType()),
                        StructField("type", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                    ])),
                    StructField("period", LongType()),
                    StructField("play_pattern", StructType([
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ])),
                    StructField("player", StructType([
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ])),
                    StructField("position", StructType([
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ])),
                    StructField("possession", LongType()),
                    StructField("possession_team", StructType([
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ])),
                    StructField("related_events", ArrayType(StringType())),
                    StructField("second", LongType()),
                    StructField("shot", StructType([
                        StructField("aerial_won", BooleanType()),
                        StructField("body_part", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("end_location", ArrayType(DoubleType())),
                        StructField("first_time", BooleanType()),
                        StructField("freeze_frame", ArrayType(StructType([
                            StructField("location", ArrayType(DoubleType())),
                            StructField("player", StructType([
                                StructField("id", LongType()),
                                StructField("name", StringType()),
                            ])),
                            StructField("position", StructType([
                                StructField("id", LongType()),
                                StructField("name", StringType()),
                            ])),
                            StructField("teammate", BooleanType()),
                        ]))),
                        StructField("key_pass_id", StringType()),
                        StructField("one_on_one", BooleanType()),
                        StructField("open_goal", BooleanType()),
                        StructField("outcome", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("statsbomb_xg", DoubleType()),
                        StructField("technique", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("type", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                    ])),
                    StructField("substitution", StructType([
                        StructField("outcome", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                        StructField("replacement", StructType([
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ])),
                    ])),
                    StructField("tactics", StructType([
                        StructField("formation", LongType()),
                        StructField("lineup", ArrayType(StructType([
                            StructField("jersey_number", LongType()),
                            StructField("player", StructType([
                                StructField("id", LongType()),
                                StructField("name", StringType()),
                            ])),
                            StructField("position", StructType([
                                StructField("id", LongType()),
                                StructField("name", StringType()),
                            ])),
                        ]))),
                    ])),
                    StructField("team", StructType([
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ])),
                    StructField("timestamp", StringType()),
                    StructField("type", StructType([
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ])),
                    StructField("under_pressure", BooleanType()),
                ])
