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

from pathlib import Path
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
        self.bronze_dir = f"{shared_dir}/bronze"
        self.silver_dir = f"{shared_dir}/silver"
        self.gold_dir = f"{shared_dir}/gold"


    def create_spark_session(self):
        """Create and configure Spark session."""
        logger.info("Creating Spark session...")

        self.spark = initialize_spark()
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


    def ingest_data(self, category):
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
        df = df.withColumn("ingest_date", to_date(current_timestamp()))
        df = df.withColumn("ingest_time", current_timestamp())

        logger.info(f"Dataframe successfully created.")
        return df


    def save_compressed_data(self, df, category):
        logger.info(f"Creating Parquet files for {category}.")

        match category:
            case "competitions":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/competitions", partitionBy="ingest_date")
            case "matches":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/matches", partitionBy="ingest_date")
            case "events":
                df.write.mode("overwrite").parquet(f"{self.bronze_dir}/events", partitionBy="ingest_date")

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


    def get_schema(self, category):
        """
        Get the specific schema for the different ingestions.

        Args:
            category (str):

        Returns:
            Sparkschema.
        """
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
