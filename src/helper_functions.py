"""
Helper functions for data paths, schema, and retrieval of coordinates of stadiums.
"""

import json
import logging
from pathlib import Path

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderServiceError
from geopy.extra.rate_limiter import RateLimiter

from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        DoubleType, TimestampType, BooleanType, LongType, ArrayType
    )

logger = logging.getLogger(__name__)

CACHE_PATH = Path(__file__).parent.parent / "data" / "stadium_coordinates.json" 

geo_locator = Nominatim(user_agent="dat535-football-stats")

geocode = RateLimiter(
    geo_locator.geocode,
    min_delay_seconds=15,
    error_wait_seconds=15,
    max_retries=0,
    swallow_exceptions=False
)

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


def get_paths(path: str | Path = Path(__file__).parent.parent.parent / "open-data"):
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


def get_coordinates(stadium, country = None):
    """
    A function that gives you coordinates from a dictionary or through
    geocoding if it isnt a part of the dictionary.

    Args:
        stadium: Name of the stadium.
        country: Optional to add country the stadium is in.

    Returns:
        Tuple with latitude and longitude, or None if not found.
    """
    if stadium:
        stadium = stadium.strip()
    else:
        raise KeyError("Empty string")

    # TODO: Remove when transferred to cache-file.
    if stadium in stadium_coordinates:
        return stadium_coordinates[stadium]
    else:
        raise KeyError("No stadium found in cache")
    
    # load cached coordinates
    if CACHE_PATH.exists():
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            cache = json.load(f)
    else:
        cache =  {}

    # make search query
    if country:
        query = f"{stadium}, {country}"
    else:
        query = stadium

    if query in cache:
        return tuple(cache[query])
    else:
        coords = geolocate(query)
        cache[query] = coords
        with open(CACHE_PATH, "w", encoding="utf-8") as f:
            json.dumps(cache, f, indent=4, ensure_ascii=False)


def geolocate(query):
    """En eller annen docstring"""
    try:
        location = geocode(query, timeout=10)
    except GeocoderServiceError as e:
        logger.warning(f"Geocoding failed for {query}: {e}")
        return None

    if location:
        if location.raw.get("type") != "stadium":
            logger.warning(f"Invalid stadium location for {query}")
            return None
        
        coordinates = (location.latitude, location.longitude)
        return coordinates

    logger.warning(f"No coordinates found for {query}")
    return None


def get_schema(category: str):
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