import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from src.transform import process_player_stats, aggregate_hero_performance

@pytest.fixture(scope="module")
def spark():
    """Create Spark session for tests."""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_process_player_stats(spark):
    """Test player stats processing with PySpark."""
    # Define schema
    player_schema = StructType([
        StructField("player_slot", IntegerType(), True),
        StructField("hero_id", IntegerType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("gold_per_min", IntegerType(), True),
        StructField("xp_per_min", IntegerType(), True),
        StructField("last_hits", IntegerType(), True),
        StructField("denies", IntegerType(), True),
        StructField("hero_damage", IntegerType(), True),
        StructField("tower_damage", IntegerType(), True),
        StructField("hero_healing", IntegerType(), True),
        StructField("level", IntegerType(), True)
    ])
    
    match_schema = StructType([
        StructField("match_id", IntegerType(), True),
        StructField("radiant_win", BooleanType(), True),
        StructField("duration", IntegerType(), True),
        StructField("game_mode", IntegerType(), True),
        StructField("players", ArrayType(player_schema), True)
    ])
    
    # Create sample data
    data = [{
        "match_id": 1,
        "radiant_win": True,
        "duration": 1800,
        "game_mode": 22,
        "players": [
            {
                "player_slot": 0,
                "hero_id": 1,
                "kills": 10,
                "deaths": 5,
                "assists": 15,
                "gold_per_min": 450,
                "xp_per_min": 500,
                "last_hits": 150,
                "denies": 10,
                "hero_damage": 15000,
                "tower_damage": 2000,
                "hero_healing": 1000,
                "level": 20
            }
        ]
    }]
    
    matches_df = spark.createDataFrame(data, schema=match_schema)
    result = process_player_stats(matches_df)
    
    assert result.count() == 1
    assert "kda" in result.columns
    assert "player_won" in result.columns
    assert "radiant_win" in result.columns  # NEW: Check radiant_win is kept
    
    row = result.first()
    assert row["player_won"] == 1  # Radiant player won
    assert row["radiant_win"] == True  # NEW

