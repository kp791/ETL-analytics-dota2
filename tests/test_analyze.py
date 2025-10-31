import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, BooleanType, StringType
from src.analyze import calculate_winrate_correlations, generate_key_findings, generate_winrate_insights

@pytest.fixture(scope="module")
def spark():
    """Create Spark session."""
    spark = SparkSession.builder \
        .appName("test") \
        .master("local[1]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_calculate_winrate_correlations(spark):
    """Test correlation calculation."""
    schema = StructType([
        StructField("player_won", IntegerType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("kda", FloatType(), True),
        StructField("gpm", IntegerType(), True),
        StructField("xpm", IntegerType(), True),
        StructField("hero_damage", IntegerType(), True),
        StructField("tower_damage", IntegerType(), True),
        StructField("hero_healing", IntegerType(), True),
        StructField("final_level", IntegerType(), True),
        StructField("last_hits", IntegerType(), True),
        StructField("denies", IntegerType(), True)
    ])
    
    data = [
        (1, 10, 2, 15, 12.5, 500, 600, 15000, 3000, 1000, 22, 200, 15),
        (1, 12, 3, 18, 10.0, 520, 620, 16000, 3200, 1200, 23, 210, 18),
        (0, 5, 10, 8, 1.3, 350, 400, 8000, 1000, 500, 16, 100, 5),
        (0, 4, 12, 6, 0.83, 320, 380, 7000, 800, 400, 15, 90, 4)
    ]
    
    df = spark.createDataFrame(data, schema=schema)
    correlations = calculate_winrate_correlations(df)
    
    assert isinstance(correlations, dict)
    assert "kills" in correlations
    assert "deaths" in correlations
    assert correlations["deaths"] < 0  # Deaths negatively correlated

def test_generate_key_findings(spark):
    """Test key findings generation."""
    schema = StructType([
        StructField("gpm", IntegerType(), True)
    ])
    
    winner_data = [(500,), (520,)]
    loser_data = [(350,), (320,)]
    
    winners = spark.createDataFrame(winner_data, schema=schema)
    losers = spark.createDataFrame(loser_data, schema=schema)
    
    correlations = {"gpm": 0.85, "deaths": -0.75}
    findings = generate_key_findings(correlations, winners, losers)
    
    assert isinstance(findings, list)
    assert len(findings) > 0

def test_generate_winrate_insights(spark):
    """Test full insights generation."""
    player_schema = StructType([
        StructField("match_id", IntegerType(), True),
        StructField("player_won", IntegerType(), True),
        StructField("is_radiant", BooleanType(), True),
        StructField("kills", IntegerType(), True),
        StructField("deaths", IntegerType(), True),
        StructField("assists", IntegerType(), True),
        StructField("kda", FloatType(), True),
        StructField("gpm", IntegerType(), True),
        StructField("xpm", IntegerType(), True),
        StructField("hero_damage", IntegerType(), True),
        StructField("tower_damage", IntegerType(), True),
        StructField("hero_healing", IntegerType(), True),
        StructField("final_level", IntegerType(), True),
        StructField("last_hits", IntegerType(), True),
        StructField("denies", IntegerType(), True)
    ])
    
    hero_schema = StructType([
        StructField("hero_id", IntegerType(), True),
        StructField("localized_name", StringType(), True),
        StructField("games_played", IntegerType(), True),
        StructField("wins", IntegerType(), True),
        StructField("win_rate", FloatType(), True)
    ])
    
    player_data = [
        (1, 1, True, 10, 2, 15, 12.5, 500, 600, 15000, 3000, 1000, 22, 200, 15),
        (1, 1, True, 12, 3, 18, 10.0, 520, 620, 16000, 3200, 1200, 23, 210, 18),
        (1, 0, False, 5, 10, 8, 1.3, 350, 400, 8000, 1000, 500, 16, 100, 5),
        (2, 0, False, 4, 12, 6, 0.83, 320, 380, 7000, 800, 400, 15, 90, 4)
    ]
    
    hero_data = [
        (1, "Anti-Mage", 10, 7, 70.0),
        (2, "Axe", 8, 3, 37.5)
    ]
    
    player_df = spark.createDataFrame(player_data, schema=player_schema)
    hero_df = spark.createDataFrame(hero_data, schema=hero_schema)
    
    insights = generate_winrate_insights(player_df, hero_df)
    
    assert "total_matches_analyzed" in insights
    assert "correlation_with_winrate" in insights
    assert "top_5_winrate_factors" in insights
    assert "overall_radiant_winrate" in insights
    assert insights["total_matches_analyzed"] == 2

