from __future__ import annotations

import json
import os
import logging
import glob
import shutil
from typing import Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def get_spark_session() -> SparkSession:
    """Create Spark session with S3/MinIO and PostgreSQL support."""
    spark = SparkSession.builder \
        .appName("DotaETL") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.367,"
                "org.postgresql:postgresql:42.6.0") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created with S3/MinIO and PostgreSQL support")
    return spark

def load_raw_data(directory: str, execution_date: str) -> Tuple:
    """Load raw JSON data into Spark DataFrames."""
    try:
        spark = get_spark_session()
        
        matches_file = os.path.join(directory, f'matches_{execution_date}.json')
        hero_stats_file = os.path.join(directory, f'hero_stats_{execution_date}.json')
        
        logger.info(f"Loading {matches_file}")
        matches_df = spark.read.option("multiLine", "true").option("mode", "PERMISSIVE").json(matches_file)
        
        if "_corrupt_record" in matches_df.columns:
            corrupt_count = matches_df.filter(F.col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                logger.warning(f"Found {corrupt_count} corrupt records")
                matches_df = matches_df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
        
        logger.info(f"Loading {hero_stats_file}")
        hero_stats_df = spark.read.option("multiLine", "true").json(hero_stats_file)
        
        matches_count = matches_df.count()
        hero_count = hero_stats_df.count()
        
        logger.info(f"Loaded {matches_count} matches and {hero_count} hero stats")
        
        if matches_count == 0:
            raise ValueError("No matches loaded")
        
        return matches_df, hero_stats_df, spark
    except Exception as e:
        logger.error(f"Error loading raw data: {e}")
        raise

def process_player_stats(matches_df: DataFrame) -> DataFrame:
    """Extract and process player-level statistics."""
    try:
        required_cols = ["match_id", "radiant_win", "duration", "players"]
        missing_cols = [col for col in required_cols if col not in matches_df.columns]
        
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        players_df = matches_df.select(
            F.col("match_id"),
            F.col("radiant_win"),
            F.col("duration"),
            F.col("game_mode"),
            F.explode("players").alias("player")
        )
        
        player_stats = players_df.select(
            "match_id",
            "radiant_win",
            F.col("duration").alias("match_duration"),
            F.col("game_mode"),
            F.coalesce(F.col("player.player_slot"), F.lit(0)).alias("player_slot"),
            F.coalesce(F.col("player.hero_id"), F.lit(0)).alias("hero_id"),
            F.coalesce(F.col("player.kills"), F.lit(0)).alias("kills"),
            F.coalesce(F.col("player.deaths"), F.lit(1)).alias("deaths"),
            F.coalesce(F.col("player.assists"), F.lit(0)).alias("assists"),
            F.coalesce(F.col("player.gold_per_min"), F.lit(0)).alias("gpm"),
            F.coalesce(F.col("player.xp_per_min"), F.lit(0)).alias("xpm"),
            F.coalesce(F.col("player.last_hits"), F.lit(0)).alias("last_hits"),
            F.coalesce(F.col("player.denies"), F.lit(0)).alias("denies"),
            F.coalesce(F.col("player.hero_damage"), F.lit(0)).alias("hero_damage"),
            F.coalesce(F.col("player.tower_damage"), F.lit(0)).alias("tower_damage"),
            F.coalesce(F.col("player.hero_healing"), F.lit(0)).alias("hero_healing"),
            F.coalesce(F.col("player.level"), F.lit(1)).alias("final_level")
        ).withColumn(
            "is_radiant",
            F.when(F.col("player_slot") < 128, True).otherwise(False)
        ).withColumn(
            "player_won",
            F.when(
                (F.col("radiant_win") & F.col("is_radiant")) | 
                (~F.col("radiant_win") & ~F.col("is_radiant")),
                1
            ).otherwise(0)
        ).withColumn(
            "kda",
            F.when(
                F.col("deaths") > 0,
                (F.col("kills") + F.col("assists")) / F.col("deaths")
            ).otherwise(F.col("kills") + F.col("assists"))
        )
        
        logger.info(f"Processed {player_stats.count()} player records")
        return player_stats
    except Exception as e:
        logger.error(f"Error processing player stats: {e}")
        raise

def aggregate_hero_performance(player_stats: DataFrame, hero_stats_df: DataFrame) -> DataFrame:
    """Aggregate performance metrics by hero."""
    try:
        hero_performance = player_stats.groupBy("hero_id").agg(
            F.count("*").alias("games_played"),
            F.sum("player_won").alias("wins"),
            F.avg("kills").alias("avg_kills"),
            F.avg("deaths").alias("avg_deaths"),
            F.avg("assists").alias("avg_assists"),
            F.avg("gpm").alias("avg_gpm"),
            F.avg("xpm").alias("avg_xpm"),
            F.avg("kda").alias("avg_kda"),
            F.avg("hero_damage").alias("avg_hero_damage"),
            F.avg("tower_damage").alias("avg_tower_damage"),
            F.avg("hero_healing").alias("avg_healing"),
            F.avg("final_level").alias("avg_final_level")
        ).withColumn(
            "win_rate",
            (F.col("wins") / F.col("games_played") * 100)
        )
        
        hero_enriched = hero_performance.join(
            hero_stats_df.select("id", "localized_name", "primary_attr", "attack_type", "roles"),
            hero_performance.hero_id == hero_stats_df.id,
            "left"
        ).select(
            "hero_id",
            "localized_name",
            "primary_attr",
            "attack_type",
            "games_played",
            "wins",
            "win_rate",
            "avg_kills",
            "avg_deaths",
            "avg_assists",
            "avg_kda",
            "avg_gpm",
            "avg_xpm",
            "avg_hero_damage",
            "avg_tower_damage",
            "avg_healing",
            "avg_final_level"
        )
        
        logger.info(f"Aggregated performance for {hero_enriched.count()} heroes")
        return hero_enriched
    except Exception as e:
        logger.error(f"Error aggregating hero performance: {e}")
        raise

def save_spark_df(df: DataFrame, output_path: str):
    """Save Spark DataFrame to CSV."""
    try:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        temp_path = output_path + "_temp"
        
        if os.path.exists(temp_path):
            shutil.rmtree(temp_path)
        
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_path)
        
        csv_files = glob.glob(f"{temp_path}/part-*.csv")
        
        if csv_files:
            if os.path.exists(output_path):
                os.remove(output_path)
            shutil.move(csv_files[0], output_path)
            logger.info(f"Saved DataFrame to {output_path}")
        else:
            raise FileNotFoundError(f"No CSV file found in {temp_path}")
        
        if os.path.exists(temp_path):
            shutil.rmtree(temp_path)
            
    except Exception as e:
        logger.error(f"Error saving DataFrame: {e}")
        raise

