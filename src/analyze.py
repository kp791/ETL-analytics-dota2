from __future__ import annotations

import json
import os
import logging
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

def get_spark_session() -> SparkSession:
    """Get or create Spark session."""
    spark = SparkSession.builder \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    return spark

def load_from_postgres(execution_date: str):
    """Load data from PostgreSQL."""
    try:
        spark = get_spark_session()
        
        jdbc_url = "jdbc:postgresql://localhost:5432/dota_pipeline"
        properties = {
            "user": "dota",
            "password": "password123",
            "driver": "org.postgresql.Driver"
        }
        
        logger.info(f"Loading player stats from PostgreSQL for {execution_date}...")
        player_stats = spark.read.jdbc(
            url=jdbc_url,
            table="player_stats",
            properties=properties
        )
        
        logger.info(f"Loading hero performance from PostgreSQL for {execution_date}...")
        hero_performance = spark.read.jdbc(
            url=jdbc_url,
            table="hero_performance",
            properties=properties
        )
        
        logger.info(f"Loaded {player_stats.count()} player records from PostgreSQL")
        logger.info(f"Loaded {hero_performance.count()} hero records from PostgreSQL")
        
        return player_stats, hero_performance
    except Exception as e:
        logger.error(f"Error loading from PostgreSQL: {e}")
        raise

def load_from_csv(execution_date: str, input_dir: str):
    """Load data from local CSV files (fallback)."""
    try:
        spark = get_spark_session()
        
        player_stats = spark.read.option("header", True).option("inferSchema", True) \
            .csv(f"{input_dir}/player_stats_{execution_date}.csv")
        
        hero_performance = spark.read.option("header", True).option("inferSchema", True) \
            .csv(f"{input_dir}/hero_performance_{execution_date}.csv")
        
        logger.info(f"Loaded data from CSV for {execution_date}")
        return player_stats, hero_performance
    except Exception as e:
        logger.error(f"Error loading from CSV: {e}")
        raise

def calculate_winrate_correlations(player_stats_df: DataFrame) -> Dict:
    """Calculate correlations between metrics and win rate."""
    try:
        feature_cols = [
            "kills", "deaths", "assists", "kda",
            "gpm", "xpm", "hero_damage", "tower_damage",
            "hero_healing", "final_level", "last_hits", "denies"
        ]
        
        correlations = {}
        for col in feature_cols:
            if col in player_stats_df.columns:
                corr = player_stats_df.stat.corr("player_won", col)
                correlations[col] = round(corr, 4) if corr else 0.0
        
        logger.info(f"Calculated correlations")
        return correlations
    except Exception as e:
        logger.error(f"Error calculating correlations: {e}")
        raise

def generate_winrate_insights(player_stats: DataFrame, hero_performance: DataFrame) -> Dict:
    """Generate comprehensive winrate-related insights."""
    try:
        correlations = calculate_winrate_correlations(player_stats)
        
        sorted_factors = sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True)
        top_factors = dict(sorted_factors[:5])
        
        top_heroes = hero_performance.orderBy(F.desc("win_rate")).limit(5)
        bottom_heroes = hero_performance.orderBy("win_rate").limit(5)
        
        winners = player_stats.filter(F.col("player_won") == 1)
        losers = player_stats.filter(F.col("player_won") == 0)
        
        def safe_avg(df, col):
            result = df.agg(F.avg(col)).first()
            return round(result[0], 2) if result and result[0] else 0.0
        
        # Calculate radiant winrate from is_radiant and player_won
        radiant_players = player_stats.filter(F.col("is_radiant") == True)
        radiant_count = radiant_players.count()
        radiant_winrate = radiant_players.agg(F.avg("player_won")).first()[0] if radiant_count > 0 else 0.5
        
        insights = {
            "total_matches_analyzed": player_stats.select("match_id").distinct().count(),
            "total_player_records": player_stats.count(),
            "overall_radiant_winrate": round(radiant_winrate * 100, 2),
            "correlation_with_winrate": correlations,
            "top_5_winrate_factors": top_factors,
            "winning_team_avg_stats": {
                "avg_kills": safe_avg(winners, "kills"),
                "avg_deaths": safe_avg(winners, "deaths"),
                "avg_kda": safe_avg(winners, "kda"),
                "avg_gpm": safe_avg(winners, "gpm"),
                "avg_xpm": safe_avg(winners, "xpm"),
            },
            "losing_team_avg_stats": {
                "avg_kills": safe_avg(losers, "kills"),
                "avg_deaths": safe_avg(losers, "deaths"),
                "avg_kda": safe_avg(losers, "kda"),
                "avg_gpm": safe_avg(losers, "gpm"),
                "avg_xpm": safe_avg(losers, "xpm"),
            },
            "top_5_heroes_by_winrate": [
                {
                    "name": row["localized_name"],
                    "winrate": round(row["win_rate"], 2) if row["win_rate"] else 0.0,
                    "games": row["games_played"]
                } for row in top_heroes.collect()
            ],
            "bottom_5_heroes_by_winrate": [
                {
                    "name": row["localized_name"],
                    "winrate": round(row["win_rate"], 2) if row["win_rate"] else 0.0,
                    "games": row["games_played"]
                } for row in bottom_heroes.collect()
            ],
            "key_findings": generate_key_findings(correlations, winners, losers)
        }
        
        logger.info(f"Generated comprehensive winrate insights")
        return insights
    except Exception as e:
        logger.error(f"Error generating insights: {e}")
        raise

def generate_key_findings(correlations: Dict, winners: DataFrame, losers: DataFrame) -> list:
    """Generate human-readable key findings."""
    findings = []
    
    if correlations:
        top_corr = max(correlations.items(), key=lambda x: abs(x[1]))
        findings.append(f"Strongest winrate predictor: {top_corr[0]} (correlation: {top_corr[1]})")
    
    if "deaths" in correlations:
        findings.append(f"Deaths have {correlations['deaths']:.2f} correlation with losing")
    
    winner_gpm_result = winners.agg(F.avg("gpm")).first()
    loser_gpm_result = losers.agg(F.avg("gpm")).first()
    
    if winner_gpm_result and loser_gpm_result and winner_gpm_result[0] and loser_gpm_result[0]:
        gpm_diff = winner_gpm_result[0] - loser_gpm_result[0]
        findings.append(f"Winners average {gpm_diff:.0f} more GPM than losers")
    
    return findings

def save_insights(insights: Dict, directory: str, execution_date: str) -> str:
    """Save insights to JSON file."""
    try:
        os.makedirs(directory, exist_ok=True)
        filepath = os.path.join(directory, f'winrate_insights_{execution_date}.json')
        
        with open(filepath, 'w') as f:
            json.dump(insights, f, indent=2)
        
        logger.info(f"Saved insights to {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Error saving insights: {e}")
        raise

