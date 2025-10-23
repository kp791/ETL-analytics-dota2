from __future__ import annotations

import logging
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.storage import ensure_bucket, create_tables, MINIO_BUCKET

logger = logging.getLogger(__name__)

def load_to_minio(player_stats_df: DataFrame, hero_performance_df: DataFrame, execution_date: str):
    """Load data to MinIO (HDFS-like storage)."""
    try:
        ensure_bucket()
        
        # Load player stats as Parquet (partitioned by date)
        s3_player_path = f"s3a://{MINIO_BUCKET}/warehouse/player_stats_fact/date={execution_date}"
        player_stats_df.write.mode("overwrite").parquet(s3_player_path)
        logger.info(f"✓ Loaded player stats to MinIO: {s3_player_path}")
        
        # Load hero performance as Parquet
        s3_hero_path = f"s3a://{MINIO_BUCKET}/warehouse/hero_dim/date={execution_date}"
        hero_performance_df.write.mode("overwrite").parquet(s3_hero_path)
        logger.info(f"✓ Loaded hero performance to MinIO: {s3_hero_path}")
        
        return s3_player_path, s3_hero_path
    except Exception as e:
        logger.error(f"Error loading to MinIO: {e}")
        raise

def load_to_postgres(player_stats_df: DataFrame, hero_performance_df: DataFrame, execution_date: str):
    """Load data to PostgreSQL."""
    try:
        create_tables()
        
        # Add execution_date column with proper DATE type
        player_stats_with_date = player_stats_df.withColumn(
            "execution_date", 
            F.to_date(F.lit(execution_date), 'yyyy-MM-dd')
        )
        
        hero_performance_with_date = hero_performance_df.withColumn(
            "execution_date", 
            F.to_date(F.lit(execution_date), 'yyyy-MM-dd')
        )
        
        # JDBC connection properties
        jdbc_url = "jdbc:postgresql://localhost:5432/dota_pipeline"
        properties = {
            "user": "dota",
            "password": "password123",
            "driver": "org.postgresql.Driver"
        }
        
        # Write to PostgreSQL
        logger.info("Writing player stats to PostgreSQL...")
        player_stats_with_date.write.jdbc(
            url=jdbc_url,
            table="player_stats",
            mode="append",
            properties=properties
        )
        logger.info(f"✓ Loaded {player_stats_with_date.count()} player stats to PostgreSQL")
        
        logger.info("Writing hero performance to PostgreSQL...")
        hero_performance_with_date.write.jdbc(
            url=jdbc_url,
            table="hero_performance",
            mode="append",
            properties=properties
        )
        logger.info(f"✓ Loaded {hero_performance_with_date.count()} hero records to PostgreSQL")
        
    except Exception as e:
        logger.error(f"Error loading to PostgreSQL: {e}")
        raise

