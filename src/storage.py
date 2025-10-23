from minio import Minio
from minio.error import S3Error
import psycopg2
from psycopg2 import sql
import logging

logger = logging.getLogger(__name__)

# MinIO Configuration
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
MINIO_BUCKET = "dota-pipeline"

# PostgreSQL Configuration
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "dota_pipeline"
POSTGRES_USER = "dota"
POSTGRES_PASSWORD = "password123"

def get_minio_client():
    """Get MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ensure_bucket(bucket_name: str = MINIO_BUCKET):
    """Ensure MinIO bucket exists."""
    try:
        client = get_minio_client()
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Created bucket: {bucket_name}")
        else:
            logger.info(f"Bucket exists: {bucket_name}")
    except S3Error as e:
        logger.error(f"Error with bucket: {e}")
        raise

def get_postgres_connection():
    """Get PostgreSQL connection."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def create_tables():
    """Create PostgreSQL tables."""
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    try:
        # Player stats fact table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS player_stats (
                id SERIAL PRIMARY KEY,
                match_id BIGINT,
                execution_date DATE,
                radiant_win BOOLEAN,
                player_won INTEGER,
                hero_id INTEGER,
                kills INTEGER,
                deaths INTEGER,
                assists INTEGER,
                kda FLOAT,
                gpm INTEGER,
                xpm INTEGER,
                hero_damage INTEGER,
                tower_damage INTEGER,
                hero_healing INTEGER,
                final_level INTEGER,
                last_hits INTEGER,
                denies INTEGER,
                is_radiant BOOLEAN,
                match_duration INTEGER,
                game_mode INTEGER,
                player_slot INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Hero performance dimension table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hero_performance (
                id SERIAL PRIMARY KEY,
                execution_date DATE,
                hero_id INTEGER,
                localized_name VARCHAR(255),
                primary_attr VARCHAR(50),
                attack_type VARCHAR(50),
                games_played INTEGER,
                wins INTEGER,
                win_rate FLOAT,
                avg_kills FLOAT,
                avg_deaths FLOAT,
                avg_assists FLOAT,
                avg_kda FLOAT,
                avg_gpm FLOAT,
                avg_xpm FLOAT,
                avg_hero_damage FLOAT,
                avg_tower_damage FLOAT,
                avg_healing FLOAT,
                avg_final_level FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        logger.info("PostgreSQL tables created/verified")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        conn.rollback

