import os
import psycopg2
from dotenv import load_dotenv
from logging_config import get_logger

# Load environment variables from .env file
load_dotenv()
logger = get_logger("sms_sync.db")


def get_db_connection():
    """Establish a connection to the PostgreSQL database using DB_URL env var."""
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise ValueError("DB_URL environment variable not set.")
    try:
        return psycopg2.connect(db_url)
    except Exception as e:
        raise RuntimeError(f"Database connection failed: {e}")


def setup_database():
    """Ensure the 'sms_messages' table exists."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Users table for authentication
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sms_messages (
                    id SERIAL PRIMARY KEY,
                    user_name VARCHAR(255) NOT NULL,
                    sms_id BIGINT NOT NULL,
                    address VARCHAR(255),
                    body TEXT,
                    date_received BIGINT,
                    message_type INTEGER,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (sms_id, user_name)
                );
                """
            )
            # Helpful index for user-scoped queries
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sms_messages_user_created
                ON sms_messages (user_name, created_at DESC);
                """
            )
        conn.commit()
    finally:
        conn.close()
