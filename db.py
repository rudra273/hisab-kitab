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
    """Ensure all required tables exist."""
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
           
            # SMS messages table
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
                    is_processed BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (sms_id, user_name)
                );
                """
            )
           
            # Transactions table (UPDATED with date_received)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    user_name VARCHAR(255) NOT NULL,
                    sms_id BIGINT NOT NULL,
                    address VARCHAR(255),
                    bank VARCHAR(100),
                    amount DECIMAL(15,2),
                    transaction_type VARCHAR(20),
                    merchant VARCHAR(255),
                    date_received BIGINT,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE (sms_id, user_name)
                );
                """
            )
           
            # Helpful indexes for user-scoped queries
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sms_messages_user_created
                ON sms_messages (user_name, created_at DESC);
                """
            )
           
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sms_messages_processed
                ON sms_messages (is_processed, created_at ASC);
                """
            )
           
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_transactions_user_created
                ON transactions (user_name, created_at DESC);
                """
            )
           
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_transactions_type_amount
                ON transactions (transaction_type, amount);
                """
            )

            # NEW: Index on date_received for better query performance
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_transactions_date_received
                ON transactions (user_name, date_received DESC);
                """
            )
           
        conn.commit()
        logger.info("Database setup completed successfully - all tables created")
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        raise
    finally:
        conn.close()

