
import os
from typing import List
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import datetime 


# Load environment variables from .env file
load_dotenv()

# --- Pydantic Models for Data Validation ---

class SmsData(BaseModel):
    """Defines the structure of a single SMS message from the client."""
    id: int = Field(..., alias='id') # The original SMS ID from the phone
    address: str
    body: str
    date: int
    type: int

class SmsSyncRequest(BaseModel):
    """Defines the structure of the incoming sync request payload."""
    user_name: str
    messages: List[SmsData]

# --- FastAPI Application Initialization ---

app = FastAPI(
    title="SMS Sync Service",
    description="API to receive and store SMS data from an Android app.",
    version="1.0.0"
)

# --- Database Connection and Setup ---

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        # Fetch the database URL from environment variables
        db_url = os.getenv("DB_URL")
        if not db_url:
            raise ValueError("DB_URL environment variable not set.")
            
        conn = psycopg2.connect(db_url)
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        raise

def setup_database():
    """Ensures the necessary table exists in the database."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # SQL to create the table. Using ON CONFLICT on sms_id ensures no duplicates.
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS sms_messages (
                id SERIAL PRIMARY KEY,
                user_name VARCHAR(255) NOT NULL,
                sms_id BIGINT NOT NULL,
                address VARCHAR(255),
                body TEXT,
                date_received BIGINT,
                message_type INTEGER,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (sms_id, user_name)  -- A message is unique to a user
            );
            """
            cur.execute(create_table_sql)
        conn.commit()
        print("Database table checked/created successfully.")
    finally:
        conn.close()


# Run database setup on application startup
@app.on_event("startup")
def on_startup():
    setup_database()

# --- API Endpoint to setup database ---
@app.post("/setup-db", summary="Setup Database", status_code=status.HTTP_200_OK)
def setup_db_api():
    """Endpoint to (re)create the database table if needed."""
    try:
        setup_database()
        return {"message": "Database setup completed successfully."}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database setup failed: {e}"
        )


# --- API Endpoints ---

@app.get("/", summary="Root Endpoint")
def read_root():
    """A simple endpoint to confirm the API is running."""
    return {"status": "ok", "message": "Welcome to the SMS Sync API"}




@app.post("/sync", status_code=status.HTTP_202_ACCEPTED, summary="Sync SMS Messages")
def sync_sms_messages(payload: SmsSyncRequest):
    """
    Receives a list of SMS messages and inserts new ones into the database.
    It uses 'ON CONFLICT DO NOTHING' to efficiently ignore duplicates.
    """
    insert_sql = """
        INSERT INTO sms_messages (user_name, sms_id, address, body, date_received, message_type)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (sms_id, user_name) DO NOTHING;
    """
    
    # Prepare data for batch execution
    data_to_insert = [
        (payload.user_name, msg.id, msg.address, msg.body, msg.date, msg.type)
        for msg in payload.messages
    ]
    
    if not data_to_insert:
        return {"message": "No new messages to sync.", "inserted_count": 0}

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Use execute_batch for efficient bulk insertion
            execute_batch(cur, insert_sql, data_to_insert)
            inserted_count = cur.rowcount
            conn.commit()
            
        return {
            "message": "Sync completed successfully.",
            "received_count": len(payload.messages),
            "inserted_count": inserted_count
        }
    except Exception as e:
        print(f"Error during database operation: {e}")
        # Rollback in case of error
        if conn:
            conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while syncing data: {e}"
        )
    finally:
        if conn:
            conn.close()


@app.get("/messages", summary="Get All SMS Messages")
def get_all_messages():
    """Fetches all SMS messages from the database and formats the timestamp."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT user_name, sms_id, address, body, date_received, message_type, created_at FROM sms_messages ORDER BY created_at DESC;")
            rows = cur.fetchall()
            messages = []
            for row in rows:
                # --- START OF FIX ---
                timestamp_ms = row[4] # This is the big number, e.g., 1754742120556
                
                # Convert milliseconds to a datetime object
                # Python's fromtimestamp uses seconds, so we divide by 1000
                date_object = datetime.datetime.fromtimestamp(timestamp_ms / 1000)
                
                # Format the datetime object into a human-readable string
                formatted_date = date_object.strftime('%Y-%m-%d %H:%M:%S')
                # --- END OF FIX ---

                messages.append({
                    "user_name": row[0],
                    "sms_id": row[1],
                    "address": row[2],
                    "body": row[3],
                    "date_received": formatted_date,
                    "message_type": row[5],
                    "created_at": row[6].isoformat() if row[6] else None
                })
        return {"messages": messages, "count": len(messages)}
    except Exception as e:
        print(f"Error fetching messages: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching messages: {e}"
        )
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)

