from fastapi import APIRouter, status, HTTPException, Depends
from db import get_db_connection, setup_database
from schemas import SmsSyncRequest
from auth import basic_auth
from convert import convert_all_messages
from psycopg2.extras import execute_batch
from logging_config import get_logger

system_router = APIRouter()
logger = get_logger("sms_sync.api")

@system_router.post("/setup-db", summary="Setup Database", status_code=status.HTTP_200_OK)
def setup_db_api(_: str = Depends(basic_auth)):
    try:
        setup_database()
        return {"message": "Database setup completed successfully."}
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database setup failed: {e}",
        )

@system_router.post("/sync", status_code=status.HTTP_202_ACCEPTED, summary="Sync SMS Messages")
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
            execute_batch(cur, insert_sql, data_to_insert)
            inserted_count = cur.rowcount
            conn.commit()
        return {
            "message": "Sync completed successfully.",
            "received_count": len(payload.messages),
            "inserted_count": inserted_count,
        }
    except Exception as e:
        if conn:
            conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while syncing data: {e}",
        )
    finally:
        if conn:
            conn.close()

@system_router.post("/convert", summary="Convert SMS Messages to Transactions", status_code=status.HTTP_200_OK)
def convert_sms_to_transactions(_: str = Depends(basic_auth)):
    """
    Admin API endpoint to convert all unprocessed SMS messages to transaction data.
    This processes all users' data, not just the authenticated user's data.
    """
    logger.info("Convert API called - starting SMS to transaction conversion")
    try:
        result = convert_all_messages()
        if result["status"] == "success":
            return {
                "status": "success",
                "message": result["message"],
                "details": {
                    "processed_count": result["processed_count"],
                    "failed_count": result["failed_count"],
                    "total_messages": result.get("total_messages", 0)
                }
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result["message"]
            )
    except Exception as e:
        logger.error(f"Convert API error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Conversion process failed: {str(e)}"
        )


