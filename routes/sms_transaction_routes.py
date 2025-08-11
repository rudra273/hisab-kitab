from fastapi import APIRouter, status, HTTPException, Depends
from db import get_db_connection
from schemas import SmsSyncRequest
from auth import basic_auth
from convert import convert_all_messages
from psycopg2.extras import execute_batch
import datetime
from logging_config import get_logger

sms_transaction_router = APIRouter()
logger = get_logger("sms_sync.api")

@sms_transaction_router.get("/messages", summary="Get All SMS Messages")
def get_all_messages(auth_user: str = Depends(basic_auth)):
    """Fetch all SMS messages from the database and format the timestamp."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT user_name, sms_id, address, body, date_received, message_type, created_at "
                "FROM sms_messages WHERE user_name = %s ORDER BY created_at DESC;",
                (auth_user,),
            )
            rows = cur.fetchall()
            messages = []
            for row in rows:
                timestamp_ms = row[4]
                date_object = datetime.datetime.fromtimestamp(timestamp_ms / 1000)
                formatted_date = date_object.strftime("%Y-%m-%d %H:%M:%S")

                messages.append(
                    {
                        "user_name": row[0],
                        "sms_id": row[1],
                        "address": row[2],
                        "body": row[3],
                        "date_received": formatted_date,
                        "message_type": row[5],
                        "created_at": row[6].isoformat() if row[6] else None,
                    }
                )
        return {"messages": messages, "count": len(messages)}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching messages: {e}",
        )
    finally:
        if conn:
            conn.close()

@sms_transaction_router.get("/transactions", summary="Get All Transactions")
def get_all_transactions(auth_user: str = Depends(basic_auth)):
    """Fetch all transactions from the database for the authenticated user."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_name, sms_id, address, bank, amount, transaction_type, 
                       merchant, created_at
                FROM transactions 
                WHERE user_name = %s 
                ORDER BY created_at DESC;
                """,
                (auth_user,),
            )
            rows = cur.fetchall()
            transactions = []
            for row in rows:
                transactions.append({
                    "user_name": row[0],
                    "sms_id": row[1],
                    "address": row[2],
                    "bank": row[3],
                    "amount": float(row[4]) if row[4] else None,
                    "transaction_type": row[5],
                    "merchant": row[6],
                    "created_at": row[7].isoformat() if row[7] else None,
                })
        return {"transactions": transactions, "count": len(transactions)}
    except Exception as e:
        logger.error(f"Error fetching transactions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching transactions: {e}",
        )
    finally:
        if conn:
            conn.close()

@sms_transaction_router.get("/admin/transactions", summary="Get All Transactions (Admin)")
def get_all_transactions_admin(_: str = Depends(basic_auth)):
    """Admin endpoint to fetch all transactions from all users."""
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_name, sms_id, address, bank, amount, transaction_type, 
                       merchant, created_at
                FROM transactions 
                ORDER BY created_at DESC;
                """
            )
            rows = cur.fetchall()
            transactions = []
            for row in rows:
                transactions.append({
                    "user_name": row[0],
                    "sms_id": row[1],
                    "address": row[2],
                    "bank": row[3],
                    "amount": float(row[4]) if row[4] else None,
                    "transaction_type": row[5],
                    "merchant": row[6],
                    "created_at": row[7].isoformat() if row[7] else None,
                })
        return {"transactions": transactions, "count": len(transactions)}
    except Exception as e:
        logger.error(f"Error fetching all transactions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching transactions: {e}",
        )
    finally:
        if conn:
            conn.close()
