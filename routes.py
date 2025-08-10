from fastapi import APIRouter, HTTPException, Request, status, Depends
from typing import Optional
from psycopg2.extras import execute_batch
import datetime
from logging_config import get_logger
from fastapi.templating import Jinja2Templates

from db import get_db_connection, setup_database
from schemas import SmsSyncRequest, UserCreate
from auth import basic_auth
from convert import convert_all_messages  

router = APIRouter()
logger = get_logger("sms_sync.api")
templates = Jinja2Templates(directory="templates")


@router.post("/setup-db", summary="Setup Database", status_code=status.HTTP_200_OK)
def setup_db_api(_: str = Depends(basic_auth)):
    """Endpoint to (re)create the database table if needed."""
    try:
        setup_database()
        return {"message": "Database setup completed successfully."}
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database setup failed: {e}",
        )


@router.get("/", summary="Root Endpoint")
def read_root(request: Request):
    """Render HTML home page using Jinja2 template."""
    logger.debug("Root endpoint called - rendering template")
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/dashboard", summary="User Dashboard")
def user_dashboard(request: Request, auth_user: str = Depends(basic_auth)):
    """Render a simple dashboard with per-user insights."""
    logger.debug("Dashboard requested - computing aggregates")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            # Totals
            cur.execute("SELECT COUNT(*) FROM sms_messages WHERE user_name = %s;", (auth_user,))
            total_messages = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT address) FROM sms_messages WHERE user_name = %s;", (auth_user,))
            unique_senders = cur.fetchone()[0]

            # Daily counts last 60 days
            cur.execute(
                """
                SELECT to_char(to_timestamp(date_received/1000)::date, 'YYYY-MM-DD') AS day,
                       COUNT(*)
                FROM sms_messages
                WHERE user_name = %s
                  AND to_timestamp(date_received/1000) >= NOW() - INTERVAL '60 days'
                GROUP BY 1
                ORDER BY 1;
                """,
                (auth_user,),
            )
            daily_rows = cur.fetchall()
            daily = [{"day": r[0], "count": r[1]} for r in daily_rows]

            # Top senders
            cur.execute(
                """
                SELECT address, COUNT(*) AS cnt
                FROM sms_messages
                WHERE user_name = %s
                GROUP BY address
                ORDER BY cnt DESC
                LIMIT 5;
                """,
                (auth_user,),
            )
            ts_rows = cur.fetchall()
            top_senders = [{"address": r[0] or "(unknown)", "count": r[1]} for r in ts_rows]

            # By message type
            cur.execute(
                """
                SELECT message_type, COUNT(*)
                FROM sms_messages
                WHERE user_name = %s
                GROUP BY message_type
                ORDER BY COUNT(*) DESC;
                """,
                (auth_user,),
            )
            type_rows = cur.fetchall()
            types = [{"message_type": r[0] if r[0] is not None else -1, "count": r[1]} for r in type_rows]

        context = {
            "request": request,
            "total_messages": total_messages,
            "unique_senders": unique_senders,
            "daily": daily,
            "top_senders": top_senders,
            "types": types,
            "username": auth_user,
        }
        return templates.TemplateResponse("dashboard.html", context)
    except Exception as e:
        logger.exception("Error building dashboard")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Error building dashboard: {e}")
    finally:
        if conn:
            conn.close()

@router.get("/db", summary="Database Browser")
def view_db(
    request: Request,
    q: Optional[str] = None,
    address: Optional[str] = None,
    message_type: Optional[int] = None,
    start: Optional[str] = None,  # YYYY-MM-DD
    end: Optional[str] = None,    # YYYY-MM-DD
    page: int = 1,
    page_size: int = 25,
    auth_user: str = Depends(basic_auth),
):
    """Render a searchable/filterable table of SMS messages (server-rendered)."""
    logger.debug("DB browser requested - querying data for server render")

    if page < 1:
        page = 1
    if page_size < 1:
        page_size = 25

    where = ["user_name = %s"]
    params = [auth_user]
    if q:
        where.append("(address ILIKE %s OR body ILIKE %s)")
        like = f"%{q}%"
        params.extend([like, like])
    if address:
        where.append("address ILIKE %s")
        params.append(f"%{address}%")
    if message_type is not None:
        where.append("message_type = %s")
        params.append(int(message_type))
    if start:
        where.append("created_at::date >= %s")
        params.append(start)
    if end:
        where.append("created_at::date <= %s")
        params.append(end)

    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    count_sql = f"SELECT COUNT(*) FROM sms_messages{where_sql};"
    data_sql = (
        "SELECT user_name, sms_id, address, body, date_received, message_type, created_at "
        f"FROM sms_messages{where_sql} ORDER BY created_at DESC LIMIT %s OFFSET %s;"
    )

    offset = (page - 1) * page_size

    conn = None
    total = 0
    items = []
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(count_sql, params)
            total = cur.fetchone()[0]
            cur.execute(data_sql, params + [page_size, offset])
            rows = cur.fetchall()
        for row in rows:
            created_iso = row[6].isoformat() if row[6] else None
            items.append(
                {
                    "user_name": row[0],
                    "sms_id": row[1],
                    "address": row[2],
                    "body": row[3],
                    "date_received": row[4],
                    "message_type": row[5],
                    "created_at": created_iso,
                }
            )
    except Exception as e:
        logger.exception("Error rendering DB browser")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred while fetching DB data: {e}",
        )
    finally:
        if conn:
            conn.close()

    pages = (total + page_size - 1) // page_size if page_size else 1
    context = {
        "request": request,
        "items": items,
        "page": page,
        "page_size": page_size,
        "total": total,
        "pages": pages,
        "q": q or "",
        "address": address or "",
        "message_type": message_type,
        "start": start or "",
        "end": end or "",
    }
    return templates.TemplateResponse("db.html", context)


@router.post("/register", status_code=status.HTTP_201_CREATED, summary="Register a new user")
def register_user(payload: UserCreate):
    """Create a new user with a bcrypt-hashed password."""
    from auth import hash_password  # local import to avoid circular

    username = payload.username.strip()
    password = payload.password
    if not username or not password:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Username and password are required")

    pw_hash = hash_password(password)
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (username, password_hash)
                VALUES (%s, %s)
                ON CONFLICT (username) DO NOTHING;
                """,
                (username, pw_hash),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Username already exists")
        conn.commit()
        return {"message": "User registered successfully"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Registration failed: {e}")
    finally:
        if conn:
            conn.close()


@router.post("/sync", status_code=status.HTTP_202_ACCEPTED, summary="Sync SMS Messages")
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



@router.get("/messages", summary="Get All SMS Messages")
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

#  NEW CONVERT API ENDPOINT
@router.post("/convert", summary="Convert SMS Messages to Transactions", status_code=status.HTTP_200_OK)
def convert_sms_to_transactions(_: str = Depends(basic_auth)):
    """
    Admin API endpoint to convert all unprocessed SMS messages to transaction data.
    This processes all users' data, not just the authenticated user's data.
    """
    logger.info("Convert API called - starting SMS to transaction conversion")
    
    try:
        # Call the conversion function
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

# NEW TRANSACTIONS API ENDPOINT
@router.get("/transactions", summary="Get All Transactions")
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

# ADMIN ENDPOINT TO GET ALL TRANSACTIONS (ALL USERS)
@router.get("/admin/transactions", summary="Get All Transactions (Admin)")
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
