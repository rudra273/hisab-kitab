from fastapi import APIRouter, Request, status, HTTPException, Depends
from fastapi.templating import Jinja2Templates
from db import get_db_connection, setup_database
from auth import basic_auth
from logging_config import get_logger
from typing import Optional
import datetime

dash_router = APIRouter()
logger = get_logger("sms_sync.api")
templates = Jinja2Templates(directory="templates")

@dash_router.post("/setup-db", summary="Setup Database", status_code=status.HTTP_200_OK)
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

@dash_router.get("/", summary="Root Endpoint")
def read_root(request: Request):
    logger.debug("Root endpoint called - rendering template")
    return templates.TemplateResponse("index.html", {"request": request})

@dash_router.get("/dashboard", summary="User Dashboard")
def user_dashboard(request: Request, auth_user: str = Depends(basic_auth)):
    logger.debug("Dashboard requested - computing aggregates")
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sms_messages WHERE user_name = %s;", (auth_user,))
            total_messages = cur.fetchone()[0]
            cur.execute("SELECT COUNT(DISTINCT address) FROM sms_messages WHERE user_name = %s;", (auth_user,))
            unique_senders = cur.fetchone()[0]
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

@dash_router.get("/db", summary="Database Browser")
def view_db(
    request: Request,
    q: Optional[str] = None,
    address: Optional[str] = None,
    message_type: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    page: int = 1,
    page_size: int = 25,
    auth_user: str = Depends(basic_auth),
):
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
