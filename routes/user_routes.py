from fastapi import APIRouter, status, HTTPException, Depends
from db import get_db_connection
from schemas import UserCreate
from auth import basic_auth
from logging_config import get_logger

user_router = APIRouter()
logger = get_logger("sms_sync.api")

@user_router.post("/register", status_code=status.HTTP_201_CREATED, summary="Register a new user")
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
