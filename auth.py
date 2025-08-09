from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from passlib.context import CryptContext
from db import get_db_connection

security = HTTPBasic()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, password_hash: str) -> bool:
    return pwd_context.verify(plain_password, password_hash)


def basic_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    """Authenticate using the users table (HTTP Basic).

    Returns the authenticated username on success.
    """
    username = credentials.username
    password = credentials.password

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT password_hash FROM users WHERE username = %s;", (username,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Unauthorized",
                    headers={"WWW-Authenticate": "Basic"},
                )
            stored_hash = row[0]
            if not verify_password(password, stored_hash):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Unauthorized",
                    headers={"WWW-Authenticate": "Basic"},
                )
            return username
    finally:
        if conn:
            conn.close()
