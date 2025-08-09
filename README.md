# SMS Sync Service (FastAPI)

A simple, professional FastAPI service to receive, store, and retrieve SMS messages synced from an Android client.

## Overview
- Stores messages in PostgreSQL with per-user de-duplication.
- Minimal modular structure for clarity and maintainability.

## Project Structure
```
personal-finance-assistant/
├─ main.py          # App init, mounts router, DB setup on startup
├─ routes.py        # API endpoints (/, /setup-db, /sync, /messages)
├─ db.py            # DB connection + table creation
├─ schemas.py       # Pydantic models (SmsData, SmsSyncRequest)
├─ requirements.txt # Python dependencies
├─ Dockerfile       # Production server via Uvicorn
├─ .env.example     # Example environment variables
```

## Requirements
- Python 3.11+
- PostgreSQL database

## Environment Variables
Copy `.env.example` to `.env` and update values:
```
DB_URL=postgresql://<USER>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>
```
Examples:
- Local: `postgresql://postgres:postgres@localhost:5432/sms_db`
- Render/FlyIO/etc. may provide a `postgres://...` URL (both `postgres://` and `postgresql://` are accepted by psycopg2).

## Setup
1. Create and activate a virtualenv (recommended):
   ```bash
   python -m venv .venv
   # Windows PowerShell
   .venv\\Scripts\\Activate.ps1
   # macOS/Linux
   source .venv/bin/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create `.env` from example and set `DB_URL`.

## Run (Local)
```bash
uvicorn main:app --reload
```
App runs on http://localhost:8000

## Run (Docker)
```bash
docker build -t sms-sync .
docker run -p 8000:8000 -e DB_URL="postgresql://user:pass@host:5432/db" sms-sync
```

## API
- `GET /` — health check
- `POST /setup-db` — create table if not exists
- `POST /sync` — sync messages
- `GET /messages` — fetch all stored messages

### Sample: POST /sync
Request body:
```json
{
  "user_name": "alice",
  "messages": [
    {
      "id": 123456,
      "address": "+15551234567",
      "body": "Your OTP is 1234",
      "date": 1754742120556,
      "type": 1
    }
  ]
}
```
Curl example:
```bash
curl -X POST http://localhost:8000/sync \
  -H "Content-Type: application/json" \
  -d '{
    "user_name": "alice",
    "messages": [{"id": 1, "address": "+1555", "body": "hi", "date": 1754742120556, "type": 1}]
  }'
```

## Notes
- On startup, the app ensures the `sms_messages` table exists (`db.py::setup_database()`).
- De-duplication is enforced via unique constraint `(sms_id, user_name)`.
- Timestamps from device are assumed to be in milliseconds since epoch and are formatted when returned by `GET /messages`.
