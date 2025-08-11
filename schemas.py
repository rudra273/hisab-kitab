from typing import List
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class SmsData(BaseModel):
    """Defines the structure of a single SMS message from the client."""
    id: int = Field(..., alias="id")
    address: str
    body: str
    date: int
    type: int


class SmsSyncRequest(BaseModel):
    """Defines the structure of the incoming sync request payload."""
    user_name: str
    messages: List[SmsData]


class UserCreate(BaseModel):
    """Schema for creating a new user."""
    username: str
    password: str


# Transaction schemas
class Transaction(BaseModel):
    """Schema representing a single parsed transaction."""
    user_name: str
    sms_id: int
    address: Optional[str] = None
    bank: Optional[str] = None
    amount: Optional[float] = None
    transaction_type: str
    merchant: Optional[str] = None
    created_at: Optional[datetime] = None
    date_received: Optional[int] = None


class TransactionsResponse(BaseModel):
    """Response wrapper for transactions list endpoints."""
    transactions: List[Transaction]
    count: int
