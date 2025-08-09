from typing import List
from pydantic import BaseModel, Field


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
