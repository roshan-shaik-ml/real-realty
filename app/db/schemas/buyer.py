from pydantic import BaseModel, EmailStr, Field
from typing import Optional

class BuyerBase(BaseModel):
    name: str = Field(..., example="John Doe")
    email: EmailStr = Field(..., example="johndoe@example.com")
    phone_number: Optional[str] = Field(None, example="+1 234 567 8900")

class BuyerCreate(BuyerBase):
    pass

class BuyerUpdate(BuyerBase):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone_number: Optional[str] = None

class BuyerResponse(BuyerBase):
    id: int

    class Config:
        from_attributes = True
