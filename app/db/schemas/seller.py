from pydantic import BaseModel, EmailStr
from typing import Optional

class SellerBase(BaseModel):
    name: str
    email: EmailStr
    phone_number: Optional[str] = None

class SellerCreate(SellerBase):
    pass  # No additional fields required for creation

class SellerUpdate(SellerBase):
    name: Optional[str] = None
    email: Optional[EmailStr] = None
    phone_number: Optional[str] = None

class SellerResponse(SellerBase):
    id: int

    class Config:
        from_attributes = True
