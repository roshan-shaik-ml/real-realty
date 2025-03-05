from pydantic import BaseModel, EmailStr, Field
from typing import Optional

class Seller(BaseModel):
    id: int
    name: Optional[str] = None  # If available
    broker_name: Optional[str] = None  # If agent info is given

    class Config:
        orm_mode = True
