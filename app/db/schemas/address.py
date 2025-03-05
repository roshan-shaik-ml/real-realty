from pydantic import BaseModel
from typing import Optional

class Address(BaseModel):
    id: int
    street: str
    city: str
    state: str
    zipcode: str
    latitude: float
    longitude: float

    class Config:
        orm_mode = True