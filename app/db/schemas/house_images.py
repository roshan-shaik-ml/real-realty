from pydantic import BaseModel, EmailStr, Field
from typing import Optional

class HouseImage(BaseModel):
    id: int
    house_id: int  # ForeignKey to HouseListing
    image_url: str

    class Config:
        orm_mode = True