from pydantic import BaseModel, Field
from typing import Optional

class HouseBase(BaseModel):
    address: str = Field(..., example="123 Main St, Los Angeles, CA")
    price: float = Field(..., example=90000.0)
    lot_area: Optional[float] = Field(None, example=4999.0)
    status: str = Field(..., example="For Sale")
    image_url: Optional[str] = Field(None, example="https://example.com/image.jpg")
    latitude: Optional[float] = Field(None, example=34.09424)
    longitude: Optional[float] = Field(None, example=-118.21569)

class HouseCreate(HouseBase):
    pass  # No extra fields needed

class HouseUpdate(HouseBase):
    address: Optional[str] = None
    price: Optional[float] = None
    lot_area: Optional[float] = None
    status: Optional[str] = None
    image_url: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

class HouseResponse(HouseBase):
    id: int

    class Config:
        from_attributes = True
