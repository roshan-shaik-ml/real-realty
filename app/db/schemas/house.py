from pydantic import BaseModel
from typing import Optional

class HouseListing(BaseModel):
    zpid: str
    price: float
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    living_area: Optional[float] = None
    lot_area_value: Optional[float] = None
    home_type: str
    home_status: str
    days_on_zillow: Optional[int] = None
    rent_zestimate: Optional[float] = None
    address_id: Optional[int] = None  # ForeignKey to Address
    seller_id: Optional[int] = None  # ForeignKey to Seller

    class Config:
        from_attributes = True