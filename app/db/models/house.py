from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from app.db.postgresql import Base

class House(Base):
    __tablename__ = "houses"

    id = Column(Integer, primary_key=True, index=True)
    zpid = Column(String, unique=True, nullable=False)
    address = Column(String, nullable=False)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    zipcode = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    lot_size = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    seller_id = Column(Integer, ForeignKey("sellers.id"), nullable=True)

    seller = relationship("Seller", back_populates="houses")
