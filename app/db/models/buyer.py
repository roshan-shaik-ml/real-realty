from sqlalchemy import Column, Integer, String
from app.db.postgresql import Base

class Buyer(Base):
    __tablename__ = "buyers"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    phone = Column(String, unique=True, nullable=False)
    budget = Column(Float, nullable=True)
    preferred_city = Column(String, nullable=True)
