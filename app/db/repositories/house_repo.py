from sqlalchemy.orm import Session
from app.db.models.house import House
from app.db.schemas.house import HouseCreate, HouseUpdate

class HouseRepository:
    @staticmethod
    def create(db: Session, house_data: HouseCreate) -> House:
        house = House(**house_data.dict())
        db.add(house)
        db.commit()
        db.refresh(house)
        return house

    @staticmethod
    def get_by_id(db: Session, house_id: int) -> House:
        return db.query(House).filter(House.id == house_id).first()

    @staticmethod
    def get_all(db: Session):
        return db.query(House).all()

    @staticmethod
    def update(db: Session, house_id: int, house_data: HouseUpdate):
        house = db.query(House).filter(House.id == house_id).first()
        if house:
            for key, value in house_data.dict(exclude_unset=True).items():
                setattr(house, key, value)
            db.commit()
            db.refresh(house)
        return house

    @staticmethod
    def delete(db: Session, house_id: int):
        house = db.query(House).filter(House.id == house_id).first()
        if house:
            db.delete(house)
            db.commit()
        return house
