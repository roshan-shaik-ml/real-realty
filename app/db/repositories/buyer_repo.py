from sqlalchemy.orm import Session
from app.db.models.buyer import Buyer
from app.db.schemas.buyer import BuyerCreate, BuyerUpdate

class BuyerRepository:
    @staticmethod
    def create(db: Session, buyer_data: BuyerCreate) -> Buyer:
        buyer = Buyer(**buyer_data.dict())
        db.add(buyer)
        db.commit()
        db.refresh(buyer)
        return buyer

    @staticmethod
    def get_by_id(db: Session, buyer_id: int) -> Buyer:
        return db.query(Buyer).filter(Buyer.id == buyer_id).first()

    @staticmethod
    def get_all(db: Session):
        return db.query(Buyer).all()

    @staticmethod
    def update(db: Session, buyer_id: int, buyer_data: BuyerUpdate):
        buyer = db.query(Buyer).filter(Buyer.id == buyer_id).first()
        if buyer:
            for key, value in buyer_data.dict(exclude_unset=True).items():
                setattr(buyer, key, value)
            db.commit()
            db.refresh(buyer)
        return buyer

    @staticmethod
    def delete(db: Session, buyer_id: int):
        buyer = db.query(Buyer).filter(Buyer.id == buyer_id).first()
        if buyer:
            db.delete(buyer)
            db.commit()
        return buyer
