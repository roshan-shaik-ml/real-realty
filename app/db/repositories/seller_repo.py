from sqlalchemy.orm import Session
from app.db.models.seller import Seller
from app.db.schemas.seller import SellerCreate, SellerUpdate

class SellerRepository:
    @staticmethod
    def create(db: Session, seller_data: SellerCreate) -> Seller:
        seller = Seller(**seller_data.dict())
        db.add(seller)
        db.commit()
        db.refresh(seller)
        return seller

    @staticmethod
    def get_by_id(db: Session, seller_id: int) -> Seller:
        return db.query(Seller).filter(Seller.id == seller_id).first()

    @staticmethod
    def get_all(db: Session):
        return db.query(Seller).all()

    @staticmethod
    def update(db: Session, seller_id: int, seller_data: SellerUpdate):
        seller = db.query(Seller).filter(Seller.id == seller_id).first()
        if seller:
            for key, value in seller_data.dict(exclude_unset=True).items():
                setattr(seller, key, value)
            db.commit()
            db.refresh(seller)
        return seller

    @staticmethod
    def delete(db: Session, seller_id: int):
        seller = db.query(Seller).filter(Seller.id == seller_id).first()
        if seller:
            db.delete(seller)
            db.commit()
        return seller
