from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import Optional
from dotenv import load_dotenv
import os

load_dotenv()

class PostgreSQL:
    _instance: Optional['PostgreSQL'] = None
    Base = declarative_base()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PostgreSQL, cls).__new__(cls)
            cls._instance.engine = None
            cls._instance.SessionLocal = None
        return cls._instance
    
    def connect(self):
        if self.engine is None:
            # Connection details from docker-compose.yml with defaults
            POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
            POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
            POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
            POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5434")
            POSTGRES_DB = os.getenv("POSTGRES_DB", "entities")
            
            # Use POSTGRES_URI if available, otherwise construct from components
            DATABASE_URL = os.getenv("POSTGRES_URI")
            if not DATABASE_URL:
                DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
            
            try:
                self.engine = create_engine(DATABASE_URL)
                self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
                print("Successfully connected to PostgreSQL")
            except Exception as e:
                print(f"Failed to connect to PostgreSQL: {e}")
                raise
    
    def get_session(self):
        if self.SessionLocal is None:
            self.connect()
        return self.SessionLocal()
    
    def create_tables(self):
        if self.engine is None:
            self.connect()
        self.Base.metadata.create_all(bind=self.engine)
    
    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.SessionLocal = None
