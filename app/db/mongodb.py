from pymongo import MongoClient
from typing import Optional

class MongoDB:
    _instance: Optional['MongoDB'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MongoDB, cls).__new__(cls)
            cls._instance.client = None
        return cls._instance
    
    def connect(self):
        if self.client is None:
            # Connection details from docker-compose.yml
            MONGO_USER = "root"
            MONGO_PASSWORD = "example"
            MONGO_HOST = "localhost"  # or "mongodb" if running inside docker
            MONGO_PORT = "27017"
            
            connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/"
            self.client = MongoClient(connection_string)
            
            # Test the connection
            try:
                self.client.server_info()
                print("Successfully connected to MongoDB")
            except Exception as e:
                print(f"Failed to connect to MongoDB: {e}")
                raise
    
    def get_database(self, db_name: str = "mydatabase"):
        if self.client is None:
            self.connect()
        return self.client[db_name]
    
    def close(self):
        if self.client:
            self.client.close()
            self.client = None
