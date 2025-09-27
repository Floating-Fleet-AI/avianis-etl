from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import Config
import logging

config = Config()

class DatabaseManager:
    def __init__(self):
        # Optimized connection pool settings for better performance
        self.engine = create_engine(
            config.mysql_connection_string,
            pool_size=20,          # Increased pool size for concurrent operations
            max_overflow=30,       # Allow overflow connections
            pool_pre_ping=True,    # Validate connections before use
            pool_recycle=3600,     # Recycle connections every hour
            connect_args={
                "charset": "utf8mb4",
                "autocommit": False,
                "connect_timeout": 30,
                "read_timeout": 30,
                "write_timeout": 30
            }
        )
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
    
    def get_session(self):
        """Get database session"""
        return self.SessionLocal()
    
    def close_connection(self):
        """Close database connection"""
        self.engine.dispose()