from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from app.config import Settings

# conexão principal
engine = create_engine(
    Settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True
)

# fábrica de sessões
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# base ORM
Base = declarative_base()