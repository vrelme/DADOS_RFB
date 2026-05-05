from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import Settings

# =====================================================
# ENGINE (POOL OTIMIZADO)
# =====================================================
engine = create_engine(
    Settings.DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600,
)

# =====================================================
# SESSION
# =====================================================
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# =====================================================
# BASE ORM
# =====================================================
Base = declarative_base()