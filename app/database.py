from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from app.config import Settings

# =====================================================
# ENGINE
# =====================================================
engine = create_engine(
    Settings.DATABASE_URL,

    echo=Settings.ORM_ECHO,

    future=Settings.ORM_FUTURE,

    pool_size=Settings.DB_POOL_SIZE,

    max_overflow=Settings.DB_MAX_OVERFLOW,

    pool_recycle=Settings.DB_POOL_RECYCLE,

    pool_timeout=Settings.DB_POOL_TIMEOUT,

    pool_pre_ping=Settings.DB_POOL_PRE_PING
)

# =====================================================
# SESSION
# =====================================================
SessionLocal = sessionmaker(
    bind=engine,

    autoflush=Settings.ORM_AUTOFLUSH,

    autocommit=Settings.ORM_AUTOCOMMIT,

    expire_on_commit=Settings.ORM_EXPIRE_ON_COMMIT
)

# =====================================================
# BASE ORM
# =====================================================
Base = declarative_base()