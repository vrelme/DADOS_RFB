from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

from app.config import Settings

def build_database_url(database_name=None):
    db_name = database_name or Settings.DB_NAME
    return (
        f"{Settings.DB_DRIVER}://"
        f"{Settings.DB_USER}:{Settings.DB_PASSWORD}"
        f"@{Settings.DB_HOST}:{Settings.DB_PORT}/{db_name}"
        f"?charset={Settings.DB_CHARSET}"
    )


def create_engine_for_database(database_name=None):
    return create_engine(
        build_database_url(database_name),
        echo=Settings.ORM_ECHO,
        future=Settings.ORM_FUTURE,
        pool_size=Settings.DB_POOL_SIZE,
        max_overflow=Settings.DB_MAX_OVERFLOW,
        pool_recycle=Settings.DB_POOL_RECYCLE,
        pool_timeout=Settings.DB_POOL_TIMEOUT,
        pool_pre_ping=Settings.DB_POOL_PRE_PING,
        connect_args={"local_infile": Settings.DB_LOCAL_INFILE}
    )


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

    pool_pre_ping=Settings.DB_POOL_PRE_PING,

    connect_args={
        "local_infile": Settings.DB_LOCAL_INFILE
    }
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
