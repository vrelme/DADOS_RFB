from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import Settings

Base = declarative_base()

def get_database_url():
    if Settings.DB_TYPE == "mysql":
        return (
            f"mysql+pymysql://{Settings.DB_USER}:"
            f"{Settings.DB_PASSWORD}@{Settings.DB_HOST}:"
            f"{Settings.DB_PORT}/{Settings.DB_NAME}"
        )

    elif Settings.DB_TYPE == "postgresql":
        return (
            f"postgresql+psycopg2://{Settings.DB_USER}:"
            f"{Settings.DB_PASSWORD}@{Settings.DB_HOST}:"
            f"{Settings.DB_PORT}/{Settings.DB_NAME}"
        )

    return f"sqlite:///{Settings.DB_NAME}"

DATABASE_URL = get_database_url()

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    future=True
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False
)