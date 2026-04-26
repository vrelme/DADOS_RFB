from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src import config

def build_database_url():

    if config.DB_TYPE == "mysql":
        return (
            f"mysql+pymysql://"
            f"{config.DB_USER}:{config.DB_PASSWORD}"
            f"@{config.DB_HOST}:{config.DB_PORT}/"
            f"{config.DB_NAME}?charset=utf8mb4"
        )

    elif config.DB_TYPE == "postgresql":
        return (
            f"postgresql+psycopg2://"
            f"{config.DB_USER}:{config.DB_PASSWORD}"
            f"@{config.DB_HOST}:{config.DB_PORT}/"
            f"{config.DB_NAME}"
        )

    elif config.DB_TYPE == "sqlite":
        return "sqlite:///rfb.db"

    raise ValueError("Banco não suportado")


DATABASE_URL = build_database_url()

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False
)