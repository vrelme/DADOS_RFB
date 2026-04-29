from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.config import Settings

def get_database_url():

    if Settings.DB_TYPE == "mysql":
        return (
            f"mysql+pymysql://{Settings.DB_USER}:"
            f"{Settings.DB_PASSWORD}@"
            f"{Settings.DB_HOST}:"
            f"{Settings.DB_PORT}/"
            f"{Settings.DB_NAME}"
        )

    if Settings.DB_TYPE == "postgresql":
        return (
            f"postgresql://{Settings.DB_USER}:"
            f"{Settings.DB_PASSWORD}@"
            f"{Settings.DB_HOST}:"
            f"{Settings.DB_PORT}/"
            f"{Settings.DB_NAME}"
        )

    return f"sqlite:///{Settings.DB_NAME}.db"


engine = create_engine(get_database_url(), pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)