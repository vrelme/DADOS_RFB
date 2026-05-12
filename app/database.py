import time

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import NullPool

from app.config import Settings

def build_server_url():
    return (
        f"{Settings.DB_DRIVER}://"
        f"{Settings.DB_USER}:{Settings.DB_PASSWORD}"
        f"@{Settings.DB_HOST}:{Settings.DB_PORT}/"
        f"?charset={Settings.DB_CHARSET}"
    )


def build_database_url(database_name=None):
    db_name = database_name or Settings.ACTIVE_DB_NAME
    return (
        f"{Settings.DB_DRIVER}://"
        f"{Settings.DB_USER}:{Settings.DB_PASSWORD}"
        f"@{Settings.DB_HOST}:{Settings.DB_PORT}/{db_name}"
        f"?charset={Settings.DB_CHARSET}"
    )


def mysql_connect_args():
    return {
        "local_infile": Settings.DB_LOCAL_INFILE,
        "connect_timeout": Settings.DB_CONNECT_TIMEOUT,
        "read_timeout": Settings.DB_READ_TIMEOUT,
        "write_timeout": Settings.DB_WRITE_TIMEOUT,
    }


def mysql_error_code(error):
    original = getattr(error, "orig", None)
    args = getattr(original, "args", None)
    return args[0] if args else None


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
        connect_args=mysql_connect_args(),
    )


def ensure_database_exists(database_name=None):
    db_name = database_name or Settings.ACTIVE_DB_NAME
    safe_name = db_name.replace("`", "``")
    last_error = None

    for attempt in range(1, Settings.DB_CREATE_RETRIES + 1):
        server_engine = create_engine(
            build_server_url(),
            echo=Settings.ORM_ECHO,
            future=Settings.ORM_FUTURE,
            poolclass=NullPool,
            pool_pre_ping=Settings.DB_POOL_PRE_PING,
            connect_args=mysql_connect_args(),
        )

        try:
            with server_engine.begin() as conn:
                conn.exec_driver_sql(
                    f"CREATE DATABASE IF NOT EXISTS `{safe_name}` "
                    f"CHARACTER SET {Settings.DB_CHARSET}"
                )
            return

        except SQLAlchemyError as exc:
            last_error = exc
            code = mysql_error_code(exc)
            server_engine.dispose()

            if code not in {2006, 2013} or attempt == Settings.DB_CREATE_RETRIES:
                raise

            time.sleep(Settings.DB_CREATE_RETRY_DELAY)

        finally:
            server_engine.dispose()

    if last_error:
        raise last_error


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

    connect_args=mysql_connect_args()
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
