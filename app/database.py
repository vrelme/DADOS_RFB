import time

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
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


def mysql_connect_args(connect_timeout=None, read_timeout=None, write_timeout=None):
    return {
        "local_infile": Settings.DB_LOCAL_INFILE,
        "connect_timeout": connect_timeout or Settings.DB_CONNECT_TIMEOUT,
        "read_timeout": read_timeout or Settings.DB_READ_TIMEOUT,
        "write_timeout": write_timeout or Settings.DB_WRITE_TIMEOUT,
    }


def mysql_error_code(error):
    seen = set()
    current = error

    while current and id(current) not in seen:
        seen.add(id(current))

        wrapped = getattr(current, "original_exception", None)
        if wrapped is not None and id(wrapped) not in seen:
            wrapped_code = mysql_error_code(wrapped)
            if wrapped_code is not None:
                return wrapped_code

        original = getattr(current, "orig", None)
        args = getattr(original, "args", None)
        if args and isinstance(args[0], int):
            return args[0]

        args = getattr(current, "args", None)
        if args and isinstance(args[0], int):
            return args[0]

        current = getattr(current, "__cause__", None) or getattr(
            current,
            "__context__",
            None,
        )

    return None


def is_database_connection_lost(error):
    wrapped = getattr(error, "original_exception", None)
    if wrapped is not None and is_database_connection_lost(wrapped):
        return True

    code = mysql_error_code(error)
    if code in {2003, 2006, 2013, 2014, 2055}:
        return True

    message = str(error).lower()
    markers = (
        "lost connection",
        "server has gone away",
        "can't connect to mysql",
        "connection was killed",
        "connection reset",
        "connection refused",
        "broken pipe",
        "pymysql.err.operationalerror",
        "during query",
    )
    return any(marker in message for marker in markers)


def format_duration(seconds):
    seconds = float(seconds or 0)
    hours, remainder = divmod(int(seconds), 3600)
    minutes, whole_seconds = divmod(remainder, 60)
    milliseconds = int((seconds - int(seconds)) * 1000)
    if hours:
        return f"{hours:02d}:{minutes:02d}:{whole_seconds:02d}.{milliseconds:03d}"
    return f"{minutes:02d}:{whole_seconds:02d}.{milliseconds:03d}"


def database_recovery_interval(elapsed_seconds):
    if elapsed_seconds < Settings.DB_RECOVERY_FAST_UNTIL_SECONDS:
        return Settings.DB_RECOVERY_FAST_INTERVAL_SECONDS
    if elapsed_seconds < Settings.DB_RECOVERY_MEDIUM_UNTIL_SECONDS:
        return Settings.DB_RECOVERY_MEDIUM_INTERVAL_SECONDS
    return Settings.DB_RECOVERY_SLOW_INTERVAL_SECONDS


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


def create_server_engine(connect_timeout=None, read_timeout=None, write_timeout=None):
    return create_engine(
        build_server_url(),
        echo=Settings.ORM_ECHO,
        future=Settings.ORM_FUTURE,
        poolclass=NullPool,
        pool_pre_ping=Settings.DB_POOL_PRE_PING,
        connect_args=mysql_connect_args(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
        ),
    )


def create_session_factory(bind):
    return sessionmaker(
        bind=bind,
        autoflush=Settings.ORM_AUTOFLUSH,
        autocommit=Settings.ORM_AUTOCOMMIT,
        expire_on_commit=Settings.ORM_EXPIRE_ON_COMMIT,
    )


def check_database_available():
    timeout = max(1, Settings.DB_HEALTH_CHECK_TIMEOUT)
    server_engine = create_server_engine(
        connect_timeout=timeout,
        read_timeout=timeout,
        write_timeout=timeout,
    )

    try:
        with server_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            yield "servidor"

            for label, database_name in (
                ("carga", Settings.ACTIVE_DB_NAME),
                ("operacional", Settings.OPERATIONAL_DB_NAME),
            ):
                row = conn.execute(
                    text(
                        "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                        "WHERE SCHEMA_NAME = :database_name"
                    ),
                    {"database_name": database_name},
                ).first()
                status = "existe" if row else "ausente"
                yield f"{label} ({database_name}: {status})"

    finally:
        server_engine.dispose()


def wait_for_database_recovery(logger, context):
    started_at = time.time()
    attempt = 0

    while True:
        attempt += 1
        elapsed = time.time() - started_at

        try:
            engine.dispose()
            operational_engine.dispose()
            for label in check_database_available():
                logger.info(f"DB RECOVERY | conexao {label} OK")
            logger.info(
                f"DB RECOVERY | banco voltou | "
                f"tentativas={attempt} | espera={format_duration(elapsed)} | "
                f"{context}"
            )
            return

        except SQLAlchemyError as exc:
            if (
                Settings.DB_RECOVERY_MAX_WAIT_SECONDS
                and elapsed >= Settings.DB_RECOVERY_MAX_WAIT_SECONDS
            ):
                logger.error(
                    f"DB RECOVERY | limite de espera atingido | "
                    f"tentativas={attempt} | espera={format_duration(elapsed)} | "
                    f"{context}"
                )
                raise

            interval = database_recovery_interval(elapsed)
            logger.error(
                f"DB RECOVERY | banco indisponivel | "
                f"tentativa={attempt} | espera={format_duration(elapsed)} | "
                f"proxima={interval}s | erro={exc} | {context}"
            )
            time.sleep(interval)


def ensure_database_exists(database_name=None):
    db_name = database_name or Settings.ACTIVE_DB_NAME
    safe_name = db_name.replace("`", "``")
    last_error = None

    for attempt in range(1, Settings.DB_CREATE_RETRIES + 1):
        server_engine = create_server_engine()

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
engine = create_engine_for_database(Settings.ACTIVE_DB_NAME)

operational_engine = create_engine_for_database(Settings.OPERATIONAL_DB_NAME)

# =====================================================
# SESSION
# =====================================================
SessionLocal = create_session_factory(engine)

OperationalSessionLocal = create_session_factory(operational_engine)

# =====================================================
# BASE ORM
# =====================================================
Base = declarative_base()
