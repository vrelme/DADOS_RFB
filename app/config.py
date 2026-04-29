import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    DB_TYPE = os.getenv("DB_TYPE", "my")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "dados_rfb")