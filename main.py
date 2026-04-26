from src.database import SessionLocal
from src.services.loader_service import LoaderService


def main():
    # cria tabelas automaticamente
    session = SessionLocal()

    try:
        loader = LoaderService(
            session = session, 
            extract_dir="data/extracted"
            )
        
        loader.run()

    finally:
        session.close
        

if __name__ == "__main__":
    main()