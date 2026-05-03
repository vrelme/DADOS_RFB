import pandas as pd
import numpy as np
import logging
import time

from app.database import SessionLocal
from app.etl.transformer import DataTransformer
from app.etl.validator import Validator
from app.etl.bulk_repository import BulkRepository
from app.etl.deduplicator import Deduplicator

logger = logging.getLogger(__name__)


class ETLOrchestrator:

    def __init__(self, logger_instance=None):
        self.logger = logger_instance or logger

        self.transformer = DataTransformer()
        self.validator = Validator()
        self.deduplicator = Deduplicator()

    # =========================
    # EXECUÇÃO PRINCIPAL
    # =========================
    def run(self, file_path, model, columns, key):

        start_total = time.time()

        self.logger.info(f"Iniciando processamento: {file_path}")

        db = SessionLocal()
        repo = BulkRepository(db)

        total = 0
        chunk_count = 0

        try:
            for chunk in pd.read_csv(
                file_path,
                sep=";",
                names=columns,
                dtype=str,
                chunksize=50000,
                encoding="latin1"
            ):

                start_chunk = time.time()
                chunk_count += 1

                # =========================
                # TRANSFORM
                # =========================
                chunk = self.transformer.sanitize(chunk)

                # =========================
                # VALIDATE
                # =========================
                chunk = self._validate(model, chunk)

                # =========================
                # DEDUPLICAÇÃO
                # =========================
                chunk = self.deduplicator.drop_duplicates(chunk, key)

                # =========================
                # NaN → None
                # =========================
                chunk = chunk.replace({np.nan: None})

                data = chunk.to_dict(orient="records")

                if not data:
                    self.logger.debug(f"Chunk {chunk_count} vazio — ignorado")
                    continue

                # =========================
                # INSERT
                # =========================
                inserted = repo.bulk_insert(model, data)

                total += inserted if inserted else 0

                # =========================
                # PERFORMANCE
                # =========================
                end_chunk = time.time()
                duration = end_chunk - start_chunk

                throughput = int(len(data) / duration) if duration > 0 else 0

                mem_mb = chunk.memory_usage(deep=True).sum() / 1024**2

                self.logger.info(
                    f"{model.__tablename__} | "
                    f"Chunk {chunk_count} | "
                    f"{len(data)} registros | "
                    f"{duration:.2f}s | "
                    f"{throughput} reg/s | "
                    f"{mem_mb:.2f} MB"
                )

        except Exception as e:
            self.logger.error(f"Erro crítico: {e}", exc_info=True)
            raise

        finally:
            db.close()

        # =========================
        # FINAL
        # =========================
        end_total = time.time()
        total_time = end_total - start_total

        avg_throughput = int(total / total_time) if total_time > 0 else 0

        self.logger.info("=" * 70)
        self.logger.info(f"FINALIZADO: {total} registros")
        self.logger.info(f"Tempo total: {total_time:.2f}s")
        self.logger.info(f"Throughput médio: {avg_throughput} reg/s")
        self.logger.info("=" * 70)

    # =========================
    # VALIDAÇÃO DINÂMICA
    # =========================
    def _validate(self, model, df):

        name = model.__tablename__

        if name == "empresa":
            return self.validator.validate_empresa(df)

        elif name == "estabelecimento":
            return self.validator.validate_estabelecimento(df)

        elif name == "socio":
            return self.validator.validate_socio(df)

        return df