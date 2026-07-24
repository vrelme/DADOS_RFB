# app/etl/etl_execution_repository.py

import logging

from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.models import ETLExecution


logger = logging.getLogger(__name__)


class ETLExecutionRepository:

    def __init__(self, db: Session):
        self.db = db

    # =====================================================
    # START EXECUTION
    # =====================================================
    def start(
        self,
        pipeline: str,
        file_name: str,
        table_name: str,
        worker: str,
        environment: str
    ) -> ETLExecution:

        try:

            execution = ETLExecution(
                pipeline=pipeline,
                file_name=file_name,
                table_name=table_name,
                status="STARTED",
                worker=worker,
                environment=environment,
                started_at=datetime.utcnow()
            )

            self.db.add(execution)

            self.db.commit()

            self.db.refresh(execution)
            texto = f"id={execution.id}"
            logger.info(
                f"ETL STARTED          | "
                f"{texto:<15} | "
                f"file={file_name}"
            )

            return execution

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro ao iniciar execução ETL: {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # SUCCESS
    # =====================================================
    def success(
        self,
        execution: ETLExecution,
        records_processed: int
    ):

        try:

            finished_at = datetime.utcnow()

            execution.status = "SUCCESS"

            execution.records_processed = records_processed

            execution.finished_at = finished_at

            execution.duration_seconds = int(
                (finished_at - execution.started_at).total_seconds()
            )

            self.db.commit()

            logger.info(
                f"ETL SUCCESS          | "
                f"id={execution.id:<15} | "
                f"records={records_processed} | "
                f"duration={execution.duration_seconds}s"
            )

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro ao finalizar execução ETL: {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # FAILED
    # =====================================================
    def failed(
        self,
        execution: ETLExecution,
        error_message: str
    ):

        try:

            finished_at = datetime.utcnow()

            execution.status = "FAILED"

            execution.finished_at = finished_at

            execution.duration_seconds = int(
                (finished_at - execution.started_at).total_seconds()
            )

            execution.error_message = str(error_message)[:5000]

            self.db.commit()

            logger.error(
                f"ETL FAILED           | "
                f"id={execution.id:<11} | "
                f"duration={execution.duration_seconds}s"
            )

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro ao registrar falha ETL: {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # RETRY
    # =====================================================
    def retry(
        self,
        execution: ETLExecution
    ):

        try:

            execution.status = "RETRY"

            execution.retry_count += 1

            self.db.commit()

            logger.warning(
                f"ETL RETRY            | "
                f"id={execution.id:<11} | "
                f"retry={execution.retry_count}"
            )

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro ao registrar retry ETL: {e}",
                exc_info=True
            )

            raise

    # =====================================================
    # GET FAILED EXECUTIONS
    # =====================================================
    def get_failed(self):

        return (
            self.db.query(ETLExecution)
            .filter(ETLExecution.status == "FAILED")
            .all()
        )

    # =====================================================
    # GET RUNNING EXECUTIONS
    # =====================================================
    def get_running(self):

        return (
            self.db.query(ETLExecution)
            .filter(ETLExecution.status == "STARTED")
            .all()
        )

    # =====================================================
    # GET SUCCESS EXECUTIONS
    # =====================================================
    def get_success(self):

        return (
            self.db.query(ETLExecution)
            .filter(ETLExecution.status == "SUCCESS")
            .all()
        )

    # =====================================================
    # FIND BY FILE
    # =====================================================
    def find_by_file(
        self,
        file_name: str
    ):

        return (
            self.db.query(ETLExecution)
            .filter(ETLExecution.file_name == file_name)
            .order_by(ETLExecution.started_at.desc())
            .first()
        )

    # =====================================================
    # EXECUTION EXISTS
    # =====================================================
    def already_processed(
        self,
        file_name: str
    ) -> bool:

        result = (
            self.db.query(ETLExecution)
            .filter(
                ETLExecution.file_name == file_name,
                ETLExecution.status == "SUCCESS"
            )
            .first()
        )

        return result is not None

    # =====================================================
    # METRICS
    # =====================================================
    def metrics(self):

        total = self.db.query(ETLExecution).count()

        success = (
            self.db.query(ETLExecution)
            .filter(ETLExecution.status == "SUCCESS")
            .count()
        )

        failed = (
            self.db.query(ETLExecution)
            .filter(ETLExecution.status == "FAILED")
            .count()
        )

        running = (
            self.db.query(ETLExecution)
            .filter(ETLExecution.status == "STARTED")
            .count()
        )

        return {
            "total": total,
            "success": success,
            "failed": failed,
            "running": running
        }

    # =====================================================
    # DELETE OLD EXECUTIONS
    # =====================================================
    def delete_old(
        self,
        days: int = 30
    ):

        try:

            sql = f"""
                DELETE FROM etl_execution
                WHERE started_at < NOW() - INTERVAL {days} DAY
            """

            self.db.execute(sql)

            self.db.commit()

            logger.info(
                f"Execuções antigas removidas ({days} dias)"
            )

        except SQLAlchemyError as e:

            self.db.rollback()

            logger.error(
                f"Erro limpeza etl_execution: {e}",
                exc_info=True
            )

            raise