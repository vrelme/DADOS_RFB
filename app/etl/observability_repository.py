# app/etl/observability_repository.py

import json
import logging

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.database import mysql_error_code
from app.models import (
    ETLCheckpoint,
    ETLDeadLetter,
    ETLFileProgress,
    ETLMetric,
    ETLRun,
    ETLRunPhase,
)


logger = logging.getLogger(__name__)


class ObservabilityRepository:

    def __init__(self, db: Session):
        self.db = db

    def start_run(
        self,
        pipeline,
        sync_strategy,
        load_target,
        active_database,
        total_files=0,
    ):
        run = ETLRun(
            pipeline=pipeline,
            status="RUNNING",
            sync_strategy=sync_strategy,
            load_target=load_target,
            active_database=active_database,
            total_files=total_files or 0,
            completed_files=0,
            failed_files=0,
            progress_percent=0,
            started_at=datetime.utcnow(),
            heartbeat_at=datetime.utcnow(),
        )
        self.db.add(run)
        self.db.commit()
        self.db.refresh(run)
        return run

    def heartbeat_run(self, run_id):
        self._update_run_best_effort(
            run_id,
            "UPDATE etl_run "
            "SET heartbeat_at = :heartbeat_at "
            "WHERE id = :run_id AND status = 'RUNNING'",
            {
                "heartbeat_at": datetime.utcnow(),
                "run_id": run_id,
            },
            warning_message="Falha ao atualizar heartbeat do run",
        )

    def _update_run_best_effort(self, run_id, sql, params, warning_message):
        try:
            self.db.execute(text(sql), params)
            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            if mysql_error_code(exc) == 1020:
                logger.info(
                    "Atualizacao do etl_run ignorada por concorrencia; "
                    f"run_id={run_id}"
                )
                return
            logger.warning(f"{warning_message}: {exc}")

    def resume_run(self, run_id):
        try:
            run = self.db.get(ETLRun, run_id)
            if not run:
                return
            run.status = "RUNNING"
            run.finished_at = None
            run.error_message = None
            run.heartbeat_at = datetime.utcnow()
            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao retomar run ETL: {exc}")

    def update_run_context(self, run_id, phase=None, table_name=None, file_name=None):
        updates = ["heartbeat_at = :heartbeat_at"]
        params = {
            "run_id": run_id,
            "heartbeat_at": datetime.utcnow(),
        }

        if phase is not None:
            updates.append("current_phase = :current_phase")
            params["current_phase"] = phase
        if table_name is not None:
            updates.append("current_table = :current_table")
            params["current_table"] = table_name
        if file_name is not None:
            updates.append("current_file = :current_file")
            params["current_file"] = file_name

        self._update_run_best_effort(
            run_id,
            "UPDATE etl_run SET "
            + ", ".join(updates)
            + " WHERE id = :run_id AND status = 'RUNNING'",
            params,
            warning_message="Falha ao atualizar contexto do run",
        )

    def finish_run(self, run_id, status="SUCCESS", error_message=None):
        try:
            run = self.db.get(ETLRun, run_id)
            if not run:
                return
            finished_at = datetime.utcnow()
            run.status = status
            run.finished_at = finished_at
            run.heartbeat_at = finished_at
            run.duration_seconds = int((finished_at - run.started_at).total_seconds())
            run.progress_percent = 100 if status == "SUCCESS" else run.progress_percent
            run.error_message = str(error_message)[:5000] if error_message else None
            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao finalizar run ETL: {exc}")

    def start_phase(self, run_id, phase_name, table_name=None, message=None):
        try:
            phase = ETLRunPhase(
                run_id=run_id,
                phase_name=phase_name,
                table_name=table_name,
                status="RUNNING",
                started_at=datetime.utcnow(),
                message=message,
            )
            self.db.add(phase)
            self.db.commit()
            self.db.refresh(phase)
            self.update_run_context(
                run_id,
                phase=phase_name,
                table_name=table_name,
            )
            return phase
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao iniciar fase ETL: {exc}")
            return None

    def finish_phase(self, phase, status="SUCCESS", message=None):
        if not phase:
            return
        try:
            phase = self.db.merge(phase)
            finished_at = datetime.utcnow()
            phase.status = status
            phase.finished_at = finished_at
            phase.duration_seconds = int((finished_at - phase.started_at).total_seconds())
            if message:
                phase.message = message
            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao finalizar fase ETL: {exc}")

    def start_file_progress(self, run_id, pipeline, table_name, file_name):
        try:
            item = (
                self.db.query(ETLFileProgress)
                .filter(
                    ETLFileProgress.run_id == run_id,
                    ETLFileProgress.table_name == table_name,
                    ETLFileProgress.file_name == file_name,
                )
                .one_or_none()
            )
            if not item:
                item = ETLFileProgress(
                    run_id=run_id,
                    pipeline=pipeline,
                    table_name=table_name,
                    file_name=file_name,
                )
                self.db.add(item)
            item.status = "RUNNING"
            item.records_processed = 0
            item.started_at = datetime.utcnow()
            item.finished_at = None
            item.error_message = None

            self.db.commit()
            self.db.refresh(item)
            self.update_run_context(
                run_id,
                table_name=table_name,
                file_name=file_name,
            )
            return item
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao iniciar progresso de arquivo: {exc}")
            return None

    def finish_file_progress(
        self,
        item,
        run_id,
        status="SUCCESS",
        records_processed=0,
        error_message=None,
    ):
        try:
            finished_at = datetime.utcnow()
            if item:
                item.status = status
                item.records_processed = records_processed or 0
                item.finished_at = finished_at
                item.duration_seconds = int((finished_at - item.started_at).total_seconds())
                item.throughput_rows_per_second = (
                    float(item.records_processed) / item.duration_seconds
                    if item.duration_seconds and item.duration_seconds > 0
                    else 0
                )
                item.error_message = str(error_message)[:5000] if error_message else None

            self.db.commit()
            self._finish_file_progress_run_update(
                run_id,
                status,
                records_processed,
                finished_at,
            )
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao finalizar progresso de arquivo: {exc}")

    def _finish_file_progress_run_update(
        self,
        run_id,
        status,
        records_processed,
        finished_at,
    ):
        if status in {"SUCCESS", "SKIPPED"}:
            sql = """
                UPDATE etl_run
                SET
                    completed_files = COALESCE(completed_files, 0) + 1,
                    total_records = COALESCE(total_records, 0) + :records_processed,
                    progress_percent = CASE
                        WHEN COALESCE(total_files, 0) > 0 THEN
                            ROUND(
                                (
                                    (COALESCE(completed_files, 0)
                                     + COALESCE(failed_files, 0)
                                     + 1) / total_files
                                ) * 100,
                                2
                            )
                        ELSE progress_percent
                    END,
                    heartbeat_at = :heartbeat_at
                WHERE id = :run_id AND status = 'RUNNING'
            """
            params = {
                "run_id": run_id,
                "records_processed": records_processed or 0,
                "heartbeat_at": finished_at,
            }
        elif status == "FAILED":
            sql = """
                UPDATE etl_run
                SET
                    failed_files = COALESCE(failed_files, 0) + 1,
                    progress_percent = CASE
                        WHEN COALESCE(total_files, 0) > 0 THEN
                            ROUND(
                                (
                                    (COALESCE(completed_files, 0)
                                     + COALESCE(failed_files, 0)
                                     + 1) / total_files
                                ) * 100,
                                2
                            )
                        ELSE progress_percent
                    END,
                    heartbeat_at = :heartbeat_at
                WHERE id = :run_id AND status = 'RUNNING'
            """
            params = {
                "run_id": run_id,
                "heartbeat_at": finished_at,
            }
        else:
            sql = """
                UPDATE etl_run
                SET heartbeat_at = :heartbeat_at
                WHERE id = :run_id AND status = 'RUNNING'
            """
            params = {
                "run_id": run_id,
                "heartbeat_at": finished_at,
            }

        self._update_run_best_effort(
            run_id,
            sql,
            params,
            warning_message="Falha ao atualizar progresso do run",
        )


    def metric(
        self,
        pipeline,
        metric_name,
        metric_value,
        unit=None,
        table_name=None,
        file_name=None,
        worker=None,
    ):
        try:
            self.db.add(
                ETLMetric(
                    pipeline=pipeline,
                    table_name=table_name,
                    file_name=file_name,
                    metric_name=metric_name,
                    metric_value=float(metric_value or 0),
                    unit=unit,
                    worker=worker,
                )
            )
            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao gravar métrica ETL: {exc}")

    def checkpoint(
        self,
        pipeline,
        table_name,
        file_name,
        status,
        records_processed=0,
        checksum=None,
        error_message=None,
    ):
        try:
            item = (
                self.db.query(ETLCheckpoint)
                .filter(
                    ETLCheckpoint.pipeline == pipeline,
                    ETLCheckpoint.table_name == table_name,
                    ETLCheckpoint.file_name == file_name,
                )
                .one_or_none()
            )

            if not item:
                item = ETLCheckpoint(
                    pipeline=pipeline,
                    table_name=table_name,
                    file_name=file_name,
                )
                self.db.add(item)

            item.status = status
            item.records_processed = records_processed or 0
            item.checksum = checksum
            item.error_message = str(error_message)[:5000] if error_message else None
            item.updated_at = datetime.utcnow()

            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao gravar checkpoint ETL: {exc}")

    def already_successful(self, pipeline, table_name, file_name):
        item = (
            self.db.query(ETLCheckpoint)
            .filter(
                ETLCheckpoint.pipeline == pipeline,
                ETLCheckpoint.table_name == table_name,
                ETLCheckpoint.file_name == file_name,
                ETLCheckpoint.status == "SUCCESS",
            )
            .one_or_none()
        )
        return item is not None

    def already_successful_in_run(self, run_id, pipeline, table_name, file_name):
        item = (
            self.db.query(ETLFileProgress)
            .filter(
                ETLFileProgress.run_id == run_id,
                ETLFileProgress.pipeline == pipeline,
                ETLFileProgress.table_name == table_name,
                ETLFileProgress.file_name == file_name,
                ETLFileProgress.status == "SUCCESS",
            )
            .one_or_none()
        )
        return item is not None

    def dead_letter(
        self,
        pipeline,
        table_name,
        file_name,
        reason_code,
        reason_message,
        row_number=None,
        raw_payload=None,
    ):
        try:
            payload = raw_payload
            if isinstance(raw_payload, (dict, list)):
                payload = json.dumps(raw_payload, ensure_ascii=False, default=str)

            self.db.add(
                ETLDeadLetter(
                    pipeline=pipeline,
                    table_name=table_name,
                    file_name=file_name,
                    row_number=row_number,
                    reason_code=reason_code,
                    reason_message=reason_message,
                    raw_payload=payload,
                )
            )
            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao gravar DLQ ETL: {exc}")
