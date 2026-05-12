# app/etl/observability_repository.py

import json
import logging

from datetime import datetime

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

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
        try:
            run = self.db.get(ETLRun, run_id)
            if run and run.status == "RUNNING":
                run.heartbeat_at = datetime.utcnow()
                self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao atualizar heartbeat do run: {exc}")

    def update_run_context(self, run_id, phase=None, table_name=None, file_name=None):
        try:
            run = self.db.get(ETLRun, run_id)
            if not run:
                return
            if phase is not None:
                run.current_phase = phase
            if table_name is not None:
                run.current_table = table_name
            if file_name is not None:
                run.current_file = file_name
            run.heartbeat_at = datetime.utcnow()
            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao atualizar contexto do run: {exc}")

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
            run = self.db.get(ETLRun, run_id)
            if run:
                run.current_phase = phase_name
                run.current_table = table_name
                run.heartbeat_at = datetime.utcnow()
            self.db.commit()
            self.db.refresh(phase)
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

            run = self.db.get(ETLRun, run_id)
            if run:
                run.current_table = table_name
                run.current_file = file_name
                run.heartbeat_at = datetime.utcnow()

            self.db.commit()
            self.db.refresh(item)
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

            run = self.db.get(ETLRun, run_id)
            if run:
                if status in {"SUCCESS", "SKIPPED"}:
                    run.completed_files = (run.completed_files or 0) + 1
                    run.total_records = (run.total_records or 0) + (records_processed or 0)
                elif status == "FAILED":
                    run.failed_files = (run.failed_files or 0) + 1

                done = (run.completed_files or 0) + (run.failed_files or 0)
                if run.total_files:
                    run.progress_percent = round((done / run.total_files) * 100, 2)

                elapsed = (finished_at - run.started_at).total_seconds()
                if done > 0 and run.total_files and done < run.total_files:
                    seconds_per_file = elapsed / done
                    remaining = max(run.total_files - done, 0) * seconds_per_file
                    run.estimated_finish_at = datetime.fromtimestamp(
                        finished_at.timestamp() + remaining
                    )

                run.heartbeat_at = finished_at

            self.db.commit()
        except SQLAlchemyError as exc:
            self.db.rollback()
            logger.warning(f"Falha ao finalizar progresso de arquivo: {exc}")


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
