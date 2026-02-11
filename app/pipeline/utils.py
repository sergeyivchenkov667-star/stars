import os
import uuid
from pathlib import Path


def get_speaker_name(path: str) -> str:
    """Имя файла без пути и расширения."""
    return os.path.splitext(os.path.basename(path))[0]


def generate_unique_operation_id() -> str:
    """Генерация уникального идентификатора операции."""
    return str(uuid.uuid4())


def get_unique_result_path(base_path: Path, operation_id: str) -> Path:
    """
    Создаёт уникальную директорию для результатов одной операции.
    """
    result_dir = base_path / operation_id
    result_dir.mkdir(parents=True, exist_ok=True)
    return result_dir


# app/utils/db_helpers.py
from datetime import datetime
from app.pipeline.shag.bd import SessionLocal, PipelineOperation, PipelineTempData
from sqlalchemy.orm import Session


def mark_done(operation_id: str, result_url: str, extra_data: dict | None = None):
    """
    Пометить операцию как DONE и сохранить presigned URL
    """
    with SessionLocal() as db:
        db.query(PipelineOperation)\
            .filter_by(operation_id=operation_id)\
            .update({
                "status": "DONE",
                "progress": 100,
                "step": "Finished",
                "result_url": result_url,
                "data": extra_data,
                "updated_at": datetime.utcnow()
            })
        db.commit()


def mark_failed(operation_id: str, extra_data: dict | None = None):
    """
    Пометить операцию как FAILED
    """
    with SessionLocal() as db:
        db.query(PipelineOperation)\
            .filter_by(operation_id=operation_id)\
            .update({
                "status": "FAILED",
                "step": "Failed",
                "data": extra_data,
                "updated_at": datetime.utcnow()
            })
        db.commit()


def update_temp_data(db: Session, operation_id: str, step: str, data: dict):
    temp = db.query(PipelineTempData).filter_by(operation_id=operation_id, step=step).first()
    if not temp:
        temp = PipelineTempData(operation_id=operation_id, step=step, data=data)
        db.add(temp)
    else:
        temp.data = data
    db.commit()


def get_temp_data(db: Session, operation_id: str, step: str):
    temp = db.query(PipelineTempData).filter_by(operation_id=operation_id, step=step).first()
    if temp is None or temp.data is None:
        raise ValueError(f"No temp data for operation {operation_id} at step {step}")
    return temp.data


def get_pipeline_operation(db: Session, operation_id: str) -> PipelineOperation:
    return db.query(PipelineOperation).filter_by(operation_id=operation_id).first()


def set_step_status(db: Session, operation_id: str, step: str, status: str, progress: int = None):
    op = get_pipeline_operation(db, operation_id)
    if op:
        op.step = step
        op.status = status
        if progress is not None:
            op.progress = progress
        db.commit()

def update_progress(operation_id: str, step: str, progress: int):
    """
    Обновление прогресса пайплайна в БД
    """
    with SessionLocal() as db:
        db.query(PipelineOperation)\
            .filter_by(operation_id=operation_id)\
            .update({
                "step": step,
                "progress": progress,
                "updated_at": datetime.utcnow()
            })
        db.commit()