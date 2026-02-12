import os
import shutil
import logging
import uuid
from pathlib import Path
from datetime import datetime
from app.pipeline.steps.bd import SessionLocal, PipelineOperation, PipelineTempData
from sqlalchemy.orm import Session
from app.pipeline.config import TMP_PATH


# ---------------- Logging ----------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler()]
)

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


def update_temp_data(db: Session, operation_id: str, step: str, data: dict = None, status: str = None):
    temp = db.query(PipelineTempData).filter_by(operation_id=operation_id, step=step).first()
    if not temp:
        temp = PipelineTempData(operation_id=operation_id, step=step, data=data or {}, status=status or "PENDING")
        db.add(temp)
    else:
        if data is not None:
            temp.data = data
        if status is not None:
            temp.status = status
    db.commit()
    return temp


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


# ------------------------- Cleanup -------------------------
def cleanup_temp_if_done(db: Session, operation_id: str):
    """
    Проверяет, все ли шаги пайплайна для operation_id DONE,
    и если да — удаляет временные файлы.
    """
    steps = db.query(PipelineTempData).filter_by(operation_id=operation_id).all()
    if not steps:
        return

    if all(step.status == "DONE" for step in steps):
        tmp_path = Path(TMP_PATH) / operation_id
        if tmp_path.exists():
            shutil.rmtree(tmp_path)
            logger.info(f"Temporary files for operation {operation_id} removed")