# ----------------------
# Импорты
# ----------------------
from datetime import datetime, timezone
from enum import Enum
from typing import List, Optional
from uuid import uuid4

from celery.result import AsyncResult
from fastapi import FastAPI, HTTPException, Body, Path, Query, Depends
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.celery_app import celery_app
from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO
from app.pipeline.steps.bd import SessionLocal, PipelineOperation, PipelineSegment, Meeting, MeetingMicrophone
from app.pipeline.steps.pipeline_workflow import run_pipeline_chain
from app.pipeline.utils import get_unique_result_path
from app.storage.s3 import generate_presigned_url

# ----------------------
# FastAPI instance
# ----------------------
app = FastAPI(title="Audio Pipeline API - Meetings")

# ----------------------
# Enums
# ----------------------
class RoleEnum(str, Enum):
    judge = "judge"
    lawyer = "lawyer"
    defendant = "defendant"
    witness = "witness"

class MicEnum(int, Enum):
    mic0 = 0
    mic1 = 1
    mic2 = 2
    mic3 = 3
    mic4 = 4

# ----------------------
# Pydantic Models
# ----------------------
class Participant(BaseModel):
    microphone: MicEnum
    role: RoleEnum

class RenameMeetingRequest(BaseModel):
    new_name: str

class MeetingStartRequest(BaseModel):
    name: str
    participants: List[Participant]

class EndMeetingRequest(BaseModel):
    name: Optional[str] = None

class PipelineInfo(BaseModel):
    operation_id: str
    task_id: Optional[str]
    status: str
    step: Optional[str]
    progress: int
    ready: bool
    successful: bool

class MeetingInfo(BaseModel):
    id: str
    name: str
    start_time: datetime
    end_time: Optional[datetime]
    status: str
    participants: List[Participant]
    pipeline: Optional[PipelineInfo] = None

class EndMeetingResponse(BaseModel):
    meeting: MeetingInfo
    operation_id: Optional[str] = None

class StartPipelineResponse(BaseModel):
    operation_id: str
    task_id: Optional[str]

class PipelineStatusResponse(BaseModel):
    operation_id: str
    task_id: Optional[str]
    status: str
    step: Optional[str]
    progress: int
    ready: bool
    successful: bool

class PipelineResultResponse(BaseModel):
    operation_id: str
    result_url: Optional[str]

# ----------------------
# Dependency
# ----------------------
def get_session() -> Session:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

# ----------------------
# Utility Functions
# ----------------------
def meeting_to_info(meeting: Meeting, session: Session) -> MeetingInfo:
    pipeline_info = None
    if meeting.pipeline_id:
        pipeline_op = session.query(PipelineOperation).filter_by(
            operation_id=meeting.pipeline_id
        ).first()
        if pipeline_op:
            task_id = pipeline_op.task_id
            result = AsyncResult(task_id, app=celery_app) if task_id else None

            pipeline_info = PipelineInfo(
                operation_id=pipeline_op.operation_id,
                task_id=task_id,
                status=pipeline_op.status,
                step=pipeline_op.step,
                progress=pipeline_op.progress or 0,
                ready=result.ready() if result else False,
                successful=result.successful() if result else False,
            )

    return MeetingInfo(
        id=meeting.id,
        name=meeting.name,
        start_time=meeting.start_time,
        end_time=meeting.end_time,
        status=meeting.status,
        participants=[
            Participant(microphone=m.mic_number, role=m.role)
            for m in meeting.microphones
        ],
        pipeline=pipeline_info
    )

def format_time(seconds: float) -> str:
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)
    millis = int((seconds - int(seconds)) * 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"

def parse_filename(filename: str):
    parts = filename.replace(".wav", "").split("_")
    mic_number = int(parts[2])
    dt = datetime.strptime(parts[3] + parts[4], "%d%m%Y%H%M%S")
    return mic_number, dt

FAKE_FILES = [
    "SR1000_2359_1_26012026_172603_121_0.wav",
    "SR1000_2359_2_26012026_174023_23_0.wav",
    "SR1000_2359_10_26012026_174807_444_0.wav",
]

def fetch_meeting_files(meeting: Meeting):
    allowed_mics = {m.mic_number for m in meeting.microphones}
    result = []

    for fname in FAKE_FILES:
        mic, dt = parse_filename(fname)
        if mic not in allowed_mics or dt < meeting.start_time or dt > meeting.end_time:
            continue
        result.append(fname)
    return result

# ----------------------
# Meetings Endpoints
# ----------------------
@app.post("/meeting/start", response_model=MeetingInfo)
def start_meeting(req: MeetingStartRequest, session: Session = Depends(get_session)):
    running = session.query(Meeting).filter(Meeting.status == "running").first()
    if running:
        detail = (
            f"Митинг '{req.name}' уже запущен (id={running.id})"
            if running.name == req.name
            else f"Нельзя начать новый митинг пока не завершён текущий: '{running.name}' (id={running.id})"
        )
        raise HTTPException(status_code=400, detail=detail)

    existing = session.query(Meeting).filter(Meeting.name == req.name).first()
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Митинг с именем '{req.name}' уже существует (id={existing.id}). Выберите другое имя."
        )

    mic_numbers = [p.microphone for p in req.participants]
    if len(mic_numbers) != len(set(mic_numbers)):
        raise HTTPException(status_code=400, detail="Один и тот же микрофон указан несколько раз")

    ALLOWED_MICROPHONES = {0, 1, 2, 3, 4}
    for mic in mic_numbers:
        if mic not in ALLOWED_MICROPHONES:
            raise HTTPException(status_code=400, detail=f"Недопустимый номер микрофона: {mic}")

    ALLOWED_ROLES = {"judge", "lawyer", "defendant", "witness"}
    for p in req.participants:
        if p.role not in ALLOWED_ROLES:
            raise HTTPException(status_code=400, detail=f"Недопустимая роль: {p.role}")

    meeting_id = str(uuid4())
    now = datetime.now(timezone.utc)
    meeting = Meeting(id=meeting_id, name=req.name, start_time=now, status="running")
    session.add(meeting)
    session.flush()

    for p in req.participants:
        session.add(MeetingMicrophone(
            id=str(uuid4()), meeting_id=meeting.id, mic_number=p.microphone, role=p.role
        ))
    session.commit()
    return meeting_to_info(meeting, session)

@app.post("/meeting/{meeting_id}/end", response_model=EndMeetingResponse)
def end_meeting(meeting_id: str, session: Session = Depends(get_session)):
    meeting = session.query(Meeting).filter(Meeting.id == meeting_id).with_for_update().first()
    if not meeting:
        raise HTTPException(status_code=404, detail="Митинг не найден.")

    if meeting.status == "ended":
        raise HTTPException(status_code=400, detail=f"Митинг '{meeting.name}' уже завершён.")

    operation_id = str(uuid4())
    meeting.end_time = datetime.now(timezone.utc)
    meeting.status = "ended"
    meeting.pipeline_id = operation_id

    op = PipelineOperation(operation_id=operation_id, status="PENDING", progress=0)
    session.add(op)
    session.commit()

    try:
        async_result = run_pipeline_chain(operation_id)
        op.task_id = async_result.id
        session.commit()
    except Exception:
        session.rollback()
        meeting.status = "running"
        meeting.end_time = None
        meeting.pipeline_id = None
        session.delete(op)
        session.commit()
        raise HTTPException(status_code=500, detail="Failed to start pipeline")

    return EndMeetingResponse(meeting=meeting_to_info(meeting, session), operation_id=operation_id)

@app.get("/meeting/{meeting_id}/status", response_model=MeetingInfo)
def meeting_status(meeting_id: str, session: Session = Depends(get_session)):
    meeting = session.query(Meeting).filter(Meeting.id == meeting_id).first()
    if not meeting:
        raise HTTPException(status_code=404, detail="Митинг не найден.")
    return meeting_to_info(meeting, session)

@app.get("/meetings", response_model=List[MeetingInfo])
def list_meetings(limit: int = Query(50, le=200), session: Session = Depends(get_session)):
    meetings = session.query(Meeting).order_by(Meeting.start_time.desc()).limit(limit).all()
    return [meeting_to_info(m, session) for m in meetings]

@app.patch("/meeting/{meeting_id}/rename", response_model=MeetingInfo)
def rename_meeting(meeting_id: str, req: RenameMeetingRequest, session: Session = Depends(get_session)):
    meeting = session.query(Meeting).filter(Meeting.id == meeting_id).first()
    if not meeting:
        raise HTTPException(status_code=404, detail="Митинг не найден.")

    if not req.new_name.strip():
        raise HTTPException(status_code=400, detail="Новое имя не может быть пустым.")

    if meeting.name == req.new_name:
        raise HTTPException(status_code=400, detail="Новое имя совпадает с текущим.")

    existing = session.query(Meeting).filter(Meeting.name == req.new_name).first()
    if existing:
        raise HTTPException(status_code=400, detail=f"Митинг с именем '{req.new_name}' уже существует.")

    if meeting.pipeline_id:
        op = session.query(PipelineOperation).filter_by(operation_id=meeting.pipeline_id).first()
        if op and op.status not in ("DONE", "FAILED"):
            raise HTTPException(
                status_code=400,
                detail=f"Нельзя переименовать митинг: пайплайн {op.operation_id} ещё выполняется (status={op.status})."
            )

    meeting.name = req.new_name
    session.commit()
    return meeting_to_info(meeting, session)

@app.delete("/meeting/{meeting_id}", response_model=dict)
def delete_meeting(meeting_id: str, session: Session = Depends(get_session)):
    meeting = session.query(Meeting).filter(Meeting.id == meeting_id).with_for_update().first()
    if not meeting:
        raise HTTPException(status_code=404, detail="Митинг не найден.")

    op = session.query(PipelineOperation).filter_by(operation_id=meeting.pipeline_id).first()
    if op and op.status not in ("DONE", "FAILED"):
        raise HTTPException(
            status_code=400,
            detail=f"Нельзя удалить митинг: пайплайн {op.operation_id} ещё выполняется (status={op.status})."
        )

    session.query(MeetingMicrophone).filter_by(meeting_id=meeting.id).delete()
    session.delete(meeting)
    session.commit()
    return {"detail": f"Митинг '{meeting.name}' успешно удален."}

@app.get("/roles", response_model=List[str])
def get_roles():
    return [role.value for role in RoleEnum]

@app.get("/microphones", response_model=List[int])
def get_microphones():
    return [mic.value for mic in MicEnum]

# ----------------------
# Pipeline Endpoints
# ----------------------
@app.get("/pipeline/status/{operation_id}", response_model=PipelineStatusResponse)
def pipeline_status(operation_id: str = Path(...), session: Session = Depends(get_session)):
    op = session.query(PipelineOperation).filter_by(operation_id=operation_id).first()
    if not op:
        raise HTTPException(status_code=404, detail="Operation not found")

    task_id = getattr(op, "task_id", None)
    result = AsyncResult(task_id, app=celery_app) if task_id else None

    return PipelineStatusResponse(
        operation_id=operation_id,
        task_id=task_id,
        status=op.status,
        step=op.step,
        progress=op.progress or 0,
        ready=result.ready() if result else False,
        successful=result.successful() if result else False,
    )

@app.get("/pipeline/segments/{operation_id}")
def get_segments(
    operation_id: str,
    speaker: Optional[str] = None,
    search: Optional[str] = None,
    start_sec: Optional[float] = Query(None, description="Фильтр по минимальному времени начала сегмента"),
    end_sec: Optional[float] = Query(None, description="Фильтр по максимальному времени конца сегмента"),
    limit: int = Query(50, le=200),
    offset: int = 0,
    session: Session = Depends(get_session),
):
    EPS = 0.001  # 1 мс
    op = session.query(PipelineOperation).filter_by(operation_id=operation_id).first()
    if not op:
        raise HTTPException(status_code=404, detail="Operation not found")
    if op.status != "DONE":
        raise HTTPException(status_code=400, detail="Operation not ready")

    query = session.query(PipelineSegment).filter_by(operation_id=operation_id)
    if speaker:
        query = query.filter(PipelineSegment.speaker == speaker)
    if search:
        query = query.filter(PipelineSegment.transcription.ilike(f"%{search}%"))
    if start_sec is not None:
        query = query.filter(PipelineSegment.start >= start_sec - EPS)
    if end_sec is not None:
        query = query.filter(PipelineSegment.end <= end_sec + EPS)

    total = query.count()
    segments = query.order_by(PipelineSegment.start).offset(offset).limit(limit).all()

    presigned_url = generate_presigned_url(op.result_docx_s3_key, expires_in=3600)

    return {
        "total": total,
        "segments": [
            {
                "file_name": generate_presigned_url(seg.file_name, expires_in=3600) if seg.file_name else None,
                "start": format_time(seg.start),
                "end": format_time(seg.end),
                "id_speaker": seg.id_speaker,
                "speaker": seg.speaker,
                "transcription": seg.transcription,
            }
            for seg in segments
        ],
        "docx_url": presigned_url,
    }
