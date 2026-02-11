# from fastapi import FastAPI, HTTPException, Body, Path, Query
# from pydantic import BaseModel
# from uuid import uuid4
# from celery.result import AsyncResult
# from app.pipeline.shag.chain import run_pipeline_chain
# from app.pipeline.shag.bd import SessionLocal, engine, PipelineOperation, PipelineSegment, Meeting, MeetingMicrophone
# from app.celery_app import celery_app
# from typing import List, Optional
# from datetime import datetime
#
#
# app = FastAPI(title="Audio Pipeline API")
#
#
# # ---------------------- Pydantic Models ----------------------
# class StartPipelineResponse(BaseModel):
#     operation_id: str
#     task_id: str | None
#
#
# class PipelineStatusResponse(BaseModel):
#     operation_id: str
#     task_id: str | None
#     status: str
#     step: str | None
#     progress: int
#     ready: bool
#     successful: bool
#
#
# class PipelineResultResponse(BaseModel):
#     operation_id: str
#     result_url: str | None
#
#
# from enum import Enum
#
# class RoleEnum(str, Enum):
#     judge = "judge"
#     lawyer = "lawyer"
#     defendant = "defendant"
#     witness = "witness"
#
# class MicEnum(int, Enum):
#     mic0 = 0
#     mic1 = 1
#     mic2 = 2
#     mic3 = 3
#     mic4 = 4
#
#
#
# # -------------------
# # Pydantic схемы
# # -------------------
#
# class EndMeetingRequest(BaseModel):
#     name: Optional[str] = None
#
# # поменять если нужно завершать митинг сразу после старта(без передачи параметров, фронт сам все делает)
# #class EndMeetingRequest(BaseModel):
# #    meeting_id: str  # теперь обязательно передаем ID митинга
#
# class Participant(BaseModel):
#     microphone: MicEnum
#     role: RoleEnum
#
# class MeetingStartRequest(BaseModel):
#     name: str
#     participants: List[Participant]
#
#
# class PipelineInfo(BaseModel):
#     operation_id: str
#     task_id: Optional[str]
#     status: str
#     step: Optional[str]
#     progress: int
#     ready: bool
#     successful: bool
#
#
# class MeetingInfo(BaseModel):
#     id: str
#     name: str
#     start_time: datetime
#     end_time: Optional[datetime]
#     status: str
#     participants: List[Participant]
#     pipeline: Optional[PipelineInfo] = None
#
#
# class EndMeetingResponse(BaseModel):
#     meeting: MeetingInfo
#     operation_id: str | None = None
#
# # -------------------
# # FastAPI app
# # -------------------
# app = FastAPI(title="Audio Pipeline API - Meetings")
#
#
# def get_session():
#     session = SessionLocal()
#     try:
#         yield session
#     finally:
#         session.close()
#
#
#
# def meeting_to_info(meeting: Meeting) -> MeetingInfo:
#     return MeetingInfo(
#         id=meeting.id,
#         name=meeting.name,
#         start_time=meeting.start_time,
#         end_time=meeting.end_time,
#         status=meeting.status,
#         participants=[
#             Participant(
#                 microphone=m.mic_number,
#                 role=m.role
#             )
#             for m in meeting.microphones
#         ]
#     )
#
#
# # -------------------
# # POST /meeting/start
# # -------------------
# @app.post("/meeting/start", response_model=MeetingInfo)
# def start_meeting(req: MeetingStartRequest):
#     session = SessionLocal()
#     try:
#         # 1) Проверка: есть ли незавершённый митинг
#         running = session.query(Meeting).filter(Meeting.status == "running").first()
#         if running:
#             # Если попытка запустить митинг с тем же именем, это "повторный старт" — отдаём понятное сообщение
#             if running.name == req.name:
#                 raise HTTPException(status_code=400, detail=f"Митинг '{req.name}' уже запущен (повторный старт). id={running.id}")
#             else:
#                 raise HTTPException(
#                     status_code=400,
#                     detail=f"Нельзя начать новый митинг пока не завершён текущий: '{running.name}' (id={running.id})."
#                 )
#
#         # 2) Имя должно быть уникальным (и среди завершённых тоже нельзя повторять имя)
#         existing_by_name = session.query(Meeting).filter(Meeting.name == req.name).first()
#         if existing_by_name:
#             # требование: нельзя называть митинг именем которое уже есть
#             raise HTTPException(status_code=400, detail=f"Митинг с именем '{req.name}' уже существует (id={existing_by_name.id}). Выберите другое имя.")
#
#         mic_numbers = [p.microphone for p in req.participants]
#
#         # 3️⃣ Один микрофон — один раз
#         if len(mic_numbers) != len(set(mic_numbers)):
#             raise HTTPException(
#                 status_code=400,
#                 detail="Один и тот же микрофон указан несколько раз"
#             )
#
#         # 4️⃣ Проверка допустимых микрофонов
#         ALLOWED_MICROPHONES = {0, 1, 2, 3, 4}
#         for mic in mic_numbers:
#             if mic not in ALLOWED_MICROPHONES:
#                 raise HTTPException(
#                     status_code=400,
#                     detail=f"Недопустимый номер микрофона: {mic}"
#                 )
#
#         # 5️⃣ Проверка ролей
#         ALLOWED_ROLES = {"judge", "lawyer", "defendant", "witness"}
#         for p in req.participants:
#             if p.role not in ALLOWED_ROLES:
#                 raise HTTPException(
#                     status_code=400,
#                     detail=f"Недопустимая роль: {p.role}"
#                 )
#
#
#         # 3) Создаём митинг
#         meeting_id = str(uuid4())
#         now = datetime.utcnow()
#         meeting = Meeting(
#             id=meeting_id,
#             name=req.name,
#             start_time=now,
#             status="running",
#         )
#         session.add(meeting)
#         session.flush()  # чтобы meeting.id был доступен
#
#
#         for p in req.participants:
#             session.add(
#                 MeetingMicrophone(
#                     id=str(uuid4()),
#                     meeting_id=meeting.id,
#                     mic_number=p.microphone,
#                     role=p.role,
#                 )
#             )
#
#         session.commit()
#
#         return meeting_to_info(meeting)
#
#     finally:
#         session.close()
#
#
# # -------------------
# # POST /meeting/end
# # -------------------
# @app.post("/meeting/end", response_model=EndMeetingResponse)
# def end_meeting(req: EndMeetingRequest = Body(...)):
#     session = SessionLocal()
#     try:
#         if not req.name:
#             raise HTTPException(status_code=400, detail="Нужно указать id или name митинга для завершения.")
#
#         meeting = session.query(Meeting).filter(Meeting.name == req.name).first()
#         if not meeting:
#             raise HTTPException(status_code=404, detail="Митинг не найден (id или name неверны).")
#
#         # Если уже завершён — предупреждение (idempotent)
#         if meeting.status == "ended":
#             raise HTTPException(status_code=400, detail=f"Митинг '{meeting.name}' (id={meeting.id}) уже завершён в {meeting.end_time.isoformat()}.")
#
#         meeting.end_time = datetime.utcnow()
#         meeting.status = "ended"
#         session.commit()
#
#         # -----------------------------
#         # Запускаем пайплайн
#         # -----------------------------
#         operation_id = str(uuid4())
#         op = PipelineOperation(
#             operation_id=operation_id,
#             status="PENDING",
#             progress=0
#         )
#         session.add(op)
#         session.commit()
#
#         # Celery chain
#         async_result = run_pipeline_chain(operation_id)
#         op.task_id = async_result.id
#         session.add(op)
#         session.commit()
#
#         return EndMeetingResponse(
#             meeting=meeting_to_info(meeting),
#             operation_id=operation_id
#         )
#
#     finally:
#         session.close()
#
# # поменять если нужно завершать митинг сразу после старта(без передачи параметров, фронт сам все делает)
# # @app.post("/meeting/end", response_model=MeetingInfo)
# # def end_meeting(req: EndMeetingRequest):
# #     session = SessionLocal()
# #     try:
# #         # Ищем митинг по ID
# #         meeting = session.query(Meeting).filter(Meeting.id == req.meeting_id).first()
# #         if not meeting:
# #             raise HTTPException(status_code=404, detail="Митинг не найден")
# #
# #         # Если уже завершён — предупреждение (idempotent)
# #         if meeting.status == "ended":
# #             raise HTTPException(
# #                 status_code=400,
# #                 detail=f"Митинг '{meeting.name}' (id={meeting.id}) уже завершён в {meeting.end_time.isoformat()}"
# #             )
# #
# #         meeting.end_time = datetime.utcnow()
# #         meeting.status = "ended"
# #         session.commit()
# #
# #         return meeting_to_info(meeting)
# #
# #     finally:
# #         session.close()
#
# # -------------------
# # GET /meeting/status
# # -------------------
# @app.get("/meeting/status", response_model=MeetingInfo)
# def meeting_status(id: Optional[str] = Query(None), name: Optional[str] = Query(None)):
#     session = SessionLocal()
#     try:
#         if not id and not name:
#             raise HTTPException(status_code=400, detail="Укажите id или name митинга для получения статуса.")
#
#         if id:
#             meeting = session.query(Meeting).filter(Meeting.id == id).first()
#         else:
#             meeting = session.query(Meeting).filter(Meeting.name == name).first()
#
#         if not meeting:
#             raise HTTPException(status_code=404, detail="Митинг не найден.")
#
#         return meeting_to_info(meeting)
#     finally:
#         session.close()
#
#
# @app.get("/roles", response_model=List[str])
# def get_roles():
#     return [role.value for role in RoleEnum]
#
# @app.get("/microphones", response_model=List[int])
# def get_microphones():
#     return [mic.value for mic in MicEnum]
#
#
# def parse_filename(filename: str):
#     parts = filename.replace(".wav", "").split("_")
#
#     mic_number = int(parts[2])
#     date_part = parts[3]
#     time_part = parts[4]
#
#     dt = datetime.strptime(date_part + time_part, "%d%m%Y%H%M%S")
#
#     return mic_number, dt
#
#
# FAKE_FILES = [
#     "SR1000_2359_1_26012026_172603_121_0.wav",
#     "SR1000_2359_2_26012026_174023_23_0.wav",
#     "SR1000_2359_10_26012026_174807_444_0.wav",
# ]
#
# def fetch_meeting_files(meeting: Meeting):
#     allowed_mics = {m.mic_number for m in meeting.microphones}
#
#     result = []
#
#     for fname in FAKE_FILES:
#         mic, dt = parse_filename(fname)
#
#         if mic not in allowed_mics:
#             continue
#         if dt < meeting.start_time:
#             continue
#         if dt > meeting.end_time:
#             continue
#
#         result.append(fname)
#
#     return result
#
#
# @app.get("/pipeline/status/{operation_id}", response_model=PipelineStatusResponse)
# def pipeline_status(operation_id: str = Path(...)):
#     session = SessionLocal()
#     try:
#         op = session.query(PipelineOperation).filter_by(operation_id=operation_id).first()
#         if not op:
#             raise HTTPException(status_code=404, detail="Operation not found")
#
#         task_id = getattr(op, "task_id", None)
#         result = AsyncResult(task_id, app=celery_app) if task_id else None
#
#         return PipelineStatusResponse(
#             operation_id=operation_id,
#             task_id=task_id,
#             status=op.status,
#             step=op.step,
#             progress=op.progress or 0,
#             ready=result.ready() if result else False,
#             successful=result.successful() if result else False
#         )
#     finally:
#         session.close()
#
#
# def format_time(seconds: float) -> str:
#     hours = int(seconds // 3600)
#     minutes = int((seconds % 3600) // 60)
#     secs = int(seconds % 60)
#     millis = int((seconds - int(seconds)) * 1000)
#     return f"{hours:02d}:{minutes:02d}:{secs:02d}.{millis:03d}"
#
#
# @app.get("/pipeline/segments/{operation_id}")
# def get_segments(
#     operation_id: str,
#     speaker: str | None = None,
#     search: str | None = None,
#     limit: int = 50,
#     offset: int = 0
# ):
#     session = SessionLocal()
#     try:
#         op = session.query(PipelineOperation).filter_by(operation_id=operation_id).first()
#         if not op:
#             raise HTTPException(status_code=404, detail="Operation not found")
#         if op.status != "DONE":
#             raise HTTPException(status_code=400, detail="Operation not ready")
#
#         query = session.query(PipelineSegment).filter_by(operation_id=operation_id)
#
#         if speaker:
#             query = query.filter(PipelineSegment.speaker == speaker)
#         if search:
#             query = query.filter(PipelineSegment.transcription.ilike(f"%{search}%"))
#
#         total = query.count()
#         segments = query.order_by(PipelineSegment.start).offset(offset).limit(limit).all()
#
#         return {
#             "total": total,
#             "segments": [
#                 {
#                     "file_name": seg.file_name,
#                     "start": format_time(seg.start),
#                     "end": format_time(seg.end),
#                     "id_speaker": seg.id_speaker,
#                     "speaker": seg.speaker,
#                     "transcription": seg.transcription,
#                 } for seg in segments
#             ]
#         }
#     finally:
#         session.close()







from fastapi import FastAPI, HTTPException, Body, Path, Query
from pydantic import BaseModel
from uuid import uuid4
from celery.result import AsyncResult
from datetime import datetime, timezone
from typing import List, Optional
from enum import Enum

# ----------------------
# Импорты из проекта
# ----------------------
from app.pipeline.shag.chain_v import run_pipeline_chain
from app.pipeline.shag.bd import SessionLocal, engine, PipelineOperation, PipelineSegment, Meeting, MeetingMicrophone
from app.celery_app import celery_app
from app.pipeline.utils import get_unique_result_path
from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO

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
    operation_id: str | None = None


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
# Utility functions
# ----------------------
def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


# def meeting_to_info(meeting: Meeting) -> MeetingInfo:
#     return MeetingInfo(
#         id=meeting.id,
#         name=meeting.name,
#         start_time=meeting.start_time,
#         end_time=meeting.end_time,
#         status=meeting.status,
#         participants=[
#             Participant(microphone=m.mic_number, role=m.role)
#             for m in meeting.microphones
#         ],
#     )


def meeting_to_info(meeting: Meeting, session) -> MeetingInfo:
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
def start_meeting(req: MeetingStartRequest):
    session = SessionLocal()
    try:
        # Проверка текущего running митинга
        running = session.query(Meeting).filter(Meeting.status == "running").first()
        if running:
            detail = (
                f"Митинг '{req.name}' уже запущен (повторный старт). id={running.id}"
                if running.name == req.name
                else f"Нельзя начать новый митинг пока не завершён текущий: '{running.name}' (id={running.id})."
            )
            raise HTTPException(status_code=400, detail=detail)

        # Проверка уникальности имени
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

        # Создание митинга
        meeting_id = str(uuid4())
        now = datetime.now(timezone.utc)
        meeting = Meeting(id=meeting_id, name=req.name, start_time=now, status="running")
        session.add(meeting)
        session.flush()

        for p in req.participants:
            session.add(
                MeetingMicrophone(id=str(uuid4()), meeting_id=meeting.id, mic_number=p.microphone, role=p.role)
            )
        session.commit()
        return meeting_to_info(meeting, session)
    finally:
        session.close()


@app.post("/meeting/end", response_model=EndMeetingResponse)
def end_meeting(req: EndMeetingRequest = Body(...)):
    session = SessionLocal()
    try:
        if not req.name:
            raise HTTPException(status_code=400, detail="Нужно указать id или name митинга для завершения.")

        meeting = session.query(Meeting).filter(Meeting.name == req.name).first()
        if not meeting:
            raise HTTPException(status_code=404, detail="Митинг не найден (id или name неверны).")

        if meeting.status == "ended":
            raise HTTPException(status_code=400, detail=f"Митинг '{meeting.name}' уже завершён.")

        # Завершение митинга
        meeting.end_time = datetime.now(timezone.utc)
        meeting.status = "ended"
        session.commit()


        # Запуск пайплайна
        operation_id = str(uuid4())
        #operation_id = str('1f4472ec-4a94-4a5a-bdde-ddd986bb5d90')
        op = PipelineOperation(operation_id=operation_id, status="PENDING", progress=0)
        session.add(op)
        meeting.pipeline_id = operation_id
        session.commit()

        async_result = run_pipeline_chain(operation_id)
        op.task_id = async_result.id
        session.add(op)
        session.commit()

        return EndMeetingResponse(meeting=meeting_to_info(meeting, session), operation_id=operation_id)
    finally:
        session.close()


# @app.get("/meeting/status", response_model=MeetingInfo)
# def meeting_status(id: Optional[str] = Query(None), name: Optional[str] = Query(None)):
#     session = SessionLocal()
#     try:
#         if not id and not name:
#             raise HTTPException(status_code=400, detail="Укажите id или name митинга.")
#         meeting = session.query(Meeting).filter(Meeting.id == id if id else Meeting.name == name).first()
#         if not meeting:
#             raise HTTPException(status_code=404, detail="Митинг не найден.")
#         return meeting_to_info(meeting)
#     finally:
#         session.close()
#
#
# @app.get("/meetings", response_model=List[MeetingInfo])
# def list_meetings():
#     session = SessionLocal()
#     try:
#         meetings = session.query(Meeting).order_by(Meeting.start_time.desc()).all()
#         return [meeting_to_info(m) for m in meetings]
#     finally:
#         session.close()




@app.get("/meeting/status", response_model=MeetingInfo)
def meeting_status(id: Optional[str] = Query(None), name: Optional[str] = Query(None)):
    session = SessionLocal()
    try:
        if not id and not name:
            raise HTTPException(status_code=400, detail="Укажите id или name митинга.")

        meeting = session.query(Meeting).filter(
            Meeting.id == id if id else Meeting.name == name
        ).first()

        if not meeting:
            raise HTTPException(status_code=404, detail="Митинг не найден.")

        return meeting_to_info(meeting, session)
    finally:
        session.close()


@app.get("/meetings", response_model=List[MeetingInfo])
def list_meetings():
    session = SessionLocal()
    try:
        meetings = session.query(Meeting).order_by(Meeting.start_time.desc()).all()
        return [meeting_to_info(m, session) for m in meetings]
    finally:
        session.close()



@app.delete("/meeting/{meeting_id}", response_model=dict)
def delete_meeting(meeting_id: str):
    session = SessionLocal()
    try:
        meeting = session.query(Meeting).filter(Meeting.id == meeting_id).first()
        if not meeting:
            raise HTTPException(status_code=404, detail="Митинг не найден.")

        # Проверка пайплайна
        op = session.query(PipelineOperation).filter_by(operation_id=meeting.pipeline_id if hasattr(meeting, "pipeline_id") else None).first()
        if op:
            # Пайплайн должен быть завершен (успешно или с ошибкой)
            if op.status not in ("DONE", "FAILED"):
                raise HTTPException(
                    status_code=400,
                    detail=f"Нельзя удалить митинг: пайплайн {op.operation_id} еще выполняется (status={op.status})."
                )

        # Удаляем микрофоны
        session.query(MeetingMicrophone).filter_by(meeting_id=meeting.id).delete()
        # Удаляем митинг
        session.delete(meeting)
        session.commit()

        return {"detail": f"Митинг '{meeting.name}' успешно удален."}
    finally:
        session.close()



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
def pipeline_status(operation_id: str = Path(...)):
    session = SessionLocal()
    try:
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
    finally:
        session.close()


# @app.get("/pipeline/segments/{operation_id}")
# def get_segments(
#     operation_id: str,
#     speaker: Optional[str] = None,
#     search: Optional[str] = None,
#     limit: int = 50,
#     offset: int = 0
# ):
#     session = SessionLocal()
#     try:
#         op = session.query(PipelineOperation).filter_by(operation_id=operation_id).first()
#         if not op:
#             raise HTTPException(status_code=404, detail="Operation not found")
#         if op.status != "DONE":
#             raise HTTPException(status_code=400, detail="Operation not ready")
#
#         query = session.query(PipelineSegment).filter_by(operation_id=operation_id)
#         if speaker:
#             query = query.filter(PipelineSegment.speaker == speaker)
#         if search:
#             query = query.filter(PipelineSegment.transcription.ilike(f"%{search}%"))
#
#         total = query.count()
#         segments = query.order_by(PipelineSegment.start).offset(offset).limit(limit).all()
#
#         return {
#             "total": total,
#             "segments": [
#                 {
#                     "file_name": seg.file_name,
#                     "start": format_time(seg.start),
#                     "end": format_time(seg.end),
#                     "id_speaker": seg.id_speaker,
#                     "speaker": seg.speaker,
#                     "transcription": seg.transcription,
#                 }
#                 for seg in segments
#             ],
#         }
#     finally:
#         session.close()


from fastapi.responses import FileResponse
from fastapi import HTTPException


@app.get("/pipeline/docx/{operation_id}")
def download_docx(operation_id: str):
    # путь к файлу DOCX
    docx_path = (
            get_unique_result_path(TMP_PATH, operation_id)
            / "final_results"
            / "pipeline_result.docx"
    )

    if not docx_path.exists():
        raise HTTPException(status_code=404, detail="DOCX not found")

    return FileResponse(
        path=docx_path,
        filename=f"{operation_id}.docx",  # имя файла при скачивании
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    )



from fastapi import Query

@app.get("/pipeline/segments/{operation_id}")
def get_segments(
    operation_id: str,
    speaker: Optional[str] = None,
    search: Optional[str] = None,
    start_sec: Optional[float] = Query(None, description="Фильтр по минимальному времени начала сегмента"),
    end_sec: Optional[float] = Query(None, description="Фильтр по максимальному времени конца сегмента"),
    limit: int = 50,
    offset: int = 0
):
    session = SessionLocal()
    try:
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
            query = query.filter(PipelineSegment.start >= start_sec)
        if end_sec is not None:
            query = query.filter(PipelineSegment.end <= end_sec)

        total = query.count()
        segments = query.order_by(PipelineSegment.start).offset(offset).limit(limit).all()

        # Путь к DOCX
        tmp = get_unique_result_path(TMP_PATH, operation_id)
        docx_path = tmp / "final_results" / "pipeline_result.docx"
        docx_url = None
        if docx_path.exists():
            docx_url = f"http://localhost:8000/pipeline/docx/{operation_id}"  # можно отдавать URL для скачивания

        return {
            "total": total,
            "segments": [
                {
                    "file_name": seg.file_name,
                    "start": format_time(seg.start),
                    "end": format_time(seg.end),
                    "id_speaker": seg.id_speaker,
                    "speaker": seg.speaker,
                    "transcription": seg.transcription,
                }
                for seg in segments
            ],
            "docx_url": docx_url,  # добавляем ссылку на скачивание DOCX
        }
    finally:
        session.close()
