from sqlalchemy import Column, String, Integer, DateTime, JSON
from sqlalchemy.sql import func
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import Numeric
from sqlalchemy.ext.declarative import declarative_base

DATABASE_URL = "postgresql+psycopg2://postgres:password@localhost:5432/mydb"


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    connect_args={"options": "-c search_path=public"}
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


Base = declarative_base()


# class PipelineStatus(str, Enum):
#     PENDING = "PENDING"
#     RUNNING = "RUNNING"
#     FAILED = "FAILED"
#     DONE = "DONE"
#
#
# class PipelineStep(IntEnum):
#     MERGE_AUDIO = 0
#     DIARIZATION = 1
#     MERGE_INTERVALS = 2
#     VAD_HUNGARIAN = 3
#     EXTRACT_SEGMENTS = 4
#     TRANSCRIPTION = 5
#     EXPORT = 6


class PipelineTempData(Base):
    __tablename__ = "pipeline_temp_data"

    id = Column(Integer, primary_key=True)
    operation_id = Column(String, ForeignKey("pipeline_operations.operation_id"), nullable=False, index=True)
    step = Column(String, nullable=False)  # MERGE_AUDIO, DIARIZATION и т.д.
    data = Column(JSON, nullable=True)     # хранит промежуточный результат

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    operation = relationship("PipelineOperation")


class PipelineOperation(Base):
    __tablename__ = "pipeline_operations"

    id = Column(Integer, primary_key=True, index=True)
    operation_id = Column(String, unique=True, index=True, nullable=False)
    status = Column(String, default="PENDING")  # PENDING, RUNNING, DONE, FAILED
    step = Column(String, nullable=True)         # текущий шаг пайплайна
    progress = Column(Integer, default=0)        # 0-100
    result_url = Column(String, nullable=True)   # presigned URL JSON
    data = Column(JSON, nullable=True)           # дополнительная информация
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    task_id = Column(String, nullable=True)


class PipelineSegment(Base):
    __tablename__ = "pipeline_segments"

    id = Column(Integer, primary_key=True, index=True)
    operation_id = Column(String, ForeignKey("pipeline_operations.operation_id"), nullable=False)
    start = Column(Numeric(10, 3))  # миллисекунды
    end = Column(Numeric(10, 3))
    id_speaker = Column(Integer, nullable=False)
    speaker = Column(String, nullable=False)
    transcription = Column(String, nullable=False)
    file_name = Column(String, nullable=True)  # путь к wav/mp3, если нужно
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    operation = relationship("PipelineOperation", back_populates="segments")


PipelineOperation.segments = relationship("PipelineSegment", back_populates="operation", cascade="all, delete-orphan")



# -------------------
# Models
# -------------------
class Meeting(Base):
    __tablename__ = "meetings"
    __table_args__ = {"schema": "public"}

    id = Column(String, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime)
    status = Column(String, nullable=False)  # running | ended

    microphones = relationship("MeetingMicrophone", back_populates="meeting")

    pipeline_id = Column(String, ForeignKey("pipeline_operations.operation_id"), nullable=True)
    pipeline = relationship("PipelineOperation")


class MeetingMicrophone(Base):
    __tablename__ = "meeting_microphones"
    __table_args__ = {"schema": "public"}

    id = Column(String, primary_key=True)
    meeting_id = Column(String, ForeignKey("public.meetings.id"))
    mic_number = Column(Integer, nullable=False)
    role = Column(String, nullable=False)

    meeting = relationship("Meeting", back_populates="microphones")



Base.metadata.create_all(bind=engine)



