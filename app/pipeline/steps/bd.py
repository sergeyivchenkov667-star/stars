from sqlalchemy import (
    Column,
    String,
    Integer,
    DateTime,
    JSON,
    Index,
    text,
    ForeignKey,
    Numeric,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.orm import relationship, sessionmaker, declarative_base
from sqlalchemy.sql import func
from pydantic_settings import BaseSettings



class Settings(BaseSettings):
    database_url_sync: str

    class Config:
        env_file = ".env"


# ============================================================
# Database
# ============================================================

DATABASE_URL = "postgresql+psycopg2://postgres:password@localhost:5432/mydb"

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
    connect_args={"options": "-c search_path=public"},
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

Base = declarative_base()

# ============================================================
# Pipeline
# ============================================================


class PipelineOperation(Base):
    __tablename__ = "pipeline_operations"

    id = Column(Integer, primary_key=True)
    operation_id = Column(String, unique=True, index=True, nullable=False)

    status = Column(String, nullable=False, default="PENDING")
    step = Column(String, nullable=True)
    progress = Column(Integer, nullable=False, default=0)

    result_json_s3_key = Column(String, nullable=True)
    result_docx_s3_key = Column(String, nullable=True)

    data = Column(JSON, nullable=True)
    task_id = Column(String, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    segments = relationship(
        "PipelineSegment",
        back_populates="operation",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    temp_data = relationship(
        "PipelineTempData",
        back_populates="operation",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class PipelineTempData(Base):
    __tablename__ = "pipeline_temp_data"

    id = Column(Integer, primary_key=True)

    operation_id = Column(
        String,
        ForeignKey("pipeline_operations.operation_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    step = Column(String, nullable=False)
    data = Column(JSON, nullable=True)

    status = Column(String, nullable=False, default="PENDING")

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    operation = relationship("PipelineOperation", back_populates="temp_data")


class PipelineSegment(Base):
    __tablename__ = "pipeline_segments"

    id = Column(Integer, primary_key=True)

    operation_id = Column(
        String,
        ForeignKey("pipeline_operations.operation_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    start = Column(Numeric(10, 3), nullable=False)
    end = Column(Numeric(10, 3), nullable=False)

    id_speaker = Column(Integer, nullable=False)
    speaker = Column(String, nullable=False)

    transcription = Column(String, nullable=False)
    file_name = Column(String, nullable=True)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    operation = relationship("PipelineOperation", back_populates="segments")


# ============================================================
# Meetings
# ============================================================


class Meeting(Base):
    __tablename__ = "meetings"


    id = Column(String, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    start_time = Column(DateTime, nullable=False)
    end_time = Column(DateTime, nullable=True)

    status = Column(String, nullable=False)

    pipeline_id = Column(
        String,
        ForeignKey("pipeline_operations.operation_id"),
        nullable=True,
    )

    pipeline = relationship("PipelineOperation")
    microphones = relationship(
        "MeetingMicrophone",
        back_populates="meeting",
        cascade="all, delete-orphan",
    )


class MeetingMicrophone(Base):
    __tablename__ = "meeting_microphones"
    __table_args__ = (
        UniqueConstraint(
            "meeting_id",
            "mic_number",
            name="unique_mic_per_meeting",
        ),
    )

    id = Column(String, primary_key=True)

    meeting_id = Column(
        String,
        ForeignKey("meetings.id", ondelete="CASCADE"),
        nullable=False,
    )

    mic_number = Column(Integer, nullable=False)
    role = Column(String, nullable=False)

    meeting = relationship("Meeting", back_populates="microphones")


# ============================================================
# Create tables
# ============================================================

# В продакшене НЕ использовать!
# Использовать Alembic.




