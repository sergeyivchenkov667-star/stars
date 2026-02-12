# import time
# import logging
# import json
# from pathlib import Path
# from app.celery_app import celery_app
# from app.pipeline.steps.Merge_audio import run_merge_audio_step
# from app.pipeline.steps.diarization_step import diarization_step
# from app.pipeline.steps.merge_intervals import merge_intervals_step
# from app.pipeline.steps.vad_hungarian import vad_hungarian_step
# from app.pipeline.steps.prepare_audio_segments import prepare_audio_segments
# from app.pipeline.steps.transcribe_yandex import transcribe_step_yandex
# from app.pipeline.steps.export_pipeline_results import export_pipeline_results
# from app.pipeline.utils import get_unique_result_path
# from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO
# from pyannote.database.util import load_rttm
# from app.pipeline.utils import update_progress, mark_done, mark_failed, update_temp_data, get_temp_data, set_step_status
# from app.pipeline.steps.bd import SessionLocal, PipelineOperation, PipelineTempData
# from sqlalchemy.orm import Session
# from functools import wraps
# from requests.exceptions import Timeout, ConnectionError
# from celery.exceptions import SoftTimeLimitExceeded
#
# logger = logging.getLogger(__name__)
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s | %(levelname)s | %(message)s',
#     handlers=[logging.StreamHandler()]
# )
#
# def deserialize(obj):
#     if isinstance(obj, dict):
#         return {k: deserialize(v) for k, v in obj.items()}
#     if isinstance(obj, list):
#         return [deserialize(v) for v in obj]
#     if isinstance(obj, str) and (obj.endswith(".wav") or "/" in obj):
#         return Path(obj)
#     return obj
#
#
# def serialize(obj):
#     if isinstance(obj, Path):
#         return str(obj)
#     if isinstance(obj, dict):
#         return {k: serialize(v) for k, v in obj.items()}
#     if isinstance(obj, list):
#         return [serialize(v) for v in obj]
#     return obj
#
#
# def with_db(func):
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         db = SessionLocal()
#         try:
#             return func(db, *args, **kwargs)
#         finally:
#             db.close()
#     return wrapper
#
# # -------------------------
# # Merge Audio
# # -------------------------
# @celery_app.task(bind=True, acks_late=True, queue="cpu")
# @with_db
# def merge_audio_task(db: Session, self, operation_id: str):
#     try:
#         set_step_status(db, operation_id, "MERGE_AUDIO", "RUNNING", progress=5)
#         merged_audio_path = run_merge_audio_step(operation_id)
#         update_temp_data(db, operation_id, "MERGE_AUDIO", {"merged_audio_path": str(merged_audio_path)})
#         set_step_status(db, operation_id, "MERGE_AUDIO", "DONE", progress=10)
#     except Exception as exc:
#         set_step_status(db, operation_id, "MERGE_AUDIO", "FAILED")
#         raise
#
#
# # -------------------------
# # Diarization
# # -------------------------
# @celery_app.task(bind=True, acks_late=True, queue="gpu")
# @with_db
# def diarization_task(db: Session, self, operation_id: str):
#     try:
#         set_step_status(db, operation_id, "DIARIZATION", "RUNNING", progress=15)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#         merge_data = get_temp_data(db, operation_id, "MERGE_AUDIO")
#         merged_audio_path = Path(merge_data["merged_audio_path"])
#
#         diarization_result = diarization_step(
#             input_audio=merged_audio_path,
#             speakers_folder=PATH_TO_AUDIO,
#             output_dir=tmp / "diarization_rttm",
#             pad_end=0.4,
#         )
#         update_temp_data(db, operation_id, "DIARIZATION", {"rttm_path": str(diarization_result)})
#         set_step_status(db, operation_id, "DIARIZATION", "DONE", progress=25)
#     except Exception as exc:
#         set_step_status(db, operation_id, "DIARIZATION", "FAILED")
#         raise
#
#
# # -------------------------
# # Merge Intervals
# # -------------------------
# @celery_app.task(bind=True, acks_late=True, queue="cpu")
# @with_db
# def merge_intervals_task(db: Session, self, operation_id: str):
#     try:
#         set_step_status(db, operation_id, "MERGE_INTERVALS", "RUNNING", progress=30)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#         diarization_data = get_temp_data(db, operation_id, "DIARIZATION")
#         rttm_path = Path(diarization_data["rttm_path"])
#
#         rttm_data = load_rttm(rttm_path)
#         ann = next(iter(rttm_data.values()))
#         intervals = [(turn.start, turn.end, spk) for turn, _, spk in ann.itertracks(yield_label=True)]
#         intervals_path = merge_intervals_step(intervals, tmp)
#         update_temp_data(db, operation_id, "MERGE_INTERVALS", {"intervals_path": str(intervals_path)})
#         set_step_status(db, operation_id, "MERGE_INTERVALS", "DONE", progress=35)
#     except Exception as exc:
#         set_step_status(db, operation_id, "MERGE_INTERVALS", "FAILED")
#         raise
#
#
# # -------------------------
# # VAD + Hungarian
# # -------------------------
# @celery_app.task(bind=True, acks_late=True, queue="cpu")
# @with_db
# def vad_hungarian_task(db: Session, self, operation_id: str):
#     try:
#         set_step_status(db, operation_id, "VAD_HUNGARIAN", "RUNNING", progress=40)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#         merge_intervals_data = get_temp_data(db, operation_id, "MERGE_INTERVALS")
#         intervals_path = Path(merge_intervals_data["intervals_path"])
#         with open(intervals_path, "r") as f:
#             intervals_merged = [tuple(x) for x in json.load(f)]
#
#         merge_data = get_temp_data(db, operation_id, "MERGE_AUDIO")
#         merged_audio_path = Path(merge_data["merged_audio_path"])
#
#         vad_path = vad_hungarian_step(
#             intervals=intervals_merged,
#             merged_audio_path=merged_audio_path,
#             wav_files=list(Path(PATH_TO_AUDIO).glob("*.wav")),
#             tmp_dir=tmp
#         )
#         update_temp_data(db, operation_id, "VAD_HUNGARIAN", {"vad_path": str(vad_path)})
#         set_step_status(db, operation_id, "VAD_HUNGARIAN", "DONE", progress=50)
#     except Exception as exc:
#         set_step_status(db, operation_id, "VAD_HUNGARIAN", "FAILED")
#         raise
#
# # -------------------------
# # Extract Segments
# # -------------------------
# @celery_app.task(bind=True, acks_late=True, queue="cpu")
# @with_db
# def extract_segments_task(db: Session, self, operation_id: str):
#     try:
#         set_step_status(db, operation_id, "EXTRACT_SEGMENTS", "RUNNING", progress=55)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#         merge_intervals_data = get_temp_data(db, operation_id, "MERGE_INTERVALS")
#         intervals_path = Path(merge_intervals_data["intervals_path"])
#
#         vad_data = get_temp_data(db, operation_id, "VAD_HUNGARIAN")
#         vad_path = Path(vad_data["vad_path"])
#
#         with open(intervals_path, "r") as f:
#             intervals_merged = [tuple(x) for x in json.load(f)]
#         with open(vad_path, "r") as f:
#             vad_json = json.load(f)
#         speaker_to_label = vad_json["speaker_to_label"]
#
#         segments_file = prepare_audio_segments(
#             wav_files=list(Path(PATH_TO_AUDIO).glob("*.wav")),
#             intervals=intervals_merged,
#             speaker_to_label=speaker_to_label,
#             unique_tmp_path=tmp,
#             sample_rate=8000,
#         )
#         update_temp_data(db, operation_id, "EXTRACT_SEGMENTS", {"segments_file": str(segments_file)})
#         set_step_status(db, operation_id, "EXTRACT_SEGMENTS", "DONE", progress=65)
#     except Exception as exc:
#         set_step_status(db, operation_id, "EXTRACT_SEGMENTS", "FAILED")
#         raise
#
#
# # -------------------------
# # Transcription
# # -------------------------
# @celery_app.task(bind=True, acks_late=True, queue="cpu", autoretry_for=(Timeout, ConnectionError), soft_time_limit = 4 * 60 * 60, time_limit = 4 * 60 * 60 + 300, retry_backoff=True, retry_backoff_max=600, retry_jitter=True, retry_kwargs={'max_retries': 5})
# @with_db
# def transcription_task(db: Session, self, operation_id: str):
#     try:
#         set_step_status(db, operation_id, "TRANSCRIPTION", "RUNNING", progress=70)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#         extract_data = get_temp_data(db, operation_id, "EXTRACT_SEGMENTS")
#         segments_file = Path(extract_data["segments_file"])
#
#         with open(segments_file, "r", encoding="utf-8") as f:
#             segments_json = json.load(f)
#         interval_segments = segments_json["interval_segments"]
#
#         transcription_file = transcribe_step_yandex(
#             interval_segments=interval_segments,
#             output_path=tmp / "yandex_transcription.json",
#         )
#         update_temp_data(db, operation_id, "TRANSCRIPTION", {"transcription_file": str(transcription_file)})
#         set_step_status(db, operation_id, "TRANSCRIPTION", "DONE", progress=85)
#     except Exception as exc:
#         set_step_status(db, operation_id, "TRANSCRIPTION", "FAILED")
#         raise
#
#
#
# # -------------------------
# # Export Results
# # -------------------------
# @celery_app.task(bind=True, acks_late=True, queue="cpu", autoretry_for=(Timeout, ConnectionError), soft_time_limit = 4 * 60 * 60, time_limit = 4 * 60 * 60 + 300, retry_backoff=True, retry_backoff_max=600, retry_jitter=True, retry_kwargs={'max_retries': 5})
# @with_db
# def export_results_task(db: Session, self, operation_id: str):
#     try:
#         set_step_status(db, operation_id, "EXPORT_RESULTS", "RUNNING", progress=90)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#
#         extract_data = get_temp_data(db, operation_id, "EXTRACT_SEGMENTS")
#         segments_file = Path(extract_data["segments_file"])
#         vad_data = get_temp_data(db, operation_id, "VAD_HUNGARIAN")
#         vad_path = Path(vad_data["vad_path"])
#         merge_data = get_temp_data(db, operation_id, "MERGE_AUDIO")
#         merged_audio_path = Path(merge_data["merged_audio_path"])
#         trans_data = get_temp_data(db, operation_id, "TRANSCRIPTION")
#         transcription_file = Path(trans_data["transcription_file"])
#
#         with open(segments_file, "r", encoding="utf-8") as f:
#             segments_json = json.load(f)
#         speaker_to_file_v = segments_json["speaker_to_file"]
#         label_to_file_v = segments_json["label_to_file"]
#
#         with open(vad_path, "r") as f:
#             vad_json = json.load(f)
#         speaker_to_label = vad_json["speaker_to_label"]
#
#         with open(transcription_file, "r", encoding="utf-8") as f:
#             transcription_json = json.load(f)
#         intervals_with_text = transcription_json["intervals_with_text"]
#
#         result = export_pipeline_results(
#             intervals_with_text=intervals_with_text,
#             speaker_to_label=speaker_to_label,
#             speaker_to_file=speaker_to_file_v,
#             label_to_file=label_to_file_v,
#             s3_prefix=f"segments/{operation_id}",
#             merged_audio_path=merged_audio_path,
#             results_path_op=tmp / "final_results",
#             operation_id=operation_id,
#         )
#         update_temp_data(db, operation_id, "EXPORT_RESULTS", {"result_url": result["json_url"]})
#         set_step_status(db, operation_id, "EXPORT_RESULTS", "DONE", progress=100)
#     except Exception as exc:
#         set_step_status(db, operation_id, "EXPORT_RESULTS", "FAILED")
#         raise



import time
import logging
import json
from pathlib import Path
from functools import wraps
from requests.exceptions import Timeout, ConnectionError
from celery.exceptions import SoftTimeLimitExceeded

from sqlalchemy.orm import Session
from app.celery_app import celery_app
from app.pipeline.steps.Merge_audio import run_merge_audio_step
from app.pipeline.steps.diarization_step import diarization_step
from app.pipeline.steps.merge_intervals import merge_intervals_step
from app.pipeline.steps.vad_hungarian import vad_hungarian_step
from app.pipeline.steps.prepare_audio_segments import prepare_audio_segments
from app.pipeline.steps.transcribe_yandex import transcribe_step_yandex
from app.pipeline.steps.export_pipeline_results import export_pipeline_results
from app.pipeline.utils import get_unique_result_path
from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO
from pyannote.database.util import load_rttm
from app.pipeline.utils import update_progress, mark_done, mark_failed, update_temp_data, get_temp_data, set_step_status, cleanup_temp_if_done
from app.pipeline.steps.bd import SessionLocal, PipelineOperation, PipelineTempData

# ------------------------- Logging -------------------------
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler()]
)

# ------------------------- Utils -------------------------
def deserialize(obj):
    if isinstance(obj, dict):
        return {k: deserialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [deserialize(v) for v in obj]
    if isinstance(obj, str) and (obj.endswith(".wav") or "/" in obj):
        return Path(obj)
    return obj


def serialize(obj):
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, dict):
        return {k: serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [serialize(v) for v in obj]
    return obj


def with_db(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        db = SessionLocal()
        try:
            return func(db, *args, **kwargs)
        finally:
            db.close()
    return wrapper

# ------------------------- Centralized Step Executor -------------------------
def execute_step(
    db: Session,
    operation_id: str,
    step_name: str,
    progress_start: int,
    progress_done: int,
    func,
    retry_exceptions: tuple = (),
):
    try:
        # Обновляем статус шага на RUNNING
        update_temp_data(db, operation_id, step_name, status="RUNNING")
        set_step_status(db, operation_id, step_name, "RUNNING", progress=progress_start)

        result = func()

        # Шаг успешно выполнен
        update_temp_data(db, operation_id, step_name, status="DONE")
        set_step_status(db, operation_id, step_name, "DONE", progress=progress_done)
        return result

    except retry_exceptions as exc:
        logger.warning(f"{step_name} temporary error: {exc}")
        update_temp_data(db, operation_id, step_name, status="RETRYING")
        set_step_status(db, operation_id, step_name, "RETRYING")
        raise

    except SoftTimeLimitExceeded:
        logger.error(f"{step_name} soft time limit exceeded")
        update_temp_data(db, operation_id, step_name, status="FAILED_TIMEOUT")
        set_step_status(db, operation_id, step_name, "FAILED_TIMEOUT")
        raise

    except Exception as exc:
        logger.exception(f"{step_name} failed permanently: {exc}")
        update_temp_data(db, operation_id, step_name, status="FAILED")
        set_step_status(db, operation_id, step_name, "FAILED")
        raise

# ------------------------- Tasks -------------------------

# 1️⃣ Merge Audio (CPU, no retry)
@celery_app.task(bind=True, acks_late=True, queue="cpu")
@with_db
def merge_audio_task(db: Session, self, operation_id: str):
    def logic():
        merged_audio_path = run_merge_audio_step(operation_id)
        update_temp_data(db, operation_id, "MERGE_AUDIO", {"merged_audio_path": str(merged_audio_path)})

    execute_step(db, operation_id, "MERGE_AUDIO", 5, 10, logic)


# 2️⃣ Diarization (GPU, CPU-heavy, no retry)
@celery_app.task(bind=True, acks_late=True, queue="gpu", soft_time_limit=4*60*60, time_limit=4*60*60 + 300)
@with_db
def diarization_task(db: Session, self, operation_id: str):
    def logic():
        tmp = get_unique_result_path(TMP_PATH, operation_id)
        merge_data = get_temp_data(db, operation_id, "MERGE_AUDIO")
        merged_audio_path = Path(merge_data["merged_audio_path"])

        diarization_result = diarization_step(
            input_audio=merged_audio_path,
            speakers_folder=PATH_TO_AUDIO,
            output_dir=tmp / "diarization_rttm",
            pad_end=0.4,
        )

        update_temp_data(db, operation_id, "DIARIZATION", {"rttm_path": str(diarization_result)})

    execute_step(db, operation_id, "DIARIZATION", 15, 25, logic)


# 3️⃣ Merge Intervals (CPU, no retry)
@celery_app.task(bind=True, acks_late=True, queue="cpu", soft_time_limit=4*60*60, time_limit=4*60*60 + 300)
@with_db
def merge_intervals_task(db: Session, self, operation_id: str):
    def logic():
        tmp = get_unique_result_path(TMP_PATH, operation_id)
        diarization_data = get_temp_data(db, operation_id, "DIARIZATION")
        rttm_path = Path(diarization_data["rttm_path"])

        rttm_data = load_rttm(rttm_path)
        ann = next(iter(rttm_data.values()))
        intervals = [(turn.start, turn.end, spk) for turn, _, spk in ann.itertracks(yield_label=True)]

        intervals_path = merge_intervals_step(intervals, tmp)
        update_temp_data(db, operation_id, "MERGE_INTERVALS", {"intervals_path": str(intervals_path)})

    execute_step(db, operation_id, "MERGE_INTERVALS", 30, 35, logic)


# 4️⃣ VAD + Hungarian (CPU, no retry)
@celery_app.task(bind=True, acks_late=True, queue="cpu", soft_time_limit=4*60*60, time_limit=4*60*60 + 300)
@with_db
def vad_hungarian_task(db: Session, self, operation_id: str):
    def logic():
        tmp = get_unique_result_path(TMP_PATH, operation_id)
        merge_intervals_data = get_temp_data(db, operation_id, "MERGE_INTERVALS")
        intervals_path = Path(merge_intervals_data["intervals_path"])
        with open(intervals_path, "r") as f:
            intervals_merged = [tuple(x) for x in json.load(f)]

        merge_data = get_temp_data(db, operation_id, "MERGE_AUDIO")
        merged_audio_path = Path(merge_data["merged_audio_path"])

        vad_path = vad_hungarian_step(
            intervals=intervals_merged,
            merged_audio_path=merged_audio_path,
            wav_files=list(Path(PATH_TO_AUDIO).glob("*.wav")),
            tmp_dir=tmp
        )
        update_temp_data(db, operation_id, "VAD_HUNGARIAN", {"vad_path": str(vad_path)})

    execute_step(db, operation_id, "VAD_HUNGARIAN", 40, 50, logic)


# 5️⃣ Extract Segments (CPU, no retry)
@celery_app.task(bind=True, acks_late=True, queue="cpu", soft_time_limit=4*60*60, time_limit=4*60*60 + 300)
@with_db
def extract_segments_task(db: Session, self, operation_id: str):
    def logic():
        tmp = get_unique_result_path(TMP_PATH, operation_id)
        merge_intervals_data = get_temp_data(db, operation_id, "MERGE_INTERVALS")
        intervals_path = Path(merge_intervals_data["intervals_path"])
        vad_data = get_temp_data(db, operation_id, "VAD_HUNGARIAN")
        vad_path = Path(vad_data["vad_path"])

        with open(intervals_path, "r") as f:
            intervals_merged = [tuple(x) for x in json.load(f)]
        with open(vad_path, "r") as f:
            vad_json = json.load(f)
        speaker_to_label = vad_json["speaker_to_label"]

        segments_file = prepare_audio_segments(
            wav_files=list(Path(PATH_TO_AUDIO).glob("*.wav")),
            intervals=intervals_merged,
            speaker_to_label=speaker_to_label,
            unique_tmp_path=tmp,
            sample_rate=8000,
        )
        update_temp_data(db, operation_id, "EXTRACT_SEGMENTS", {"segments_file": str(segments_file)})

    execute_step(db, operation_id, "EXTRACT_SEGMENTS", 55, 65, logic)


# 6️⃣ Transcription (CPU, retry on network errors)
@celery_app.task(
    bind=True,
    acks_late=True,
    queue="cpu",
    autoretry_for=(Timeout, ConnectionError),
    retry_backoff=True,
    retry_backoff_max=1800,
    retry_jitter=True,
    retry_kwargs={"max_retries": 7},
    soft_time_limit=4*60*60,
    time_limit=4*60*60 + 300,
)
@with_db
def transcription_task(db: Session, self, operation_id: str):
    def logic():
        tmp = get_unique_result_path(TMP_PATH, operation_id)
        extract_data = get_temp_data(db, operation_id, "EXTRACT_SEGMENTS")
        segments_file = Path(extract_data["segments_file"])

        with open(segments_file, "r", encoding="utf-8") as f:
            segments_json = json.load(f)
        interval_segments = segments_json["interval_segments"]

        transcription_file = transcribe_step_yandex(
            interval_segments=interval_segments,
            output_path=tmp / "yandex_transcription.json",
        )
        update_temp_data(db, operation_id, "TRANSCRIPTION", {"transcription_file": str(transcription_file)})

    execute_step(db, operation_id, "TRANSCRIPTION", 70, 85, logic, retry_exceptions=(Timeout, ConnectionError))


# 7️⃣ Export Results (CPU, retry on network errors)
@celery_app.task(
    bind=True,
    acks_late=True,
    queue="cpu",
    autoretry_for=(Timeout, ConnectionError),
    retry_backoff=True,
    retry_backoff_max=1800,
    retry_jitter=True,
    retry_kwargs={"max_retries": 7},
)
@with_db
def export_results_task(db: Session, self, operation_id: str):
    def logic():
        tmp = get_unique_result_path(TMP_PATH, operation_id)

        extract_data = get_temp_data(db, operation_id, "EXTRACT_SEGMENTS")
        vad_data = get_temp_data(db, operation_id, "VAD_HUNGARIAN")
        merge_data = get_temp_data(db, operation_id, "MERGE_AUDIO")
        trans_data = get_temp_data(db, operation_id, "TRANSCRIPTION")

        transcription_json = json.load(open(Path(trans_data["transcription_file"]), encoding="utf-8"))
        vad_json = json.load(open(Path(vad_data["vad_path"])))
        segments_json = json.load(open(Path(extract_data["segments_file"]), encoding="utf-8"))

        result = export_pipeline_results(
            intervals_with_text=transcription_json["intervals_with_text"],
            speaker_to_label=vad_json["speaker_to_label"],
            speaker_to_file=segments_json["speaker_to_file"],
            label_to_file=segments_json["label_to_file"],
            s3_prefix=f"segments/{operation_id}",
            merged_audio_path=Path(merge_data["merged_audio_path"]),
            results_path_op=tmp / "final_results",
            operation_id=operation_id,
        )
        update_temp_data(db, operation_id, "EXPORT_RESULTS", {"result_url": result["json_url"]})
        cleanup_temp_if_done(db, operation_id)

    execute_step(db, operation_id, "EXPORT_RESULTS", 90, 100, logic, retry_exceptions=(Timeout, ConnectionError))
