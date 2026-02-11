# import time
# import logging
# from app.celery_app import celery_app
# from app.pipeline.shag.Merge_audio_steps import run_merge_audio_step
# from typing import List
#
# import torch
# import torchaudio
# from tqdm import tqdm
#
# logger = logging.getLogger(__name__)
# logging.basicConfig(
#     level=logging.INFO,                    # Уровень: выводить INFO и выше
#     format='%(asctime)s | %(levelname)s | %(message)s',  # Формат сообщения
#     handlers=[
#         logging.StreamHandler()           # Вывод в терминал (stdout)
#     ]
# )
# @celery_app.task(queue="cpu", bind=True)
# def merge_audio_task(self, operation_id: str) -> str:
#     logger.info("1. Merge audio started")
#     t0 = time.time()
#
#     merged_audio_path = run_merge_audio_step(operation_id)
#
#     logger.info("1. Merge audio finished: %.2f sec", time.time() - t0)
#     return str(merged_audio_path)
#
#
#
# from app.celery_app import celery_app
# from app.pipeline.shag.run_diarization_step import diarization_step
# from app.pipeline.utils import get_unique_result_path
# from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO
#
#
# @celery_app.task(queue="gpu", bind=True)
# def diarization_task(self, merged_audio_path: str, operation_id: str):
#     logger.info("2. Diarization started")
#     t0 = time.time()
#
#     tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#
#     result = diarization_step(
#         input_audio=Path(merged_audio_path),
#         speakers_folder=PATH_TO_AUDIO,
#         output_dir=tmp_path / "diarization_rttm",
#         pad_end=0.4,
#     )
#
#     torch.cuda.empty_cache()
#     logger.info("2. Diarization finished: %.2f sec", time.time() - t0)
#     return result
#
#
#
# from app.celery_app import celery_app
# from app.pipeline.shag.intervals_merged_steps import merge_intervals_step
# from app.pipeline.utils import get_unique_result_path
# from app.pipeline.config import TMP_PATH
#
#
# @celery_app.task(queue="cpu")
# def merge_intervals_task(diarization_result, operation_id: str):
#     logger.info("3. Merge intervals started")
#     t0 = time.time()
#
#     tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     intervals = list(diarization_result.values())[0]
#
#     merged = merge_intervals_step(intervals, tmp_path)
#
#     logger.info("3. Merge intervals finished: %.2f sec", time.time() - t0)
#     return merged
#
#
#
#
#
# import torch
# from app.celery_app import celery_app
# from app.pipeline.shag.VAD_embeding import vad_hungarian_step
# from app.pipeline.utils import get_unique_result_path
# from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO
#
#
# @celery_app.task(queue="gpu", bind=True)
# def vad_hungarian_task(operation_id: str):
#     logger.info("4. VAD + Hungarian started")
#     t0 = time.time()
#
#     tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     merged_audio = tmp_path / "Merged.wav"
#     wav_files = list(Path(PATH_TO_AUDIO).glob("*.wav"))
#
#     updated_intervals, speaker_to_label = vad_hungarian_step(
#         intervals=merged,
#         merged_audio_path=merged_audio,
#         wav_files=wav_files,
#         tmp_dir=tmp_path,
#     )
#
#     torch.cuda.empty_cache()
#     logger.info("4. VAD + Hungarian finished: %.2f sec", time.time() - t0)
#
#     return {
#         "intervals": updated_intervals,
#         "speaker_to_label": speaker_to_label,
#     }
#
#
#
# from app.celery_app import celery_app
# from app.pipeline.shag.extract_segments_from_speaker_files_steps import prepare_audio_segments
# from app.pipeline.utils import get_unique_result_path
# from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO
#
#
# @celery_app.task(queue="cpu")
# def prepare_segments_task(operation_id: str):
#     logger.info("5. Extract segments started")
#     t0 = time.time()
#
#     tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#
#     interval_segments, _, _,speaker_to_file, label_to_file = prepare_audio_segments(
#         wav_files=list(Path(PATH_TO_AUDIO).glob("*.wav")),
#         intervals=merged,
#         speaker_to_label=speaker_to_label,
#         unique_tmp_path=tmp_path,
#         sample_rate=8000,
#     )
#
#     logger.info("5. Extract segments finished: %.2f sec", time.time() - t0)
#     return result
#
#
#
# from app.celery_app import celery_app
# from app.pipeline.shag.Yandex_STT_steps import transcribe_step_yandex
# from app.pipeline.utils import get_unique_result_path
# from app.pipeline.config import TMP_PATH
#
#
# @celery_app.task(queue="cpu")
# def stt_task(operation_id: str):
#     logger.info("6. Transcription started")
#     t0 = time.time()
#
#     tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     #interval_segments = segments_result[0]
#
#     result, _ = transcribe_step_yandex(
#         interval_segments=interval_segments,
#         output_path=tmp_path / "yandex_transcription.json",
#     )
#
#     logger.info("6. Transcription finished: %.2f sec", time.time() - t0)
#     return result
#
#
#
#
# # app/pipeline/tasks/export.py
# from pathlib import Path
# from app.celery_app import celery_app
# from app.pipeline.shag.export_pipeline_results_steps import export_pipeline_results
# from app.pipeline.utils import get_unique_result_path
# from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO
#
#
# @celery_app.task(queue="cpu")
# def export_task(stt_result, segments_result, vad_result, operation_id: str):
#     logger.info("7. Export started")
#     t0 = time.time()
#
#     intervals_with_text, _ = stt_result
#     _, _, _, speaker_to_file, label_to_file = segments_result
#     tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#
#     export_pipeline_results(
#         intervals_with_text=intervals_with_text,
#         speaker_to_label=vad_result["speaker_to_label"],
#         speaker_to_file=speaker_to_file,
#         label_to_file=label_to_file,
#         wav_dir=Path(PATH_TO_AUDIO),
#         merged_audio_path=tmp_path / "Merged.wav",
#         results_path_op=tmp_path / "final_results",
#     )
#
#     logger.info("7. Export finished: %.2f sec", time.time() - t0)



# import time
# import logging
# from pathlib import Path
# from app.celery_app import celery_app
# from app.pipeline.shag.Merge_audio_steps import run_merge_audio_step
# from app.pipeline.shag.run_diarization_step import diarization_step
# from app.pipeline.shag.intervals_merged_steps import merge_intervals_step
# from app.pipeline.shag.VAD_embeding import vad_hungarian_step
# from app.pipeline.shag.extract_segments_from_speaker_files_steps import prepare_audio_segments
# from app.pipeline.shag.Yandex_STT_steps import transcribe_step_yandex
# from app.pipeline.shag.export_pipeline_results_steps import export_pipeline_results
# from app.pipeline.utils import get_unique_result_path
# from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO
#
# logger = logging.getLogger(__name__)
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s | %(levelname)s | %(message)s',
#     handlers=[logging.StreamHandler()]
# )
#
# @celery_app.task(bind=True, queue="cpu")
# def merge_audio_task(self, operation_id: str) -> str:
#     logger.info("Merge audio started")
#     t0 = time.time()
#     merged_audio_path = run_merge_audio_step(operation_id)
#     logger.info("Merge audio finished: %.2f sec", time.time() - t0)
#     return merged_audio_path
#
#
# @celery_app.task(bind=True, queue="gpu")
# def diarization_task(self, merged_audio_path: str, operation_id: str) -> dict:
#     logger.info("Diarization started")
#     t0 = time.time()
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     diarization_result = diarization_step(
#         input_audio=merged_audio_path,
#         speakers_folder=PATH_TO_AUDIO,
#         output_dir=unique_tmp_path / "diarization_rttm",
#         pad_end=0.4,
#     )
#     logger.info("Diarization finished: %.2f sec", time.time() - t0)
#     return {Path(merged_audio_path).name: diarization_result[Path(merged_audio_path).name]}
#
#
# @celery_app.task(bind=True, queue="cpu")
# def merge_intervals_task(self, intervals: list, operation_id: str) -> list:
#     logger.info("Merge intervals started")
#     t0 = time.time()
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     intervals_merged = merge_intervals_step(intervals, unique_tmp_path)
#     logger.info("Merge intervals finished: %.2f sec", time.time() - t0)
#     return intervals_merged
#
#
# @celery_app.task(bind=True, queue="cpu")
# def vad_hungarian_task(self, intervals_merged: list, merged_audio_path: str, operation_id: str) -> tuple:
#     logger.info("VAD + Hungarian started")
#     t0 = time.time()
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     wav_files = [str(f) for f in Path(PATH_TO_AUDIO).glob("*.wav")]
#     updated_intervals, speaker_to_label = vad_hungarian_step(
#         intervals=intervals_merged,
#         merged_audio_path=merged_audio_path,
#         wav_files=wav_files,
#         tmp_dir=unique_tmp_path,
#     )
#     logger.info("VAD + Hungarian finished: %.2f sec", time.time() - t0)
#     return updated_intervals, speaker_to_label
#
#
# @celery_app.task(bind=True, queue="cpu")
# def extract_segments_task(self, intervals_merged: list, speaker_to_label: dict, operation_id: str) -> dict:
#     logger.info("Extract segments started")
#     t0 = time.time()
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     wav_files = [str(f) for f in Path(PATH_TO_AUDIO).glob("*.wav")]
#     interval_segments, _, _, speaker_to_file, label_to_file = prepare_audio_segments(
#         wav_files=wav_files,
#         intervals=intervals_merged,
#         speaker_to_label=speaker_to_label,
#         unique_tmp_path=unique_tmp_path,
#         sample_rate=8000,
#     )
#     logger.info("Extract segments finished: %.2f sec", time.time() - t0)
#     return {
#         "interval_segments": interval_segments,
#         "speaker_to_file": speaker_to_file,
#         "label_to_file": label_to_file,
#     }
#
#
# @celery_app.task(bind=True, queue="cpu")
# def transcription_task(self, interval_segments: list, operation_id: str) -> list:
#     logger.info("Transcription started")
#     t0 = time.time()
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     intervals_with_text, _ = transcribe_step_yandex(
#         interval_segments=interval_segments,
#         output_path=unique_tmp_path / "yandex_transcription.json",
#     )
#     logger.info("Transcription finished: %.2f sec", time.time() - t0)
#     return intervals_with_text
#
#
# @celery_app.task(bind=True, queue="cpu")
# def export_results_task(self, intervals_with_text: list, speaker_to_label: dict, speaker_to_file: dict,
#                         label_to_file: dict, merged_audio_path: str, operation_id: str) -> str:
#     logger.info("Export started")
#     t0 = time.time()
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     export_pipeline_results(
#         intervals_with_text=intervals_with_text,
#         speaker_to_label=speaker_to_label,
#         speaker_to_file=speaker_to_file,
#         label_to_file=label_to_file,
#         wav_dir=Path(PATH_TO_AUDIO),
#         merged_audio_path=merged_audio_path,
#         results_path_op=unique_tmp_path / "final_results",
#     )
#     logger.info("Export finished: %.2f sec", time.time() - t0)
#     return str(unique_tmp_path / "final_results")




import time
import logging
from pathlib import Path
from app.celery_app import celery_app
from app.pipeline.shag.Merge_audio_steps import run_merge_audio_step
from app.pipeline.shag.run_diarization_step import diarization_step
from app.pipeline.shag.intervals_merged_steps import merge_intervals_step
from app.pipeline.shag.VAD_embeding import vad_hungarian_step
from app.pipeline.shag.extract_segments_from_speaker_files_steps import prepare_audio_segments
from app.pipeline.shag.Yandex_STT_steps import transcribe_step_yandex
from app.pipeline.shag.export_pipeline_results_steps import export_pipeline_results
from app.pipeline.utils import get_unique_result_path
from app.pipeline.config import TMP_PATH, PATH_TO_AUDIO
from pyannote.database.util import load_rttm

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler()]
)



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

# # --------------------- Merge audio ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def merge_audio_task(self, operation_id: str) -> dict:
#     logger.info("Merge audio started")
#     t0 = time.time()
#     merged_audio_path = run_merge_audio_step(operation_id)
#     logger.info("Merge audio finished: %.2f sec", time.time() - t0)
#     return {"merged_audio_path": str(merged_audio_path), "operation_id": operation_id}
#
# # --------------------- Diarization ---------------------
# @celery_app.task(bind=True, queue="gpu")
# def diarization_task(self, data: dict) -> dict:
#     logger.info("Diarization started")
#     t0 = time.time()
#     operation_id = data["operation_id"]
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#
#     diarization_result = diarization_step(
#         input_audio=unique_tmp_path / "Merged.wav",
#         speakers_folder=PATH_TO_AUDIO,
#         output_dir=unique_tmp_path / "diarization_rttm",
#         pad_end=0.4,
#     )
#     data["intervals"] = diarization_result[Path(data["merged_audio_path"]).name]
#     logger.info("Diarization finished: %.2f sec", time.time() - t0)
#     return data
#
# # --------------------- Merge intervals ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def merge_intervals_task(self, data: dict) -> dict:
#     logger.info("Merge intervals started")
#     t0 = time.time()
#     operation_id = data["operation_id"]
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     data["intervals_merged"] = merge_intervals_step(data["intervals"], unique_tmp_path)
#     logger.info("Merge intervals finished: %.2f sec", time.time() - t0)
#     return data
#
# # --------------------- VAD + Hungarian ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def vad_hungarian_task(self, data: dict) -> dict:
#     logger.info("VAD + Hungarian started")
#     t0 = time.time()
#     operation_id = data["operation_id"]
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     wav_files = list(Path(PATH_TO_AUDIO).glob("*.wav"))
#
#     updated_intervals, speaker_to_label = vad_hungarian_step(
#         intervals=data["intervals_merged"],
#         merged_audio_path=Path(data["merged_audio_path"]),
#         wav_files=wav_files,
#         tmp_dir=unique_tmp_path,
#     )
#     data["intervals_mer"] = updated_intervals
#     data["speaker_to_label"] = speaker_to_label
#     logger.info("VAD + Hungarian finished: %.2f sec", time.time() - t0)
#     return data
#
# # --------------------- Extract segments ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def extract_segments_task(self, data: dict) -> dict:
#     logger.info("Extract segments started")
#     t0 = time.time()
#     operation_id = data["operation_id"]
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#     wav_files = list(Path(PATH_TO_AUDIO).glob("*.wav"))
#
#     interval_segments, _, _, speaker_to_file, label_to_file = prepare_audio_segments(
#         wav_files=wav_files,
#         intervals=data["intervals_merged"],
#         speaker_to_label=data["speaker_to_label"],
#         unique_tmp_path=unique_tmp_path,
#         sample_rate=8000,
#     )
#     data.update({
#         "interval_segments": serialize(interval_segments),
#         "speaker_to_file": serialize(speaker_to_file),
#         "label_to_file": serialize(label_to_file),
#     })
#     logger.info("Extract segments finished: %.2f sec", time.time() - t0)
#     return data
#
# # --------------------- Transcription ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def transcription_task(self, data: dict) -> dict:
#     logger.info("Transcription started")
#     t0 = time.time()
#     operation_id = data["operation_id"]
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#
#     intervals_with_text, _ = transcribe_step_yandex(
#         interval_segments=deserialize(data["interval_segments"]),
#         output_path=unique_tmp_path / "yandex_transcription.json",
#     )
#     data["intervals_with_text"] = serialize(intervals_with_text)
#     logger.info("Transcription finished: %.2f sec", time.time() - t0)
#     return data
#
# # --------------------- Export results ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def export_results_task(self, data: dict) -> dict:
#     logger.info("Export started")
#     t0 = time.time()
#     operation_id = data["operation_id"]
#     unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#
#     result = export_pipeline_results(
#         intervals_with_text=deserialize(data["intervals_with_text"]),
#         speaker_to_label=data["speaker_to_label"],
#         speaker_to_file=data["speaker_to_file"],
#         label_to_file=data["label_to_file"],
#         s3_prefix=f"segments/{operation_id}",
#         merged_audio_path=data["merged_audio_path"],
#         results_path_op=unique_tmp_path / "final_results",
#     )
#     logger.info("Export finished: %.2f sec", time.time() - t0)
#     return {
#         "operation_id": operation_id,
#         "json_s3_key": f"segments/{operation_id}/pipeline_intervals.json",
#         "json_url": result["json_url"],  # presigned
#     }























# # app/pipeline/shag/tasks.py
from app.pipeline.utils import update_progress, mark_done, mark_failed, update_temp_data, get_temp_data, set_step_status
from datetime import datetime
from app.pipeline.shag.bd import SessionLocal, PipelineOperation, PipelineTempData
import json
from sqlalchemy.orm import Session
#
# # --------------------- Merge audio ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def merge_audio_task(self, operation_id: str) -> dict:
#     try:
#         update_progress(operation_id, "Merge Audio", 5)
#         merged_audio_path = run_merge_audio_step(operation_id)
#         update_progress(operation_id, "Merge Audio", 10)
#         print(f"ОТВЕТ: {(str(merged_audio_path))}")
#         return {"merged_audio_path": str(merged_audio_path), "operation_id": operation_id}
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
# # --------------------- Diarization ---------------------
# @celery_app.task(bind=True, queue="gpu")
# def diarization_task(self, data: dict) -> dict:
#     try:
#         operation_id = data["operation_id"]
#         update_progress(operation_id, "Diarization", 15)
#         unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#
#         diarization_result = diarization_step(
#             input_audio=unique_tmp_path / "Merged.wav",
#             speakers_folder=PATH_TO_AUDIO,
#             output_dir=unique_tmp_path / "diarization_rttm",
#             pad_end=0.4,
#         )
#         data["intervals"] = diarization_result[Path(data["merged_audio_path"]).name]
#         update_progress(operation_id, "Diarization", 25)
#         print(f'ОТВЕТ: {(data["intervals"])}')
#         return data
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
# # --------------------- Merge intervals ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def merge_intervals_task(self, data: dict) -> dict:
#     try:
#         operation_id = data["operation_id"]
#         update_progress(operation_id, "Merge Intervals", 30)
#         unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#         data["intervals_merged"] = merge_intervals_step(data["intervals"], unique_tmp_path)
#         update_progress(operation_id, "Merge Intervals", 35)
#         print(f'ОТВЕТ: {(data["intervals_merged"])}')
#         return data
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
# # --------------------- VAD + Hungarian ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def vad_hungarian_task(self, data: dict) -> dict:
#     try:
#         operation_id = data["operation_id"]
#         update_progress(operation_id, "VAD + Hungarian", 40)
#         unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#         wav_files = list(Path(PATH_TO_AUDIO).glob("*.wav"))
#
#         updated_intervals, speaker_to_label = vad_hungarian_step(
#             intervals=data["intervals_merged"],
#             merged_audio_path=Path(data["merged_audio_path"]),
#             wav_files=wav_files,
#             tmp_dir=unique_tmp_path,
#         )
#         data["intervals_mer"] = updated_intervals
#         data["speaker_to_label"] = speaker_to_label
#         update_progress(operation_id, "VAD + Hungarian", 50)
#         print(f'ОТВЕТ: {(data["intervals_mer"])}')
#         print(f'ОТВЕТ: {(data["speaker_to_label"])}')
#         return data
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
# # --------------------- Extract segments ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def extract_segments_task(self, data: dict) -> dict:
#     try:
#         operation_id = data["operation_id"]
#         update_progress(operation_id, "Extract Segments", 55)
#         unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#         wav_files = list(Path(PATH_TO_AUDIO).glob("*.wav"))
#
#         interval_segments, _, _, speaker_to_file, label_to_file = prepare_audio_segments(
#             wav_files=wav_files,
#             intervals=data["intervals_merged"],
#             speaker_to_label=data["speaker_to_label"],
#             unique_tmp_path=unique_tmp_path,
#             sample_rate=8000,
#         )
#         data.update({
#             "interval_segments": serialize(interval_segments),
#             "speaker_to_file": serialize(speaker_to_file),
#             "label_to_file": serialize(label_to_file),
#         })
#         update_progress(operation_id, "Extract Segments", 65)
#         print(f'ОТВЕТ: {(data["interval_segments"])}')
#         print(f'ОТВЕТ: {(data["speaker_to_file"])}')
#         print(f'ОТВЕТ: {(data["label_to_file"])}')
#         return data
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
# # --------------------- Transcription ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def transcription_task(self, data: dict) -> dict:
#     try:
#         operation_id = data["operation_id"]
#         update_progress(operation_id, "Transcription", 70)
#         unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#
#         intervals_with_text, _ = transcribe_step_yandex(
#             interval_segments=deserialize(data["interval_segments"]),
#             output_path=unique_tmp_path / "yandex_transcription.json",
#         )
#         data["intervals_with_text"] = serialize(intervals_with_text)
#         update_progress(operation_id, "Transcription", 85)
#         print(f'ОТВЕТ: {(data["intervals_with_text"])}')
#         return data
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
# # --------------------- Export results ---------------------
# @celery_app.task(bind=True, queue="cpu")
# def export_results_task(self, data: dict) -> dict:
#     operation_id = data["operation_id"]
#     try:
#         update_progress(operation_id, "Export Results", 90)
#         unique_tmp_path = get_unique_result_path(TMP_PATH, operation_id)
#
#         result = export_pipeline_results(
#             intervals_with_text=deserialize(data["intervals_with_text"]),
#             speaker_to_label=data["speaker_to_label"],
#             speaker_to_file=data["speaker_to_file"],
#             label_to_file=data["label_to_file"],
#             s3_prefix=f"segments/{operation_id}",
#             merged_audio_path=data["merged_audio_path"],
#             results_path_op=unique_tmp_path / "final_results",
#             operation_id=operation_id,
#         )
#
#         # После успешного экспорта обновляем статус DONE и сохраняем presigned URL
#         mark_done(operation_id, result_url=result["json_url"])
#         return {
#             "operation_id": operation_id,
#             "json_s3_key": f"segments/{operation_id}/pipeline_intervals.json",
#             "json_url": result["json_url"],
#         }
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise













# @celery_app.task(bind=True, queue="cpu")
# def merge_audio_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Merge Audio", 5)
#         merged_audio_path = run_merge_audio_step(operation_id)
#         update_progress(operation_id, "Merge Audio", 10)
#
#         # сохраняем в temp
#         update_temp_data(operation_id, "MERGE_AUDIO", {"merged_audio_path": str(merged_audio_path)})
#         print(f"MERGE_AUDIO: {merged_audio_path}")
#         print(f"TMP_PATH={TMP_PATH} ({type(TMP_PATH)})")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="gpu")
# def diarization_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Diarization", 15)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#
#         merge_data = get_temp_data(operation_id, "MERGE_AUDIO")
#         merged_audio_path = Path(merge_data["merged_audio_path"])
#
#         diarization_result = diarization_step(
#             input_audio=merged_audio_path,
#             speakers_folder=PATH_TO_AUDIO,
#             output_dir=tmp / "diarization_rttm",
#             pad_end=0.4,
#         )
#
#         # сохраняем промежуточные результаты
#         update_temp_data(operation_id, "DIARIZATION", {"rttm_path": str(diarization_result)})
#
#         update_progress(operation_id, "Diarization", 25)
#         print(f"DIARIZATION: {diarization_result}")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="cpu")
# def merge_intervals_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Merge Intervals", 30)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#
#         diarization_data = get_temp_data(operation_id, "DIARIZATION")
#         rttm_path = Path(diarization_data["rttm_path"])
#
#         rttm_data = load_rttm(rttm_path)
#         ann = next(iter(rttm_data.values()))
#
#         intervals = [
#             (turn.start, turn.end, spk)
#             for turn, _, spk in ann.itertracks(yield_label=True)
#         ]
#
#         intervals_path = merge_intervals_step(intervals, tmp)
#
#         update_temp_data(operation_id, "MERGE_INTERVALS", {"intervals_path": str(intervals_path)})
#         update_progress(operation_id, "Merge Intervals", 35)
#         print(f"MERGE_INTERVALS: {intervals_path}")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="cpu")
# def vad_hungarian_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "VAD + Hungarian", 40)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#         wav_files = list(Path(PATH_TO_AUDIO).glob("*.wav"))
#
#         merge_data = get_temp_data(operation_id, "MERGE_AUDIO")
#         merged_audio_path = merge_data["merged_audio_path"]
#
#         merge_intervals_data = get_temp_data(operation_id, "MERGE_INTERVALS")
#         intervals_path = Path(merge_intervals_data["intervals_path"])
#
#         with open(intervals_path, "r") as f:
#             intervals_merged = [tuple(x) for x in json.load(f)]
#
#         vad_path = vad_hungarian_step(
#             intervals=intervals_merged,
#             merged_audio_path=Path(merged_audio_path),
#             wav_files=wav_files,
#             tmp_dir=tmp,
#         )
#
#         update_temp_data(operation_id, "VAD_HUNGARIAN", {
#             "vad_path": str(vad_path)
#         })
#         update_progress(operation_id, "VAD + Hungarian", 50)
#         print(f"VAD_HUNGARIAN intervals: {vad_path}")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="cpu")
# def extract_segments_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Extract Segments", 55)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#         wav_files = list(Path(PATH_TO_AUDIO).glob("*.wav"))
#
#         merge_intervals_data = get_temp_data(operation_id, "MERGE_INTERVALS")
#         intervals_path = Path(merge_intervals_data["intervals_path"])
#
#         vad_data = get_temp_data(operation_id, "VAD_HUNGARIAN")
#         vad_path = Path(vad_data["vad_path"])
#
#         with open(intervals_path, "r") as f:
#             intervals_merged = [tuple(x) for x in json.load(f)]
#
#
#         with open(vad_path, "r") as f:
#             vad_json = json.load(f)
#
#         speaker_to_label = vad_json["speaker_to_label"]
#
#         segments_file = prepare_audio_segments(
#             wav_files=wav_files,
#             intervals=intervals_merged,
#             speaker_to_label=speaker_to_label,
#             unique_tmp_path=tmp,
#             sample_rate=8000,
#         )
#
#         update_temp_data(operation_id, "EXTRACT_SEGMENTS", {
#             "segments_file": str(segments_file)
#         })
#
#         update_progress(operation_id, "Extract Segments", 65)
#         print(f"EXTRACT_SEGMENTS segments_file: {segments_file}")
#
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
#
# @celery_app.task(bind=True, queue="cpu")
# def transcription_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Transcription", 70)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#
#         extract_data = get_temp_data(operation_id, "EXTRACT_SEGMENTS")
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
#
#         update_temp_data(operation_id, "TRANSCRIPTION", {
#             "transcription_file": str(transcription_file)
#         })
#         update_progress(operation_id, "Transcription", 85)
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="cpu")
# def export_results_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Export Results", 90)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#
#         #trans_data = get_temp_data(operation_id, "TRANSCRIPTION")
#         trans_data = get_temp_data(operation_id, "TRANSCRIPTION")
#         transcription_file = Path(trans_data["transcription_file"])
#
#         # extract_data = get_temp_data(operation_id, "EXTRACT_SEGMENTS")
#         extract_data = get_temp_data(operation_id, "EXTRACT_SEGMENTS")
#         segments_file = Path(extract_data["segments_file"])
#         #vad_data = get_temp_data(operation_id, "VAD_HUNGARIAN")
#         vad_data = get_temp_data(operation_id, "VAD_HUNGARIAN")
#         vad_path = Path(vad_data["vad_path"])
#         #merge_data = get_temp_data(operation_id, "MERGE_AUDIO")
#         merge_data = get_temp_data(operation_id, "MERGE_AUDIO")
#         merged_audio_path = Path(merge_data["merged_audio_path"])
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
#
#         mark_done(operation_id, result_url=result["json_url"])
#         print(f"EXPORT_RESULTS: {result['json_url']}")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise




from functools import wraps

def with_db(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        db = SessionLocal()
        try:
            return func(db, *args, **kwargs)
        finally:
            db.close()
    return wrapper

# -------------------------
# Merge Audio
# -------------------------
@celery_app.task(bind=True, acks_late=True, queue="cpu", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 10})
@with_db
def merge_audio_task(db: Session, self, operation_id: str):
    try:
        set_step_status(db, operation_id, "MERGE_AUDIO", "RUNNING", progress=5)
        merged_audio_path = run_merge_audio_step(operation_id)
        update_temp_data(db, operation_id, "MERGE_AUDIO", {"merged_audio_path": str(merged_audio_path)})
        set_step_status(db, operation_id, "MERGE_AUDIO", "DONE", progress=10)
    except Exception as exc:
        set_step_status(db, operation_id, "MERGE_AUDIO", "FAILED")
        raise


# -------------------------
# Diarization
# -------------------------
@celery_app.task(bind=True, acks_late=True, queue="gpu", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 10})
@with_db
def diarization_task(db: Session, self, operation_id: str):
    try:
        set_step_status(db, operation_id, "DIARIZATION", "RUNNING", progress=15)
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
        set_step_status(db, operation_id, "DIARIZATION", "DONE", progress=25)
    except Exception as exc:
        set_step_status(db, operation_id, "DIARIZATION", "FAILED")
        raise


# -------------------------
# Merge Intervals
# -------------------------
@celery_app.task(bind=True, acks_late=True, queue="cpu", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 10})
@with_db
def merge_intervals_task(db: Session, self, operation_id: str):
    try:
        set_step_status(db, operation_id, "MERGE_INTERVALS", "RUNNING", progress=30)
        tmp = get_unique_result_path(TMP_PATH, operation_id)
        diarization_data = get_temp_data(db, operation_id, "DIARIZATION")
        rttm_path = Path(diarization_data["rttm_path"])

        rttm_data = load_rttm(rttm_path)
        ann = next(iter(rttm_data.values()))
        intervals = [(turn.start, turn.end, spk) for turn, _, spk in ann.itertracks(yield_label=True)]
        intervals_path = merge_intervals_step(intervals, tmp)
        update_temp_data(db, operation_id, "MERGE_INTERVALS", {"intervals_path": str(intervals_path)})
        set_step_status(db, operation_id, "MERGE_INTERVALS", "DONE", progress=35)
    except Exception as exc:
        set_step_status(db, operation_id, "MERGE_INTERVALS", "FAILED")
        raise


# -------------------------
# VAD + Hungarian
# -------------------------
@celery_app.task(bind=True, acks_late=True, queue="cpu", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 10})
@with_db
def vad_hungarian_task(db: Session, self, operation_id: str):
    try:
        set_step_status(db, operation_id, "VAD_HUNGARIAN", "RUNNING", progress=40)
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
        set_step_status(db, operation_id, "VAD_HUNGARIAN", "DONE", progress=50)
    except Exception as exc:
        set_step_status(db, operation_id, "VAD_HUNGARIAN", "FAILED")
        raise

# -------------------------
# Extract Segments
# -------------------------
@celery_app.task(bind=True, acks_late=True, queue="cpu", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 10})
@with_db
def extract_segments_task(db: Session, self, operation_id: str):
    try:
        set_step_status(db, operation_id, "EXTRACT_SEGMENTS", "RUNNING", progress=55)
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
        set_step_status(db, operation_id, "EXTRACT_SEGMENTS", "DONE", progress=65)
    except Exception as exc:
        set_step_status(db, operation_id, "EXTRACT_SEGMENTS", "FAILED")
        raise


# -------------------------
# Transcription
# -------------------------
@celery_app.task(bind=True, acks_late=True, queue="cpu", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 10})
@with_db
def transcription_task(db: Session, self, operation_id: str):
    try:
        set_step_status(db, operation_id, "TRANSCRIPTION", "RUNNING", progress=70)
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
        set_step_status(db, operation_id, "TRANSCRIPTION", "DONE", progress=85)
    except Exception as exc:
        set_step_status(db, operation_id, "TRANSCRIPTION", "FAILED")
        raise



# -------------------------
# Export Results
# -------------------------
@celery_app.task(bind=True, acks_late=True, queue="cpu", autoretry_for=(Exception,), retry_kwargs={'max_retries': 3, 'countdown': 10})
@with_db
def export_results_task(db: Session, self, operation_id: str):
    try:
        set_step_status(db, operation_id, "EXPORT_RESULTS", "RUNNING", progress=90)
        tmp = get_unique_result_path(TMP_PATH, operation_id)

        extract_data = get_temp_data(db, operation_id, "EXTRACT_SEGMENTS")
        segments_file = Path(extract_data["segments_file"])
        vad_data = get_temp_data(db, operation_id, "VAD_HUNGARIAN")
        vad_path = Path(vad_data["vad_path"])
        merge_data = get_temp_data(db, operation_id, "MERGE_AUDIO")
        merged_audio_path = Path(merge_data["merged_audio_path"])
        trans_data = get_temp_data(db, operation_id, "TRANSCRIPTION")
        transcription_file = Path(trans_data["transcription_file"])

        with open(segments_file, "r", encoding="utf-8") as f:
            segments_json = json.load(f)
        speaker_to_file_v = segments_json["speaker_to_file"]
        label_to_file_v = segments_json["label_to_file"]

        with open(vad_path, "r") as f:
            vad_json = json.load(f)
        speaker_to_label = vad_json["speaker_to_label"]

        with open(transcription_file, "r", encoding="utf-8") as f:
            transcription_json = json.load(f)
        intervals_with_text = transcription_json["intervals_with_text"]

        result = export_pipeline_results(
            intervals_with_text=intervals_with_text,
            speaker_to_label=speaker_to_label,
            speaker_to_file=speaker_to_file_v,
            label_to_file=label_to_file_v,
            s3_prefix=f"segments/{operation_id}",
            merged_audio_path=merged_audio_path,
            results_path_op=tmp / "final_results",
            operation_id=operation_id,
        )
        update_temp_data(db, operation_id, "EXPORT_RESULTS", {"result_url": result["json_url"]})
        set_step_status(db, operation_id, "EXPORT_RESULTS", "DONE", progress=100)
    except Exception as exc:
        set_step_status(db, operation_id, "EXPORT_RESULTS", "FAILED")
        raise