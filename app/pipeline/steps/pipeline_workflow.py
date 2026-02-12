# app/pipeline/steps/runner.py
from celery import chain
from sqlalchemy.orm import Session
from app.pipeline.steps.bd import SessionLocal
from app.pipeline.steps.pipeline_tasks import (
    merge_audio_task,
    diarization_task,
    merge_intervals_task,
    vad_hungarian_task,
    extract_segments_task,
    transcription_task,
    export_results_task
)

STEP_TASK_MAPPING = [
    ("MERGE_AUDIO", merge_audio_task),
    ("DIARIZATION", diarization_task),
    ("MERGE_INTERVALS", merge_intervals_task),
    ("VAD_HUNGARIAN", vad_hungarian_task),
    ("EXTRACT_SEGMENTS", extract_segments_task),
    ("TRANSCRIPTION", transcription_task),
    ("EXPORT_RESULTS", export_results_task),
]

def run_pipeline_chain(operation_id: str):
    workflow = chain(
        merge_audio_task.si(operation_id),
        diarization_task.si(operation_id),
        merge_intervals_task.si(operation_id),
        vad_hungarian_task.si(operation_id),
        extract_segments_task.si(operation_id),
        transcription_task.si(operation_id),
        export_results_task.si(operation_id),
    )
    return workflow.apply_async()