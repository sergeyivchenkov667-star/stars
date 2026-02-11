# app/pipeline/shag/runner.py
from celery import chain
from sqlalchemy.orm import Session
from app.pipeline.shag.bd import SessionLocal
from app.pipeline.shag.tasks import (
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

# def run_pipeline_chain(operation_id: str):
#     db: Session = SessionLocal()
#     try:
#         from app.pipeline.shag.bd import PipelineOperation
#         op = db.query(PipelineOperation).filter_by(operation_id=operation_id).first()
#         if not op:
#             raise ValueError(f"PipelineOperation {operation_id} not found")
#
#         tasks_to_run = []
#         start_next = False
#
#         for step_name, task in STEP_TASK_MAPPING:
#             step_status = "PENDING" if op.step != step_name else op.status
#             if step_status in ("PENDING", "FAILED"):
#                 start_next = True
#             if start_next:
#                 tasks_to_run.append(task.si(operation_id))
#
#         if not tasks_to_run:
#             print(f"Pipeline {operation_id} уже завершен")
#             return None
#
#         workflow = tasks_to_run[0]
#         for t in tasks_to_run[1:]:
#             workflow |= t
#
#         return workflow.apply_async()
#     finally:
#         db.close()
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