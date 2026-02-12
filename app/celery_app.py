from celery import Celery

celery_app = Celery(
    "audio_pipeline",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0",
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_track_started=True,
    task_acks_late = True,
    worker_prefetch_multiplier = 1,
    task_reject_on_worker_lost = True
)

celery_app.autodiscover_tasks(["app.pipeline.steps.pipeline_tasks"])
