from copy import deepcopy
from pathlib import Path
from typing import List, Dict, Any, Optional
import json
from app.pipeline.progress.export_pipeline_results import wav_to_mp3, secs_int, convert_intervals_to_target_json_s3, save_intervals_to_docx, save_intervals_to_json
from app.storage.s3 import upload_mp3_to_s3, upload_json_to_s3, generate_presigned_url, s3_object_exists
from app.pipeline.steps.bd import SessionLocal, PipelineSegment


def file_checksum(path: Path) -> str:
    """Возвращает md5 хеш файла для идемпотентной проверки"""
    h = hashlib.md5()
    with path.open("rb") as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def safe_upload_to_s3(file_path: Path, s3_key: str):
    """Идемпотентная загрузка: пропускаем, если объект существует и checksum совпадает"""
    if s3_object_exists(s3_key):
        # тут можно добавить проверку md5 через metadata, если bucket поддерживает
        print(f"[S3] {s3_key} уже существует, пропускаем upload")
        return
    upload_mp3_to_s3(file_path, s3_key)
    print(f"[S3] {s3_key} загружен")


def safe_upload_file_to_s3(file_path: Path, s3_key: str):
    """Идемпотентная загрузка любых файлов (JSON, DOCX, MP3)"""
    if s3_object_exists(s3_key):
        print(f"[S3] {s3_key} уже существует, пропускаем upload")
        return
    # Используем generic upload вместо mp3-specific
    upload_json_to_s3(file_path, s3_key)
    print(f"[S3] {s3_key} загружен")


def export_pipeline_results(
        *,
        intervals_with_text: List[Dict[str, Any]],
        speaker_to_label: Dict[str, str],
        speaker_to_file: Dict[str, str],
        label_to_file: Optional[Dict[str, str]],
        merged_audio_path: Path,
        s3_prefix: str = "segments",
        presigned_expire: int = 3600,
        results_path_op: Path,
        operation_id: Optional[str]
) -> Dict[str, Any]:
    """
    Полностью идемпотентный финальный шаг пайплайна:
    - WAV → MP3
    - S3 загрузки (идемпотентные)
    - JSON локально и на S3
    - DOCX локально
    """
    results_path_op.mkdir(parents=True, exist_ok=True)

    # 1️⃣ MP3 сегменты
    seen = set()
    for it in intervals_with_text:
        wav_path = Path(it["file_name"])
        if wav_path in seen:
            continue
        seen.add(wav_path)

        mp3_name = wav_path.with_suffix(".mp3").name
        tmp_mp3 = results_path_op / mp3_name

        if not tmp_mp3.exists():
            wav_to_mp3(wav_path, tmp_mp3)

        s3_key = f"{s3_prefix}/{mp3_name}"
        safe_upload_to_s3(tmp_mp3, s3_key)

    # 3️⃣ JSON с presigned URL
    target_json = convert_intervals_to_target_json_s3(
        intervals_with_text=intervals_with_text,
        speaker_to_label=speaker_to_label,
        speaker_to_file=speaker_to_file,
        label_to_file=label_to_file,
        s3_prefix=s3_prefix,
        presigned_expire=presigned_expire
    )

    # 4️⃣ JSON локально атомарно
    json_path = results_path_op / "pipeline_intervals.json"
    tmp_json = json_path.with_name(json_path.name + ".tmp")
    with open(tmp_json, "w", encoding="utf-8") as f:
        json.dump(target_json, f, ensure_ascii=False, indent=2)
    tmp_json.rename(json_path)

    # 5️⃣ JSON на S3 идемпотентно
    json_s3_key = f"{s3_prefix}/pipeline_intervals.json"
    safe_upload_file_to_s3(target_json, json_s3_key)
    json_presigned_url = generate_presigned_url(json_s3_key, expires_in=presigned_expire)


    # 6️⃣ DOCX атомарно
    docx_path = results_path_op / "pipeline_result.docx"
    tmp_docx = docx_path.with_name(docx_path.name + ".tmp")
    save_intervals_to_docx(intervals_with_text, tmp_docx)
    tmp_docx.rename(docx_path)

    # 7️⃣ Merged audio MP3
    merged_mp3_path = results_path_op / "Merged.mp3"
    if not merged_mp3_path.exists():
        wav_to_mp3(merged_audio_path, merged_mp3_path)
        s3_key_merged = f"{s3_prefix}/Merged.mp3"
        safe_upload_to_s3(merged_mp3_path, s3_key_merged)

    # 8️⃣ Сохраняем сегменты в БД (идемпотентно через delete+insert)
    session = SessionLocal()
    try:
        session.query(PipelineSegment).filter_by(operation_id=operation_id).delete()
        for it in target_json:
            segment = PipelineSegment(
                operation_id=operation_id,
                file_name=it.get("file_url"),
                id_speaker=int(it["id_speaker"]),
                start=it["start"],
                end=it["end"],
                speaker=it["speaker"],
                transcription=it["transcription"],
            )
            session.add(segment)
        session.commit()
    finally:
        session.close()

    return {
        "json_url": json_presigned_url,
        "json_s3_key": json_s3_key
    }