from pathlib import Path
import hashlib
import logging
from typing import List, Dict, Any, Optional
import json
from requests.exceptions import Timeout, ConnectionError
from app.pipeline.progress.export_pipeline_results import wav_to_mp3, convert_intervals_to_target_json_s3, save_intervals_to_docx
from app.storage.s3 import upload_mp3_to_s3, upload_json_to_s3, s3_object_exists, upload_file_to_s3, get_s3_object_md5
from app.pipeline.steps.bd import PipelineSegment
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

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
        remote_md5 = get_s3_object_md5(s3_key)
        local_md5 = file_checksum(file_path)
        if remote_md5 == local_md5:
            logger.info(f"[S3] {s3_key} уже существует и md5 совпадает, пропускаем upload")
            return
        else:
            logger.warning(f"[S3] {s3_key} существует, но md5 не совпадает, перезаписываем")
    try:
        upload_mp3_to_s3(file_path, s3_key)
        print(f"[S3] {s3_key} загружен")
    except (Timeout, ConnectionError) as e:
        print(f"[S3] {s3_key} временная ошибка: {e}")
        raise


def safe_upload_file_to_s3(file_path: Path, s3_key: str):
    """Идемпотентная загрузка любых файлов (JSON, DOCX, MP3)"""
    if s3_object_exists(s3_key):
        if s3_object_exists(s3_key):
            remote_md5 = get_s3_object_md5(s3_key)
            local_md5 = file_checksum(file_path)
            if remote_md5 == local_md5:
                logger.info(f"[S3] {s3_key} уже существует и md5 совпадает, пропускаем upload")
                return
            else:
                logger.warning(f"[S3] {s3_key} существует, но md5 не совпадает, перезаписываем")
    try:
        upload_json_to_s3(file_path, s3_key)
        print(f"[S3] {s3_key} загружен")
    except (Timeout, ConnectionError) as e:
        print(f"[S3] {s3_key} временная ошибка: {e}")
        raise




def export_pipeline_results(
        *,
        db: Session,
        intervals_with_text: List[Dict[str, Any]],
        speaker_to_label: Dict[str, str],
        speaker_to_file: Dict[str, str],
        label_to_file: Optional[Dict[str, str]],
        merged_audio_path: Path,
        s3_prefix: str = "segments",
        results_path_op: Path,
        operation_id: str
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


    # 6️⃣ DOCX атомарно
    docx_path = results_path_op / "pipeline_result.docx"
    tmp_docx = docx_path.with_name(docx_path.name + ".tmp")
    save_intervals_to_docx(intervals_with_text, tmp_docx)
    tmp_docx.rename(docx_path)

    docx_s3_key = f"{s3_prefix}/pipeline_result.docx"
    upload_file_to_s3(docx_path, docx_s3_key)

    # 7️⃣ Merged audio MP3
    merged_mp3_path = results_path_op / "Merged.mp3"
    if not merged_mp3_path.exists():
        wav_to_mp3(merged_audio_path, merged_mp3_path)
        s3_key_merged = f"{s3_prefix}/Merged.mp3"
        safe_upload_to_s3(merged_mp3_path, s3_key_merged)

    # 8️⃣ Сохраняем сегменты в БД (идемпотентно через delete+insert)
    db.query(PipelineSegment).filter_by(operation_id=operation_id).delete()
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
        db.add(segment)
    db.commit()

    return {
        "json_s3_key": json_s3_key,
        "docx_s3_key": docx_s3_key,
    }