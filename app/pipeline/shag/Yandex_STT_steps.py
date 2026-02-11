from datetime import datetime
import json
from typing import List, Dict, Any
from app.pipeline.progress.Yandex_SST import transcribe_with_yandex_async
import tempfile, os
from pathlib import Path

def json_safe(obj):
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_safe(v) for v in obj]
    return obj


def transcribe_step_yandex(
    interval_segments: List[Dict[str, Any]],
    output_path: str | Path,
    *,
    force: bool = False,
    model: str = "general",
    language_code: str = "ru-RU",
) -> Path:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not force:
        return output_path

    intervals_with_text, intervals_with_text_all = transcribe_with_yandex_async(
        interval_segments=interval_segments,
        not_recognized_text="Распознать текст не удалось",
    )

    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output_path.parent, delete=False) as tmp:
        json.dump({
            "meta": {"engine": "yandex_speechkit_v3", "model": model, "language": language_code, "created_at": datetime.utcnow().isoformat(), "intervals_total": len(interval_segments), "intervals_recognized": len(intervals_with_text)},
            "intervals_all": json_safe(intervals_with_text_all),
            "intervals_with_text": json_safe(intervals_with_text)
        }, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = Path(tmp.name)
    tmp_path.replace(output_path)

    return output_path  # возвращаем путь к JSON
