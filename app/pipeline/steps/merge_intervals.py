import json
from pathlib import Path
from typing import List, Tuple

from app.pipeline.progress.intervals_merged import merge_consecutive_intervals

Interval = Tuple[float, float, str]

def merge_intervals_step(intervals: List[Interval], tmp_path: Path) -> Path:
    merge_tmp_path = tmp_path / "merge_intervals.json"
    if merge_tmp_path.exists():
        print(f"[merge_intervals_step] Загружаем сохраненный файл {merge_tmp_path}")
        return merge_tmp_path

    merged = merge_consecutive_intervals(intervals)
    merge_tmp_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_tmp_path = merge_tmp_path.with_name(merge_tmp_path.name + ".tmp")
    with open(tmp_tmp_path, "w") as f:
        json.dump(merged, f)
    tmp_tmp_path.rename(merge_tmp_path)
    print(f"[merge_intervals_step] Результат сохранен атомарно во временный файл {merge_tmp_path}")
    return merge_tmp_path