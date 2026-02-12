import json
import random
from pathlib import Path
from copy import deepcopy
from typing import List, Tuple
from app.pipeline.progress.VAD import (
    build_src_speaker_voiceprints,
    build_file_voiceprints,
    assign_unique_labels_to_speakers
)


Interval = Tuple[float, float, str]

def vad_hungarian_step(intervals: List[Interval], merged_audio_path: Path, wav_files: List[Path], tmp_dir: Path) -> Path:
    """
    Шаг пайплайна: VAD + Венгерский алгоритм для присвоения уникальных меток спикеров.

    Args:
        intervals: исходные интервалы (от merge_consecutive_intervals)
        merged_audio_path: основной аудиофайл
        wav_files: список исходных файлов спикеров
        tmp_dir: директория для временного хранения результата

    Returns:
        updated_intervals: интервалы с уникальными метками спикеров
    """
    tmp_dir.mkdir(parents=True, exist_ok=True)
    step_tmp_path = tmp_dir / "vad_hungarian_intervals.json"

    # ✅ если временный файл есть — загружаем его
    if step_tmp_path.exists():
        print(f"[VAD+Hungarian] Загружаем сохраненные интервалы {step_tmp_path}")
        # with open(step_tmp_path, "r") as f:
        #     data = json.load(f)
        # updated_intervals = [tuple(item) for item in data["intervals"]]
        # speaker_to_label = data["speaker_to_label"]
        return step_tmp_path

    random.seed(42)
    # выполняем VAD + эмбеддинги + Венгерский алгоритм
    spk_to_vec = build_src_speaker_voiceprints(intervals, merged_audio_path)
    file_to_vec = build_file_voiceprints(wav_files)
    speaker_to_label, _, _ = assign_unique_labels_to_speakers(spk_to_vec, file_to_vec)

    print(speaker_to_label)
    updated_intervals = deepcopy(intervals)
    for i in range(len(updated_intervals)):
        updated_intervals[i] = (updated_intervals[i][0], updated_intervals[i][1], speaker_to_label[updated_intervals[i][2]])

    # атомарное сохранение
    tmp_file = step_tmp_path.with_name(step_tmp_path.name + ".tmp")
    with open(tmp_file, "w") as f:
        json.dump({"intervals": updated_intervals, "speaker_to_label": speaker_to_label}, f)
    tmp_file.rename(step_tmp_path)

    return step_tmp_path