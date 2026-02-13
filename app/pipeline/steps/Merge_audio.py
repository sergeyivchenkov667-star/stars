from pathlib import Path
from app.pipeline.progress.Merge_audio import make_one_channel_audio, save_audio
from app.pipeline.utils import get_unique_result_path
from app.pipeline.config import PATH_TO_AUDIO, TMP_PATH
SAMPLE_RATE = 8000

def run_merge_audio_step(operation_id: str) -> Path:
    tmp_dir = get_unique_result_path(TMP_PATH, operation_id)
    output_path = tmp_dir / "Merged.wav"

    # ✅ Проверка: если файл уже есть, ничего не делаем
    if output_path.exists():
        return output_path

    # --- остальная логика шага ---
    audio_dir = Path(PATH_TO_AUDIO)
    wav_files = [f for f in audio_dir.iterdir() if f.suffix.lower() == ".wav"]


    if not wav_files:
        raise FileNotFoundError("Нет входных wav-файлов")


    merged = make_one_channel_audio(wav_files, SAMPLE_RATE)

    # атомарная запись
    tmp_output = tmp_dir / "Merged_tmp.wav"
    save_audio(merged, tmp_output, sample_rate=SAMPLE_RATE)
    tmp_output.rename(output_path)

    return output_path