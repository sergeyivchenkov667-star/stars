import os
from pathlib import Path
from typing import List, Tuple

import torch
import torchaudio
from pyannote.audio import Pipeline


Interval = Tuple[float, float, str]  # (start, end, speaker)


def get_intervals_diarization(
    pipeline: Pipeline,
    input_path: Path,
    max_speakers: int,
    save_rttm_path: Path,
    pad_end: float = 0.4
) -> List[Interval]:
    """Запускает диаризацию и сохраняет RTTM."""
    os.makedirs(save_rttm_path.parent, exist_ok=True)

    # Загружаем аудио -> mono -> 16kHz
    waveform, sr = torchaudio.load(input_path)
    if waveform.size(0) > 1:
        waveform = waveform.mean(dim=0, keepdim=True)  # mono

    target_sr = 16000
    if sr != target_sr:
        waveform = torchaudio.functional.resample(waveform, sr, target_sr)
        sr = target_sr

    device = getattr(pipeline, "device", torch.device("cpu"))
    with torch.inference_mode():
        out = pipeline({"waveform": waveform.to(device), "sample_rate": sr},
                       num_speakers=max_speakers)
    print("gfh")
    ann = getattr(out, "speaker_diarization", None) or getattr(out, "annotation", None) or out

    # Атомарная запись RTTM
    tmp_rttm = save_rttm_path.with_suffix(".tmp")
    with open(tmp_rttm, 'w') as f:
        ann.write_rttm(f)
    tmp_rttm.rename(save_rttm_path)

    # Возвращаем интервалы с добавлением pad_end
    return [(turn.start, turn.end + pad_end, spk)
            for turn, _, spk in ann.itertracks(yield_label=True)]