from pathlib import Path

import torch
import torchaudio
from pyannote.audio import Pipeline
from pyannote.database.util import load_rttm
from app.pipeline.global_diarization import get_pipeline


# def diarization_step(input_audio: Path, speakers_folder, output_dir: Path, pad_end: float = 0.4):
#     output_dir.mkdir(parents=True, exist_ok=True)
#     rttm_path = output_dir / f"{input_audio.stem}.rttm"
#
#     if rttm_path.exists():
#         rttm_data = load_rttm(rttm_path)
#         ann = next(iter(rttm_data.values()))
#         intervals = [(turn.start, turn.end + pad_end, spk) for turn, _, spk in ann.itertracks(yield_label=True)]
#         return {input_audio.name: intervals}
#
#     # рассчёт диаризации
#     max_speakers = sum(1 for f in Path(speakers_folder).iterdir() if f.suffix.lower() == ".wav")
#     waveform, sr = torchaudio.load(input_audio)
#     if waveform.size(0) > 1:
#         waveform = waveform.mean(dim=0, keepdim=True)
#     if sr != 16000:
#         waveform = torchaudio.functional.resample(waveform, sr, 16000)
#         sr = 16000
#
#     pipeline = get_pipeline()
#     device = pipeline.device
#
#     with torch.inference_mode():
#         out = pipeline({"waveform": waveform.to(device), "sample_rate": sr}, num_speakers=max_speakers)
#     ann = getattr(out, "speaker_diarization", getattr(out, "annotation", out))
#
#     tmp_rttm = rttm_path.with_suffix(".tmp.rttm")
#     try:
#         with open(tmp_rttm, "w") as f:
#             ann.write_rttm(f)
#         tmp_rttm.rename(rttm_path)
#     finally:
#         if tmp_rttm.exists():
#             tmp_rttm.unlink(missing_ok=True)
#
#     intervals = [(turn.start, turn.end + pad_end, spk) for turn, _, spk in ann.itertracks(yield_label=True)]
#     return {input_audio.name: intervals}








# def diarization_step(input_audio: Path, speakers_folder: Path, output_dir: Path, pad_end: float = 0.4):
#     """
#     Диаризация аудио с кэшированием результата.
#
#     Args:
#         input_audio: путь к аудио
#         speakers_folder: папка с файлами спикеров для определения max_speakers
#         output_dir: директория для RTTM и кэша
#         pad_end: дополнительное время в конце каждого сегмента (сек)
#
#     Returns:
#         dict: {имя файла: [(start, end+pad_end, spk), ...]}
#     """
#     output_dir.mkdir(parents=True, exist_ok=True)
#
#     # --- Формируем уникальный хэш от параметров ---
#     key_str = f"{input_audio.stem}_pad{pad_end}"
#     rttm_path = output_dir / f"{key_str}.rttm"
#
#     # --- Если файл есть, загружаем и возвращаем ---
#     if rttm_path.exists():
#         rttm_data = load_rttm(rttm_path)
#         ann = next(iter(rttm_data.values()))
#         intervals = [(turn.start, turn.end + pad_end, spk) for turn, _, spk in ann.itertracks(yield_label=True)]
#         return {input_audio.name: intervals}
#
#     # --- Рассчёт диаризации ---
#     max_speakers = sum(1 for f in Path(speakers_folder).iterdir() if f.suffix.lower() == ".wav")
#
#     waveform, sr = torchaudio.load(input_audio)
#     if waveform.size(0) > 1:
#         waveform = waveform.mean(dim=0, keepdim=True)
#     if sr != 16000:
#         waveform = torchaudio.functional.resample(waveform, sr, 16000)
#         sr = 16000
#
#     pipeline = get_pipeline()
#     device = pipeline.device
#
#     with torch.inference_mode():
#         out = pipeline({"waveform": waveform.to(device), "sample_rate": sr}, num_speakers=max_speakers)
#
#     # pyannote может вернуть либо speaker_diarization, либо annotation
#     ann = getattr(out, "speaker_diarization", getattr(out, "annotation", out))
#
#     # --- Атомарная запись RTTM ---
#     tmp_rttm = rttm_path.with_suffix(".tmp.rttm")
#     try:
#         with open(tmp_rttm, "w") as f:
#             ann.write_rttm(f)
#         tmp_rttm.rename(rttm_path)
#     finally:
#         if tmp_rttm.exists():
#             tmp_rttm.unlink(missing_ok=True)
#
#     intervals = [(turn.start, turn.end + pad_end, spk) for turn, _, spk in ann.itertracks(yield_label=True)]
#     return {input_audio.name: intervals}



def diarization_step(input_audio: Path, speakers_folder: Path, output_dir: Path, pad_end: float = 0.4):
    """
    Диаризация аудио с кэшированием результата.

    Args:
        input_audio: путь к аудио
        speakers_folder: папка с файлами спикеров для определения max_speakers
        output_dir: директория для RTTM и кэша
        pad_end: дополнительное время в конце каждого сегмента (сек)

    Returns:
        dict: {имя файла: [(start, end+pad_end, spk), ...]}
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Формируем уникальный хэш от параметров ---
    key_str = f"{input_audio.stem}_pad{pad_end}"
    rttm_path = output_dir / f"{key_str}.rttm"

    # --- Если файл есть, загружаем и возвращаем ---
    if rttm_path.exists():
        return rttm_path

    # --- Рассчёт диаризации ---
    max_speakers = sum(1 for f in Path(speakers_folder).iterdir() if f.suffix.lower() == ".wav")

    waveform, sr = torchaudio.load(input_audio)
    if waveform.size(0) > 1:
        waveform = waveform.mean(dim=0, keepdim=True)
    if sr != 16000:
        waveform = torchaudio.functional.resample(waveform, sr, 16000)
        sr = 16000

    pipeline = get_pipeline()
    device = pipeline.device

    with torch.inference_mode():
        out = pipeline({"waveform": waveform.to(device), "sample_rate": sr}, num_speakers=max_speakers)

    # pyannote может вернуть либо speaker_diarization, либо annotation
    ann = getattr(out, "speaker_diarization", getattr(out, "annotation", out))

    # --- Атомарная запись RTTM ---
    tmp_rttm = rttm_path.with_suffix(".tmp.rttm")
    try:
        with open(tmp_rttm, "w") as f:
            ann.write_rttm(f)
        tmp_rttm.rename(rttm_path)
    finally:
        if tmp_rttm.exists():
            tmp_rttm.unlink(missing_ok=True)

    intervals = [(turn.start, turn.end + pad_end, spk) for turn, _, spk in ann.itertracks(yield_label=True)]
    return rttm_path