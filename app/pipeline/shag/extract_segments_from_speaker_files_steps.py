# %% md
### Шаг: Вырезание сегментов аудио по спикерам (с возможностью кэширования)
# %%

from pathlib import Path
from typing import List, Dict, Tuple, Any
import os
import torch
import torchaudio
import pickle

from app.pipeline.utils import get_speaker_name
import hashlib
import json


# def prepare_audio_segments(
#         wav_files: List[Path],
#         intervals: List[Tuple[float, float, str]],
#         speaker_to_label: Dict[str, str],
#         unique_tmp_path: Path,
#         sample_rate: int = 8000,
#         cache_name: str = "audio_segments.pkl"
# ) -> Tuple[List[Dict[str, Any]], List[str], List[Tuple[float, float, str]], Dict[str, str], Dict[str, str]]:
#     """
#     Загружает аудио файлов спикеров, вырезает сегменты по интервалам, сохраняет сегменты и метаданные.
#     Результат можно кэшировать для повторного использования.
#
#     Возвращает:
#         interval_segments: полный словарь метаданных сегментов
#         all_segment_paths: список путей ко всем сегментам
#         interval_segments_basic: список (start, end, path) для простого использования
#     """
#
#     # Папка для сегментов
#     SEGMENTS_DIR = unique_tmp_path / "audio_segments"
#     os.makedirs(SEGMENTS_DIR, exist_ok=True)
#
#     cache_path = SEGMENTS_DIR / cache_name
#     if cache_path.exists():
#         print(f"[INFO] Загружаем кэшированные сегменты из {cache_path}")
#         with open(cache_path, "rb") as f:
#             interval_segments, all_segment_paths, speaker_to_file, label_to_file = pickle.load(f)
#         interval_segments_basic = [
#             (d["start"], d["end"], d["segment_path"]) for d in interval_segments
#         ]
#         return interval_segments, all_segment_paths, interval_segments_basic, speaker_to_file, label_to_file
#
#     # --- 1. Маппинг label -> файл, speaker -> файл ---
#     label_to_file: Dict[str, str] = {get_speaker_name(f): f for f in wav_files}
#
#     speaker_to_file: Dict[str, str] = {}
#     for spk, label in speaker_to_label.items():
#         fpath = label_to_file.get(label)
#         if fpath is not None:
#             speaker_to_file[spk] = fpath
#         else:
#             print(f"[WARN] Для label '{label}' не найден файл в merged_audio")
#
#     print("\n[INFO] label -> file:")
#     for lbl, p in label_to_file.items():
#         print(f"  {lbl:15s} -> {p}")
#
#     print("\n[INFO] diarization speaker -> file:")
#     for spk, p in speaker_to_file.items():
#         print(f"  {spk:10s} -> {p}")
#
#     # --- 2. Вспомогательные функции ---
#     def load_and_prepare_wav(path: str, target_sr: int) -> Tuple[torch.Tensor, int]:
#         wav, sr = torchaudio.load(path)
#         if wav.size(0) > 1:
#             wav = wav.mean(dim=0, keepdim=True)
#         if sr != target_sr:
#             wav = torchaudio.functional.resample(wav, sr, target_sr)
#             sr = target_sr
#         return wav, sr
#
#     # --- 3. Вырезание сегментов ---
#     spk_audio: Dict[str, Tuple[torch.Tensor, int]] = {}
#     spk_in_intervals = sorted({spk for _, _, spk in intervals})
#     for spk in spk_in_intervals:
#         fpath = speaker_to_file.get(spk)
#         if fpath is None:
#             print(f"[SEGMENTS] Для {spk} нет файла в merged_audio, пропускаем этот спикер.")
#             continue
#         print(f"[SEGMENTS] Загружаем файл спикера {spk}: {os.path.basename(fpath)}")
#         wav, sr = load_and_prepare_wav(fpath, sample_rate)
#         spk_audio[spk] = (wav, sr)
#
#     all_segment_paths: List[str] = []
#     interval_segments: List[Dict[str, Any]] = []
#
#     for idx, (start, end, spk) in enumerate(intervals):
#         if spk not in spk_audio:
#             print(f"[SEGMENTS] interval {idx:04d} ({spk}) — нет аудио спикера, пропуск")
#             continue
#         wav, sr = spk_audio[spk]
#         if end <= start:
#             print(f"[SEGMENTS] interval {idx:04d} ({spk}) — end <= start, пропуск")
#             continue
#
#         start_sample = max(0, int(start * sr))
#         end_sample = min(wav.size(1), int(end * sr))
#         if end_sample <= start_sample:
#             print(f"[SEGMENTS] interval {idx:04d} ({spk}) — пустой диапазон после обрезки, пропуск")
#             continue
#
#         seg_wave = wav[:, start_sample:end_sample]
#         spk_label = speaker_to_label.get(spk, spk)
#         safe_label = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in spk_label)
#         seg_filename = f"seg_{idx:04d}_{safe_label}.wav"
#         seg_path = SEGMENTS_DIR / seg_filename
#
#         torchaudio.save(seg_path, seg_wave, sr)
#
#         all_segment_paths.append(str(seg_path))
#         interval_segments.append(
#             {
#                 "start": start,
#                 "end": end,
#                 "speaker_id": spk,
#                 "speaker_label": spk_label,
#                 "segment_path": str(seg_path),
#                 "speaker_file": speaker_to_file.get(spk),
#             }
#         )
#
#     # --- 4. Сохраняем кэш ---
#     tmp_cache = cache_path.with_name(cache_path.name + ".tmp")
#     with open(tmp_cache, "wb") as f:
#         pickle.dump((interval_segments, all_segment_paths, speaker_to_file, label_to_file), f)
#     tmp_cache.rename(cache_path)
#
#     interval_segments_basic = [
#         (d["start"], d["end"], d["segment_path"])
#         for d in interval_segments
#     ]
#     print(interval_segments, all_segment_paths, interval_segments_basic, speaker_to_file, label_to_file)
#     return interval_segments, all_segment_paths, interval_segments_basic, speaker_to_file, label_to_file



def hash_intervals(intervals: List[Tuple[float, float, str]], speaker_to_label: Dict[str, str]) -> Path:
    """
    Создаёт короткий детерминированный hash для intervals + speaker_to_label
    """
    m = hashlib.sha256()
    m.update(json.dumps(intervals, sort_keys=True).encode())
    m.update(json.dumps(speaker_to_label, sort_keys=True).encode())
    return m.hexdigest()[:16]  # короткий hash

def prepare_audio_segments(
        wav_files: List[Path],
        intervals: List[Tuple[float, float, str]],
        speaker_to_label: Dict[str, str],
        unique_tmp_path: Path,
        sample_rate: int = 8000
) -> Tuple[List[Dict[str, Any]], List[str], List[Tuple[float, float, str]], Dict[str, str], Dict[str, str]]:
    """
    Загружает аудио файлов спикеров, вырезает сегменты по интервалам, сохраняет сегменты и метаданные.
    Кэш хранится в JSON + hash(intervals + speaker_to_label) в имени.
    """
    SEGMENTS_DIR = unique_tmp_path / "audio_segments"
    SEGMENTS_DIR.mkdir(parents=True, exist_ok=True)

    cache_hash = hash_intervals(intervals, speaker_to_label)
    cache_path = SEGMENTS_DIR / f"audio_segments_{cache_hash}.json"

    # --- 1️⃣ Попробуем загрузить кэш ---
    if cache_path.exists():
        print(f"[INFO] Загружаем кэшированные сегменты из {cache_path}")
        return cache_path

    # --- 2️⃣ Маппинг label -> файл, speaker -> файл ---
    label_to_file: Dict[str, str] = {get_speaker_name(f): str(f) for f in wav_files}
    speaker_to_file: Dict[str, str] = {}
    for spk, label in speaker_to_label.items():
        fpath = label_to_file.get(label)
        if fpath:
            speaker_to_file[spk] = fpath
        else:
            print(f"[WARN] Для label '{label}' не найден файл")

    print("\n[INFO] label -> file:")
    for lbl, p in label_to_file.items():
        print(f"  {lbl:15s} -> {p}")

    print("\n[INFO] diarization speaker -> file:")
    for spk, p in speaker_to_file.items():
        print(f"  {spk:10s} -> {p}")

    # --- 3️⃣ Вспомогательные функции ---
    def load_and_prepare_wav(path: str, target_sr: int) -> Tuple[torch.Tensor, int]:
        wav, sr = torchaudio.load(path)
        if wav.size(0) > 1:
            wav = wav.mean(dim=0, keepdim=True)
        if sr != target_sr:
            wav = torchaudio.functional.resample(wav, sr, target_sr)
            sr = target_sr
        return wav, sr

    spk_audio: Dict[str, Tuple[torch.Tensor, int]] = {}
    for spk in sorted({spk for _, _, spk in intervals}):
        fpath = speaker_to_file.get(spk)
        if fpath is None:
            print(f"[SEGMENTS] Для {spk} нет файла в merged_audio, пропускаем этот спикер.")
            continue
        print(f"[SEGMENTS] Загружаем файл спикера {spk}: {os.path.basename(fpath)}")
        wav, sr = load_and_prepare_wav(fpath, sample_rate)
        spk_audio[spk] = (wav, sr)

    all_segment_paths: List[str] = []
    interval_segments: List[Dict[str, Any]] = []

    for idx, (start, end, spk) in enumerate(intervals):
        if spk not in spk_audio:
            print(f"[SEGMENTS] interval {idx:04d} ({spk}) — нет аудио спикера, пропуск")
            continue
        wav, sr = spk_audio[spk]
        if end <= start:
            print(f"[SEGMENTS] interval {idx:04d} ({spk}) — end <= start, пропуск")
            continue
        start_sample = max(0, int(start * sr))
        end_sample = min(wav.size(1), int(end * sr))
        if end_sample <= start_sample:
            print(f"[SEGMENTS] interval {idx:04d} ({spk}) — пустой диапазон после обрезки, пропуск")
            continue

        seg_wave = wav[:, start_sample:end_sample]
        spk_label = speaker_to_label.get(spk, spk)
        safe_label = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in spk_label)
        seg_filename = f"seg_{idx:04d}_{safe_label}.wav"
        seg_path = SEGMENTS_DIR / seg_filename

        # ✅ атомарная запись
        tmp_path = seg_path.with_name(seg_path.name + ".tmp.wav")
        torchaudio.save(tmp_path, seg_wave, sr)
        tmp_path.rename(seg_path)

        all_segment_paths.append(str(seg_path))
        interval_segments.append({
            "start": start,
            "end": end,
            "speaker_id": spk,
            "speaker_label": spk_label,
            "segment_path": str(seg_path),
            "speaker_file": speaker_to_file.get(spk),
        })

    # --- 4️⃣ Сохраняем JSON кэш ---
    tmp_cache = cache_path.with_name(cache_path.name + ".tmp")
    with open(tmp_cache, "w", encoding="utf-8") as f:
        json.dump({
            "interval_segments": interval_segments,
            "all_segment_paths": all_segment_paths,
            "speaker_to_file": speaker_to_file,
            "label_to_file": label_to_file
        }, f, ensure_ascii=False, indent=2)
    tmp_cache.rename(cache_path)

    interval_segments_basic = [(d["start"], d["end"], d["segment_path"]) for d in interval_segments]
    return cache_path
