import os
import re
import json
from pathlib import Path
from copy import deepcopy
from typing import List, Dict, Any, Optional
from uuid import uuid4
from docx import Document
from pydub import AudioSegment
from app.storage.s3 import generate_presigned_url


# %%
def format_time_hms(seconds: float) -> str:
    """Перевод секунд в формат h:mm:ss."""
    total_seconds = int(round(float(seconds)))
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h}:{m:02d}:{s:02d}"


def format_intervals_for_docx(
        intervals_with_text: List[Dict],
) -> List[str]:
    """
    Преобразует интервалы в строки вида:

    <Имя спикера> <h:mm:ss>-<h:mm:ss>: <Транскрибированный текст>

    Возвращает список строк (без пустых строк между ними).
    """
    lines: List[str] = []
    for item in intervals_with_text:
        speaker_name = (
                item.get("speaker_label")
                or item.get("id_speaker")
                or item.get("speaker")
                or "Unknown"
        )
        speaker_name = str(speaker_name)

        start_str = format_time_hms(item["start"])
        end_str = format_time_hms(item["end"])
        text = item["transcription"]

        line = f"{speaker_name} {start_str}-{end_str}: {text}"
        lines.append(line)
    return lines


def format_intervals_as_big_text(
        intervals_with_text: List[Dict],
        sep: str = "\n\n",
) -> str:
    """
    Возвращает один большой текст, в котором строки из format_intervals_for_docx
    разделены `sep` (по умолчанию — пустая строка между блоками).
    """
    lines = format_intervals_for_docx(intervals_with_text)
    return sep.join(lines)




# %%
def save_intervals_to_docx(
        intervals_with_text: List[Dict],
        output_path: str,
) -> None:
    """
    Формирует .docx вида:
    <Имя спикера> <h:mm:ss>-<h:mm:ss>: <Транскрибированный текст>

    Между записями — пустая строка.
    """
    doc = Document()
    lines = format_intervals_for_docx(intervals_with_text)
    for line in lines:
        doc.add_paragraph(line)
    doc.save(output_path)


def save_intervals_to_json(
        intervals_with_text: List[Dict[str, Any]],
        output_path: str,
) -> None:
    """
    Сохраняет JSON в формате:
    [
      {
        "file_name": "...",
        "id_speaker": ...,
        "start": ...,
        "end": ...,
        "transcription": "..."
      },
      ...
    ]
    """
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(intervals_with_text, f, ensure_ascii=False, indent=2)


# %%
def wav_to_mp3(input_path: str, output_path: str | None = None, bitrate: str = "192k") -> str:
    """
    Конвертация WAV в MP3 с помощью pydub + ffmpeg.

    :param input_path: путь к исходному .wav файлу
    :param output_path: путь к целевому .mp3 файлу (если None, то тот же путь, но с .mp3)
    :param bitrate: целевой битрейт (например, "128k", "192k", "256k")
    :return: путь к созданному .mp3 файлу
    """
    in_path = Path(input_path)
    if output_path is None:
        output_path = in_path.with_suffix(".mp3")
    else:
        output_path = Path(output_path)

    # загружаем wav
    audio = AudioSegment.from_wav(in_path)

    # экспортируем как mp3
    audio.export(output_path, format="mp3", bitrate=bitrate)
    return str(output_path)


# %%
def speaker_name_to_id(spk: str) -> int:
    """
    'SPEAKER_02' -> 2
    """
    m = re.search(r"(\d+)$", spk)
    if not m:
        raise ValueError(f"Cannot parse numeric id from speaker name: {spk}")
    return int(m.group(1))


def build_label_to_id(speaker_to_label: Dict[str, str]) -> Dict[str, int]:
    """
    SPEAKER_00 -> 'Судья'  =>  'Судья' -> 0
    """
    label_to_id: Dict[str, int] = {}
    for spk, label in speaker_to_label.items():
        label_to_id[label] = speaker_name_to_id(spk)
    return label_to_id


def build_label_to_wav(
        speaker_to_label: Dict[str, str],
        speaker_to_file: Dict[str, str],
        label_to_file: Optional[Dict[str, str]] = None,
) -> Dict[str, str]:
    """
    Собираем label -> wav_path:
    - если есть label_to_file, используем его (самый прямой путь)
    - иначе строим через speaker_to_file + speaker_to_label
    """
    out: Dict[str, str] = {}
    if label_to_file:
        out.update(label_to_file)
    for spk, label in speaker_to_label.items():
        if label in out:
            continue
        f = speaker_to_file.get(spk)
        if f:
            out[label] = f
    return out


def secs_int(x: float, mode: str = "round") -> int:
    """
    mode: 'round' | 'floor' | 'ceil'
    """
    x = float(x)
    if mode == "floor":
        return int(x // 1)
    if mode == "ceil":
        return int(-(-x // 1))
    return int(round(x))


# def convert_intervals_to_target_json(
#         intervals_with_text: List[Dict[str, Any]],
#         speaker_to_label: Dict[str, str],
#         speaker_to_file: Dict[str, str],
#         label_to_file: Optional[Dict[str, str]] = None
# ) -> List[Dict[str, Any]]:
#     """
#     Делает JSON формата:
#     [{
#         "file_name": "<main_speaker_file>.mp3",
#         "id_speaker": <int>,
#         "start": <int seconds>,
#         "end": <int seconds>,
#         "speaker": "<speaker_label>",
#         "transcription": "..."
#     }, ...]
#     """
#     label_to_id = build_label_to_id(speaker_to_label)
#     label_to_wav = build_label_to_wav(speaker_to_label, speaker_to_file, label_to_file)
#
#     out: List[Dict[str, Any]] = []
#     for it in intervals_with_text:
#         speaker_label = (
#                 it.get("speaker_label")
#                 or it.get("speaker")
#                 or (it.get("id_speaker") if isinstance(it.get("id_speaker"), str) else None)
#         )
#         if not speaker_label:
#             speaker_label = "Unknown"
#
#         spk_id = label_to_id.get(speaker_label)
#         if spk_id is None:
#             spk_id = -1
#
#         wav_path = label_to_wav.get(speaker_label)
#         if wav_path:
#             mp3_name = Path(wav_path).with_suffix(".mp3").name
#         else:
#             mp3_name = f"{speaker_label}.mp3"
#
#         start_s = secs_int(it["start"], mode="floor")
#         end_s = secs_int(it["end"], mode="round")
#
#         out.append({
#             "file_name": mp3_name,
#             "id_speaker": int(spk_id),
#             "start": int(start_s),
#             "end": int(end_s),
#             "speaker": speaker_label,
#             "transcription": (it.get("transcription") or "").strip(),
#         })
#
#     return out


def convert_intervals_to_target_json_s3(
        intervals_with_text: List[Dict[str, Any]],
        speaker_to_label: Dict[str, str],
        speaker_to_file: Dict[str, str],
        label_to_file: Optional[Dict[str, str]] = None,
        s3_prefix: str = "segments",
        presigned_expire: int = 3600
) -> List[Dict[str, Any]]:

    label_to_id = build_label_to_id(speaker_to_label)
    label_to_wav = build_label_to_wav(speaker_to_label, speaker_to_file, label_to_file)

    out: List[Dict[str, Any]] = []

    for it in intervals_with_text:
        speaker_label = (
            it.get("speaker_label") or it.get("speaker") or (it.get("id_speaker") if isinstance(it.get("id_speaker"), str) else None)
        )
        if not speaker_label:
            speaker_label = "Unknown"

        spk_id = label_to_id.get(speaker_label, -1)
        wav_path = it.get("file_name")
        if wav_path:
            wav_path = Path(wav_path)
            mp3_name = wav_path.with_suffix(".mp3").name
            s3_key = f"{s3_prefix}/{mp3_name}"
            file_url = generate_presigned_url(
                s3_key,
                expires_in=presigned_expire
            )
        else:
            file_url = None  # fallback

        #start_s = secs_int(it["start"], mode="floor")
        #end_s = secs_int(it["end"], mode="round")

        out.append({
            "file_url": file_url,
            "id_speaker": int(spk_id),
            "start": it["start"],
            "end": it["end"],
            "speaker": speaker_label,
            "transcription": (it.get("transcription") or "").strip(),
        })

    return out








# @celery_app.task(bind=True, queue="cpu")
# def merge_audio_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Merge Audio", 5)
#         merged_audio_path = run_merge_audio_step(operation_id)
#         update_progress(operation_id, "Merge Audio", 10)
#
#         # сохраняем в temp
#         update_temp_data(operation_id, "MERGE_AUDIO", {"merged_audio_path": str(merged_audio_path)})
#         print(f"MERGE_AUDIO: {merged_audio_path}")
#         print(f"TMP_PATH={TMP_PATH} ({type(TMP_PATH)})")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="gpu")
# def diarization_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Diarization", 15)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#
#         merge_data = get_temp_data(operation_id, "MERGE_AUDIO")
#         merged_audio_path = Path(merge_data["merged_audio_path"])
#
#         diarization_result = diarization_step(
#             input_audio=merged_audio_path,
#             speakers_folder=PATH_TO_AUDIO,
#             output_dir=tmp / "diarization_rttm",
#             pad_end=0.4,
#         )
#
#         # сохраняем промежуточные результаты
#         update_temp_data(operation_id, "DIARIZATION", {"rttm_path": str(diarization_result)})
#
#         update_progress(operation_id, "Diarization", 25)
#         print(f"DIARIZATION: {diarization_result}")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="cpu")
# def merge_intervals_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Merge Intervals", 30)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#
#         diarization_data = get_temp_data(operation_id, "DIARIZATION")
#         rttm_path = Path(diarization_data["rttm_path"])
#
#         rttm_data = load_rttm(rttm_path)
#         ann = next(iter(rttm_data.values()))
#
#         intervals = [
#             (turn.start, turn.end, spk)
#             for turn, _, spk in ann.itertracks(yield_label=True)
#         ]
#
#         intervals_path = merge_intervals_step(intervals, tmp)
#
#         update_temp_data(operation_id, "MERGE_INTERVALS", {"intervals_path": str(intervals_path)})
#         update_progress(operation_id, "Merge Intervals", 35)
#         print(f"MERGE_INTERVALS: {intervals_path}")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="cpu")
# def vad_hungarian_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "VAD + Hungarian", 40)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#         wav_files = list(Path(PATH_TO_AUDIO).glob("*.wav"))
#
#         merge_data = get_temp_data(operation_id, "MERGE_AUDIO")
#         merged_audio_path = merge_data["merged_audio_path"]
#
#         merge_intervals_data = get_temp_data(operation_id, "MERGE_INTERVALS")
#         intervals_path = Path(merge_intervals_data["intervals_path"])
#
#         with open(intervals_path, "r") as f:
#             intervals_merged = [tuple(x) for x in json.load(f)]
#
#         vad_path = vad_hungarian_step(
#             intervals=intervals_merged,
#             merged_audio_path=Path(merged_audio_path),
#             wav_files=wav_files,
#             tmp_dir=tmp,
#         )
#
#         update_temp_data(operation_id, "VAD_HUNGARIAN", {
#             "vad_path": str(vad_path)
#         })
#         update_progress(operation_id, "VAD + Hungarian", 50)
#         print(f"VAD_HUNGARIAN intervals: {vad_path}")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="cpu")
# def extract_segments_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Extract Segments", 55)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#         wav_files = list(Path(PATH_TO_AUDIO).glob("*.wav"))
#
#         merge_intervals_data = get_temp_data(operation_id, "MERGE_INTERVALS")
#         intervals_path = Path(merge_intervals_data["intervals_path"])
#
#         vad_data = get_temp_data(operation_id, "VAD_HUNGARIAN")
#         vad_path = Path(vad_data["vad_path"])
#
#         with open(intervals_path, "r") as f:
#             intervals_merged = [tuple(x) for x in json.load(f)]
#
#
#         with open(vad_path, "r") as f:
#             vad_json = json.load(f)
#
#         speaker_to_label = vad_json["speaker_to_label"]
#
#         segments_file = prepare_audio_segments(
#             wav_files=wav_files,
#             intervals=intervals_merged,
#             speaker_to_label=speaker_to_label,
#             unique_tmp_path=tmp,
#             sample_rate=8000,
#         )
#
#         update_temp_data(operation_id, "EXTRACT_SEGMENTS", {
#             "segments_file": str(segments_file)
#         })
#
#         update_progress(operation_id, "Extract Segments", 65)
#         print(f"EXTRACT_SEGMENTS segments_file: {segments_file}")
#
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
#
# @celery_app.task(bind=True, queue="cpu")
# def transcription_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Transcription", 70)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#
#         extract_data = get_temp_data(operation_id, "EXTRACT_SEGMENTS")
#         segments_file = Path(extract_data["segments_file"])
#
#         with open(segments_file, "r", encoding="utf-8") as f:
#             segments_json = json.load(f)
#         interval_segments = segments_json["interval_segments"]
#
#         transcription_file = transcribe_step_yandex(
#             interval_segments=interval_segments,
#             output_path=tmp / "yandex_transcription.json",
#         )
#
#         update_temp_data(operation_id, "TRANSCRIPTION", {
#             "transcription_file": str(transcription_file)
#         })
#         update_progress(operation_id, "Transcription", 85)
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise
#
#
# @celery_app.task(bind=True, queue="cpu")
# def export_results_task(self, operation_id: str):
#     try:
#         update_progress(operation_id, "Export Results", 90)
#         tmp = get_unique_result_path(TMP_PATH, operation_id)
#
#         #trans_data = get_temp_data(operation_id, "TRANSCRIPTION")
#         trans_data = get_temp_data(operation_id, "TRANSCRIPTION")
#         transcription_file = Path(trans_data["transcription_file"])
#
#         # extract_data = get_temp_data(operation_id, "EXTRACT_SEGMENTS")
#         extract_data = get_temp_data(operation_id, "EXTRACT_SEGMENTS")
#         segments_file = Path(extract_data["segments_file"])
#         #vad_data = get_temp_data(operation_id, "VAD_HUNGARIAN")
#         vad_data = get_temp_data(operation_id, "VAD_HUNGARIAN")
#         vad_path = Path(vad_data["vad_path"])
#         #merge_data = get_temp_data(operation_id, "MERGE_AUDIO")
#         merge_data = get_temp_data(operation_id, "MERGE_AUDIO")
#         merged_audio_path = Path(merge_data["merged_audio_path"])
#
#         with open(segments_file, "r", encoding="utf-8") as f:
#             segments_json = json.load(f)
#         speaker_to_file_v = segments_json["speaker_to_file"]
#         label_to_file_v = segments_json["label_to_file"]
#
#         with open(vad_path, "r") as f:
#             vad_json = json.load(f)
#         speaker_to_label = vad_json["speaker_to_label"]
#
#         with open(transcription_file, "r", encoding="utf-8") as f:
#             transcription_json = json.load(f)
#         intervals_with_text = transcription_json["intervals_with_text"]
#
#
#         result = export_pipeline_results(
#             intervals_with_text=intervals_with_text,
#             speaker_to_label=speaker_to_label,
#             speaker_to_file=speaker_to_file_v,
#             label_to_file=label_to_file_v,
#             s3_prefix=f"segments/{operation_id}",
#             merged_audio_path=merged_audio_path,
#             results_path_op=tmp / "final_results",
#             operation_id=operation_id,
#         )
#
#         mark_done(operation_id, result_url=result["json_url"])
#         print(f"EXPORT_RESULTS: {result['json_url']}")
#     except Exception as exc:
#         mark_failed(operation_id, {"error": str(exc)})
#         raise