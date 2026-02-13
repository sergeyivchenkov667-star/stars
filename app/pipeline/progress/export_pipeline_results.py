import re
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from docx import Document
from pydub import AudioSegment


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


def convert_intervals_to_target_json_s3(
        intervals_with_text: List[Dict[str, Any]],
        speaker_to_label: Dict[str, str],
        speaker_to_file: Dict[str, str],
        label_to_file: Optional[Dict[str, str]] = None,
        s3_prefix: str = "segments",
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
            file_url = s3_key
        else:
            file_url = None  # fallback

        out.append({
            "file_url": file_url,
            "id_speaker": int(spk_id),
            "start": it["start"],
            "end": it["end"],
            "speaker": speaker_label,
            "transcription": (it.get("transcription") or "").strip(),
        })

    return out