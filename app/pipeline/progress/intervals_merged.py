from typing import List, Tuple

Interval = Tuple[float, float, str]

def merge_consecutive_intervals(intervals: List[Interval]) -> List[Interval]:
    """
    Склеивает подряд идущие интервалы одного спикера.

    Предполагается, что intervals уже отсортированы по времени начала.
    На выходе список того же формата: (start, end, speaker).
    """
    if not intervals:
        return []

    merged: List[Interval] = []

    cur_start, cur_end, cur_spk = intervals[0]

    for start, end, spk in intervals[1:]:
        if spk == cur_spk:
            # тот же спикер, расширяем текущий интервал до конца последнего
            cur_end = max(cur_end, end)
        else:
            merged.append((cur_start, cur_end, cur_spk))
            cur_start, cur_end, cur_spk = start, end, spk

    merged.append((cur_start, cur_end, cur_spk))

    return merged