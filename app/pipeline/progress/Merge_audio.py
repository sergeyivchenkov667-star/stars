from typing import List

import torch
import torchaudio
from tqdm import tqdm
SAMPLE_RATE = 16000

def make_one_channel_audio(files_path: List[str], sr: int = SAMPLE_RATE) -> torch.Tensor:
    '''
    :param List[str] files_path: Paths to the audiofile or one-dimensional signal
    :param int sr: Signal sample rate
    :rtype torch.tensor: Concated waveform among all files
    '''
    concated_waveform, sample_rate = torchaudio.load(files_path[0])
    for i in tqdm(range(1, len(files_path))):
        waveform, sample_rate = torchaudio.load(files_path[i])

        if waveform.size(1) > concated_waveform.size(1):
            padding = waveform.size(1) - concated_waveform.size(1)
            concated_waveform = torch.nn.functional.pad(concated_waveform, (0, padding))
        elif waveform.size(1) < concated_waveform.size(1):
            padding = concated_waveform.size(1) - waveform.size(1)
            waveform = torch.nn.functional.pad(waveform, (0, padding))

        concated_waveform += waveform

    concated_waveform = concated_waveform / torch.max(torch.abs(concated_waveform))
    return concated_waveform


def save_audio(tensor, file_path, sample_rate):
    if len(tensor.shape) == 0 or tensor.shape[0] != 1:
        tensor = tensor.unsqueeze(0)
    torchaudio.save(file_path, tensor, sample_rate)