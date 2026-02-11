import os
import random
import warnings
from pathlib import Path

import numpy as np
import torch

# =====================
# PATHS & PARAMETERS
# =====================

PATH_TO_AUDIO = Path("/home/sergey/FastAPIProject/app/api/data/audio")
RESULTS_PATH = Path("/home/sergey/FastAPIProject/app/api/results")
TMP_PATH = Path("/home/sergey/FastAPIProject/app/api/tmp")
METRICS_PATH = Path("/home/sergey/FastAPIProject/app/api/metrics")

CALC_METRICS = True
RANDOM_SEED = 42

# =====================
# WARNINGS & TORCH
# =====================

warnings.filterwarnings(
    "ignore",
    message="In 2.9, this function's implementation will be changed to use torchaudio.load_with_torchcodec"
)

torch.backends.cuda.matmul.allow_tf32 = True
torch.backends.cudnn.allow_tf32 = True

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# =====================
# INIT DIRECTORIES
# =====================

PATH_TO_AUDIO.mkdir(parents=True, exist_ok=True)
RESULTS_PATH.mkdir(parents=True, exist_ok=True)
TMP_PATH.mkdir(parents=True, exist_ok=True)
METRICS_PATH.mkdir(parents=True, exist_ok=True)

if CALC_METRICS and next(METRICS_PATH.glob("*.xlsx"), None) is None:
    print(f"[WARNING] В директории {METRICS_PATH} нет .xlsx файлов разметки")
    CALC_METRICS = False

# =====================
# SEED
# =====================

def set_global_seed(seed: int) -> None:
    os.environ["PYTHONHASHSEED"] = str(seed)
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)

    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)


set_global_seed(RANDOM_SEED)

print(f"Device: {DEVICE}")