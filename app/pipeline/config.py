import os
import random
import warnings
from pathlib import Path

import numpy as np
import torch

# =====================
# PATHS & PARAMETERS
# =====================

BASE_DIR = Path.cwd()  # текущая рабочая директория запуска

PATH_TO_AUDIO = BASE_DIR / "app" / "api" / "data" / "audio"
RESULTS_PATH = BASE_DIR / "app" / "api" / "results"
TMP_PATH = BASE_DIR / "app" / "api" / "tmp"
METRICS_PATH = BASE_DIR / "app" / "api" / "metrics"

CALC_METRICS = True
RANDOM_SEED = 42

# =====================
# WARNINGS & TORCH SETTINGS
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

for path in [PATH_TO_AUDIO, RESULTS_PATH, TMP_PATH, METRICS_PATH]:
    path.mkdir(parents=True, exist_ok=True)

# Проверка наличия разметки, если нужно вычислять метрики
if CALC_METRICS and not any(METRICS_PATH.glob("*.xlsx")):
    warnings.warn(f"В директории {METRICS_PATH} нет .xlsx файлов разметки. METRICS отключены.")
    CALC_METRICS = False

# =====================
# SEED INITIALIZATION
# =====================

def set_global_seed(seed: int) -> None:
    """Устанавливает глобальный seed для reproducibility."""
    os.environ["PYTHONHASHSEED"] = str(seed)
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)

    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)

set_global_seed(RANDOM_SEED)
