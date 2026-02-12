import os
from pathlib import Path

BASE_DIR = Path.cwd()

TMP_PATH = BASE_DIR / "app" / "api" / "tmp"

print(os.access(TMP_PATH, os.W_OK))