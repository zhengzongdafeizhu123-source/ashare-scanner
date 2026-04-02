from __future__ import annotations

import json
import os
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
TUSHARE_CONFIG_LOCAL_FILE = PROJECT_DIR / "tushare_config.local.json"
TUSHARE_CONFIG_FILE = PROJECT_DIR / "tushare_config.json"
TUSHARE_CONFIG_EXAMPLE_FILE = PROJECT_DIR / "tushare_config.example.json"
PLACEHOLDER_TOKENS = {
    "",
    "YOUR_TUSHARE_TOKEN_HERE",
    "your_tushare_token_here",
    "REPLACE_WITH_YOUR_TUSHARE_TOKEN",
    "你的token",
}


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _normalize_token(value: object) -> str:
    token = str(value or "").strip()
    return "" if token in PLACEHOLDER_TOKENS else token


def load_tushare_token() -> str:
    token = _normalize_token(os.environ.get("TUSHARE_TOKEN", ""))
    if token:
        return token

    for path in [TUSHARE_CONFIG_LOCAL_FILE, TUSHARE_CONFIG_FILE, TUSHARE_CONFIG_EXAMPLE_FILE]:
        token = _normalize_token(_load_json(path).get("token", ""))
        if token:
            return token

    raise RuntimeError(
        "未找到 Tushare Token。请设置环境变量 TUSHARE_TOKEN，"
        "或在项目根目录创建 tushare_config.local.json：{\"token\": \"你的token\"}"
    )
