from __future__ import annotations

import json
import os
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parent
LEGACY_BASE_DIR = Path(r"W:\AshareScanner")
DEFAULT_BASE_DIR = PROJECT_DIR / ".runtime"
APP_CONFIG_FILE = PROJECT_DIR / "app_config.json"
APP_CONFIG_LOCAL_FILE = PROJECT_DIR / "app_config.local.json"
APP_CONFIG_EXAMPLE_FILE = PROJECT_DIR / "app_config.example.json"
DEFAULT_OUTPUT_PROFILE = "main"
VALID_OUTPUT_PROFILES = {"main", "test"}


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _merge_config_layers(layers: list[dict]) -> dict:
    merged: dict = {}
    for layer in layers:
        if not isinstance(layer, dict):
            continue
        merged.update(layer)
    return merged


def _normalize_path(value: str | Path | None) -> Path | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute():
        path = PROJECT_DIR / path
    return path


def load_runtime_config() -> dict:
    env_config_path = os.environ.get("ASHARE_APP_CONFIG", "").strip()
    env_base_dir = os.environ.get("ASHARE_BASE_DIR", "").strip()
    env_output_profile = os.environ.get("ASHARE_OUTPUT_PROFILE", "").strip()

    layers = [
        _load_json(APP_CONFIG_EXAMPLE_FILE),
        _load_json(APP_CONFIG_FILE),
        _load_json(APP_CONFIG_LOCAL_FILE),
    ]

    if env_config_path:
        layers.append(_load_json(_normalize_path(env_config_path) or Path(env_config_path)))

    config = _merge_config_layers(layers)

    if env_base_dir:
        config["base_dir"] = env_base_dir
    if env_output_profile:
        config["output_profile"] = env_output_profile

    return config


def load_app_config() -> dict:
    return load_runtime_config()


def resolve_base_dir() -> Path:
    config = load_runtime_config()
    configured = _normalize_path(config.get("base_dir"))
    if configured is not None:
        return configured
    if LEGACY_BASE_DIR.exists():
        return LEGACY_BASE_DIR
    return DEFAULT_BASE_DIR


def resolve_output_profile() -> str:
    config = load_runtime_config()
    profile = str(config.get("output_profile", DEFAULT_OUTPUT_PROFILE)).strip().lower()
    return profile if profile in VALID_OUTPUT_PROFILES else DEFAULT_OUTPUT_PROFILE


def get_config_priority_description() -> list[str]:
    return [
        "ASHARE_APP_CONFIG / ASHARE_BASE_DIR / ASHARE_OUTPUT_PROFILE",
        "app_config.local.json",
        "app_config.json",
        "app_config.example.json",
        f"legacy fallback: {LEGACY_BASE_DIR}",
        f"default sandbox fallback: {DEFAULT_BASE_DIR}",
    ]


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


BASE_DIR = resolve_base_dir()
OUTPUT_PROFILE = resolve_output_profile()
DATA_ROOT = BASE_DIR / "data"
OUTPUT_BASE_DIR = BASE_DIR / "output"
OUTPUT_ROOT = OUTPUT_BASE_DIR / OUTPUT_PROFILE
LOGS_DIR = BASE_DIR / "logs"

DAILY_HIST_DIR = DATA_ROOT / "daily_hist"
PACKED_DIR = DATA_ROOT / "packed"
RESEARCH_RAW_DIR = DATA_ROOT / "research_raw"

UNIVERSE_OUTPUT_DIR = OUTPUT_ROOT / "universe"
BOOTSTRAP_OUTPUT_DIR = OUTPUT_ROOT / "bootstrap"
SAMPLES_OUTPUT_DIR = OUTPUT_ROOT / "samples"
MAINTENANCE_OUTPUT_DIR = OUTPUT_ROOT / "maintenance"
DIAGNOSTICS_OUTPUT_DIR = OUTPUT_ROOT / "diagnostics"
SCAN_OUTPUT_DIR = OUTPUT_ROOT / "scan"
WATCHLIST_OUTPUT_DIR = OUTPUT_ROOT / "watchlist"
RESEARCH_OUTPUT_DIR = OUTPUT_ROOT / "research"
RESEARCH_RAW_SYNC_OUTPUT_DIR = OUTPUT_ROOT / "research_raw_sync"


def ensure_runtime_dirs() -> None:
    for path in [
        OUTPUT_ROOT,
        LOGS_DIR,
        DAILY_HIST_DIR,
        PACKED_DIR,
        RESEARCH_RAW_DIR,
        UNIVERSE_OUTPUT_DIR,
        BOOTSTRAP_OUTPUT_DIR,
        SAMPLES_OUTPUT_DIR,
        MAINTENANCE_OUTPUT_DIR,
        DIAGNOSTICS_OUTPUT_DIR,
        SCAN_OUTPUT_DIR,
        WATCHLIST_OUTPUT_DIR,
        RESEARCH_OUTPUT_DIR,
        RESEARCH_RAW_SYNC_OUTPUT_DIR,
    ]:
        ensure_dir(path)
