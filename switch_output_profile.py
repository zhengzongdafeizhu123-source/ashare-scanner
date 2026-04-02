from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from project_paths import APP_CONFIG_LOCAL_FILE, load_runtime_config


PROJECT_DIR = Path(__file__).resolve().parent
VALID_PROFILES = {"main", "test"}

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def load_config() -> dict:
    data = load_runtime_config()
    return data if isinstance(data, dict) else {}


def save_config(config: dict) -> None:
    APP_CONFIG_LOCAL_FILE.write_text(json.dumps(config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="切换输出模式：main 或 test")
    parser.add_argument("profile", nargs="?", default="", help="main 或 test；留空只显示当前模式")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config()
    current = str(config.get("output_profile", "main")).strip().lower() or "main"
    if current not in VALID_PROFILES:
        current = "main"

    if not args.profile:
        print(f"当前 output_profile: {current}")
        print("用法: python switch_output_profile.py main")
        print("   或: python switch_output_profile.py test")
        return

    profile = args.profile.strip().lower()
    if profile not in VALID_PROFILES:
        raise SystemExit("profile 只能是 main 或 test")

    config["output_profile"] = profile
    save_config(config)

    print(f"已切换 output_profile -> {profile}")
    print(f"配置文件: {APP_CONFIG_LOCAL_FILE}")
    print(f"输出目录将写入: output/{profile}/...")


if __name__ == "__main__":
    main()
