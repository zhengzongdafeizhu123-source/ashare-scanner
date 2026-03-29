from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import os
import subprocess
import sys

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_BASE_DIR = Path(r"W:\AshareScanner")
APP_CONFIG_FILE = PROJECT_DIR / "app_config.json"
TODAY_STR = datetime.now().strftime("%Y%m%d")
PYTHON_EXE = sys.executable


def _load_app_config():
    if not APP_CONFIG_FILE.exists():
        return {}

    try:
        config = json.loads(APP_CONFIG_FILE.read_text(encoding="utf-8"))
        return config if isinstance(config, dict) else {}
    except Exception:
        return {}


def _resolve_base_dir():
    config = _load_app_config()
    return Path(config["base_dir"]) if config.get("base_dir") else DEFAULT_BASE_DIR


BASE_DIR = _resolve_base_dir()
DATA_DIR = BASE_DIR / "data" / "daily_hist"
PACK_DIR = BASE_DIR / "data" / "packed"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"


def _result(success, step_name, message, output_paths=None, **extra):
    result = {
        "success": bool(success),
        "step_name": step_name,
        "message": message,
        "output_paths": output_paths or [],
    }
    result.update(extra)
    return result


def _collect_existing_output_paths(output_patterns=None):
    if not output_patterns:
        return []

    found_paths = []
    seen = set()

    for pattern in output_patterns:
        pattern_path = Path(pattern)
        has_wildcard = any(token in str(pattern_path) for token in "*?[")

        if has_wildcard:
            parent = pattern_path.parent
            if not parent.exists():
                continue
            matches = sorted(parent.glob(pattern_path.name))
        else:
            matches = [pattern_path] if pattern_path.exists() else []

        for match in matches:
            resolved = str(match.resolve())
            if resolved not in seen:
                seen.add(resolved)
                found_paths.append(resolved)

    return found_paths


def _run_script(step_name, script_name, args=None, output_patterns=None, log_callback=None):
    script_path = PROJECT_DIR / script_name
    command = [PYTHON_EXE, str(script_path)] + (args or [])
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"

    process = subprocess.Popen(
        command,
        cwd=str(PROJECT_DIR),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        shell=False,
    )

    stdout_lines = []
    if process.stdout is not None:
        for line in process.stdout:
            clean_line = line.rstrip("\r\n")
            stdout_lines.append(clean_line)
            if log_callback is not None:
                log_callback(step_name, clean_line)

    stderr_text = ""
    if process.stderr is not None:
        stderr_text = process.stderr.read().strip()

    returncode = process.wait()
    stdout_text = "\n".join(stdout_lines).strip()
    output_paths = _collect_existing_output_paths(output_patterns)

    if returncode == 0:
        message = stdout_lines[-1] if stdout_lines else f"{script_name} finished"
        return _result(
            True,
            step_name,
            message,
            output_paths=output_paths,
            stdout=stdout_text,
            stderr=stderr_text,
            returncode=returncode,
            command=command,
        )

    failure_message = stderr_text or stdout_text or f"{script_name} failed"
    return _result(
        False,
        step_name,
        failure_message,
        output_paths=output_paths,
        stdout=stdout_text,
        stderr=stderr_text,
        returncode=returncode,
        command=command,
    )


def _latest_universe_file():
    files = sorted(OUTPUT_DIR.glob("p3_universe_filtered_*.csv"))
    if not files:
        return None
    return files[-1]


def _load_universe_df(universe_file):
    df = pd.read_csv(universe_file, dtype={"code": str})
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str).str.strip()
    return df


def _write_missing_stock_list(missing_codes):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    list_file = OUTPUT_DIR / f"gui_missing_stocks_{TODAY_STR}.csv"
    pd.DataFrame({"code": missing_codes}).to_csv(list_file, index=False, encoding="utf-8-sig")
    return list_file


def _safe_csv_row_count(file_path):
    try:
        return len(pd.read_csv(file_path))
    except Exception:
        return 0


def sync_universe(log_callback=None):
    output_patterns = [
        str(OUTPUT_DIR / f"p3_universe_raw_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p3_universe_filtered_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p3_universe_summary_{TODAY_STR}.csv"),
        str(LOGS_DIR / f"p3_build_universe_{TODAY_STR}.log"),
    ]
    return _run_script(
        "sync_universe",
        "p3_build_universe.py",
        output_patterns=output_patterns,
        log_callback=log_callback,
    )


def find_missing_stocks():
    universe_file = _latest_universe_file()
    if universe_file is None:
        return _result(
            False,
            "find_missing_stocks",
            "Universe file not found. Please run sync_universe first.",
            output_paths=[],
            missing_codes=[],
            missing_count=0,
        )

    universe_df = _load_universe_df(universe_file)
    existing_codes = {file.stem.zfill(6) for file in DATA_DIR.glob("*.csv")}
    expected_codes = set(universe_df["code"].tolist())
    missing_codes = sorted(expected_codes - existing_codes)

    output_paths = [str(universe_file.resolve())]
    return _result(
        True,
        "find_missing_stocks",
        f"Found {len(missing_codes)} missing stocks.",
        output_paths=output_paths,
        missing_codes=missing_codes,
        missing_count=len(missing_codes),
    )


def bootstrap_missing_stocks(missing_codes=None, log_callback=None):
    if missing_codes is None:
        missing_result = find_missing_stocks()
        if not missing_result["success"]:
            return missing_result
        missing_codes = missing_result.get("missing_codes", [])

    if not missing_codes:
        return _result(
            True,
            "bootstrap_missing_stocks",
            "No missing stocks to bootstrap.",
            output_paths=[],
            created_count=0,
            missing_count=0,
        )

    universe_file = _latest_universe_file()
    if universe_file is None:
        return _result(
            False,
            "bootstrap_missing_stocks",
            "Universe file not found. Please run sync_universe first.",
            output_paths=[],
            created_count=0,
        )

    stock_list_file = _write_missing_stock_list(missing_codes)
    output_patterns = [
        str(stock_list_file),
        str(OUTPUT_DIR / f"p4_bootstrap_all_success_{TODAY_STR}_list_*.csv"),
        str(OUTPUT_DIR / f"p4_bootstrap_all_errors_{TODAY_STR}_list_*.csv"),
        str(OUTPUT_DIR / f"p4_bootstrap_all_skipped_{TODAY_STR}_list_*.csv"),
        str(LOGS_DIR / f"p4_bootstrap_all_{TODAY_STR}_list_*.log"),
    ]

    result = _run_script(
        "bootstrap_missing_stocks",
        "p4_bootstrap_hist_all_resume.py",
        args=[
            "--stock-list-file",
            str(stock_list_file),
            "--universe-file",
            str(universe_file),
        ],
        output_patterns=output_patterns,
        log_callback=log_callback,
    )
    success_files = [Path(path) for path in result["output_paths"] if Path(path).name.startswith(f"p4_bootstrap_all_success_{TODAY_STR}_list_")]
    error_files = [Path(path) for path in result["output_paths"] if Path(path).name.startswith(f"p4_bootstrap_all_errors_{TODAY_STR}_list_")]
    skip_files = [Path(path) for path in result["output_paths"] if Path(path).name.startswith(f"p4_bootstrap_all_skipped_{TODAY_STR}_list_")]

    result["missing_count"] = len(missing_codes)
    result["created_count"] = _safe_csv_row_count(success_files[-1]) if success_files else 0
    result["error_count"] = _safe_csv_row_count(error_files[-1]) if error_files else 0
    result["skipped_count"] = _safe_csv_row_count(skip_files[-1]) if skip_files else 0
    return result


def update_daily_hist(log_callback=None):
    output_patterns = [
        str(OUTPUT_DIR / f"p6_update_daily_hist_success_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p6_update_daily_hist_errors_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p6_update_daily_hist_skipped_{TODAY_STR}.csv"),
        str(LOGS_DIR / f"p6_update_daily_hist_{TODAY_STR}.log"),
    ]
    return _run_script(
        "update_daily_hist",
        "p6_update_daily_hist.py",
        output_patterns=output_patterns,
        log_callback=log_callback,
    )


def pack_to_parquet(log_callback=None):
    output_patterns = [
        str(PACK_DIR / "daily_hist_all.parquet"),
        str(OUTPUT_DIR / f"p6b_pack_hist_summary_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p6b_pack_hist_errors_{TODAY_STR}.csv"),
        str(LOGS_DIR / f"p6b_pack_hist_{TODAY_STR}.log"),
    ]
    return _run_script(
        "pack_to_parquet",
        "p6b_pack_hist_to_parquet.py",
        output_patterns=output_patterns,
        log_callback=log_callback,
    )


def scan_from_parquet(log_callback=None):
    output_patterns = [
        str(OUTPUT_DIR / f"p7_scan_from_parquet_all_results_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p7_scan_from_parquet_all_selected_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p7_scan_from_parquet_all_candidate_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p7_scan_from_parquet_all_watch_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p7_scan_from_parquet_all_errors_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p7_scan_from_parquet_all_skipped_{TODAY_STR}.csv"),
        str(OUTPUT_DIR / f"p7_scan_from_parquet_all_summary_{TODAY_STR}.csv"),
        str(LOGS_DIR / f"p7_scan_from_parquet_all_{TODAY_STR}.log"),
    ]
    return _run_script(
        "scan_from_parquet",
        "p7_scan_from_parquet_all.py",
        output_patterns=output_patterns,
        log_callback=log_callback,
    )


def run_daily_pipeline(log_callback=None):
    steps = []

    sync_result = sync_universe(log_callback=log_callback)
    steps.append(sync_result)
    if not sync_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {sync_result['step_name']}.",
            output_paths=sync_result["output_paths"],
            steps=steps,
        )

    missing_result = find_missing_stocks()
    steps.append(missing_result)
    if not missing_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {missing_result['step_name']}.",
            output_paths=missing_result["output_paths"],
            steps=steps,
        )

    missing_codes = missing_result.get("missing_codes", [])
    if missing_codes:
        bootstrap_result = bootstrap_missing_stocks(missing_codes, log_callback=log_callback)
        steps.append(bootstrap_result)
        if not bootstrap_result["success"]:
            return _result(
                False,
                "run_daily_pipeline",
                f"Pipeline stopped at {bootstrap_result['step_name']}.",
                output_paths=bootstrap_result["output_paths"],
                steps=steps,
            )

    update_result = update_daily_hist(log_callback=log_callback)
    steps.append(update_result)
    if not update_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {update_result['step_name']}.",
            output_paths=update_result["output_paths"],
            steps=steps,
        )

    pack_result = pack_to_parquet(log_callback=log_callback)
    steps.append(pack_result)
    if not pack_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {pack_result['step_name']}.",
            output_paths=pack_result["output_paths"],
            steps=steps,
        )

    scan_result = scan_from_parquet(log_callback=log_callback)
    steps.append(scan_result)
    if not scan_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {scan_result['step_name']}.",
            output_paths=scan_result["output_paths"],
            steps=steps,
        )

    all_paths = []
    seen = set()
    for step in steps:
        for path in step.get("output_paths", []):
            if path not in seen:
                seen.add(path)
                all_paths.append(path)

    return _result(
        True,
        "run_daily_pipeline",
        "Daily pipeline finished successfully.",
        output_paths=all_paths,
        steps=steps,
    )


if __name__ == "__main__":
    pipeline_result = run_daily_pipeline()
    print(pipeline_result["message"])
