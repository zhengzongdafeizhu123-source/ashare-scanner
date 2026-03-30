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
PIPELINE_STEPS = [
    "sync_universe",
    "find_missing_stocks",
    "bootstrap_missing_stocks",
    "update_daily_hist",
    "pack_to_parquet",
    "scan_from_parquet",
]
SCAN_OUTPUT_GLOBS = {
    "results": "p7_scan_from_parquet_all_results_*.csv",
    "selected": "p7_scan_from_parquet_all_selected_*.csv",
    "candidate": "p7_scan_from_parquet_all_candidate_*.csv",
    "watch": "p7_scan_from_parquet_all_watch_*.csv",
    "errors": "p7_scan_from_parquet_all_errors_*.csv",
    "skipped": "p7_scan_from_parquet_all_skipped_*.csv",
    "summary": "p7_scan_from_parquet_all_summary_*.csv",
    "log": "p7_scan_from_parquet_all_*.log",
}


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



def _notify(event_callback, event_type, step_name, payload=None):
    if event_callback is not None:
        event_callback(event_type, step_name, payload)



def get_runtime_info():
    base_dir = BASE_DIR
    mode = "测试目录" if ".runtime" in str(base_dir).lower() else "正式目录"
    return {
        "base_dir": str(base_dir),
        "mode": mode,
        "data_dir": str(DATA_DIR),
        "output_dir": str(OUTPUT_DIR),
        "logs_dir": str(LOGS_DIR),
    }



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

    found_paths: list[str] = []
    seen: set[str] = set()

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



def _run_script(step_name, script_name, args=None, output_patterns=None, log_callback=None, event_callback=None):
    script_path = PROJECT_DIR / script_name
    command = [PYTHON_EXE, str(script_path)] + (args or [])
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        value = env.get(key, "").strip().lower()
        if value in {"http://127.0.0.1:9", "https://127.0.0.1:9"}:
            env.pop(key, None)

    _notify(event_callback, "step_start", step_name, {"command": command, "script": script_name})

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

    stdout_lines: list[str] = []
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
        result = _result(
            True,
            step_name,
            message,
            output_paths=output_paths,
            stdout=stdout_text,
            stderr=stderr_text,
            returncode=returncode,
            command=command,
        )
        _notify(event_callback, "step_done", step_name, result)
        return result

    failure_message = stderr_text or stdout_text or f"{script_name} failed"
    result = _result(
        False,
        step_name,
        failure_message,
        output_paths=output_paths,
        stdout=stdout_text,
        stderr=stderr_text,
        returncode=returncode,
        command=command,
    )
    _notify(event_callback, "step_done", step_name, result)
    return result



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



def _latest_matching_file(directory: Path, pattern: str):
    files = sorted(directory.glob(pattern))
    if not files:
        return None
    return files[-1]



def get_latest_scan_output_files():
    files = {
        key: _latest_matching_file(LOGS_DIR if key == "log" else OUTPUT_DIR, pattern)
        for key, pattern in SCAN_OUTPUT_GLOBS.items()
    }
    return {
        key: str(path.resolve()) if path is not None else ""
        for key, path in files.items()
    }



def _read_csv_safe(path_str: str):
    if not path_str:
        return pd.DataFrame()

    file_path = Path(path_str)
    if not file_path.exists():
        return pd.DataFrame()

    for encoding in ("utf-8-sig", "utf-8", None):
        try:
            kwargs = {"dtype": {"股票代码": str, "code": str}} if encoding is not None else {"dtype": {"股票代码": str, "code": str}}
            if encoding is not None:
                kwargs["encoding"] = encoding
            df = pd.read_csv(file_path, **kwargs)
            if "股票代码" in df.columns:
                df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
            if "code" in df.columns:
                df["code"] = df["code"].astype(str).str.zfill(6)
            return df
        except Exception:
            continue
    return pd.DataFrame()



def load_latest_scan_frames():
    files = get_latest_scan_output_files()
    frames = {
        key: _read_csv_safe(files.get(key, ""))
        for key in ["results", "selected", "candidate", "watch", "errors", "skipped", "summary"]
    }
    return {
        "files": files,
        "frames": frames,
        "found_any": any(bool(files.get(key)) for key in ["results", "selected", "candidate", "watch", "summary"]),
    }



def sync_universe(log_callback=None, event_callback=None):
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
        event_callback=event_callback,
    )



def find_missing_stocks(log_callback=None, event_callback=None):
    _notify(event_callback, "step_start", "find_missing_stocks", None)

    universe_file = _latest_universe_file()
    if universe_file is None:
        result = _result(
            False,
            "find_missing_stocks",
            "Universe file not found. Please run sync_universe first.",
            output_paths=[],
            missing_codes=[],
            missing_count=0,
        )
        if log_callback is not None:
            log_callback("find_missing_stocks", result["message"])
        _notify(event_callback, "step_done", "find_missing_stocks", result)
        return result

    universe_df = _load_universe_df(universe_file)
    existing_codes = {file.stem.zfill(6) for file in DATA_DIR.glob("*.csv")}
    expected_codes = set(universe_df["code"].tolist())
    missing_codes = sorted(expected_codes - existing_codes)

    output_paths = [str(universe_file.resolve())]
    result = _result(
        True,
        "find_missing_stocks",
        f"Found {len(missing_codes)} missing stocks.",
        output_paths=output_paths,
        missing_codes=missing_codes,
        missing_count=len(missing_codes),
    )
    if log_callback is not None:
        log_callback("find_missing_stocks", result["message"])
    _notify(event_callback, "step_done", "find_missing_stocks", result)
    return result



def bootstrap_missing_stocks(missing_codes=None, log_callback=None, event_callback=None):
    if missing_codes is None:
        missing_result = find_missing_stocks(log_callback=log_callback, event_callback=event_callback)
        if not missing_result["success"]:
            return missing_result
        missing_codes = missing_result.get("missing_codes", [])

    if not missing_codes:
        result = _result(
            True,
            "bootstrap_missing_stocks",
            "No missing stocks to bootstrap.",
            output_paths=[],
            created_count=0,
            missing_count=0,
            error_count=0,
            skipped_count=0,
        )
        _notify(event_callback, "step_start", "bootstrap_missing_stocks", None)
        if log_callback is not None:
            log_callback("bootstrap_missing_stocks", result["message"])
        _notify(event_callback, "step_done", "bootstrap_missing_stocks", result)
        return result

    universe_file = _latest_universe_file()
    if universe_file is None:
        result = _result(
            False,
            "bootstrap_missing_stocks",
            "Universe file not found. Please run sync_universe first.",
            output_paths=[],
            created_count=0,
            error_count=0,
            skipped_count=0,
        )
        _notify(event_callback, "step_start", "bootstrap_missing_stocks", None)
        if log_callback is not None:
            log_callback("bootstrap_missing_stocks", result["message"])
        _notify(event_callback, "step_done", "bootstrap_missing_stocks", result)
        return result

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
        event_callback=event_callback,
    )
    success_files = [Path(path) for path in result["output_paths"] if Path(path).name.startswith(f"p4_bootstrap_all_success_{TODAY_STR}_list_")]
    error_files = [Path(path) for path in result["output_paths"] if Path(path).name.startswith(f"p4_bootstrap_all_errors_{TODAY_STR}_list_")]
    skip_files = [Path(path) for path in result["output_paths"] if Path(path).name.startswith(f"p4_bootstrap_all_skipped_{TODAY_STR}_list_")]

    result["missing_count"] = len(missing_codes)
    result["created_count"] = _safe_csv_row_count(success_files[-1]) if success_files else 0
    result["error_count"] = _safe_csv_row_count(error_files[-1]) if error_files else 0
    result["skipped_count"] = _safe_csv_row_count(skip_files[-1]) if skip_files else 0
    return result



def update_daily_hist(log_callback=None, event_callback=None):
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
        event_callback=event_callback,
    )



def pack_to_parquet(log_callback=None, event_callback=None):
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
        event_callback=event_callback,
    )



def scan_from_parquet(log_callback=None, event_callback=None):
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
        event_callback=event_callback,
    )



def run_daily_pipeline(skip_bootstrap=False, log_callback=None, event_callback=None):
    steps = []
    _notify(event_callback, "pipeline_start", "run_daily_pipeline", {"total_steps": len(PIPELINE_STEPS), "skip_bootstrap": skip_bootstrap})

    sync_result = sync_universe(log_callback=log_callback, event_callback=event_callback)
    steps.append(sync_result)
    if not sync_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {sync_result['step_name']}.",
            output_paths=sync_result["output_paths"],
            steps=steps,
            missing_count=0,
            created_count=0,
            error_count=sync_result.get("error_count", 0),
            skipped_count=sync_result.get("skipped_count", 0),
        )

    missing_result = find_missing_stocks(log_callback=log_callback, event_callback=event_callback)
    steps.append(missing_result)
    if not missing_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {missing_result['step_name']}.",
            output_paths=missing_result["output_paths"],
            steps=steps,
            missing_count=missing_result.get("missing_count", 0),
            created_count=0,
            error_count=missing_result.get("error_count", 0),
            skipped_count=missing_result.get("skipped_count", 0),
        )

    missing_codes = missing_result.get("missing_codes", [])
    bootstrap_result = None
    if missing_codes and not skip_bootstrap:
        bootstrap_result = bootstrap_missing_stocks(missing_codes, log_callback=log_callback, event_callback=event_callback)
        steps.append(bootstrap_result)
        if not bootstrap_result["success"]:
            return _result(
                False,
                "run_daily_pipeline",
                f"Pipeline stopped at {bootstrap_result['step_name']}.",
                output_paths=bootstrap_result["output_paths"],
                steps=steps,
                missing_count=missing_result.get("missing_count", len(missing_codes)),
                created_count=bootstrap_result.get("created_count", 0),
                error_count=bootstrap_result.get("error_count", 0),
                skipped_count=bootstrap_result.get("skipped_count", 0),
            )
    elif missing_codes and skip_bootstrap:
        bootstrap_result = _result(
            True,
            "bootstrap_missing_stocks",
            "Skipped bootstrap by user choice.",
            output_paths=[],
            missing_count=len(missing_codes),
            created_count=0,
            error_count=0,
            skipped_count=len(missing_codes),
        )
        _notify(event_callback, "step_start", "bootstrap_missing_stocks", {"skipped": True})
        if log_callback is not None:
            log_callback("bootstrap_missing_stocks", bootstrap_result["message"])
        _notify(event_callback, "step_done", "bootstrap_missing_stocks", bootstrap_result)
        steps.append(bootstrap_result)
    else:
        bootstrap_result = _result(
            True,
            "bootstrap_missing_stocks",
            "No missing stocks. Bootstrap step skipped.",
            output_paths=[],
            missing_count=0,
            created_count=0,
            error_count=0,
            skipped_count=0,
        )
        _notify(event_callback, "step_start", "bootstrap_missing_stocks", {"skipped": True})
        if log_callback is not None:
            log_callback("bootstrap_missing_stocks", bootstrap_result["message"])
        _notify(event_callback, "step_done", "bootstrap_missing_stocks", bootstrap_result)
        steps.append(bootstrap_result)

    update_result = update_daily_hist(log_callback=log_callback, event_callback=event_callback)
    steps.append(update_result)
    if not update_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {update_result['step_name']}.",
            output_paths=update_result["output_paths"],
            steps=steps,
            missing_count=missing_result.get("missing_count", 0),
            created_count=bootstrap_result.get("created_count", 0),
            error_count=bootstrap_result.get("error_count", 0) + update_result.get("error_count", 0),
            skipped_count=bootstrap_result.get("skipped_count", 0) + update_result.get("skipped_count", 0),
        )

    pack_result = pack_to_parquet(log_callback=log_callback, event_callback=event_callback)
    steps.append(pack_result)
    if not pack_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {pack_result['step_name']}.",
            output_paths=pack_result["output_paths"],
            steps=steps,
            missing_count=missing_result.get("missing_count", 0),
            created_count=bootstrap_result.get("created_count", 0),
            error_count=bootstrap_result.get("error_count", 0) + update_result.get("error_count", 0) + pack_result.get("error_count", 0),
            skipped_count=bootstrap_result.get("skipped_count", 0) + update_result.get("skipped_count", 0) + pack_result.get("skipped_count", 0),
        )

    scan_result = scan_from_parquet(log_callback=log_callback, event_callback=event_callback)
    steps.append(scan_result)
    if not scan_result["success"]:
        return _result(
            False,
            "run_daily_pipeline",
            f"Pipeline stopped at {scan_result['step_name']}.",
            output_paths=scan_result["output_paths"],
            steps=steps,
            missing_count=missing_result.get("missing_count", 0),
            created_count=bootstrap_result.get("created_count", 0),
            error_count=(
                bootstrap_result.get("error_count", 0)
                + update_result.get("error_count", 0)
                + pack_result.get("error_count", 0)
                + scan_result.get("error_count", 0)
            ),
            skipped_count=(
                bootstrap_result.get("skipped_count", 0)
                + update_result.get("skipped_count", 0)
                + pack_result.get("skipped_count", 0)
                + scan_result.get("skipped_count", 0)
            ),
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
        missing_count=missing_result.get("missing_count", 0),
        created_count=bootstrap_result.get("created_count", 0),
        error_count=sum(step.get("error_count", 0) for step in steps),
        skipped_count=sum(step.get("skipped_count", 0) for step in steps),
    )


if __name__ == "__main__":
    pipeline_result = run_daily_pipeline()
    print(pipeline_result["message"])
