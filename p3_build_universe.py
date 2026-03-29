from pathlib import Path
from datetime import datetime
import json
import os
import sys
import time
import pandas as pd
import akshare as ak

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_BASE_DIR = Path(r"W:\AshareScanner")
APP_CONFIG_FILE = PROJECT_DIR / "app_config.json"


def load_app_config():
    if not APP_CONFIG_FILE.exists():
        return {}

    try:
        config = json.loads(APP_CONFIG_FILE.read_text(encoding="utf-8"))
        return config if isinstance(config, dict) else {}
    except Exception:
        return {}


def resolve_base_dir():
    config = load_app_config()
    return Path(config["base_dir"]) if config.get("base_dir") else DEFAULT_BASE_DIR


def sanitize_proxy_env():
    bad_proxy_values = {
        "http://127.0.0.1:9",
        "https://127.0.0.1:9",
    }
    for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        value = os.environ.get(key, "").strip().lower()
        if value in bad_proxy_values:
            os.environ.pop(key, None)


sanitize_proxy_env()

BASE_DIR = resolve_base_dir()
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")

raw_file = OUTPUT_DIR / f"p3_universe_raw_{today_str}.csv"
filtered_file = OUTPUT_DIR / f"p3_universe_filtered_{today_str}.csv"
summary_file = OUTPUT_DIR / f"p3_universe_summary_{today_str}.csv"
log_file = LOGS_DIR / f"p3_build_universe_{today_str}.log"


def _normalize_stock_info_a_code_name(df):
    if "code" not in df.columns or "name" not in df.columns:
        raise ValueError(f"stock_info_a_code_name 返回字段异常: {list(df.columns)}")
    out = df[["code", "name"]].copy()
    out["source"] = "stock_info_a_code_name"
    return out


def _normalize_stock_zh_a_spot_em(df):
    if "代码" not in df.columns or "名称" not in df.columns:
        raise ValueError(f"stock_zh_a_spot_em 返回字段异常: {list(df.columns)}")
    out = df[["代码", "名称"]].copy()
    out.columns = ["code", "name"]
    out["source"] = "stock_zh_a_spot_em"
    return out


def _fetch_source_with_retry(source_name, fetch_func, normalize_func, retry_delays):
    last_error = None
    total_attempts = len(retry_delays)

    print(f"[开始] 准备获取股票池，数据源: {source_name}")

    for attempt, delay in enumerate(retry_delays, start=1):
        print(f"[尝试] {source_name} {attempt}/{total_attempts}")
        try:
            df = fetch_func()
            if df is None or df.empty:
                raise ValueError("返回结果为空")
            return normalize_func(df), ""
        except Exception as e:
            last_error = e
            print(f"[失败] {source_name} {attempt}/{total_attempts}: {repr(e)}")
            if attempt < total_attempts:
                print(f"[等待] {source_name} 将在 {delay} 秒后重试")
                time.sleep(delay)

    error_message = f"{source_name} failed after {total_attempts} attempts: {repr(last_error)}"
    return None, error_message


def fetch_universe():
    """
    优先尝试 stock_info_a_code_name；
    如果失败，再回退到 stock_zh_a_spot_em。
    """
    retry_delays = [3, 5, 8, 12]
    errors = []

    primary_df, primary_error = _fetch_source_with_retry(
        source_name="stock_info_a_code_name",
        fetch_func=ak.stock_info_a_code_name,
        normalize_func=_normalize_stock_info_a_code_name,
        retry_delays=retry_delays,
    )
    if primary_df is not None:
        return primary_df
    errors.append(primary_error)

    fallback_df, fallback_error = _fetch_source_with_retry(
        source_name="stock_zh_a_spot_em",
        fetch_func=ak.stock_zh_a_spot_em,
        normalize_func=_normalize_stock_zh_a_spot_em,
        retry_delays=retry_delays,
    )
    if fallback_df is not None:
        return fallback_df
    errors.append(fallback_error)

    raise RuntimeError("All universe sources failed. " + " | ".join(errors))


def normalize_code(code):
    return str(code).strip().zfill(6)


def is_main_a_share(code):
    """
    先只保留常见 A 股代码段
    """
    valid_prefixes = (
        "000", "001", "002", "003",
        "300", "301",
        "600", "601", "603", "605",
        "688"
    )
    return code.startswith(valid_prefixes)


def is_excluded_name(name):
    """
    基础过滤：
    - ST / *ST
    - 退市
    """
    name = str(name).strip().upper()
    if "ST" in name:
        return True
    if "退" in name:
        return True
    return False


try:
    raw_df = fetch_universe()

    raw_df["code"] = raw_df["code"].apply(normalize_code)
    raw_df["name"] = raw_df["name"].astype(str).str.strip()

    raw_df = raw_df.drop_duplicates(subset=["code"]).reset_index(drop=True)

    raw_df.to_csv(raw_file, index=False, encoding="utf-8-sig")

    filtered_df = raw_df.copy()
    filtered_df = filtered_df[filtered_df["code"].apply(is_main_a_share)]
    filtered_df = filtered_df[~filtered_df["name"].apply(is_excluded_name)]
    filtered_df = filtered_df.sort_values(by="code").reset_index(drop=True)

    filtered_df.to_csv(filtered_file, index=False, encoding="utf-8-sig")

    summary_df = pd.DataFrame([{
        "日期": today_str,
        "原始股票池数量": len(raw_df),
        "过滤后股票池数量": len(filtered_df),
        "数据来源": raw_df["source"].iloc[0] if not raw_df.empty else ""
    }])
    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")

    log_lines = [
        "P3 build universe finished",
        f"raw_count={len(raw_df)}",
        f"filtered_count={len(filtered_df)}",
        f"raw_file={raw_file}",
        f"filtered_file={filtered_file}",
        f"summary_file={summary_file}",
    ]
    log_file.write_text("\n".join(log_lines), encoding="utf-8")

    print("运行完成")
    print(f"原始股票池数量: {len(raw_df)}")
    print(f"过滤后股票池数量: {len(filtered_df)}")
    print(f"原始文件: {raw_file}")
    print(f"过滤后文件: {filtered_file}")
    print(f"汇总文件: {summary_file}")
    print(f"日志文件: {log_file}")

except Exception as e:
    log_file.write_text(
        "\n".join([
            "P3 build universe failed",
            f"error={repr(e)}"
        ]),
        encoding="utf-8"
    )
    print("运行失败")
    print(f"日志文件: {log_file}")
    raise
