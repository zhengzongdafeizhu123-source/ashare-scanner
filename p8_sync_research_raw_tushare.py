from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import sys
import time

import pandas as pd
import tushare as ts
from project_paths import LOGS_DIR, RESEARCH_RAW_SYNC_OUTPUT_DIR, resolve_base_dir
from tushare_token import load_tushare_token

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PROJECT_DIR = Path(__file__).resolve().parent
RESEARCH_CONFIG_FILE = PROJECT_DIR / "research_config.json"
TODAY_STR = datetime.now().strftime("%Y%m%d")


DATASET_SPECS = {
    "daily_basic": {
        "dir": "daily_basic",
        "file": "daily_basic.parquet",
        "fetch": "daily_basic",
        "date_col": "trade_date",
        "fields": "ts_code,trade_date,turnover_rate,turnover_rate_f,volume_ratio,total_share,float_share,free_share,total_mv,circ_mv,pe_ttm,pb",
    },
    "adj_factor": {
        "dir": "adj_factor",
        "file": "adj_factor.parquet",
        "fetch": "adj_factor",
        "date_col": "trade_date",
        "fields": None,
    },
    "stk_limit": {
        "dir": "stk_limit",
        "file": "stk_limit.parquet",
        "fetch": "stk_limit",
        "date_col": "trade_date",
        "fields": None,
    },
    "moneyflow": {
        "dir": "moneyflow",
        "file": "moneyflow.parquet",
        "fetch": "moneyflow",
        "date_col": "trade_date",
        "fields": None,
    },
}


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def parse_args():
    parser = argparse.ArgumentParser(description="同步研究所需的 Tushare 原始数据（支持一次性回补 + 后续增量补齐）")
    parser.add_argument("--start-date", default="", help="起始日期 YYYYMMDD，默认优先读 research_config.json")
    parser.add_argument("--end-date", default="", help="结束日期 YYYYMMDD，默认今天")
    parser.add_argument("--datasets", default="daily_basic,adj_factor,stk_limit,moneyflow,trade_cal,stock_basic", help="逗号分隔的数据集列表")
    parser.add_argument("--force", action="store_true", help="忽略已有缓存，强制全量重拉指定区间")
    return parser.parse_args()

def fetch_with_retry(label: str, fetch_func, max_retries: int = 4, base_wait: int = 3):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            return fetch_func()
        except Exception as exc:
            last_error = exc
            print(f"[重试] {label} 第 {attempt}/{max_retries} 次失败: {repr(exc)}")
            if attempt < max_retries:
                wait_s = base_wait * attempt
                print(f"[等待] {label} {wait_s} 秒后重试")
                time.sleep(wait_s)
    raise last_error



def normalize_date_str(value: str | pd.Timestamp | None) -> str:
    if value is None or value == "":
        return ""
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y%m%d")



def date_range_strings(start_date: str, end_date: str) -> list[str]:
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    values = []
    cur = start_dt
    while cur <= end_dt:
        values.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)
    return values



def read_parquet_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()



def save_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)



def build_dirs(base_dir: Path) -> dict[str, Path]:
    root = base_dir / "data" / "research_raw"
    output_dir = RESEARCH_RAW_SYNC_OUTPUT_DIR
    logs_dir = LOGS_DIR
    root.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    return {"root": root, "output": output_dir, "logs": logs_dir}



def load_research_sync_cfg() -> dict:
    cfg = load_json(RESEARCH_CONFIG_FILE)
    raw_sync = cfg.get("raw_sync", {}) if isinstance(cfg.get("raw_sync", {}), dict) else {}
    return raw_sync



def choose_start_end(args) -> tuple[str, str]:
    raw_sync = load_research_sync_cfg()
    start_date = normalize_date_str(args.start_date) or normalize_date_str(raw_sync.get("start_date"))
    if not start_date:
        # 默认回补最近12个月
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
    end_date = normalize_date_str(args.end_date) or datetime.now().strftime("%Y%m%d")
    return start_date, end_date



def sync_trade_cal(pro, root_dir: Path, start_date: str, end_date: str, force: bool) -> tuple[pd.DataFrame, dict]:
    cal_dir = root_dir / "trade_cal"
    cal_file = cal_dir / "trade_cal.parquet"
    existing = read_parquet_safe(cal_file)
    existing_dates = set(existing["cal_date"].astype(str).tolist()) if not existing.empty and "cal_date" in existing.columns else set()
    expected = set(date_range_strings(start_date, end_date))
    if force or not expected.issubset(existing_dates):
        df = fetch_with_retry(
            f"trade_cal {start_date}-{end_date}",
            lambda: pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date),
        )
        if df is None:
            df = pd.DataFrame()
        if not df.empty:
            df["cal_date"] = df["cal_date"].astype(str)
        merged = pd.concat([existing, df], ignore_index=True) if not existing.empty else df.copy()
        if not merged.empty:
            merged = merged.drop_duplicates(subset=["exchange", "cal_date"], keep="last").sort_values(["exchange", "cal_date"]).reset_index(drop=True)
            save_parquet(merged, cal_file)
        existing = merged
    stats = {
        "dataset": "trade_cal",
        "row_count": len(existing),
        "file": str(cal_file),
        "date_start": start_date,
        "date_end": end_date,
    }
    return existing, stats



def sync_stock_basic(pro, root_dir: Path) -> dict:
    sb_dir = root_dir / "stock_basic"
    latest_file = sb_dir / "stock_basic_latest.parquet"
    snapshot_file = sb_dir / f"stock_basic_{TODAY_STR}.parquet"
    df = fetch_with_retry(
        "stock_basic",
        lambda: pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name,area,industry,market,exchange,list_date,is_hs"),
    )
    if df is None:
        df = pd.DataFrame()
    if not df.empty:
        df["symbol"] = df["symbol"].astype(str).str.zfill(6)
        save_parquet(df, latest_file)
        save_parquet(df, snapshot_file)
    return {
        "dataset": "stock_basic",
        "row_count": len(df),
        "file": str(latest_file),
        "snapshot_file": str(snapshot_file),
    }



def open_trade_dates(cal_df: pd.DataFrame, start_date: str, end_date: str) -> list[str]:
    if cal_df.empty:
        return []
    work = cal_df.copy()
    work["cal_date"] = work["cal_date"].astype(str)
    if "is_open" in work.columns:
        work["is_open"] = work["is_open"].astype(str)
        work = work[work["is_open"] == "1"]
    work = work[(work["cal_date"] >= start_date) & (work["cal_date"] <= end_date)]
    return sorted(work["cal_date"].unique().tolist())



def sync_trade_date_dataset(pro, root_dir: Path, dataset_name: str, trade_dates: list[str], force: bool) -> dict:
    spec = DATASET_SPECS[dataset_name]
    ds_dir = root_dir / spec["dir"]
    ds_file = ds_dir / spec["file"]
    existing = read_parquet_safe(ds_file)
    date_col = spec["date_col"]
    existing_dates = set(existing[date_col].astype(str).tolist()) if not existing.empty and date_col in existing.columns else set()
    pending = trade_dates if force else [d for d in trade_dates if d not in existing_dates]

    print(f"[{dataset_name}] 需补齐交易日数量: {len(pending)}")
    fetched_frames = []
    for idx, trade_date in enumerate(pending, start=1):
        print(f"[{dataset_name}] [进度] {idx}/{len(pending)} 获取 {trade_date}")
        def _fetch():
            if spec["fields"]:
                return getattr(pro, spec["fetch"])(trade_date=trade_date, fields=spec["fields"])
            return getattr(pro, spec["fetch"])(trade_date=trade_date)
        df = fetch_with_retry(f"{dataset_name} {trade_date}", _fetch)
        if df is None or df.empty:
            continue
        if date_col in df.columns:
            df[date_col] = df[date_col].astype(str)
        if "ts_code" in df.columns:
            df["股票代码"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
        fetched_frames.append(df)
        time.sleep(0.15)

    merged = existing.copy() if not existing.empty else pd.DataFrame()
    if fetched_frames:
        new_df = pd.concat(fetched_frames, ignore_index=True)
        merged = pd.concat([existing, new_df], ignore_index=True) if not existing.empty else new_df
    if not merged.empty:
        subset = [c for c in ["ts_code", date_col] if c in merged.columns]
        if subset:
            merged = merged.drop_duplicates(subset=subset, keep="last")
        sort_cols = [c for c in [date_col, "ts_code"] if c in merged.columns]
        if sort_cols:
            merged = merged.sort_values(sort_cols).reset_index(drop=True)
        save_parquet(merged, ds_file)

    return {
        "dataset": dataset_name,
        "row_count": len(merged),
        "new_trade_dates": len(pending),
        "file": str(ds_file),
        "date_start": trade_dates[0] if trade_dates else "",
        "date_end": trade_dates[-1] if trade_dates else "",
    }



def main():
    args = parse_args()
    token = load_tushare_token()
    if not token:
        raise RuntimeError("未找到 TUSHARE_TOKEN。请设置环境变量 TUSHARE_TOKEN，或在项目根目录提供 tushare_config.local.json")

    ts.set_token(token)
    pro = ts.pro_api(token)

    base_dir = resolve_base_dir()
    dirs = build_dirs(base_dir)
    start_date, end_date = choose_start_end(args)
    datasets = [x.strip() for x in args.datasets.split(",") if x.strip()]

    print(f"研究原始数据同步范围: {start_date} ~ {end_date}")
    print(f"研究原始数据目录: {dirs['root']}")

    summary_rows = []

    cal_df = pd.DataFrame()
    if "trade_cal" in datasets:
        cal_df, stats = sync_trade_cal(pro, dirs["root"], start_date, end_date, args.force)
        summary_rows.append(stats)
    else:
        cal_file = dirs["root"] / "trade_cal" / "trade_cal.parquet"
        cal_df = read_parquet_safe(cal_file)
        if cal_df.empty:
            raise RuntimeError("未同步 trade_cal，且本地也不存在 trade_cal.parquet，无法判断交易日")

    if "stock_basic" in datasets:
        summary_rows.append(sync_stock_basic(pro, dirs["root"]))

    trade_dates = open_trade_dates(cal_df, start_date, end_date)
    print(f"开放交易日数量: {len(trade_dates)}")

    for dataset_name in ["daily_basic", "adj_factor", "stk_limit", "moneyflow"]:
        if dataset_name in datasets:
            summary_rows.append(sync_trade_date_dataset(pro, dirs["root"], dataset_name, trade_dates, args.force))

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_df = pd.DataFrame(summary_rows)
    summary_file = dirs["output"] / f"p8_research_raw_sync_summary_{stamp}.csv"
    log_file = dirs["logs"] / f"p8_research_raw_sync_{stamp}.log"
    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")
    log_lines = [
        "P8 research raw sync finished",
        f"base_dir={base_dir}",
        f"start_date={start_date}",
        f"end_date={end_date}",
        f"datasets={','.join(datasets)}",
        f"summary_file={summary_file}",
    ]
    log_file.write_text("\n".join(log_lines), encoding="utf-8")

    print("运行完成")
    print(f"汇总文件: {summary_file}")
    print(f"日志文件: {log_file}")


if __name__ == "__main__":
    main()
