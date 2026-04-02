from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os
import sys

import pandas as pd
from project_paths import LOGS_DIR, SCAN_OUTPUT_DIR, WATCHLIST_OUTPUT_DIR, resolve_base_dir

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

TODAY_STR = datetime.now().strftime("%Y%m%d")
TIMESTAMP_STR = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
BASE_DIR = resolve_base_dir()
PACK_FILE = BASE_DIR / "data" / "packed" / "daily_hist_all.parquet"
WATCHLIST_DIR = WATCHLIST_OUTPUT_DIR
SNAPSHOT_DIR = WATCHLIST_DIR / "snapshots"

WATCHLIST_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

MASTER_FILE = WATCHLIST_DIR / "watchlist_master.csv"
SNAPSHOT_FILE = SNAPSHOT_DIR / f"{TODAY_STR}_watchlist_snapshot.csv"
SUMMARY_FILE = WATCHLIST_DIR / f"watchlist_summary_{TODAY_STR}.csv"
LOG_FILE = LOGS_DIR / f"p8_build_watchlist_{TODAY_STR}.log"

SCAN_GLOBS = {
    "selected": "p7_scan_from_parquet_all_selected_*.csv",
    "candidate": "p7_scan_from_parquet_all_candidate_*.csv",
    "watch": "p7_scan_from_parquet_all_watch_*.csv",
    "results": "p7_scan_from_parquet_all_results_*.csv",
    "summary": "p7_scan_from_parquet_all_summary_*.csv",
}

SCAN_COLUMNS = [
    "股票代码",
    "股票名称",
    "日期",
    "开盘",
    "收盘",
    "最高",
    "最低",
    "涨跌幅%",
    "换手率",
    "量比前一日",
    "VR5",
    "CLV",
    "BR20",
    "命中硬过滤数",
    "硬过滤是否通过",
    "硬过滤未通过原因",
    "硬过滤结果说明",
    "分层标签",
    "分层标签说明",
]
PARQUET_COLUMNS = ["股票代码", "股票名称", "日期", "开盘", "收盘", "最高", "最低"]
PRIORITY = {"selected": 3, "candidate": 2, "watch": 1}
SOURCE_BUCKET_CN = {"selected": "入围", "candidate": "候选", "watch": "观察"}


def build_entry_reason(source_bucket: str, d0_label: str, d0_hard_pass: str, d0_failed_reason: str) -> str:
    source_cn = SOURCE_BUCKET_CN.get(source_bucket, source_bucket)
    if source_bucket == "selected":
        if str(d0_label) == "放弃":
            return "入围=硬过滤通过；原始分层标签为放弃，表示未达到候选/观察阈值"
        return f"入围=硬过滤通过；原始分层标签={d0_label}"
    if source_bucket == "candidate":
        reason = d0_failed_reason or "未写明"
        return f"候选=原始分层标签为候选；硬过滤未通过原因：{reason}" if str(d0_hard_pass) != "是" else "候选=原始分层标签为候选，同时硬过滤通过"
    if source_bucket == "watch":
        reason = d0_failed_reason or "未写明"
        return f"观察=原始分层标签为观察；硬过滤未通过原因：{reason}" if str(d0_hard_pass) != "是" else "观察=原始分层标签为观察，同时硬过滤通过"
    return source_cn


def latest_file(directory: Path, pattern: str):
    files = sorted(directory.glob(pattern))
    if not files:
        return None
    return files[-1]


def read_csv_safe(path: Path | None):
    if path is None or not path.exists():
        return pd.DataFrame()
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            df = pd.read_csv(path, encoding=encoding, dtype={"股票代码": str, "code": str})
            if "股票代码" in df.columns:
                df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
            if "code" in df.columns:
                df["code"] = df["code"].astype(str).str.zfill(6)
            return df
        except Exception:
            continue
    return pd.DataFrame()


def load_scan_frames():
    files = {key: latest_file(SCAN_OUTPUT_DIR, pattern) for key, pattern in SCAN_GLOBS.items()}
    frames = {key: read_csv_safe(path) for key, path in files.items()}
    return files, frames


def merge_watchlist_pool(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    merged_parts = []
    for bucket in ["selected", "candidate", "watch"]:
        df = frames.get(bucket, pd.DataFrame()).copy()
        if df.empty:
            continue
        df["source_bucket"] = bucket
        df["source_priority"] = PRIORITY[bucket]
        keep_cols = [col for col in SCAN_COLUMNS if col in df.columns]
        merged_parts.append(df[keep_cols + ["source_bucket", "source_priority"]])

    if not merged_parts:
        return pd.DataFrame()

    merged = pd.concat(merged_parts, ignore_index=True)
    merged = merged.sort_values(["股票代码", "source_priority"], ascending=[True, False])
    merged = merged.drop_duplicates(subset=["股票代码"], keep="first").reset_index(drop=True)
    return merged


def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["收盘"].shift(1)
    tr1 = df["最高"] - df["最低"]
    tr2 = (df["最高"] - prev_close).abs()
    tr3 = (df["最低"] - prev_close).abs()
    out = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return out


def build_price_context(pool_df: pd.DataFrame) -> pd.DataFrame:
    if pool_df.empty:
        return pd.DataFrame()
    if not PACK_FILE.exists():
        raise FileNotFoundError(f"未找到 parquet 文件: {PACK_FILE}")

    target_symbols = set(pool_df["股票代码"].astype(str).str.zfill(6).tolist())
    hist = pd.read_parquet(PACK_FILE, columns=PARQUET_COLUMNS)
    hist = hist.copy()
    hist["股票代码"] = hist["股票代码"].astype(str).str.zfill(6)
    hist["日期"] = pd.to_datetime(hist["日期"], errors="coerce")
    for col in ["开盘", "收盘", "最高", "最低"]:
        hist[col] = pd.to_numeric(hist[col], errors="coerce")
    hist = hist.dropna(subset=["日期", "开盘", "收盘", "最高", "最低"])
    hist = hist[hist["股票代码"].isin(target_symbols)].copy()
    hist = hist.sort_values(["股票代码", "日期"]).reset_index(drop=True)

    records = []
    for symbol, df in hist.groupby("股票代码", sort=False):
        df = df.reset_index(drop=True)
        if df.empty:
            continue
        df["TR"] = true_range(df)
        df["ATR14"] = df["TR"].rolling(14, min_periods=5).mean()
        latest = df.iloc[-1]
        prev20_high = float(df["最高"].iloc[-21:-1].max()) if len(df) >= 21 else float(df["最高"].max())
        breakout_price = max(float(latest["最高"]), prev20_high)
        atr14 = float(latest["ATR14"]) if pd.notna(latest["ATR14"]) else 0.0
        support_price_1 = float(latest["收盘"])
        support_price_2 = float(latest["最低"])
        mid_price = (float(latest["最高"]) + float(latest["最低"])) / 2.0
        target_price_1 = breakout_price + 0.5 * atr14
        target_price_2 = breakout_price + 1.0 * atr14
        records.append(
            {
                "股票代码": symbol,
                "atr14": round(atr14, 3),
                "prev20_high": round(prev20_high, 3),
                "breakout_price": round(breakout_price, 3),
                "support_price_1": round(support_price_1, 3),
                "support_price_2": round(support_price_2, 3),
                "mid_price": round(mid_price, 3),
                "target_price_1": round(target_price_1, 3),
                "target_price_2": round(target_price_2, 3),
            }
        )
    return pd.DataFrame(records)


def load_existing_master() -> pd.DataFrame:
    df = read_csv_safe(MASTER_FILE)
    return df if not df.empty else pd.DataFrame()


def build_watchlist_records(pool_df: pd.DataFrame, price_df: pd.DataFrame) -> pd.DataFrame:
    if pool_df.empty:
        return pd.DataFrame()
    out = pool_df.copy()
    out["股票代码"] = out["股票代码"].astype(str).str.zfill(6)
    out = out.merge(price_df, on="股票代码", how="left")
    out["setup_date"] = pd.to_datetime(out["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    out["watch_id"] = out["setup_date"].astype(str) + "_" + out["股票代码"]
    out["status"] = "D0入池"
    out["next_stage"] = "D1待复核"
    out["created_at"] = TIMESTAMP_STR
    out["updated_at"] = TIMESTAMP_STR
    out["review_note"] = ""
    out["d1_action"] = ""
    out["d2_action"] = ""
    out["final_result_tag"] = ""

    rename_map = {
        "日期": "d0_date",
        "开盘": "d0_open",
        "收盘": "d0_close",
        "最高": "d0_high",
        "最低": "d0_low",
        "涨跌幅%": "d0_pct_chg",
        "换手率": "d0_turnover",
        "量比前一日": "d0_volume_ratio_prev1",
        "VR5": "d0_vr5",
        "CLV": "d0_clv",
        "BR20": "d0_br20",
        "命中硬过滤数": "d0_hit_count",
        "硬过滤是否通过": "d0_hard_pass",
        "硬过滤未通过原因": "d0_failed_reason",
        "分层标签": "d0_label",
    }
    out = out.rename(columns=rename_map)
    out["source_bucket_cn"] = out["source_bucket"].map(SOURCE_BUCKET_CN).fillna(out["source_bucket"])
    if "d0_failed_reason" not in out.columns:
        out["d0_failed_reason"] = ""
    else:
        out["d0_failed_reason"] = out["d0_failed_reason"].fillna("")
    if "d0_hard_pass" not in out.columns:
        out["d0_hard_pass"] = ""
    else:
        out["d0_hard_pass"] = out["d0_hard_pass"].fillna("")
    if "硬过滤结果说明" not in out.columns:
        out["硬过滤结果说明"] = ""
    else:
        out["硬过滤结果说明"] = out["硬过滤结果说明"].fillna("")
    if "分层标签说明" not in out.columns:
        out["分层标签说明"] = ""
    else:
        out["分层标签说明"] = out["分层标签说明"].fillna("")
    out["entry_reason"] = out.apply(
        lambda r: build_entry_reason(str(r.get("source_bucket", "")), str(r.get("d0_label", "")), str(r.get("d0_hard_pass", "")), str(r.get("d0_failed_reason", ""))),
        axis=1,
    )
    out["d0_failed_reason_display"] = out.apply(
        lambda r: str(r.get("d0_failed_reason", "")).strip() or ("硬过滤通过" if str(r.get("d0_hard_pass", "")) == "是" else "未写明"),
        axis=1,
    )
    ordered = [
        "watch_id",
        "setup_date",
        "股票代码",
        "股票名称",
        "source_bucket",
        "source_bucket_cn",
        "status",
        "next_stage",
        "d0_date",
        "d0_open",
        "d0_close",
        "d0_high",
        "d0_low",
        "d0_pct_chg",
        "d0_turnover",
        "d0_volume_ratio_prev1",
        "d0_vr5",
        "d0_clv",
        "d0_br20",
        "d0_hit_count",
        "d0_hard_pass",
        "d0_failed_reason",
        "d0_failed_reason_display",
        "d0_label",
        "entry_reason",
        "硬过滤结果说明",
        "分层标签说明",
        "atr14",
        "prev20_high",
        "breakout_price",
        "support_price_1",
        "support_price_2",
        "mid_price",
        "target_price_1",
        "target_price_2",
        "created_at",
        "updated_at",
        "review_note",
        "d1_action",
        "d2_action",
        "final_result_tag",
    ]
    for col in ordered:
        if col not in out.columns:
            out[col] = ""
    out = out[ordered].copy()
    return out


def merge_into_master(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if existing_df.empty:
        return new_df.copy()
    existing_df = existing_df.copy()
    if "watch_id" not in existing_df.columns:
        existing_df["watch_id"] = existing_df.get("setup_date", "").astype(str) + "_" + existing_df.get("股票代码", "").astype(str)
    keep_old = existing_df[~existing_df["watch_id"].isin(set(new_df["watch_id"].tolist()))].copy()
    merged = pd.concat([keep_old, new_df], ignore_index=True)
    merged = merged.sort_values(["setup_date", "股票代码"], ascending=[False, True]).reset_index(drop=True)
    return merged


def main():
    files, frames = load_scan_frames()
    pool_df = merge_watchlist_pool(frames)
    if pool_df.empty:
        raise RuntimeError("未找到 selected/candidate/watch 扫描结果，无法生成 watchlist。")

    price_df = build_price_context(pool_df)
    watchlist_df = build_watchlist_records(pool_df, price_df)
    existing_master = load_existing_master()
    master_df = merge_into_master(existing_master, watchlist_df)

    watchlist_df.to_csv(SNAPSHOT_FILE, index=False, encoding="utf-8-sig")
    master_df.to_csv(MASTER_FILE, index=False, encoding="utf-8-sig")

    summary_df = pd.DataFrame(
        [
            {
                "snapshot_date": TODAY_STR,
                "pool_count": len(watchlist_df),
                "master_count": len(master_df),
                "selected_file": str(files.get("selected") or ""),
                "candidate_file": str(files.get("candidate") or ""),
                "watch_file": str(files.get("watch") or ""),
                "results_file": str(files.get("results") or ""),
                "pack_file": str(PACK_FILE),
                "snapshot_file": str(SNAPSHOT_FILE),
                "master_file": str(MASTER_FILE),
            }
        ]
    )
    summary_df.to_csv(SUMMARY_FILE, index=False, encoding="utf-8-sig")

    log_lines = [
        "P8 build watchlist finished",
        f"base_dir={BASE_DIR}",
        f"pool_count={len(watchlist_df)}",
        f"master_count={len(master_df)}",
        f"selected_file={files.get('selected')}",
        f"candidate_file={files.get('candidate')}",
        f"watch_file={files.get('watch')}",
        f"results_file={files.get('results')}",
        f"pack_file={PACK_FILE}",
        f"snapshot_file={SNAPSHOT_FILE}",
        f"summary_file={SUMMARY_FILE}",
        f"master_file={MASTER_FILE}",
    ]
    LOG_FILE.write_text("\n".join(log_lines), encoding="utf-8")

    print("运行完成")
    print(f"今日 watchlist 数量: {len(watchlist_df)}")
    print(f"主 watchlist 数量: {len(master_df)}")
    print(f"快照文件: {SNAPSHOT_FILE}")
    print(f"主文件: {MASTER_FILE}")
    print(f"汇总文件: {SUMMARY_FILE}")
    print(f"日志文件: {LOG_FILE}")


if __name__ == "__main__":
    main()
