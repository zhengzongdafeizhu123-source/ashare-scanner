from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import time

import pandas as pd
from project_paths import LOGS_DIR, MAINTENANCE_OUTPUT_DIR, UNIVERSE_OUTPUT_DIR, resolve_base_dir
from tushare_token import load_tushare_token


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


def sanitize_proxy_env():
    bad_proxy_values = {
        "http://127.0.0.1:9",
        "https://127.0.0.1:9",
    }
    for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        value = os.environ.get(key, "").strip().lower()
        if value in bad_proxy_values:
            os.environ.pop(key, None)

def init_tushare_pro():
    try:
        import tushare as ts
    except ImportError as exc:
        raise RuntimeError("未安装 tushare。请先执行：pip install tushare") from exc

    token = load_tushare_token()
    ts.set_token(token)
    return ts.pro_api(token)


sanitize_proxy_env()

BASE_DIR = resolve_base_dir()
DATA_DIR = BASE_DIR / "data" / "daily_hist"
OUTPUT_DIR = MAINTENANCE_OUTPUT_DIR

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")
today_date = datetime.now().date()

success_file = OUTPUT_DIR / f"p6_update_daily_hist_tushare_success_{today_str}.csv"
error_file = OUTPUT_DIR / f"p6_update_daily_hist_tushare_errors_{today_str}.csv"
skip_file = OUTPUT_DIR / f"p6_update_daily_hist_tushare_skipped_{today_str}.csv"
summary_file = OUTPUT_DIR / f"p6_update_daily_hist_tushare_summary_{today_str}.csv"
log_file = LOGS_DIR / f"p6_update_daily_hist_tushare_{today_str}.log"

success_rows = []
error_rows = []
skip_rows = []


def latest_universe_file():
    files = sorted(UNIVERSE_OUTPUT_DIR.glob("p3_universe_filtered_*.csv"))
    return files[-1] if files else None


def load_name_map(pro):
    universe_file = latest_universe_file()
    if universe_file is not None:
        df = pd.read_csv(universe_file, dtype={"code": str})
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["name"] = df["name"].astype(str).str.strip()
        return dict(zip(df["code"], df["name"])), str(universe_file)

    print("[提示] 未找到最新股票池文件，改为调用 Tushare stock_basic 获取名称映射")
    df = pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name")
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str).str.strip()
    return dict(zip(df["symbol"], df["name"])), "tushare.stock_basic"


def load_local_file_index():
    rows = []
    files = sorted(DATA_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"未找到任何历史库文件: {DATA_DIR}")

    for file_path in files:
        try:
            df = pd.read_csv(file_path, usecols=["股票代码", "股票名称", "日期"], dtype={"股票代码": str})
            if df.empty:
                rows.append({"symbol": file_path.stem.zfill(6), "name": "", "latest_date": pd.NaT, "file_path": file_path})
                continue

            df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
            df["股票名称"] = df["股票名称"].astype(str).str.strip()
            df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
            df = df.dropna(subset=["日期"]).sort_values("日期")
            if df.empty:
                rows.append({"symbol": file_path.stem.zfill(6), "name": "", "latest_date": pd.NaT, "file_path": file_path})
                continue

            latest = df.iloc[-1]
            rows.append(
                {
                    "symbol": str(latest["股票代码"]).zfill(6),
                    "name": str(latest["股票名称"]).strip(),
                    "latest_date": latest["日期"].date(),
                    "file_path": file_path,
                }
            )
        except Exception:
            rows.append({"symbol": file_path.stem.zfill(6), "name": "", "latest_date": pd.NaT, "file_path": file_path})

    return pd.DataFrame(rows)


def fetch_with_retry(fetch_func, description, max_retries=4, base_sleep=3):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            return fetch_func()
        except Exception as exc:
            last_error = exc
            print(f"[重试] {description} 第 {attempt}/{max_retries} 次失败: {repr(exc)}")
            if attempt < max_retries:
                sleep_seconds = base_sleep * attempt
                print(f"[等待] {description} {sleep_seconds} 秒后重试")
                time.sleep(sleep_seconds)
    raise last_error


def build_natural_dates(start_date_str, end_date_str):
    start_dt = datetime.strptime(start_date_str, "%Y%m%d").date()
    end_dt = datetime.strptime(end_date_str, "%Y%m%d").date()
    dates = []
    current = start_dt
    while current <= end_dt:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
    return dates


def calculate_amplitude(high_series, low_series, pre_close_series):
    high_series = pd.to_numeric(high_series, errors="coerce")
    low_series = pd.to_numeric(low_series, errors="coerce")
    pre_close_series = pd.to_numeric(pre_close_series, errors="coerce")
    amplitude = ((high_series - low_series) / pre_close_series.replace(0, pd.NA)) * 100.0
    return amplitude


def normalize_trade_date_frame(daily_df, basic_df, name_map, target_symbols):
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()

    if basic_df is None or basic_df.empty:
        basic_subset = pd.DataFrame(columns=["ts_code", "trade_date", "turnover_rate"])
    else:
        basic_subset = basic_df[["ts_code", "trade_date", "turnover_rate"]].copy()

    merged = daily_df.merge(
        basic_subset,
        on=["ts_code", "trade_date"],
        how="left",
    )
    merged["symbol"] = merged["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
    if target_symbols:
        merged = merged[merged["symbol"].isin(target_symbols)].copy()

    if merged.empty:
        return pd.DataFrame()

    merged["股票代码"] = merged["symbol"]
    merged["股票名称"] = merged["股票代码"].map(name_map).fillna(merged["股票代码"])
    merged["日期"] = pd.to_datetime(merged["trade_date"], format="%Y%m%d", errors="coerce")
    merged["开盘"] = pd.to_numeric(merged["open"], errors="coerce")
    merged["收盘"] = pd.to_numeric(merged["close"], errors="coerce")
    merged["最高"] = pd.to_numeric(merged["high"], errors="coerce")
    merged["最低"] = pd.to_numeric(merged["low"], errors="coerce")
    merged["成交量"] = pd.to_numeric(merged["vol"], errors="coerce")
    merged["成交额"] = pd.to_numeric(merged["amount"], errors="coerce") * 1000.0
    merged["换手率"] = pd.to_numeric(merged["turnover_rate"], errors="coerce").fillna(0.0)
    merged["涨跌额"] = pd.to_numeric(merged["change"], errors="coerce")
    merged["涨跌幅"] = pd.to_numeric(merged["pct_chg"], errors="coerce")
    merged["振幅"] = calculate_amplitude(merged["high"], merged["low"], merged["pre_close"])

    out = merged[
        [
            "股票代码",
            "股票名称",
            "日期",
            "开盘",
            "收盘",
            "最高",
            "最低",
            "成交量",
            "成交额",
            "振幅",
            "涨跌幅",
            "涨跌额",
            "换手率",
        ]
    ].copy()
    out = out.dropna(subset=["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额"])
    out = out.sort_values(["股票代码", "日期"]).reset_index(drop=True)
    return out


def fetch_trade_date_frame(pro, trade_date_str, name_map, target_symbols):
    daily_df = fetch_with_retry(
        lambda: pro.daily(
            trade_date=trade_date_str,
            fields="ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount",
        ),
        f"daily {trade_date_str}",
        max_retries=4,
        base_sleep=3,
    )
    basic_df = fetch_with_retry(
        lambda: pro.daily_basic(trade_date=trade_date_str, fields="ts_code,trade_date,turnover_rate"),
        f"daily_basic {trade_date_str}",
        max_retries=4,
        base_sleep=3,
    )
    frame = normalize_trade_date_frame(daily_df, basic_df, name_map, target_symbols)
    print(f"[交易日] {trade_date_str} 获取到 {len(frame)} 行目标股票数据")
    return frame


def finalize(file_index, trade_dates, start_date, end_date, name_source):
    success_df = pd.DataFrame(success_rows)
    error_df = pd.DataFrame(error_rows)
    skip_df = pd.DataFrame(skip_rows)

    success_df.to_csv(success_file, index=False, encoding="utf-8-sig")
    error_df.to_csv(error_file, index=False, encoding="utf-8-sig")
    skip_df.to_csv(skip_file, index=False, encoding="utf-8-sig")

    summary_df = pd.DataFrame(
        [
            {
                "模式": "tushare_natural_date_batch_update",
                "base_dir": str(BASE_DIR),
                "data_dir": str(DATA_DIR),
                "name_source": name_source,
                "file_total": len(file_index),
                "trade_date_count": len(trade_dates),
                "trade_date_start": trade_dates[0] if trade_dates else "",
                "trade_date_end": trade_dates[-1] if trade_dates else "",
                "requested_start_date": start_date,
                "requested_end_date": end_date,
                "success_count": len(success_rows),
                "error_count": len(error_rows),
                "skip_count": len(skip_rows),
            }
        ]
    )
    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")

    log_lines = [
        "P6 tushare natural-date batch update finished",
        f"base_dir={BASE_DIR}",
        f"data_dir={DATA_DIR}",
        f"name_source={name_source}",
        f"file_total={len(file_index)}",
        f"trade_date_count={len(trade_dates)}",
        f"trade_date_start={trade_dates[0] if trade_dates else ''}",
        f"trade_date_end={trade_dates[-1] if trade_dates else ''}",
        f"requested_start_date={start_date}",
        f"requested_end_date={end_date}",
        f"success_count={len(success_rows)}",
        f"error_count={len(error_rows)}",
        f"skip_count={len(skip_rows)}",
        f"success_file={success_file}",
        f"error_file={error_file}",
        f"skip_file={skip_file}",
        f"summary_file={summary_file}",
    ]
    log_file.write_text("\n".join(log_lines), encoding="utf-8")

    print("运行完成")
    print(f"成功数量: {len(success_rows)}")
    print(f"失败数量: {len(error_rows)}")
    print(f"跳过数量: {len(skip_rows)}")
    print(f"成功清单: {success_file}")
    print(f"异常清单: {error_file}")
    print(f"跳过清单: {skip_file}")
    print(f"汇总文件: {summary_file}")
    print(f"日志文件: {log_file}")


def main():
    pro = init_tushare_pro()
    name_map, name_source = load_name_map(pro)

    file_index = load_local_file_index()
    print(f"待检查历史库文件数量: {len(file_index)}")
    print(f"名称映射来源: {name_source}")

    file_index["latest_date"] = pd.to_datetime(file_index["latest_date"], errors="coerce")
    min_latest = file_index["latest_date"].min()
    if pd.isna(min_latest):
        raise RuntimeError("本地历史库无法识别最新日期，请先检查 CSV 文件格式。")

    start_date = (min_latest.date() + timedelta(days=1)).strftime("%Y%m%d")
    end_date = today_str

    if start_date > end_date:
        print("[提示] 本地历史库已是最新日期，无需更新。")
        for row in file_index.itertuples(index=False):
            if pd.notna(row.latest_date):
                latest_local = pd.to_datetime(row.latest_date).date()
            else:
                latest_local = ""
            skip_rows.append(
                {
                    "股票代码": row.symbol,
                    "股票名称": row.name,
                    "最新本地日期": str(latest_local),
                    "原因": "本地日期已覆盖到今日，无需更新",
                }
            )
        finalize(file_index, [], start_date, end_date, name_source)
        return

    trade_dates = build_natural_dates(start_date, end_date)
    if not trade_dates:
        print(f"[提示] {start_date} ~ {end_date} 之间没有新增日期。")
        for row in file_index.itertuples(index=False):
            if pd.notna(row.latest_date):
                latest_local = pd.to_datetime(row.latest_date).date()
            else:
                latest_local = ""
            skip_rows.append(
                {
                    "股票代码": row.symbol,
                    "股票名称": row.name,
                    "最新本地日期": str(latest_local),
                    "原因": "区间内无新增日期",
                }
            )
        finalize(file_index, trade_dates, start_date, end_date, name_source)
        return

    print(f"准备通过 Tushare 批量更新日期数量: {len(trade_dates)}")
    print(f"日期区间: {trade_dates[0]} ~ {trade_dates[-1]}")

    target_symbols = set(file_index["symbol"].astype(str).str.zfill(6).tolist())
    daily_frames = []

    for idx, trade_date in enumerate(trade_dates, start=1):
        print(f"[进度] {idx}/{len(trade_dates)} 开始获取 {trade_date}")
        frame = fetch_trade_date_frame(pro, trade_date, name_map, target_symbols)
        if frame.empty:
            print(f"[跳过] {trade_date} 无全市场新增数据（可能是非交易日）")
        daily_frames.append(frame)
        time.sleep(1.0)

    updates_df = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    if updates_df.empty:
        print("[提示] 所有日期均未获取到可追加的数据。")
        for row in file_index.itertuples(index=False):
            if pd.notna(row.latest_date):
                latest_local = pd.to_datetime(row.latest_date).date()
            else:
                latest_local = ""
            skip_rows.append(
                {
                    "股票代码": row.symbol,
                    "股票名称": row.name,
                    "最新本地日期": str(latest_local),
                    "原因": "远端未返回新增数据",
                }
            )
        finalize(file_index, trade_dates, start_date, end_date, name_source)
        return

    latest_map = {
        str(row.symbol).zfill(6): pd.to_datetime(row.latest_date).date()
        for row in file_index.itertuples(index=False)
        if pd.notna(row.latest_date)
    }
    file_map = {str(row.symbol).zfill(6): Path(row.file_path) for row in file_index.itertuples(index=False)}
    name_local_map = {
        str(row.symbol).zfill(6): (str(row.name).strip() if str(row.name).strip() else name_map.get(str(row.symbol).zfill(6), ""))
        for row in file_index.itertuples(index=False)
    }

    updated_symbols = set()
    grouped = updates_df.groupby("股票代码", sort=False)
    for symbol, group in grouped:
        file_path = file_map.get(symbol)
        if file_path is None or not file_path.exists():
            skip_rows.append(
                {
                    "股票代码": symbol,
                    "股票名称": name_local_map.get(symbol, ""),
                    "最新本地日期": "",
                    "原因": "本地历史文件不存在，当前更新脚本不会自动新建，请另行补库",
                }
            )
            continue

        try:
            local_df = pd.read_csv(file_path, dtype={"股票代码": str})
            if local_df.empty:
                raise ValueError("本地文件为空")

            local_df["股票代码"] = local_df["股票代码"].astype(str).str.zfill(6)
            local_df["股票名称"] = local_df["股票名称"].astype(str).str.strip()
            local_df["日期"] = pd.to_datetime(local_df["日期"], errors="coerce")

            latest_local_date = latest_map.get(symbol)
            group = group.copy()
            if latest_local_date is not None:
                group = group[group["日期"].dt.date > latest_local_date].copy()

            if group.empty:
                skip_rows.append(
                    {
                        "股票代码": symbol,
                        "股票名称": name_local_map.get(symbol, ""),
                        "最新本地日期": str(latest_local_date) if latest_local_date else "",
                        "原因": "所有新增日期均已存在于本地文件",
                    }
                )
                continue

            merged = pd.concat([local_df, group], ignore_index=True)
            merged = merged.drop_duplicates(subset=["日期"], keep="last").sort_values("日期").reset_index(drop=True)

            for numeric_col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]:
                if numeric_col in merged.columns:
                    merged[numeric_col] = pd.to_numeric(merged[numeric_col], errors="coerce")

            preferred_order = [
                "股票代码",
                "股票名称",
                "日期",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "成交量",
                "成交额",
                "振幅",
                "涨跌幅",
                "涨跌额",
                "换手率",
            ]
            existing_cols = [col for col in preferred_order if col in merged.columns]
            extra_cols = [col for col in merged.columns if col not in existing_cols]
            merged = merged[existing_cols + extra_cols]

            merged["日期"] = pd.to_datetime(merged["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
            merged.to_csv(file_path, index=False, encoding="utf-8-sig")

            updated_symbols.add(symbol)
            success_rows.append(
                {
                    "股票代码": symbol,
                    "股票名称": name_local_map.get(symbol, ""),
                    "原最新日期": str(latest_local_date) if latest_local_date else "",
                    "新增条数": len(group),
                    "更新后最新日期": merged.iloc[-1]["日期"],
                    "文件路径": str(file_path),
                }
            )
            print(f"[完成] {symbol} 更新成功，新增 {len(group)} 条")
        except Exception as exc:
            error_rows.append({"文件名": file_path.name, "股票代码": symbol, "错误信息": repr(exc)})
            print(f"[失败] {file_path.name}: {repr(exc)}")

    untouched = target_symbols - updated_symbols
    for symbol in sorted(untouched):
        if any(row.get("股票代码") == symbol for row in skip_rows):
            continue
        latest_local_date = latest_map.get(symbol)
        skip_rows.append(
            {
                "股票代码": symbol,
                "股票名称": name_local_map.get(symbol, ""),
                "最新本地日期": str(latest_local_date) if latest_local_date else "",
                "原因": "新增日期内无该股票可用行情（可能停牌或无新增记录）",
            }
        )

    finalize(file_index, trade_dates, start_date, end_date, name_source)


if __name__ == "__main__":
    main()
