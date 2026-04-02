from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path
import os
import sys
import time

import pandas as pd
from project_paths import BOOTSTRAP_OUTPUT_DIR, LOGS_DIR, UNIVERSE_OUTPUT_DIR, resolve_base_dir
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
OUTPUT_DIR = BOOTSTRAP_OUTPUT_DIR

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")

success_file = OUTPUT_DIR / f"p4_bootstrap_tushare_success_{today_str}.csv"
error_file = OUTPUT_DIR / f"p4_bootstrap_tushare_errors_{today_str}.csv"
skip_file = OUTPUT_DIR / f"p4_bootstrap_tushare_skipped_{today_str}.csv"
summary_file = OUTPUT_DIR / f"p4_bootstrap_tushare_summary_{today_str}.csv"
log_file = LOGS_DIR / f"p4_bootstrap_tushare_{today_str}.log"

success_rows = []
error_rows = []
skip_rows = []


def latest_universe_file():
    files = sorted(UNIVERSE_OUTPUT_DIR.glob("p3_universe_filtered_*.csv"))
    return files[-1] if files else None


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


def load_universe_df(pro, universe_file_arg: str | None):
    universe_path = Path(universe_file_arg) if universe_file_arg else latest_universe_file()
    if universe_path is not None and universe_path.exists():
        df = pd.read_csv(universe_path, dtype={"code": str})
        df["code"] = df["code"].astype(str).str.zfill(6)
        df["name"] = df["name"].astype(str).str.strip()
        return df[["code", "name"]].drop_duplicates().reset_index(drop=True), str(universe_path)

    print("[提示] 未找到最新股票池文件，改为调用 Tushare stock_basic 获取上市股票列表")
    df = fetch_with_retry(
        lambda: pro.stock_basic(exchange="", list_status="L", fields="ts_code,symbol,name"),
        "stock_basic",
        max_retries=3,
        base_sleep=2,
    )
    df["symbol"] = df["symbol"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str).str.strip()
    df = df.rename(columns={"symbol": "code"})
    return df[["code", "name"]].drop_duplicates().reset_index(drop=True), "tushare.stock_basic"


def get_open_trade_dates(pro, start_date_str, end_date_str):
    cal = fetch_with_retry(
        lambda: pro.trade_cal(exchange="SSE", start_date=start_date_str, end_date=end_date_str, is_open="1"),
        f"trade_cal {start_date_str}-{end_date_str}",
        max_retries=3,
        base_sleep=2,
    )
    if cal is None or cal.empty:
        return []
    cal["cal_date"] = cal["cal_date"].astype(str)
    return sorted(cal["cal_date"].tolist())


def normalize_trade_date_frame(daily_df, basic_df, name_map, target_symbols):
    if daily_df is None or daily_df.empty:
        return pd.DataFrame()

    merged = daily_df.merge(
        basic_df[["ts_code", "trade_date", "turnover_rate"]] if basic_df is not None and not basic_df.empty else pd.DataFrame(columns=["ts_code", "trade_date", "turnover_rate"]),
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

    out = merged[["股票代码", "股票名称", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"]].copy()
    out = out.dropna(subset=["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额"])
    out = out.sort_values(["股票代码", "日期"]).reset_index(drop=True)
    return out


def fetch_trade_date_frame(pro, trade_date_str, name_map, target_symbols):
    daily_df = fetch_with_retry(
        lambda: pro.daily(trade_date=trade_date_str, fields="ts_code,trade_date,open,high,low,close,vol,amount"),
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


def parse_args():
    parser = argparse.ArgumentParser(description="使用 Tushare 按交易日批量补建/回补本地历史库。")
    parser.add_argument("--start-date", type=str, default="", help="开始日期 YYYYMMDD；默认回补最近 400 个自然日。")
    parser.add_argument("--end-date", type=str, default=today_str, help="结束日期 YYYYMMDD；默认今天。")
    parser.add_argument("--universe-file", type=str, default="", help="指定股票池文件；默认使用最新 p3_universe_filtered_*.csv。")
    parser.add_argument("--overwrite-existing", action="store_true", help="开启后直接覆盖已有 CSV；默认与现有文件合并去重。")
    return parser.parse_args()


def main():
    args = parse_args()
    pro = init_tushare_pro()

    if args.start_date:
        start_date = args.start_date
    else:
        start_date = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
    end_date = args.end_date or today_str

    universe_df, universe_source = load_universe_df(pro, args.universe_file or None)
    target_symbols = set(universe_df["code"].astype(str).str.zfill(6).tolist())
    name_map = dict(zip(universe_df["code"], universe_df["name"]))

    print(f"目标股票数量: {len(target_symbols)}")
    print(f"股票池来源: {universe_source}")
    print(f"回补区间: {start_date} ~ {end_date}")

    trade_dates = get_open_trade_dates(pro, start_date, end_date)
    if not trade_dates:
        raise RuntimeError(f"{start_date} ~ {end_date} 之间没有可用交易日。")

    print(f"待抓取交易日数量: {len(trade_dates)}")
    print(f"交易日区间: {trade_dates[0]} ~ {trade_dates[-1]}")

    daily_frames = []
    for idx, trade_date in enumerate(trade_dates, start=1):
        print(f"[进度] {idx}/{len(trade_dates)} 开始获取 {trade_date}")
        frame = fetch_trade_date_frame(pro, trade_date, name_map, target_symbols)
        daily_frames.append(frame)
        time.sleep(1.0)

    all_df = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()
    if all_df.empty:
        raise RuntimeError("未抓取到任何可用日线数据。")

    grouped = all_df.groupby("股票代码", sort=False)
    for symbol, group in grouped:
        name = name_map.get(symbol, "")
        file_path = DATA_DIR / f"{symbol}.csv"

        try:
            group = group.copy().sort_values("日期").reset_index(drop=True)
            if file_path.exists() and not args.overwrite_existing:
                local_df = pd.read_csv(file_path, dtype={"股票代码": str})
                local_df["股票代码"] = local_df["股票代码"].astype(str).str.zfill(6)
                local_df["股票名称"] = local_df["股票名称"].astype(str).str.strip()
                local_df["日期"] = pd.to_datetime(local_df["日期"], errors="coerce")
                merged = pd.concat([local_df, group], ignore_index=True)
                merged = merged.drop_duplicates(subset=["日期"], keep="last").sort_values("日期").reset_index(drop=True)
                mode = "merge"
            else:
                merged = group
                mode = "overwrite" if args.overwrite_existing else "create"

            merged["日期"] = pd.to_datetime(merged["日期"], errors="coerce").dt.strftime("%Y-%m-%d")
            merged.to_csv(file_path, index=False, encoding="utf-8-sig")

            success_rows.append(
                {
                    "股票代码": symbol,
                    "股票名称": name,
                    "写入模式": mode,
                    "写入条数": len(group),
                    "更新后总条数": len(merged),
                    "文件路径": str(file_path),
                }
            )
            print(f"[完成] {symbol} {name} 写入成功，新增/写入 {len(group)} 条")
        except Exception as exc:
            error_rows.append(
                {
                    "股票代码": symbol,
                    "股票名称": name,
                    "文件名": file_path.name,
                    "错误信息": repr(exc),
                }
            )
            print(f"[失败] {symbol} {name}: {repr(exc)}")

    untouched_symbols = target_symbols - set(grouped.groups.keys())
    for symbol in sorted(untouched_symbols):
        skip_rows.append(
            {
                "股票代码": symbol,
                "股票名称": name_map.get(symbol, ""),
                "原因": "指定交易日范围内未返回该股票行情（可能停牌、未上市或已退市）",
            }
        )

    success_df = pd.DataFrame(success_rows)
    error_df = pd.DataFrame(error_rows)
    skip_df = pd.DataFrame(skip_rows)

    success_df.to_csv(success_file, index=False, encoding="utf-8-sig")
    error_df.to_csv(error_file, index=False, encoding="utf-8-sig")
    skip_df.to_csv(skip_file, index=False, encoding="utf-8-sig")

    summary_df = pd.DataFrame(
        [
            {
                "模式": "tushare_trade_date_bootstrap",
                "base_dir": str(BASE_DIR),
                "data_dir": str(DATA_DIR),
                "universe_source": universe_source,
                "target_symbol_count": len(target_symbols),
                "trade_date_count": len(trade_dates),
                "trade_date_start": trade_dates[0],
                "trade_date_end": trade_dates[-1],
                "success_count": len(success_rows),
                "error_count": len(error_rows),
                "skip_count": len(skip_rows),
                "overwrite_existing": bool(args.overwrite_existing),
            }
        ]
    )
    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")

    log_lines = [
        "P4 tushare bootstrap finished",
        f"base_dir={BASE_DIR}",
        f"data_dir={DATA_DIR}",
        f"universe_source={universe_source}",
        f"target_symbol_count={len(target_symbols)}",
        f"trade_date_count={len(trade_dates)}",
        f"trade_date_start={trade_dates[0]}",
        f"trade_date_end={trade_dates[-1]}",
        f"success_count={len(success_rows)}",
        f"error_count={len(error_rows)}",
        f"skip_count={len(skip_rows)}",
        f"overwrite_existing={bool(args.overwrite_existing)}",
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


if __name__ == "__main__":
    main()
