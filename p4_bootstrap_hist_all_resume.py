from pathlib import Path
from datetime import datetime
import argparse
import json
import time

import akshare as ak
import pandas as pd


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


BASE_DIR = resolve_base_dir()
DATA_DIR = BASE_DIR / "data" / "daily_hist"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")

# ========= 这些参数以后只改这里 =========
START_DATE = "20240101"
START_INDEX = 4000
BATCH_SIZE = 1500
SKIP_EXISTING = True
SLEEP_BETWEEN_STOCKS = 0.5
# =======================================


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stock-list-file", default="")
    parser.add_argument("--universe-file", default="")
    parser.add_argument("--start-date", default=START_DATE)
    parser.add_argument("--skip-existing", choices=["true", "false"], default="")
    return parser.parse_args()


def fetch_hist_with_retry(symbol, start_date, max_retries=3, sleep_seconds=2):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=today_str,
                adjust="qfq"
            )
            if df is None or df.empty:
                raise ValueError("返回结果为空")
            return df
        except Exception as e:
            last_error = e
            print(f"[重试] {symbol} 第 {attempt}/{max_retries} 次失败: {repr(e)}")
            if attempt < max_retries:
                time.sleep(sleep_seconds)
    raise last_error


def load_universe_df(universe_file_arg=""):
    if universe_file_arg:
        universe_file = Path(universe_file_arg)
        if not universe_file.is_absolute():
            universe_file = PROJECT_DIR / universe_file
    else:
        universe_file = OUTPUT_DIR / f"p3_universe_filtered_{today_str}.csv"

    if not universe_file.exists():
        raise FileNotFoundError(f"找不到股票池文件: {universe_file}")

    universe_df = pd.read_csv(universe_file, dtype={"code": str})
    universe_df["code"] = universe_df["code"].astype(str).str.zfill(6)
    universe_df["name"] = universe_df["name"].astype(str).str.strip()
    return universe_df, universe_file


def build_target_df(universe_df, stock_list_file):
    if stock_list_file:
        list_path = Path(stock_list_file)
        if not list_path.is_absolute():
            list_path = PROJECT_DIR / list_path
        if not list_path.exists():
            raise FileNotFoundError(f"找不到待补建清单文件: {list_path}")

        list_df = pd.read_csv(list_path, dtype={"code": str})
        if "code" not in list_df.columns:
            raise ValueError(f"待补建清单缺少 code 字段: {list_path}")

        list_df["code"] = list_df["code"].astype(str).str.zfill(6)
        target_df = universe_df[universe_df["code"].isin(list_df["code"])].copy()
        target_df = target_df.drop_duplicates(subset=["code"]).reset_index(drop=True)

        missing_in_universe = sorted(set(list_df["code"]) - set(target_df["code"]))
        if missing_in_universe:
            print(f"[提示] 以下代码不在当日 universe 中，将被忽略: {missing_in_universe}")

        return target_df, "list", f"list_{len(target_df)}", list_path

    end_index = min(START_INDEX + BATCH_SIZE, len(universe_df))
    target_df = universe_df.iloc[START_INDEX:end_index].copy()
    return target_df, "batch", f"{START_INDEX}_{end_index - 1}", None


def main():
    args = parse_args()
    start_date = args.start_date or START_DATE
    skip_existing = SKIP_EXISTING if not args.skip_existing else args.skip_existing == "true"

    universe_df, universe_file = load_universe_df(args.universe_file)
    target_df, mode, mode_tag, stock_list_path = build_target_df(universe_df, args.stock_list_file)

    if target_df.empty:
        raise ValueError("本次待处理股票清单为空")

    success_file = OUTPUT_DIR / f"p4_bootstrap_all_success_{today_str}_{mode_tag}.csv"
    error_file = OUTPUT_DIR / f"p4_bootstrap_all_errors_{today_str}_{mode_tag}.csv"
    skip_file = OUTPUT_DIR / f"p4_bootstrap_all_skipped_{today_str}_{mode_tag}.csv"
    log_file = LOGS_DIR / f"p4_bootstrap_all_{today_str}_{mode_tag}.log"

    success_rows = []
    error_rows = []
    skipped_rows = []

    print(f"股票池总数: {len(universe_df)}")
    print(f"本次模式: {mode}")
    print(f"本次目标数量: {len(target_df)}")
    print(f"数据目录: {DATA_DIR}")
    if stock_list_path is not None:
        print(f"待补建清单: {stock_list_path}")
    else:
        print(f"本次建库范围: {START_INDEX} -> {START_INDEX + len(target_df) - 1}")

    for idx, row in enumerate(target_df.itertuples(index=False), start=1):
        symbol = row.code
        name = row.name
        out_file = DATA_DIR / f"{symbol}.csv"

        print(f"[进度] {idx}/{len(target_df)} 开始处理 {symbol} {name}")

        try:
            if skip_existing and out_file.exists():
                skipped_rows.append({
                    "股票代码": symbol,
                    "股票名称": name,
                    "原因": "文件已存在，跳过",
                    "文件路径": str(out_file)
                })
                print(f"[跳过] {symbol} {name} 文件已存在")
                continue

            df = fetch_hist_with_retry(symbol, start_date=start_date)

            required_cols = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"]
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise ValueError(f"缺少字段: {missing_cols}")

            df = df.copy()
            df["股票代码"] = symbol
            df["股票名称"] = name

            front_cols = ["股票代码", "股票名称"]
            other_cols = [c for c in df.columns if c not in front_cols]
            df = df[front_cols + other_cols]

            df.to_csv(out_file, index=False, encoding="utf-8-sig")

            success_rows.append({
                "股票代码": symbol,
                "股票名称": name,
                "记录条数": len(df),
                "起始日期": df.iloc[0]["日期"],
                "结束日期": df.iloc[-1]["日期"],
                "文件路径": str(out_file)
            })

            print(f"[完成] {symbol} {name} 成功")
            time.sleep(SLEEP_BETWEEN_STOCKS)

        except Exception as e:
            error_rows.append({
                "股票代码": symbol,
                "股票名称": name,
                "错误信息": repr(e)
            })
            print(f"[失败] {symbol} {name}: {repr(e)}")

    pd.DataFrame(success_rows).to_csv(success_file, index=False, encoding="utf-8-sig")
    pd.DataFrame(error_rows).to_csv(error_file, index=False, encoding="utf-8-sig")
    pd.DataFrame(skipped_rows).to_csv(skip_file, index=False, encoding="utf-8-sig")

    log_lines = [
        "P4 bootstrap all resume finished",
        f"mode={mode}",
        f"universe_total={len(universe_df)}",
        f"target_count={len(target_df)}",
        f"success_count={len(success_rows)}",
        f"error_count={len(error_rows)}",
        f"skip_count={len(skipped_rows)}",
        f"start_date={start_date}",
        f"skip_existing={skip_existing}",
        f"universe_file={universe_file}",
        f"success_file={success_file}",
        f"error_file={error_file}",
        f"skip_file={skip_file}",
        f"data_dir={DATA_DIR}",
    ]
    if stock_list_path is not None:
        log_lines.append(f"stock_list_file={stock_list_path}")
    else:
        log_lines.append(f"batch_start_index={START_INDEX}")
        log_lines.append(f"batch_end_index={START_INDEX + len(target_df) - 1}")
    log_file.write_text("\n".join(log_lines), encoding="utf-8")

    print("运行完成")
    print(f"成功数量: {len(success_rows)}")
    print(f"失败数量: {len(error_rows)}")
    print(f"跳过数量: {len(skipped_rows)}")
    print(f"成功清单: {success_file}")
    print(f"异常清单: {error_file}")
    print(f"跳过清单: {skip_file}")
    print(f"日志文件: {log_file}")


if __name__ == "__main__":
    main()
