from pathlib import Path
from datetime import datetime, timedelta
import time
import pandas as pd
import akshare as ak

BASE_DIR = Path(r"W:\AshareScanner")
DATA_DIR = BASE_DIR / "data" / "daily_hist"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")
today_date = datetime.now().date()

success_file = OUTPUT_DIR / f"p6_update_daily_hist_success_{today_str}.csv"
error_file = OUTPUT_DIR / f"p6_update_daily_hist_errors_{today_str}.csv"
skip_file = OUTPUT_DIR / f"p6_update_daily_hist_skipped_{today_str}.csv"
log_file = LOGS_DIR / f"p6_update_daily_hist_{today_str}.log"

success_rows = []
error_rows = []
skip_rows = []


def fetch_hist_with_retry(symbol, start_date, end_date, max_retries=3, sleep_seconds=2):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            if df is None:
                raise ValueError("返回结果为 None")
            return df
        except Exception as e:
            last_error = e
            print(f"[重试] {symbol} 第 {attempt}/{max_retries} 次失败: {repr(e)}")
            if attempt < max_retries:
                time.sleep(sleep_seconds)
    raise last_error


files = sorted(DATA_DIR.glob("*.csv"))
if not files:
    raise FileNotFoundError(f"未找到任何历史库文件: {DATA_DIR}")

print(f"待检查文件数量: {len(files)}")
print(f"今日日期: {today_str}")

for idx, file_path in enumerate(files, start=1):
    print(f"[进度] {idx}/{len(files)} 开始处理 {file_path.name}")

    try:
        local_df = pd.read_csv(file_path, dtype={"股票代码": str})
        if local_df is None or local_df.empty:
            raise ValueError("本地文件为空")

        required_cols = ["股票代码", "股票名称", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"]
        missing_cols = [c for c in required_cols if c not in local_df.columns]
        if missing_cols:
            raise ValueError(f"缺少字段: {missing_cols}")

        local_df = local_df.copy()
        local_df["股票代码"] = local_df["股票代码"].astype(str).str.zfill(6)
        local_df["股票名称"] = local_df["股票名称"].astype(str).str.strip()
        local_df["日期"] = pd.to_datetime(local_df["日期"])
        local_df = local_df.sort_values("日期").reset_index(drop=True)

        symbol = local_df.iloc[-1]["股票代码"]
        name = local_df.iloc[-1]["股票名称"]
        latest_local_date = local_df.iloc[-1]["日期"].date()

        # 下一次应抓取的起始日期
        next_date = latest_local_date + timedelta(days=1)

        if next_date > today_date:
            skip_rows.append({
                "股票代码": symbol,
                "股票名称": name,
                "最新本地日期": str(latest_local_date),
                "原因": "本地日期已晚于今日，无需更新"
            })
            print(f"[跳过] {symbol} {name} 本地日期已晚于今日")
            continue

        start_date_str = next_date.strftime("%Y%m%d")
        new_df = fetch_hist_with_retry(symbol, start_date_str, today_str)

        # 周末 / 非交易日 / 已是最新，AKShare 可能返回空表
        if new_df.empty:
            skip_rows.append({
                "股票代码": symbol,
                "股票名称": name,
                "最新本地日期": str(latest_local_date),
                "原因": "远端无新增数据"
            })
            print(f"[跳过] {symbol} {name} 无新增数据")
            continue

        # 只保留并标准化字段
        needed_cols = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"]
        missing_new_cols = [c for c in needed_cols if c not in new_df.columns]
        if missing_new_cols:
            raise ValueError(f"新增数据缺少字段: {missing_new_cols}")

        new_df = new_df[needed_cols].copy()
        new_df["股票代码"] = symbol
        new_df["股票名称"] = name
        new_df["日期"] = pd.to_datetime(new_df["日期"])

        front_cols = ["股票代码", "股票名称"]
        other_cols = [c for c in new_df.columns if c not in front_cols]
        new_df = new_df[front_cols + other_cols]

        merged_df = pd.concat([local_df, new_df], ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset=["日期"], keep="last")
        merged_df = merged_df.sort_values("日期").reset_index(drop=True)

        # 日期写回字符串，避免 csv 里带时间
        merged_df["日期"] = merged_df["日期"].dt.strftime("%Y-%m-%d")
        merged_df.to_csv(file_path, index=False, encoding="utf-8-sig")

        success_rows.append({
            "股票代码": symbol,
            "股票名称": name,
            "原最新日期": str(latest_local_date),
            "新增条数": len(new_df),
            "更新后最新日期": merged_df.iloc[-1]["日期"],
            "文件路径": str(file_path)
        })

        print(f"[完成] {symbol} {name} 更新成功，新增 {len(new_df)} 条")
        time.sleep(0.2)

    except Exception as e:
        error_rows.append({
            "文件名": file_path.name,
            "错误信息": repr(e)
        })
        print(f"[失败] {file_path.name}: {repr(e)}")

success_df = pd.DataFrame(success_rows)
error_df = pd.DataFrame(error_rows)
skip_df = pd.DataFrame(skip_rows)

success_df.to_csv(success_file, index=False, encoding="utf-8-sig")
error_df.to_csv(error_file, index=False, encoding="utf-8-sig")
skip_df.to_csv(skip_file, index=False, encoding="utf-8-sig")

log_lines = [
    "P6 update daily hist finished",
    f"file_total={len(files)}",
    f"success_count={len(success_rows)}",
    f"error_count={len(error_rows)}",
    f"skip_count={len(skip_rows)}",
    f"success_file={success_file}",
    f"error_file={error_file}",
    f"skip_file={skip_file}",
]
log_file.write_text("\n".join(log_lines), encoding="utf-8")

print("运行完成")
print(f"成功数量: {len(success_rows)}")
print(f"失败数量: {len(error_rows)}")
print(f"跳过数量: {len(skip_rows)}")
print(f"成功清单: {success_file}")
print(f"异常清单: {error_file}")
print(f"跳过清单: {skip_file}")
print(f"日志文件: {log_file}")