from pathlib import Path
from datetime import datetime
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

universe_file = OUTPUT_DIR / f"p3_universe_filtered_{today_str}.csv"
success_file = OUTPUT_DIR / f"p4_bootstrap_hist_100_success_{today_str}.csv"
error_file = OUTPUT_DIR / f"p4_bootstrap_hist_100_errors_{today_str}.csv"
log_file = LOGS_DIR / f"p4_bootstrap_hist_100_{today_str}.log"

START_DATE = "20240101"
MAX_STOCKS = 100

success_rows = []
error_rows = []


def fetch_hist_with_retry(symbol, max_retries=3, sleep_seconds=2):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=START_DATE,
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


if not universe_file.exists():
    raise FileNotFoundError(f"找不到股票池文件: {universe_file}")

universe_df = pd.read_csv(universe_file, dtype={"code": str})
universe_df["code"] = universe_df["code"].astype(str).str.zfill(6)
universe_df["name"] = universe_df["name"].astype(str).str.strip()

sample_df = universe_df.head(MAX_STOCKS).copy()

print(f"准备建库数量: {len(sample_df)}")

for idx, row in enumerate(sample_df.itertuples(index=False), start=1):
    symbol = row.code
    name = row.name

    print(f"[进度] {idx}/{len(sample_df)} 开始处理 {symbol} {name}")

    try:
        df = fetch_hist_with_retry(symbol)

        required_cols = ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少字段: {missing_cols}")

        df = df.copy()

        # 如果接口已经带了这两列，就覆盖；没有就新增
        df["股票代码"] = symbol
        df["股票名称"] = name

        # 调整列顺序，把代码和名称放到最前面
        front_cols = ["股票代码", "股票名称"]
        other_cols = [c for c in df.columns if c not in front_cols]
        df = df[front_cols + other_cols]

        out_file = DATA_DIR / f"{symbol}.csv"
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

    except Exception as e:
        error_rows.append({
            "股票代码": symbol,
            "股票名称": name,
            "错误信息": repr(e)
        })
        print(f"[失败] {symbol} {name}: {repr(e)}")

success_df = pd.DataFrame(success_rows)
error_df = pd.DataFrame(error_rows)

success_df.to_csv(success_file, index=False, encoding="utf-8-sig")
error_df.to_csv(error_file, index=False, encoding="utf-8-sig")

log_lines = [
    "P4 bootstrap hist 100 finished",
    f"target_count={len(sample_df)}",
    f"success_count={len(success_rows)}",
    f"error_count={len(error_rows)}",
    f"success_file={success_file}",
    f"error_file={error_file}",
    f"data_dir={DATA_DIR}",
]
log_file.write_text("\n".join(log_lines), encoding="utf-8")

print("运行完成")
print(f"成功数量: {len(success_rows)}")
print(f"失败数量: {len(error_rows)}")
print(f"成功清单: {success_file}")
print(f"异常清单: {error_file}")
print(f"日志文件: {log_file}")