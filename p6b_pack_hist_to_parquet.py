from pathlib import Path
from datetime import datetime
import pandas as pd

BASE_DIR = Path(r"W:\AshareScanner")
DATA_DIR = BASE_DIR / "data" / "daily_hist"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"
PACK_DIR = BASE_DIR / "data" / "packed"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
PACK_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")

REQUIRED_COLS = [
    "股票代码", "股票名称", "日期",
    "开盘", "收盘", "最高", "最低",
    "成交量", "成交额", "换手率"
]

NUMERIC_COLS = [
    "开盘", "收盘", "最高", "最低",
    "成交量", "成交额", "换手率"
]

packed_file = PACK_DIR / "daily_hist_all.parquet"
summary_file = OUTPUT_DIR / f"p6b_pack_hist_summary_{today_str}.csv"
error_file = OUTPUT_DIR / f"p6b_pack_hist_errors_{today_str}.csv"
log_file = LOGS_DIR / f"p6b_pack_hist_{today_str}.log"

files = sorted(DATA_DIR.glob("*.csv"))
if not files:
    raise FileNotFoundError(f"未找到历史文件: {DATA_DIR}")

frames = []
errors = []

print(f"待打包文件数量: {len(files)}")

for idx, file_path in enumerate(files, start=1):
    if idx == 1 or idx % 100 == 0 or idx == len(files):
        print(f"[进度] {idx}/{len(files)}")

    try:
        df = pd.read_csv(
            file_path,
            usecols=REQUIRED_COLS,
            dtype={"股票代码": str, "股票名称": str}
        )

        if df.empty:
            continue

        df = df.copy()
        df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
        df["股票名称"] = df["股票名称"].astype(str).str.strip()
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")

        for col in NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.dropna(subset=["日期"] + NUMERIC_COLS)
        if df.empty:
            continue

        frames.append(df)

    except Exception as e:
        errors.append({
            "文件名": file_path.name,
            "错误信息": repr(e)
        })

if not frames:
    raise ValueError("没有可打包的数据")

all_df = pd.concat(frames, ignore_index=True)
all_df = all_df.sort_values(["股票代码", "日期"]).reset_index(drop=True)

all_df.to_parquet(packed_file, index=False)

summary_df = pd.DataFrame([{
    "源文件数量": len(files),
    "拼接后总行数": len(all_df),
    "股票数量": all_df["股票代码"].nunique(),
    "输出文件": str(packed_file),
    "失败数量": len(errors)
}])
summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")

pd.DataFrame(errors).to_csv(error_file, index=False, encoding="utf-8-sig")

log_lines = [
    "P6B pack hist to parquet finished",
    f"source_file_count={len(files)}",
    f"row_count={len(all_df)}",
    f"stock_count={all_df['股票代码'].nunique()}",
    f"packed_file={packed_file}",
    f"summary_file={summary_file}",
    f"error_file={error_file}",
]
log_file.write_text("\n".join(log_lines), encoding="utf-8")

print("运行完成")
print(f"打包文件: {packed_file}")
print(f"汇总文件: {summary_file}")
print(f"异常文件: {error_file}")
print(f"日志文件: {log_file}")