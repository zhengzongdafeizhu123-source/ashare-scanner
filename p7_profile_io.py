from pathlib import Path
from datetime import datetime
import time
import pandas as pd

from project_paths import BASE_DIR, DIAGNOSTICS_OUTPUT_DIR, LOGS_DIR

DATA_DIR = BASE_DIR / "data" / "daily_hist"
OUTPUT_DIR = DIAGNOSTICS_OUTPUT_DIR

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

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

profile_file = OUTPUT_DIR / f"p7_profile_io_{today_str}.csv"
slow_file = OUTPUT_DIR / f"p7_profile_io_top_slow_{today_str}.csv"
summary_file = OUTPUT_DIR / f"p7_profile_io_summary_{today_str}.csv"
log_file = LOGS_DIR / f"p7_profile_io_{today_str}.log"

files = sorted(DATA_DIR.glob("*.csv"))
if not files:
    raise FileNotFoundError(f"未找到任何历史库文件: {DATA_DIR}")

rows = []
batch_start = time.perf_counter()
total_start = time.perf_counter()

print(f"待检测文件数量: {len(files)}")

for idx, file_path in enumerate(files, start=1):
    t0 = time.perf_counter()

    file_size_mb = file_path.stat().st_size / 1024 / 1024

    read_ok = True
    error_msg = ""
    row_count = 0

    try:
        t_read_0 = time.perf_counter()
        df = pd.read_csv(
            file_path,
            usecols=REQUIRED_COLS,
            dtype={"股票代码": str, "股票名称": str}
        )
        t_read_1 = time.perf_counter()

        row_count = len(df)

        # 模拟你 P7 里最核心的清洗成本
        df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
        for col in NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["日期"] + NUMERIC_COLS)
        df = df.sort_values("日期").reset_index(drop=True)

        t_end = time.perf_counter()

        read_seconds = t_read_1 - t_read_0
        total_seconds = t_end - t0

    except Exception as e:
        read_ok = False
        error_msg = repr(e)
        read_seconds = None
        total_seconds = time.perf_counter() - t0

    rows.append({
        "序号": idx,
        "文件名": file_path.name,
        "文件大小MB": round(file_size_mb, 4),
        "原始行数": row_count,
        "read_csv耗时秒": round(read_seconds, 4) if read_seconds is not None else None,
        "单文件总耗时秒": round(total_seconds, 4),
        "是否成功": "是" if read_ok else "否",
        "错误信息": error_msg,
    })

    if idx == 1 or idx % 100 == 0 or idx == len(files):
        now = time.perf_counter()
        batch_seconds = now - batch_start
        total_seconds_all = now - total_start
        avg_per_file = total_seconds_all / idx

        print(
            f"[进度] {idx}/{len(files)} | "
            f"最近批次耗时: {batch_seconds:.2f}s | "
            f"累计耗时: {total_seconds_all:.2f}s | "
            f"平均每文件: {avg_per_file:.4f}s"
        )
        batch_start = now

profile_df = pd.DataFrame(rows)
profile_df.to_csv(profile_file, index=False, encoding="utf-8-sig")

slow_df = profile_df.sort_values(by="单文件总耗时秒", ascending=False).head(100)
slow_df.to_csv(slow_file, index=False, encoding="utf-8-sig")

summary_rows = []

# 每100个文件做一个分段统计，方便看是不是从800开始掉速
for start_idx in range(1, len(profile_df) + 1, 100):
    end_idx = min(start_idx + 99, len(profile_df))
    chunk = profile_df[(profile_df["序号"] >= start_idx) & (profile_df["序号"] <= end_idx)].copy()

    summary_rows.append({
        "分段": f"{start_idx}-{end_idx}",
        "文件数": len(chunk),
        "平均单文件耗时秒": round(chunk["单文件总耗时秒"].mean(), 4),
        "最大单文件耗时秒": round(chunk["单文件总耗时秒"].max(), 4),
        "平均文件大小MB": round(chunk["文件大小MB"].mean(), 4),
        "平均原始行数": round(chunk["原始行数"].mean(), 2),
    })

summary_df = pd.DataFrame(summary_rows)
summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")

log_lines = [
    "P7 profile io finished",
    f"file_total={len(files)}",
    f"profile_file={profile_file}",
    f"slow_file={slow_file}",
    f"summary_file={summary_file}",
]
log_file.write_text("\n".join(log_lines), encoding="utf-8")

print("运行完成")
print(f"明细文件: {profile_file}")
print(f"最慢文件TOP100: {slow_file}")
print(f"分段汇总文件: {summary_file}")
print(f"日志文件: {log_file}")
