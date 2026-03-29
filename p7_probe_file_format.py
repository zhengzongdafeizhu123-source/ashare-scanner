from pathlib import Path
import time
import pandas as pd

BASE_DIR = Path(r"W:\AshareScanner")
DATA_DIR = BASE_DIR / "data" / "daily_hist"
OUTPUT_DIR = BASE_DIR / "output"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

REQUIRED_COLS = [
    "股票代码", "股票名称", "日期",
    "开盘", "收盘", "最高", "最低",
    "成交量", "成交额", "换手率"
]

# 这里故意选几个你 profile 里已经确认很慢的文件
SLOW_FILES = [
    "688102.csv",
    "603658.csv",
    "688199.csv",
    "688063.csv",
    "688379.csv"
]

# 这里随便选几个前半段的文件做对照
FAST_FILES = [
    "000001.csv",
    "000002.csv",
    "000006.csv",
    "000009.csv",
    "000012.csv"
]

rows = []

def detect_bom(file_path: Path):
    with open(file_path, "rb") as f:
        head = f.read(4)
    if head.startswith(b"\xef\xbb\xbf"):
        return "UTF-8-BOM"
    return "NO_BOM"

def detect_newline(file_path: Path):
    with open(file_path, "rb") as f:
        sample = f.read(20000)
    crlf = sample.count(b"\r\n")
    lf = sample.count(b"\n")
    if crlf > 0:
        return "CRLF"
    if lf > 0:
        return "LF"
    return "UNKNOWN"

def bench_raw_read(file_path: Path, repeat=3):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        with open(file_path, "rb") as f:
            _ = f.read()
        times.append(time.perf_counter() - t0)
    return min(times), sum(times) / len(times)

def bench_pandas_read(file_path: Path, repeat=3):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        df = pd.read_csv(
            file_path,
            usecols=REQUIRED_COLS,
            dtype={"股票代码": str, "股票名称": str}
        )
        _ = len(df)
        times.append(time.perf_counter() - t0)
    return min(times), sum(times) / len(times)

def check_file(name, tag):
    file_path = DATA_DIR / name
    if not file_path.exists():
        rows.append({
            "分组": tag,
            "文件名": name,
            "是否存在": "否"
        })
        return

    size_mb = file_path.stat().st_size / 1024 / 1024
    bom = detect_bom(file_path)
    newline = detect_newline(file_path)

    raw_min, raw_avg = bench_raw_read(file_path)
    pd_min, pd_avg = bench_pandas_read(file_path)

    rows.append({
        "分组": tag,
        "文件名": name,
        "是否存在": "是",
        "文件大小MB": round(size_mb, 4),
        "BOM": bom,
        "换行风格": newline,
        "原始字节读取最小秒": round(raw_min, 6),
        "原始字节读取平均秒": round(raw_avg, 6),
        "pandas读取最小秒": round(pd_min, 6),
        "pandas读取平均秒": round(pd_avg, 6),
        "pandas/原始读取倍数": round(pd_avg / raw_avg, 2) if raw_avg > 0 else None
    })

for name in FAST_FILES:
    check_file(name, "FAST")

for name in SLOW_FILES:
    check_file(name, "SLOW")

result_df = pd.DataFrame(rows)
out_file = OUTPUT_DIR / "p7_probe_file_format_result.csv"
result_df.to_csv(out_file, index=False, encoding="utf-8-sig")

print("运行完成")
print(result_df.to_string(index=False))
print(f"结果文件: {out_file}")