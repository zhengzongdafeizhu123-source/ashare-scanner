from pathlib import Path
from datetime import datetime
import pandas as pd

BASE_DIR = Path(r"W:\AshareScanner")
DATA_DIR = BASE_DIR / "data" / "daily_hist"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")

result_file = OUTPUT_DIR / f"p5_scan_from_local_100_results_{today_str}.csv"
error_file = OUTPUT_DIR / f"p5_scan_from_local_100_errors_{today_str}.csv"
log_file = LOGS_DIR / f"p5_scan_from_local_100_{today_str}.log"

results = []
errors = []


def calc_90d_volatility(df_90):
    high_90 = df_90["最高"].astype(float).max()
    low_90 = df_90["最低"].astype(float).min()
    if low_90 <= 0:
        raise ValueError("90日最低价异常")
    return high_90 / low_90 - 1


def safe_float(value, field_name):
    try:
        return float(value)
    except Exception:
        raise ValueError(f"{field_name} 无法转成 float: {value}")


files = sorted(DATA_DIR.glob("*.csv"))

if not files:
    raise FileNotFoundError(f"未找到任何历史文件: {DATA_DIR}")

print(f"待扫描文件数量: {len(files)}")

for idx, file_path in enumerate(files, start=1):
    print(f"[进度] {idx}/{len(files)} 开始扫描 {file_path.name}")

    try:
        df = pd.read_csv(file_path, dtype={"股票代码": str})
        if df is None or df.empty:
            raise ValueError("文件为空")

        required_cols = ["股票代码", "股票名称", "日期", "开盘", "收盘", "最高", "最低", "成交量", "换手率"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少字段: {missing_cols}")

        df = df.copy()
        df["日期"] = pd.to_datetime(df["日期"])
        df = df.sort_values("日期").reset_index(drop=True)

        if len(df) < 90:
            raise ValueError(f"历史数据不足 90 行，当前 {len(df)} 行")

        latest = df.iloc[-1]
        prev = df.iloc[-2]
        df_90 = df.iloc[-90:].copy()

        symbol = str(latest["股票代码"]).zfill(6)
        name = str(latest["股票名称"]).strip()

        latest_open = safe_float(latest["开盘"], "开盘")
        latest_close = safe_float(latest["收盘"], "收盘")
        latest_volume = safe_float(latest["成交量"], "成交量")
        latest_turnover = safe_float(latest["换手率"], "换手率")

        prev_volume = safe_float(prev["成交量"], "前一日成交量")

        # 规则1：最近90日波动率 <= 20%
        vol_90 = calc_90d_volatility(df_90)
        rule_90d_low_vol = vol_90 <= 0.20

        # 规则2：最近一日阳线 + 放量 >= 前一日3倍 + 换手率 > 10%
        rule_bullish = latest_close > latest_open
        rule_big_volume = latest_volume >= prev_volume * 3
        rule_turnover = latest_turnover > 10

        passed = (
            rule_90d_low_vol
            and rule_bullish
            and rule_big_volume
            and rule_turnover
        )

        results.append({
            "股票代码": symbol,
            "股票名称": name,
            "日期": latest["日期"].strftime("%Y-%m-%d"),
            "90日波动率": round(vol_90, 4),
            "阳线": "是" if rule_bullish else "否",
            "放量3倍": "是" if rule_big_volume else "否",
            "换手率>10%": "是" if rule_turnover else "否",
            "是否入选": "是" if passed else "否",
            "开盘": round(latest_open, 2),
            "收盘": round(latest_close, 2),
            "成交量": latest_volume,
            "换手率": round(latest_turnover, 2)
        })

        print(f"[完成] {symbol} {name} 扫描完成 -> {'入选' if passed else '不入选'}")

    except Exception as e:
        errors.append({
            "文件名": file_path.name,
            "错误信息": repr(e)
        })
        print(f"[失败] {file_path.name}: {repr(e)}")

result_df = pd.DataFrame(results)
if not result_df.empty:
    result_df = result_df.sort_values(
        by=["是否入选", "换手率", "成交量"],
        ascending=[False, False, False]
    )

result_df.to_csv(result_file, index=False, encoding="utf-8-sig")

error_df = pd.DataFrame(errors)
error_df.to_csv(error_file, index=False, encoding="utf-8-sig")

selected_count = 0
if not result_df.empty and "是否入选" in result_df.columns:
    selected_count = (result_df["是否入选"] == "是").sum()

log_lines = [
    "P5 scan from local 100 finished",
    f"scan_total={len(files)}",
    f"selected_count={selected_count}",
    f"error_count={len(errors)}",
    f"result_file={result_file}",
    f"error_file={error_file}",
]
log_file.write_text("\n".join(log_lines), encoding="utf-8")

print("运行完成")
print(f"扫描总数: {len(files)}")
print(f"入选数量: {selected_count}")
print(f"失败数量: {len(errors)}")
print(f"结果文件: {result_file}")
print(f"异常文件: {error_file}")
print(f"日志文件: {log_file}")