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

# ========= 规则参数区：以后主要改这里 =========
VOLATILITY_WINDOW = 90          # 波动窗口
VOLATILITY_MAX = 0.20           # 90日波动率上限（20%）
REQUIRE_BULLISH = True          # 是否要求阳线
VOLUME_MULTIPLIER = 3.0         # 当日成交量 >= 前一日多少倍
TURNOVER_MIN = 10.0             # 当日换手率下限
MIN_HISTORY_BARS = 90           # 最少历史条数
# ========================================

all_result_file = OUTPUT_DIR / f"p7_scan_from_local_all_results_{today_str}.csv"
selected_file = OUTPUT_DIR / f"p7_scan_from_local_all_selected_{today_str}.csv"
error_file = OUTPUT_DIR / f"p7_scan_from_local_all_errors_{today_str}.csv"
skip_file = OUTPUT_DIR / f"p7_scan_from_local_all_skipped_{today_str}.csv"
summary_file = OUTPUT_DIR / f"p7_scan_from_local_all_summary_{today_str}.csv"
log_file = LOGS_DIR / f"p7_scan_from_local_all_{today_str}.log"

results = []
errors = []
skipped = []


def safe_float(value, field_name):
    try:
        return float(value)
    except Exception:
        raise ValueError(f"{field_name} 无法转成 float: {value}")


def calc_range_volatility(df_window):
    high_n = df_window["最高"].astype(float).max()
    low_n = df_window["最低"].astype(float).min()
    if low_n <= 0:
        raise ValueError("窗口最低价异常")
    return high_n / low_n - 1


files = sorted(DATA_DIR.glob("*.csv"))
if not files:
    raise FileNotFoundError(f"未找到任何历史库文件: {DATA_DIR}")

print(f"待扫描文件数量: {len(files)}")

for idx, file_path in enumerate(files, start=1):
    if idx == 1 or idx % 100 == 0 or idx == len(files):
        print(f"[进度] {idx}/{len(files)}")

    try:
        df = pd.read_csv(file_path, dtype={"股票代码": str})
        if df is None or df.empty:
            raise ValueError("文件为空")

        required_cols = [
            "股票代码", "股票名称", "日期",
            "开盘", "收盘", "最高", "最低",
            "成交量", "成交额", "换手率"
        ]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少字段: {missing_cols}")

        df = df.copy()
        df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
        df["股票名称"] = df["股票名称"].astype(str).str.strip()
        df["日期"] = pd.to_datetime(df["日期"])
        df = df.sort_values("日期").reset_index(drop=True)

        symbol = str(df.iloc[-1]["股票代码"]).zfill(6)
        name = str(df.iloc[-1]["股票名称"]).strip()

        if len(df) < MIN_HISTORY_BARS:
            skipped.append({
                "股票代码": symbol,
                "股票名称": name,
                "历史条数": len(df),
                "跳过原因": f"历史数据不足 {MIN_HISTORY_BARS} 行"
            })
            continue

        latest = df.iloc[-1]
        prev = df.iloc[-2]
        df_window = df.iloc[-VOLATILITY_WINDOW:].copy()

        latest_open = safe_float(latest["开盘"], "开盘")
        latest_close = safe_float(latest["收盘"], "收盘")
        latest_high = safe_float(latest["最高"], "最高")
        latest_low = safe_float(latest["最低"], "最低")
        latest_volume = safe_float(latest["成交量"], "成交量")
        latest_amount = safe_float(latest["成交额"], "成交额")
        latest_turnover = safe_float(latest["换手率"], "换手率")
        prev_volume = safe_float(prev["成交量"], "前一日成交量")

        range_vol = calc_range_volatility(df_window)

        rule_low_vol = range_vol <= VOLATILITY_MAX
        rule_bullish = latest_close > latest_open if REQUIRE_BULLISH else True
        volume_ratio = latest_volume / prev_volume if prev_volume != 0 else 0
        rule_big_volume = volume_ratio >= VOLUME_MULTIPLIER
        rule_turnover = latest_turnover > TURNOVER_MIN

        selected = (
            rule_low_vol
            and rule_bullish
            and rule_big_volume
            and rule_turnover
        )

        failed_reasons = []
        if not rule_low_vol:
            failed_reasons.append(f"{VOLATILITY_WINDOW}日波动率>{VOLATILITY_MAX:.0%}")
        if REQUIRE_BULLISH and not rule_bullish:
            failed_reasons.append("不是阳线")
        if not rule_big_volume:
            failed_reasons.append(f"未达到{VOLUME_MULTIPLIER}倍放量")
        if not rule_turnover:
            failed_reasons.append(f"换手率<={TURNOVER_MIN}%")

        hit_count = (
            int(rule_low_vol)
            + int(rule_bullish)
            + int(rule_big_volume)
            + int(rule_turnover)
        )

        results.append({
            "股票代码": symbol,
            "股票名称": name,
            "日期": latest["日期"].strftime("%Y-%m-%d"),
            "开盘": round(latest_open, 2),
            "收盘": round(latest_close, 2),
            "最高": round(latest_high, 2),
            "最低": round(latest_low, 2),
            "成交量": latest_volume,
            "成交额": latest_amount,
            "换手率": round(latest_turnover, 2),
            f"{VOLATILITY_WINDOW}日波动率": round(range_vol, 4),
            "量比前一日": round(volume_ratio, 2),
            "低波动通过": "是" if rule_low_vol else "否",
            "阳线通过": "是" if rule_bullish else "否",
            "放量通过": "是" if rule_big_volume else "否",
            "换手率通过": "是" if rule_turnover else "否",
            "命中规则数": hit_count,
            "是否入选": "是" if selected else "否",
            "未通过原因": "；".join(failed_reasons) if failed_reasons else ""
        })

    except Exception as e:
        errors.append({
            "文件名": file_path.name,
            "错误信息": repr(e)
        })

result_df = pd.DataFrame(results)
error_df = pd.DataFrame(errors)
skip_df = pd.DataFrame(skipped)

if not result_df.empty:
    result_df = result_df.sort_values(
        by=["是否入选", "命中规则数", "换手率", "量比前一日"],
        ascending=[False, False, False, False]
    )

selected_df = pd.DataFrame()
if not result_df.empty and "是否入选" in result_df.columns:
    selected_df = result_df[result_df["是否入选"] == "是"].copy()

result_df.to_csv(all_result_file, index=False, encoding="utf-8-sig")
selected_df.to_csv(selected_file, index=False, encoding="utf-8-sig")
error_df.to_csv(error_file, index=False, encoding="utf-8-sig")
skip_df.to_csv(skip_file, index=False, encoding="utf-8-sig")

summary_df = pd.DataFrame([{
    "扫描总数": len(files),
    "结果数量": len(result_df),
    "入选数量": int((result_df["是否入选"] == "是").sum()) if not result_df.empty else 0,
    "跳过数量": len(skip_df),
    "失败数量": len(error_df),
    "通过低波动数量": int((result_df["低波动通过"] == "是").sum()) if not result_df.empty else 0,
    "通过阳线数量": int((result_df["阳线通过"] == "是").sum()) if not result_df.empty else 0,
    "通过放量数量": int((result_df["放量通过"] == "是").sum()) if not result_df.empty else 0,
    "通过换手率数量": int((result_df["换手率通过"] == "是").sum()) if not result_df.empty else 0,
    "波动窗口": VOLATILITY_WINDOW,
    "波动率上限": VOLATILITY_MAX,
    "是否要求阳线": REQUIRE_BULLISH,
    "放量倍数": VOLUME_MULTIPLIER,
    "换手率下限": TURNOVER_MIN
}])
summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")

log_lines = [
    "P7 scan from local all finished",
    f"file_total={len(files)}",
    f"result_count={len(result_df)}",
    f"selected_count={int((result_df['是否入选'] == '是').sum()) if not result_df.empty else 0}",
    f"skip_count={len(skip_df)}",
    f"error_count={len(error_df)}",
    f"all_result_file={all_result_file}",
    f"selected_file={selected_file}",
    f"skip_file={skip_file}",
    f"error_file={error_file}",
    f"summary_file={summary_file}",
]
log_file.write_text("\n".join(log_lines), encoding="utf-8")

print("运行完成")
print(f"扫描总数: {len(files)}")
print(f"结果数量: {len(result_df)}")
print(f"入选数量: {int((result_df['是否入选'] == '是').sum()) if not result_df.empty else 0}")
print(f"跳过数量: {len(skip_df)}")
print(f"失败数量: {len(error_df)}")
print(f"结果文件: {all_result_file}")
print(f"入选文件: {selected_file}")
print(f"跳过文件: {skip_file}")
print(f"异常文件: {error_file}")
print(f"汇总文件: {summary_file}")
print(f"日志文件: {log_file}")