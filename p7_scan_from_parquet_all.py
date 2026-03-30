from pathlib import Path
from datetime import datetime
import json
import os
import sys
import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

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


def sanitize_proxy_env():
    bad_proxy_values = {"http://127.0.0.1:9", "https://127.0.0.1:9"}
    for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"]:
        value = os.environ.get(key, "").strip().lower()
        if value in bad_proxy_values:
            os.environ.pop(key, None)


sanitize_proxy_env()

BASE_DIR = resolve_base_dir()
PACK_FILE = BASE_DIR / "data" / "packed" / "daily_hist_all.parquet"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs"
CONFIG_FILE = Path(__file__).with_name("scan_config.json")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")

REQUIRED_COLS = [
    "股票代码", "股票名称", "日期",
    "开盘", "收盘", "最高", "最低",
    "成交量", "成交额", "换手率"
]
NUMERIC_COLS = ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"]
DEFAULT_CONFIG = {
    "hard_filters": {
        "volatility_window": 90,
        "volatility_max": 0.35,
        "require_bullish": True,
        "volume_multiplier_min": 3.0,
        "volume_multiplier_max": 5.0,
        "turnover_min": 5.0,
        "cold_volume_window": 60,
        "cold_volume_ratio": 5 / 6,
        "min_history_bars": 90,
    },
    "label_rules": {
        "candidate": {"vr5_min": 1.8, "clv_min": 0.3, "br20_min": 0.98},
        "watch": {"vr5_min": 1.2, "clv_min": 0.0, "br20_min": 0.95},
    },
}


def deep_merge(default_dict, custom_dict):
    result = dict(default_dict)
    for key, value in custom_dict.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config():
    if not CONFIG_FILE.exists():
        return DEFAULT_CONFIG
    try:
        config_raw = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        if not isinstance(config_raw, dict):
            return DEFAULT_CONFIG
        merged = deep_merge(DEFAULT_CONFIG, config_raw)
        hard = merged.setdefault("hard_filters", {})
        if "volume_multiplier_min" not in hard:
            hard["volume_multiplier_min"] = float(hard.get("volume_multiplier", 3.0))
        if "volume_multiplier_max" not in hard:
            hard["volume_multiplier_max"] = 5.0
        if "cold_volume_window" not in hard:
            hard["cold_volume_window"] = 60
        if "cold_volume_ratio" not in hard:
            hard["cold_volume_ratio"] = 5 / 6
        return merged
    except Exception:
        return DEFAULT_CONFIG


def calc_clv(high_price, low_price, close_price):
    denominator = high_price - low_price
    if denominator == 0:
        return 0.0
    return ((close_price - low_price) - (high_price - close_price)) / denominator


def get_label(vr5, clv, br20, label_rules):
    candidate_cfg = label_rules["candidate"]
    watch_cfg = label_rules["watch"]
    is_candidate = vr5 >= float(candidate_cfg["vr5_min"]) and clv >= float(candidate_cfg["clv_min"]) and br20 >= float(candidate_cfg["br20_min"])
    is_watch = vr5 >= float(watch_cfg["vr5_min"]) and clv >= float(watch_cfg["clv_min"]) and br20 >= float(watch_cfg["br20_min"])
    if is_candidate:
        return "候选", 3, True, True
    if is_watch:
        return "观察", 2, False, True
    return "放弃", 1, False, False


config = load_config()
hard_filters = config["hard_filters"]
label_rules = config["label_rules"]
VOLATILITY_WINDOW = int(hard_filters["volatility_window"])
VOLATILITY_MAX = float(hard_filters["volatility_max"])
REQUIRE_BULLISH = bool(hard_filters["require_bullish"])
VOLUME_MULTIPLIER_MIN = float(hard_filters.get("volume_multiplier_min", hard_filters.get("volume_multiplier", 3.0)))
VOLUME_MULTIPLIER_MAX = float(hard_filters.get("volume_multiplier_max", 5.0))
TURNOVER_MIN = float(hard_filters["turnover_min"])
COLD_VOLUME_WINDOW = int(hard_filters.get("cold_volume_window", 60))
COLD_VOLUME_RATIO = float(hard_filters.get("cold_volume_ratio", 5 / 6))
MIN_HISTORY_BARS = int(hard_filters["min_history_bars"])

all_result_file = OUTPUT_DIR / f"p7_scan_from_parquet_all_results_{today_str}.csv"
selected_file = OUTPUT_DIR / f"p7_scan_from_parquet_all_selected_{today_str}.csv"
candidate_file = OUTPUT_DIR / f"p7_scan_from_parquet_all_candidate_{today_str}.csv"
watch_file = OUTPUT_DIR / f"p7_scan_from_parquet_all_watch_{today_str}.csv"
error_file = OUTPUT_DIR / f"p7_scan_from_parquet_all_errors_{today_str}.csv"
skip_file = OUTPUT_DIR / f"p7_scan_from_parquet_all_skipped_{today_str}.csv"
summary_file = OUTPUT_DIR / f"p7_scan_from_parquet_all_summary_{today_str}.csv"
log_file = LOGS_DIR / f"p7_scan_from_parquet_all_{today_str}.log"

if not PACK_FILE.exists():
    raise FileNotFoundError(f"未找到 parquet 打包文件: {PACK_FILE}")

print(f"开始读取 parquet: {PACK_FILE}")
df_all = pd.read_parquet(PACK_FILE, columns=REQUIRED_COLS)
if df_all.empty:
    raise ValueError("parquet 文件为空")

df_all = df_all.copy()
df_all["股票代码"] = df_all["股票代码"].astype(str).str.zfill(6)
df_all["股票名称"] = df_all["股票名称"].astype(str).str.strip()
df_all["日期"] = pd.to_datetime(df_all["日期"], errors="coerce")
for col in NUMERIC_COLS:
    df_all[col] = pd.to_numeric(df_all[col], errors="coerce")
df_all = df_all.dropna(subset=["日期"] + NUMERIC_COLS)
if df_all.empty:
    raise ValueError("parquet 清洗后为空")
df_all = df_all.sort_values(["股票代码", "日期"]).reset_index(drop=True)

stock_count = df_all["股票代码"].nunique()
print(f"股票数量: {stock_count}")
print(f"总行数: {len(df_all)}")

results, errors, skipped = [], [], []
required_history = max(MIN_HISTORY_BARS, VOLATILITY_WINDOW, COLD_VOLUME_WINDOW + 1, 21)

grouped = df_all.groupby("股票代码", sort=False)
for idx, (symbol, df) in enumerate(grouped, start=1):
    if idx == 1 or idx % 100 == 0 or idx == stock_count:
        print(f"[进度] {idx}/{stock_count}")
    try:
        if df.empty:
            continue
        df = df.reset_index(drop=True)
        latest = df.iloc[-1]
        name = str(latest["股票名称"]).strip()
        if len(df) < required_history:
            skipped.append({"股票代码": symbol, "股票名称": name, "历史条数": len(df), "跳过原因": f"历史数据不足 {required_history} 行"})
            continue
        prev = df.iloc[-2]
        latest_open = float(latest["开盘"])
        latest_close = float(latest["收盘"])
        latest_high = float(latest["最高"])
        latest_low = float(latest["最低"])
        latest_volume = float(latest["成交量"])
        latest_amount = float(latest["成交额"])
        latest_turnover = float(latest["换手率"])
        prev_close = float(prev["收盘"])
        prev_volume = float(prev["成交量"])

        window_df = df.tail(VOLATILITY_WINDOW)
        high_n = float(window_df["最高"].max())
        low_n = float(window_df["最低"].min())
        if low_n <= 0:
            raise ValueError("窗口最低价异常")
        range_vol = high_n / low_n - 1
        volume_ratio_prev1 = latest_volume / prev_volume if prev_volume != 0 else 0.0
        prev_5_avg_vol = float(df["成交量"].iloc[-6:-1].mean())
        vr5 = latest_volume / prev_5_avg_vol if prev_5_avg_vol != 0 else 0.0
        prev_20_high = float(df["最高"].iloc[-21:-1].max())
        br20 = latest_close / prev_20_high if prev_20_high != 0 else 0.0
        clv = calc_clv(latest_high, latest_low, latest_close)
        pct_change = (latest_close / prev_close - 1) * 100 if prev_close != 0 else 0.0
        prior_cold_df = df.iloc[-(COLD_VOLUME_WINDOW + 1):-1]
        prior_max_volume = float(prior_cold_df["成交量"].max()) if not prior_cold_df.empty else 0.0

        rule_low_vol = range_vol <= VOLATILITY_MAX
        rule_bullish = (latest_close > latest_open) if REQUIRE_BULLISH else True
        rule_big_volume = VOLUME_MULTIPLIER_MIN <= volume_ratio_prev1 <= VOLUME_MULTIPLIER_MAX
        rule_turnover = latest_turnover > TURNOVER_MIN
        rule_cold_volume = prior_max_volume <= latest_volume * COLD_VOLUME_RATIO
        hard_passed = rule_low_vol and rule_bullish and rule_big_volume and rule_turnover and rule_cold_volume

        failed_reasons = []
        if not rule_low_vol:
            failed_reasons.append(f"{VOLATILITY_WINDOW}日波动率>{VOLATILITY_MAX:.0%}")
        if REQUIRE_BULLISH and not rule_bullish:
            failed_reasons.append("不是阳线")
        if not rule_big_volume:
            failed_reasons.append(f"前一日放量倍数不在[{VOLUME_MULTIPLIER_MIN:.1f},{VOLUME_MULTIPLIER_MAX:.1f}]区间")
        if not rule_turnover:
            failed_reasons.append(f"换手率<={TURNOVER_MIN}%")
        if not rule_cold_volume:
            failed_reasons.append(f"过去{COLD_VOLUME_WINDOW}日存在成交量>{COLD_VOLUME_RATIO:.4f}×筛选日成交量")

        hit_count = int(rule_low_vol) + int(rule_bullish) + int(rule_big_volume) + int(rule_turnover) + int(rule_cold_volume)
        label, label_rank, is_candidate, is_watch = get_label(vr5, clv, br20, label_rules)
        hard_filter_reason = "；".join(failed_reasons) if failed_reasons else "硬过滤通过"
        if label == "候选":
            label_explain = "候选=VR5/CLV/BR20 达到候选阈值"
        elif label == "观察":
            label_explain = "观察=VR5/CLV/BR20 达到观察阈值但未达候选阈值"
        else:
            label_explain = "放弃=未达到候选/观察分层阈值"

        results.append({
            "股票代码": symbol,
            "股票名称": name,
            "日期": latest["日期"].strftime("%Y-%m-%d"),
            "开盘": round(latest_open, 2),
            "收盘": round(latest_close, 2),
            "最高": round(latest_high, 2),
            "最低": round(latest_low, 2),
            "涨跌幅%": round(pct_change, 2),
            "成交量": latest_volume,
            "成交额": latest_amount,
            "换手率": round(latest_turnover, 2),
            f"{VOLATILITY_WINDOW}日波动率": round(range_vol, 4),
            f"{COLD_VOLUME_WINDOW}日最大前量": round(prior_max_volume, 2),
            "量比前一日": round(volume_ratio_prev1, 2),
            "VR5": round(vr5, 2),
            "CLV": round(clv, 2),
            "BR20": round(br20, 3),
            "低波动通过": "是" if rule_low_vol else "否",
            "阳线通过": "是" if rule_bullish else "否",
            "放量区间通过": "是" if rule_big_volume else "否",
            "换手率通过": "是" if rule_turnover else "否",
            "冷量条件通过": "是" if rule_cold_volume else "否",
            "命中硬过滤数": hit_count,
            "硬过滤是否通过": "是" if hard_passed else "否",
            "硬过滤未通过原因": "；".join(failed_reasons) if failed_reasons else "",
            "硬过滤结果说明": hard_filter_reason,
            "分层标签": label,
            "分层标签说明": label_explain,
            "是否候选": "是" if is_candidate else "否",
            "是否观察": "是" if is_watch else "否",
            "_硬过滤排序值": 1 if hard_passed else 0,
            "_标签排序值": label_rank,
        })
    except Exception as e:
        errors.append({"股票代码": symbol, "错误信息": repr(e)})

result_df = pd.DataFrame(results)
error_df = pd.DataFrame(errors)
skip_df = pd.DataFrame(skipped)
selected_df = pd.DataFrame()
candidate_df = pd.DataFrame()
watch_df = pd.DataFrame()
if not result_df.empty:
    result_df = result_df.sort_values(by=["_硬过滤排序值", "_标签排序值", "命中硬过滤数", "VR5", "BR20", "换手率", "量比前一日"], ascending=[False, False, False, False, False, False, False])
    selected_df = result_df[result_df["硬过滤是否通过"] == "是"].copy()
    candidate_df = result_df[result_df["分层标签"] == "候选"].copy()
    watch_df = result_df[result_df["分层标签"] == "观察"].copy()
    result_df = result_df.drop(columns=["_硬过滤排序值", "_标签排序值"])
    if not selected_df.empty:
        selected_df = selected_df.drop(columns=["_硬过滤排序值", "_标签排序值"])
    if not candidate_df.empty:
        candidate_df = candidate_df.drop(columns=["_硬过滤排序值", "_标签排序值"])
    if not watch_df.empty:
        watch_df = watch_df.drop(columns=["_硬过滤排序值", "_标签排序值"])

result_df.to_csv(all_result_file, index=False, encoding="utf-8-sig")
selected_df.to_csv(selected_file, index=False, encoding="utf-8-sig")
candidate_df.to_csv(candidate_file, index=False, encoding="utf-8-sig")
watch_df.to_csv(watch_file, index=False, encoding="utf-8-sig")
error_df.to_csv(error_file, index=False, encoding="utf-8-sig")
skip_df.to_csv(skip_file, index=False, encoding="utf-8-sig")
summary_df = pd.DataFrame([{
    "来源文件": str(PACK_FILE),
    "股票数量": stock_count,
    "总行数": len(df_all),
    "结果数量": len(result_df),
    "硬过滤通过数量": len(selected_df),
    "候选数量": len(candidate_df),
    "观察数量": len(watch_df),
    "放弃数量": int((result_df["分层标签"] == "放弃").sum()) if not result_df.empty else 0,
    "跳过数量": len(skip_df),
    "失败数量": len(error_df),
    "通过低波动数量": int((result_df["低波动通过"] == "是").sum()) if not result_df.empty else 0,
    "通过阳线数量": int((result_df["阳线通过"] == "是").sum()) if not result_df.empty else 0,
    "通过放量区间数量": int((result_df["放量区间通过"] == "是").sum()) if not result_df.empty else 0,
    "通过换手率数量": int((result_df["换手率通过"] == "是").sum()) if not result_df.empty else 0,
    "通过冷量条件数量": int((result_df["冷量条件通过"] == "是").sum()) if not result_df.empty else 0,
    "波动窗口": VOLATILITY_WINDOW,
    "波动率上限": VOLATILITY_MAX,
    "是否要求阳线": REQUIRE_BULLISH,
    "放量倍数下限": VOLUME_MULTIPLIER_MIN,
    "放量倍数上限": VOLUME_MULTIPLIER_MAX,
    "换手率下限": TURNOVER_MIN,
    "冷量回看窗口": COLD_VOLUME_WINDOW,
    "前高量占比上限": COLD_VOLUME_RATIO,
    "最少历史条数": MIN_HISTORY_BARS,
}])
summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")
log_lines = [
    "P7 parquet scan finished",
    f"pack_file={PACK_FILE}",
    f"stock_count={stock_count}",
    f"row_count={len(df_all)}",
    f"result_count={len(result_df)}",
    f"hard_pass_count={len(selected_df)}",
    f"candidate_count={len(candidate_df)}",
    f"watch_count={len(watch_df)}",
    f"skip_count={len(skip_df)}",
    f"error_count={len(error_df)}",
    f"all_result_file={all_result_file}",
    f"selected_file={selected_file}",
    f"candidate_file={candidate_file}",
    f"watch_file={watch_file}",
    f"skip_file={skip_file}",
    f"error_file={error_file}",
    f"summary_file={summary_file}",
]
log_file.write_text("\n".join(log_lines), encoding="utf-8")
print("运行完成")
print(f"来源文件: {PACK_FILE}")
print(f"股票数量: {stock_count}")
print(f"总行数: {len(df_all)}")
print(f"结果数量: {len(result_df)}")
print(f"硬过滤通过数量: {len(selected_df)}")
print(f"候选数量: {len(candidate_df)}")
print(f"观察数量: {len(watch_df)}")
print(f"跳过数量: {len(skip_df)}")
print(f"失败数量: {len(error_df)}")
print(f"结果文件: {all_result_file}")
print(f"硬过滤通过文件: {selected_file}")
print(f"候选文件: {candidate_file}")
print(f"观察文件: {watch_file}")
print(f"跳过文件: {skip_file}")
print(f"异常文件: {error_file}")
print(f"汇总文件: {summary_file}")
print(f"日志文件: {log_file}")
