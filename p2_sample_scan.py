from pathlib import Path
from datetime import datetime
import time
import pandas as pd
import akshare as ak

from project_paths import BASE_DIR, LOGS_DIR, SAMPLES_OUTPUT_DIR

# 固定 10 只样本股，先验证流程
SAMPLE_STOCKS = [
    ("000001", "平安银行"),
    ("600519", "贵州茅台"),
    ("300750", "宁德时代"),
    ("601318", "中国平安"),
    ("600036", "招商银行"),
    ("000858", "五粮液"),
    ("002594", "比亚迪"),
    ("601899", "紫金矿业"),
    ("600276", "恒瑞医药"),
    ("000333", "美的集团"),
]

DATA_DIR = BASE_DIR / "data" / "samples"
OUTPUT_DIR = SAMPLES_OUTPUT_DIR

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")
result_file = OUTPUT_DIR / f"p2_sample_scan_results_{today_str}.csv"
error_file = OUTPUT_DIR / f"p2_sample_scan_errors_{today_str}.csv"
log_file = LOGS_DIR / f"p2_sample_scan_{today_str}.log"

results = []
errors = []


def calc_clv(high_price, low_price, close_price):
    denominator = high_price - low_price
    if denominator == 0:
        return 0
    return ((close_price - low_price) - (high_price - close_price)) / denominator


def fetch_hist_with_retry(symbol, max_retries=3, sleep_seconds=2):
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date="20240101",
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


for idx, (symbol, name) in enumerate(SAMPLE_STOCKS, start=1):
    print(f"[进度] {idx}/{len(SAMPLE_STOCKS)} 开始处理 {symbol} {name}")

    try:
        df = fetch_hist_with_retry(symbol)

        # 基础字段检查
        required_cols = ["日期", "开盘", "收盘", "最高", "最低", "成交量"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            raise ValueError(f"缺少字段: {missing_cols}")

        if len(df) < 25:
            raise ValueError(f"历史数据不足，当前仅 {len(df)} 行")

        df = df.copy()
        latest = df.iloc[-1]
        prev = df.iloc[-2]

        latest_close = float(latest["收盘"])
        latest_open = float(latest["开盘"])
        latest_high = float(latest["最高"])
        latest_low = float(latest["最低"])
        latest_volume = float(latest["成交量"])

        prev_close = float(prev["收盘"])

        # 涨跌幅（用收盘对前收）
        pct_change = (latest_close / prev_close - 1) * 100

        # VR5：今日成交量 / 前5日平均成交量
        prev_5_avg_vol = df.iloc[-6:-1]["成交量"].astype(float).mean()
        vr5 = latest_volume / prev_5_avg_vol if prev_5_avg_vol != 0 else 0

        # CLV：收盘在当日区间中的位置
        clv = calc_clv(latest_high, latest_low, latest_close)

        # BR20：最新收盘 / 前20日最高价
        prev_20_high = df.iloc[-21:-1]["最高"].astype(float).max()
        br20 = latest_close / prev_20_high if prev_20_high != 0 else 0

        # 简化标签规则：先跑通，不追求“准”
        if vr5 >= 1.8 and clv >= 0.3 and br20 >= 0.98:
            tag = "候选"
        elif vr5 >= 1.2 and clv >= 0:
            tag = "观察"
        else:
            tag = "放弃"

        results.append({
            "股票代码": symbol,
            "股票名称": name,
            "日期": latest["日期"],
            "开盘": latest_open,
            "收盘": latest_close,
            "最高": latest_high,
            "最低": latest_low,
            "涨跌幅%": round(pct_change, 2),
            "VR5": round(vr5, 2),
            "CLV": round(clv, 2),
            "BR20": round(br20, 3),
            "标签": tag
        })

        print(f"[完成] {symbol} {name} 成功")

    except Exception as e:
        errors.append({
            "股票代码": symbol,
            "股票名称": name,
            "错误信息": repr(e)
        })
        print(f"[失败] {symbol} {name}: {repr(e)}")


# 导出结果
result_df = pd.DataFrame(results)
if not result_df.empty:
    result_df = result_df.sort_values(
        by=["标签", "VR5", "BR20"],
        ascending=[True, False, False]
    )
result_df.to_csv(result_file, index=False, encoding="utf-8-sig")

error_df = pd.DataFrame(errors)
error_df.to_csv(error_file, index=False, encoding="utf-8-sig")

log_lines = [
    "P2 sample scan finished",
    f"sample_total={len(SAMPLE_STOCKS)}",
    f"success_count={len(results)}",
    f"error_count={len(errors)}",
    f"result_file={result_file}",
    f"error_file={error_file}",
]
log_file.write_text("\n".join(log_lines), encoding="utf-8")

print("运行完成")
print(f"成功数量: {len(results)}")
print(f"失败数量: {len(errors)}")
print(f"结果文件: {result_file}")
print(f"异常文件: {error_file}")
print(f"日志文件: {log_file}")
