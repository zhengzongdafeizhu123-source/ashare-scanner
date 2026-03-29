from pathlib import Path
from datetime import datetime
import pandas as pd
import akshare as ak

# 固定测试标的：平安银行
SYMBOL = "000001"
NAME = "平安银行"

# 固定目录
DATA_DIR = Path(r"W:\AshareScanner\data")
OUTPUT_DIR = Path(r"W:\AshareScanner\output")
LOGS_DIR = Path(r"W:\AshareScanner\logs")

DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")
data_file = DATA_DIR / f"{SYMBOL}_{today_str}.csv"
log_file = LOGS_DIR / f"p1_single_stock_test_{today_str}.log"

try:
    # 获取单票前复权日线
    df = ak.stock_zh_a_hist(
        symbol=SYMBOL,
        period="daily",
        start_date="20240101",
        end_date=today_str,
        adjust="qfq"
    )

    if df is None or df.empty:
        raise ValueError("返回结果为空，未获取到任何数据")

    # 只保留最基础字段，先跑通
    keep_cols = [col for col in ["日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"] if col in df.columns]
    df = df[keep_cols].copy()

    # 写入 data
    df.to_csv(data_file, index=False, encoding="utf-8-sig")

    # 写入 output 简报
    summary = pd.DataFrame([{
        "股票代码": SYMBOL,
        "股票名称": NAME,
        "记录条数": len(df),
        "起始日期": df.iloc[0]["日期"],
        "结束日期": df.iloc[-1]["日期"],
        "最新收盘": df.iloc[-1]["收盘"]
    }])
    summary_file = OUTPUT_DIR / f"p1_single_stock_summary_{today_str}.csv"
    summary.to_csv(summary_file, index=False, encoding="utf-8-sig")

    # 写日志
    log_text = [
        "P1 single stock test: SUCCESS",
        f"symbol={SYMBOL}",
        f"name={NAME}",
        f"rows={len(df)}",
        f"data_file={data_file}",
        f"summary_file={summary_file}",
    ]
    log_file.write_text("\n".join(log_text), encoding="utf-8")

    print("运行成功")
    print(f"数据文件: {data_file}")
    print(f"汇总文件: {summary_file}")
    print(f"日志文件: {log_file}")

except Exception as e:
    error_text = [
        "P1 single stock test: FAILED",
        f"symbol={SYMBOL}",
        f"error={repr(e)}",
    ]
    log_file.write_text("\n".join(error_text), encoding="utf-8")
    print("运行失败")
    print(f"日志文件: {log_file}")
    raise