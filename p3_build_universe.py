from pathlib import Path
from datetime import datetime
import pandas as pd
import akshare as ak

OUTPUT_DIR = Path(r"W:\AshareScanner\output")
LOGS_DIR = Path(r"W:\AshareScanner\logs")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")

raw_file = OUTPUT_DIR / f"p3_universe_raw_{today_str}.csv"
filtered_file = OUTPUT_DIR / f"p3_universe_filtered_{today_str}.csv"
summary_file = OUTPUT_DIR / f"p3_universe_summary_{today_str}.csv"
log_file = LOGS_DIR / f"p3_build_universe_{today_str}.log"


def fetch_universe():
    """
    优先尝试 stock_info_a_code_name；
    如果失败，再回退到 stock_zh_a_spot_em。
    """
    errors = []

    try:
        df = ak.stock_info_a_code_name()
        if df is not None and not df.empty:
            if "code" in df.columns and "name" in df.columns:
                out = df[["code", "name"]].copy()
                out["source"] = "stock_info_a_code_name"
                return out
            else:
                raise ValueError(f"stock_info_a_code_name 返回字段异常: {list(df.columns)}")
    except Exception as e:
        errors.append(f"stock_info_a_code_name failed: {repr(e)}")

    try:
        df = ak.stock_zh_a_spot_em()
        if df is not None and not df.empty:
            if "代码" in df.columns and "名称" in df.columns:
                out = df[["代码", "名称"]].copy()
                out.columns = ["code", "name"]
                out["source"] = "stock_zh_a_spot_em"
                return out
            else:
                raise ValueError(f"stock_zh_a_spot_em 返回字段异常: {list(df.columns)}")
    except Exception as e:
        errors.append(f"stock_zh_a_spot_em failed: {repr(e)}")

    raise RuntimeError(" | ".join(errors))


def normalize_code(code):
    return str(code).strip().zfill(6)


def is_main_a_share(code):
    """
    先只保留常见 A 股代码段
    """
    valid_prefixes = (
        "000", "001", "002", "003",
        "300", "301",
        "600", "601", "603", "605",
        "688"
    )
    return code.startswith(valid_prefixes)


def is_excluded_name(name):
    """
    基础过滤：
    - ST / *ST
    - 退市
    """
    name = str(name).strip().upper()
    if "ST" in name:
        return True
    if "退" in name:
        return True
    return False


try:
    raw_df = fetch_universe()

    raw_df["code"] = raw_df["code"].apply(normalize_code)
    raw_df["name"] = raw_df["name"].astype(str).str.strip()

    # 原始去重
    raw_df = raw_df.drop_duplicates(subset=["code"]).reset_index(drop=True)

    # 保存原始股票池
    raw_df.to_csv(raw_file, index=False, encoding="utf-8-sig")

    # 基础过滤
    filtered_df = raw_df.copy()
    filtered_df = filtered_df[filtered_df["code"].apply(is_main_a_share)]
    filtered_df = filtered_df[~filtered_df["name"].apply(is_excluded_name)]
    filtered_df = filtered_df.sort_values(by="code").reset_index(drop=True)

    filtered_df.to_csv(filtered_file, index=False, encoding="utf-8-sig")

    summary_df = pd.DataFrame([{
        "日期": today_str,
        "原始股票池数量": len(raw_df),
        "过滤后股票池数量": len(filtered_df),
        "数据来源": raw_df["source"].iloc[0] if not raw_df.empty else ""
    }])
    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")

    log_lines = [
        "P3 build universe finished",
        f"raw_count={len(raw_df)}",
        f"filtered_count={len(filtered_df)}",
        f"raw_file={raw_file}",
        f"filtered_file={filtered_file}",
        f"summary_file={summary_file}",
    ]
    log_file.write_text("\n".join(log_lines), encoding="utf-8")

    print("运行完成")
    print(f"原始股票池数量: {len(raw_df)}")
    print(f"过滤后股票池数量: {len(filtered_df)}")
    print(f"原始文件: {raw_file}")
    print(f"过滤后文件: {filtered_file}")
    print(f"汇总文件: {summary_file}")
    print(f"日志文件: {log_file}")

except Exception as e:
    log_file.write_text(
        "\n".join([
            "P3 build universe failed",
            f"error={repr(e)}"
        ]),
        encoding="utf-8"
    )
    print("运行失败")
    print(f"日志文件: {log_file}")
    raise