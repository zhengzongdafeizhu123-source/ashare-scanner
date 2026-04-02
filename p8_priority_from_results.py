from pathlib import Path
from datetime import datetime
import pandas as pd

from project_paths import LOGS_DIR, SAMPLES_OUTPUT_DIR, SCAN_OUTPUT_DIR

OUTPUT_DIR = SAMPLES_OUTPUT_DIR

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

today_str = datetime.now().strftime("%Y%m%d")

# 优先使用今天的 all_results；没有就取最新一个
today_result_file = SCAN_OUTPUT_DIR / f"p7_scan_from_local_all_results_{today_str}.csv"

if today_result_file.exists():
    source_file = today_result_file
else:
    candidates = sorted(SCAN_OUTPUT_DIR.glob("p7_scan_from_local_all_results_*.csv"))
    if not candidates:
        raise FileNotFoundError("未找到任何 p7_scan_from_local_all_results_*.csv 文件")
    source_file = candidates[-1]

priority_file = OUTPUT_DIR / f"p8_priority_results_{today_str}.csv"
priority_a_file = OUTPUT_DIR / f"p8_priority_A_{today_str}.csv"
priority_b_file = OUTPUT_DIR / f"p8_priority_B_{today_str}.csv"
priority_c_file = OUTPUT_DIR / f"p8_priority_C_{today_str}.csv"
summary_file = OUTPUT_DIR / f"p8_priority_summary_{today_str}.csv"
log_file = LOGS_DIR / f"p8_priority_{today_str}.log"


def get_priority(hard_passed: bool, label: str):
    if hard_passed and label == "候选":
        return "A", "硬过滤通过 + 候选"
    if (hard_passed and label == "观察") or ((not hard_passed) and label == "候选"):
        return "B", "硬过滤通过+观察 或 硬过滤未过+候选"
    if (not hard_passed) and label == "观察":
        return "C", "硬过滤未过 + 观察"
    return "", ""


df = pd.read_csv(source_file, dtype={"股票代码": str})
if df.empty:
    raise ValueError(f"源文件为空: {source_file}")

required_cols = [
    "股票代码",
    "股票名称",
    "日期",
    "硬过滤是否通过",
    "分层标签",
]

missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    raise ValueError(f"源文件缺少字段: {missing_cols}")

df = df.copy()
df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
df["股票名称"] = df["股票名称"].astype(str).str.strip()

priority_list = []
priority_note_list = []

for _, row in df.iterrows():
    hard_passed = str(row["硬过滤是否通过"]).strip() == "是"
    label = str(row["分层标签"]).strip()
    p, note = get_priority(hard_passed, label)
    priority_list.append(p)
    priority_note_list.append(note)

df["优先级"] = priority_list
df["优先级说明"] = priority_note_list
df["优先级排序值"] = df["优先级"].map({"A": 3, "B": 2, "C": 1}).fillna(0)

sort_cols = []
ascending = []

for col, asc in [
    ("优先级排序值", False),
    ("命中硬过滤数", False),
    ("VR5", False),
    ("BR20", False),
    ("换手率", False),
    ("量比前一日", False),
]:
    if col in df.columns:
        sort_cols.append(col)
        ascending.append(asc)

if sort_cols:
    df = df.sort_values(by=sort_cols, ascending=ascending)

priority_df = df[df["优先级"] != ""].copy()
priority_a_df = df[df["优先级"] == "A"].copy()
priority_b_df = df[df["优先级"] == "B"].copy()
priority_c_df = df[df["优先级"] == "C"].copy()

for sub_df in [df, priority_df, priority_a_df, priority_b_df, priority_c_df]:
    if "优先级排序值" in sub_df.columns:
        sub_df.drop(columns=["优先级排序值"], inplace=True)

priority_df.to_csv(priority_file, index=False, encoding="utf-8-sig")
priority_a_df.to_csv(priority_a_file, index=False, encoding="utf-8-sig")
priority_b_df.to_csv(priority_b_file, index=False, encoding="utf-8-sig")
priority_c_df.to_csv(priority_c_file, index=False, encoding="utf-8-sig")

summary_df = pd.DataFrame([{
    "源文件": source_file.name,
    "总行数": len(df),
    "A优先级数量": len(priority_a_df),
    "B优先级数量": len(priority_b_df),
    "C优先级数量": len(priority_c_df),
    "priority总数量": len(priority_df),
}])
summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")

log_lines = [
    "P8 priority build finished",
    f"source_file={source_file}",
    f"row_count={len(df)}",
    f"priority_count={len(priority_df)}",
    f"priority_a_count={len(priority_a_df)}",
    f"priority_b_count={len(priority_b_df)}",
    f"priority_c_count={len(priority_c_df)}",
    f"priority_file={priority_file}",
    f"priority_a_file={priority_a_file}",
    f"priority_b_file={priority_b_file}",
    f"priority_c_file={priority_c_file}",
    f"summary_file={summary_file}",
]
log_file.write_text("\n".join(log_lines), encoding="utf-8")

print("运行完成")
print(f"源文件: {source_file}")
print(f"A优先级数量: {len(priority_a_df)}")
print(f"B优先级数量: {len(priority_b_df)}")
print(f"C优先级数量: {len(priority_c_df)}")
print(f"Priority文件: {priority_file}")
print(f"A级文件: {priority_a_file}")
print(f"B级文件: {priority_b_file}")
print(f"C级文件: {priority_c_file}")
print(f"汇总文件: {summary_file}")
print(f"日志文件: {log_file}")
