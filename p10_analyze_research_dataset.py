from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_RESEARCH_CONFIG_FILE = PROJECT_DIR / "research_config.json"

LABEL_META = {
    "d1_stable_flag": "D1稳健",
    "d2_sellable_flag": "D2有卖点",
    "success_composite_flag": "综合成功",
}


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def parse_args():
    parser = argparse.ArgumentParser(description="分析历史研究样本，输出并行成功标签统计")
    parser.add_argument("--dataset", required=True, help="p9_build_research_dataset.py 输出的 parquet 文件路径")
    parser.add_argument("--research-config", default="", help="研究配置文件路径，默认 research_config.json")
    return parser.parse_args()


def load_research_settings(path: Path | None) -> dict:
    cfg = load_json(path or DEFAULT_RESEARCH_CONFIG_FILE)
    return {
        "sample_filter": str(cfg.get("sample_filter", "hard_pass_or_watch")),
        "primary_success_label": str(cfg.get("primary_success_label", "success_composite_flag")),
        "feature_columns": cfg.get(
            "feature_columns",
            [
                "volume_ratio_prev1", "vr5", "clv", "br20", "d0_turnover", "d0_range_vol", "cold_max_volume",
                "d0_turnover_f", "d0_volume_ratio_basic", "d0_total_mv", "d0_circ_mv", "d0_limit_up_space_pct",
                "d0_big_order_net_amount", "d0_big_order_net_ratio", "list_age_days"
            ],
        ),
    }


def apply_sample_filter(df: pd.DataFrame, name: str) -> pd.DataFrame:
    if name == "all":
        return df.copy()
    if name == "hard_pass_only":
        return df[df["hard_pass"] == "是"].copy()
    if name == "candidate_or_watch":
        return df[df["research_bucket"].isin(["候选", "观察"])].copy()
    return df[df["research_bucket"].isin(["入围", "候选", "观察"])].copy()


def label_to_flag(df: pd.DataFrame, flag_col: str) -> pd.Series:
    if flag_col not in df.columns:
        return pd.Series(False, index=df.index)
    return df[flag_col].eq("是")


def summarize_label_overview(df: pd.DataFrame, labels: list[str]) -> pd.DataFrame:
    rows = []
    total_count = len(df)
    for label in labels:
        flag = label_to_flag(df, label)
        rows.append({
            "label_key": label,
            "label_name": LABEL_META.get(label, label),
            "sample_count": total_count,
            "success_count": int(flag.sum()),
            "success_rate": round(float(flag.mean()), 4) if total_count else 0.0,
            "avg_d1_close_ret_pct": round(float(pd.to_numeric(df.loc[flag, "d1_close_ret_pct"], errors="coerce").mean()), 4) if flag.any() else None,
            "avg_d2_close_ret_pct": round(float(pd.to_numeric(df.loc[flag, "d2_close_ret_pct"], errors="coerce").mean()), 4) if flag.any() else None,
            "avg_d2_high_ret_pct": round(float(pd.to_numeric(df.loc[flag, "d2_high_ret_pct"], errors="coerce").mean()), 4) if flag.any() else None,
        })
    return pd.DataFrame(rows)


def summarize_by_bucket(df: pd.DataFrame, labels: list[str]) -> pd.DataFrame:
    rows = []
    for label in labels:
        flag = label_to_flag(df, label)
        for bucket in ["入围", "候选", "观察", "放弃"]:
            sub = df[df["research_bucket"] == bucket]
            if sub.empty:
                continue
            ok = flag.loc[sub.index]
            rows.append({
                "label_key": label,
                "label_name": LABEL_META.get(label, label),
                "bucket": bucket,
                "sample_count": len(sub),
                "success_count": int(ok.sum()),
                "success_rate": round(float(ok.mean()), 4),
                "avg_d1_close_ret_pct": round(float(pd.to_numeric(sub["d1_close_ret_pct"], errors="coerce").mean()), 4),
                "avg_d2_close_ret_pct": round(float(pd.to_numeric(sub["d2_close_ret_pct"], errors="coerce").mean()), 4),
                "avg_d2_high_ret_pct": round(float(pd.to_numeric(sub["d2_high_ret_pct"], errors="coerce").mean()), 4),
                "target1_hit_rate": round(float(sub["d2_target1_hit"].eq("是").mean()), 4),
            })
    return pd.DataFrame(rows)


def summarize_feature_bins(df: pd.DataFrame, success_flag: pd.Series, columns: list[str]) -> pd.DataFrame:
    rows = []
    for col in columns:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        valid = s.notna()
        if valid.sum() < 50:
            continue
        try:
            bins = pd.qcut(s[valid], q=5, duplicates="drop")
        except Exception:
            continue
        work = pd.DataFrame({"value": s[valid], "bin": bins, "success": success_flag[valid].values})
        grouped = work.groupby("bin", observed=False)
        for name, sub in grouped:
            rows.append({
                "feature": col,
                "bin": str(name),
                "sample_count": len(sub),
                "success_rate": round(float(sub["success"].mean()), 4),
                "avg_value": round(float(sub["value"].mean()), 6),
            })
    return pd.DataFrame(rows)


def main():
    args = parse_args()
    settings = load_research_settings(Path(args.research_config) if args.research_config else None)
    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        raise FileNotFoundError(f"未找到研究样本文件: {dataset_path}")

    df = pd.read_parquet(dataset_path)
    filtered = apply_sample_filter(df, settings["sample_filter"])
    labels = [key for key in LABEL_META if key in filtered.columns]
    if not labels:
        raise ValueError("研究样本中未找到成功标签列，请先用新版 p9_build_research_dataset.py 重建数据集")

    primary_label = settings["primary_success_label"]
    if primary_label not in labels:
        primary_label = labels[-1]
    primary_flag = label_to_flag(filtered, primary_label)

    out_dir = dataset_path.parent
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    overview_file = out_dir / f"p10_research_analysis_overview_{stamp}.csv"
    bucket_file = out_dir / f"p10_research_analysis_bucket_stats_{stamp}.csv"
    feature_file = out_dir / f"p10_research_analysis_feature_bins_{stamp}.csv"
    markdown_file = out_dir / f"p10_research_analysis_report_{stamp}.md"

    overview_df = summarize_label_overview(filtered, labels)
    bucket_df = summarize_by_bucket(filtered, labels)
    feature_df = summarize_feature_bins(filtered, primary_flag, settings["feature_columns"])

    overview_df.to_csv(overview_file, index=False, encoding="utf-8-sig")
    bucket_df.to_csv(bucket_file, index=False, encoding="utf-8-sig")
    feature_df.to_csv(feature_file, index=False, encoding="utf-8-sig")

    lines = [
        "# 研究样本分析报告（并行成功标签）",
        "",
        f"- 数据集: `{dataset_path.name}`",
        f"- 样本筛选: `{settings['sample_filter']}`",
        f"- 主分析标签: `{LABEL_META.get(primary_label, primary_label)}`",
        "",
        "## 成功标签总览",
        "",
        overview_df.to_markdown(index=False) if not overview_df.empty else "无总览统计。",
        "",
        "## Bucket 统计",
        "",
        bucket_df.to_markdown(index=False) if not bucket_df.empty else "无 bucket 统计。",
        "",
        f"## 特征分箱统计（主标签：{LABEL_META.get(primary_label, primary_label)}）",
        "",
        feature_df.to_markdown(index=False) if not feature_df.empty else "无特征分箱统计。",
    ]
    markdown_file.write_text("\n".join(lines), encoding="utf-8")

    print(f"标签总览: {overview_file}")
    print(f"Bucket统计: {bucket_file}")
    print(f"特征分箱: {feature_file}")
    print(f"Markdown报告: {markdown_file}")


if __name__ == "__main__":
    main()
