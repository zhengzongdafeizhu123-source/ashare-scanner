from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_RESEARCH_CONFIG_FILE = PROJECT_DIR / "research_config.json"

DEFAULT_SETTINGS = {
    "sample_filter": "hard_pass_or_watch",
    "primary_success_label": "success_composite_flag",
    "parameter_interval": {
        "feature_columns": [
            "volume_ratio_prev1",
            "vr5",
            "clv",
            "br20",
            "d0_turnover",
            "d0_turnover_f",
            "d0_volume_ratio_basic",
            "d0_range_vol",
            "d0_limit_up_space_pct",
            "d0_big_order_net_amount",
            "d0_big_order_net_ratio",
            "d0_total_mv",
            "d0_circ_mv",
            "list_age_days",
        ],
        "quantile_bins": 20,
        "smooth_window": 3,
        "min_segment_sample_ratio": 0.03,
        "min_success_lift": 1.05,
        "min_success_margin": 0.02,
        "relative_band_cap": 0.10,
        "absolute_band_overrides": {
            "clv": 0.10,
            "br20": 0.03,
            "d0_limit_up_space_pct": 2.0,
            "d0_big_order_net_ratio": 0.00001,
        },
        "feature_alias": {
            "volume_ratio_prev1": "量比前一日",
            "vr5": "VR5",
            "clv": "CLV",
            "br20": "BR20",
            "d0_turnover": "D0换手率",
            "d0_turnover_f": "D0自由流通换手率",
            "d0_volume_ratio_basic": "D0量比(Tushare)",
            "d0_range_vol": "D0波动率",
            "d0_limit_up_space_pct": "距涨停空间%",
            "d0_big_order_net_amount": "大单净流入额",
            "d0_big_order_net_ratio": "大单净流入占比",
            "d0_total_mv": "总市值",
            "d0_circ_mv": "流通市值",
            "list_age_days": "上市天数",
        },
    },
}


@dataclass
class DiscoverConfig:
    sample_filter: str
    primary_success_label: str
    feature_columns: list[str]
    quantile_bins: int
    smooth_window: int
    min_segment_sample_ratio: float
    min_success_lift: float
    min_success_margin: float
    relative_band_cap: float
    absolute_band_overrides: dict[str, float]
    feature_alias: dict[str, str]


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def deep_merge(a: dict, b: dict) -> dict:
    result = dict(a)
    for k, v in b.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="发现高质量 D0 参数区间")
    parser.add_argument("--dataset", required=True, help="P9 输出的研究样本 parquet 路径")
    parser.add_argument("--research-config", default="", help="研究配置文件路径，默认 research_config.json")
    return parser.parse_args()


def load_settings(path: Path | None) -> DiscoverConfig:
    cfg = deep_merge(DEFAULT_SETTINGS, load_json(path or DEFAULT_RESEARCH_CONFIG_FILE))
    pi = cfg.get("parameter_interval", {})
    return DiscoverConfig(
        sample_filter=str(cfg.get("sample_filter", "hard_pass_or_watch")),
        primary_success_label=str(cfg.get("primary_success_label", "success_composite_flag")),
        feature_columns=[str(x) for x in pi.get("feature_columns", DEFAULT_SETTINGS["parameter_interval"]["feature_columns"])],
        quantile_bins=int(pi.get("quantile_bins", 20)),
        smooth_window=max(1, int(pi.get("smooth_window", 3))),
        min_segment_sample_ratio=float(pi.get("min_segment_sample_ratio", 0.03)),
        min_success_lift=float(pi.get("min_success_lift", 1.05)),
        min_success_margin=float(pi.get("min_success_margin", 0.02)),
        relative_band_cap=float(pi.get("relative_band_cap", 0.10)),
        absolute_band_overrides={str(k): float(v) for k, v in pi.get("absolute_band_overrides", {}).items()},
        feature_alias={str(k): str(v) for k, v in pi.get("feature_alias", {}).items()},
    )


def apply_sample_filter(df: pd.DataFrame, name: str) -> pd.DataFrame:
    if name == "all":
        return df.copy()
    if name == "hard_pass_only":
        return df[df["hard_pass"] == "是"].copy()
    if name == "candidate_or_watch":
        return df[df["research_bucket"].isin(["候选", "观察"])].copy()
    return df[df["research_bucket"].isin(["入围", "候选", "观察"])].copy()


def flag_series(df: pd.DataFrame, label: str) -> pd.Series:
    if label not in df.columns:
        raise KeyError(f"未找到成功标签列: {label}")
    return df[label].eq("是")


def safe_float(value: Any) -> float | None:
    try:
        v = float(value)
        if np.isfinite(v):
            return v
    except Exception:
        pass
    return None


def extract_interval_from_bin(bin_str: str) -> tuple[float | None, float | None]:
    text = str(bin_str).strip()
    if not text.startswith("(") and not text.startswith("["):
        return None, None
    text = text.strip("()[]")
    parts = [p.strip() for p in text.split(",")]
    if len(parts) != 2:
        return None, None
    return safe_float(parts[0]), safe_float(parts[1])


def build_bin_stats(df: pd.DataFrame, feature: str, success: pd.Series, q: int) -> pd.DataFrame:
    s = pd.to_numeric(df[feature], errors="coerce")
    valid = s.notna() & success.notna()
    if valid.sum() < max(200, q * 5):
        return pd.DataFrame()
    actual_q = min(q, max(5, int(valid.sum() // 200)))
    try:
        bins = pd.qcut(s[valid], q=actual_q, duplicates="drop")
    except Exception:
        return pd.DataFrame()

    work = pd.DataFrame({"value": s[valid], "bin": bins.astype(str), "success": success[valid].astype(bool).values})
    grouped = work.groupby("bin", sort=False)
    rows = []
    for order, (bin_name, sub) in enumerate(grouped, start=1):
        low, high = extract_interval_from_bin(bin_name)
        rows.append({
            "feature": feature,
            "bin_order": order,
            "bin": str(bin_name),
            "bin_low": low,
            "bin_high": high,
            "sample_count": len(sub),
            "success_count": int(sub["success"].sum()),
            "success_rate": float(sub["success"].mean()),
            "avg_value": float(sub["value"].mean()),
            "value_min": float(sub["value"].min()),
            "value_max": float(sub["value"].max()),
        })
    return pd.DataFrame(rows)


def add_smoothed_metrics(bin_df: pd.DataFrame, base_rate: float, smooth_window: int) -> pd.DataFrame:
    if bin_df.empty:
        return bin_df
    work = bin_df.sort_values("bin_order").reset_index(drop=True).copy()
    work["sample_ratio"] = work["sample_count"] / work["sample_count"].sum()
    work["lift"] = work["success_rate"] / base_rate if base_rate > 0 else np.nan
    work["success_margin"] = work["success_rate"] - base_rate
    work["smoothed_success_rate"] = work["success_rate"].rolling(smooth_window, center=True, min_periods=1).mean()
    work["smoothed_lift"] = work["lift"].rolling(smooth_window, center=True, min_periods=1).mean()
    work["smoothed_margin"] = work["success_margin"].rolling(smooth_window, center=True, min_periods=1).mean()
    return work


def collect_segments(bin_df: pd.DataFrame, cfg: DiscoverConfig, base_rate: float) -> pd.DataFrame:
    if bin_df.empty:
        return pd.DataFrame()
    min_samples = max(1, int(bin_df["sample_count"].sum() * cfg.min_segment_sample_ratio))
    qualifying = (
        (bin_df["sample_count"] >= min_samples)
        & (bin_df["smoothed_lift"] >= cfg.min_success_lift)
        & (bin_df["smoothed_margin"] >= cfg.min_success_margin)
    )
    rows: list[dict[str, Any]] = []
    start = None
    for idx, ok in enumerate(qualifying.tolist() + [False]):
        if ok and start is None:
            start = idx
        if not ok and start is not None:
            end = idx - 1
            seg = bin_df.iloc[start : end + 1].copy()
            sample_count = int(seg["sample_count"].sum())
            success_count = int(seg["success_count"].sum())
            success_rate = success_count / sample_count if sample_count else 0.0
            seg_low = safe_float(seg["bin_low"].min()) if seg["bin_low"].notna().any() else safe_float(seg["value_min"].min())
            seg_high = safe_float(seg["bin_high"].max()) if seg["bin_high"].notna().any() else safe_float(seg["value_max"].max())
            weighted_center = float(np.average(seg["avg_value"], weights=seg["sample_count"]))
            score = (success_rate - base_rate) * np.log1p(sample_count)
            rows.append({
                "feature": str(seg.iloc[0]["feature"]),
                "segment_start_bin_order": int(seg.iloc[0]["bin_order"]),
                "segment_end_bin_order": int(seg.iloc[-1]["bin_order"]),
                "segment_bin_count": len(seg),
                "segment_low": seg_low,
                "segment_high": seg_high,
                "segment_sample_count": sample_count,
                "segment_success_count": success_count,
                "segment_success_rate": success_rate,
                "segment_lift": success_rate / base_rate if base_rate > 0 else np.nan,
                "segment_margin": success_rate - base_rate,
                "weighted_center": weighted_center,
                "segment_score": score,
            })
            start = None
    return pd.DataFrame(rows)


def classify_range_type(best_seg: pd.Series, bin_df: pd.DataFrame) -> str:
    if bin_df.empty:
        return "unknown"
    first_order = int(bin_df["bin_order"].min())
    last_order = int(bin_df["bin_order"].max())
    start_order = int(best_seg["segment_start_bin_order"])
    end_order = int(best_seg["segment_end_bin_order"])
    if start_order == first_order and end_order < last_order:
        return "upper_bound"
    if end_order == last_order and start_order > first_order:
        return "lower_bound"
    if start_order == first_order and end_order == last_order:
        return "all_range"
    return "band"


def build_recommended_band(feature: str, best_seg: pd.Series, range_type: str, cfg: DiscoverConfig) -> tuple[float | None, float | None]:
    center = safe_float(best_seg.get("weighted_center"))
    seg_low = safe_float(best_seg.get("segment_low"))
    seg_high = safe_float(best_seg.get("segment_high"))
    if center is None:
        return seg_low, seg_high

    abs_override = cfg.absolute_band_overrides.get(feature)
    if abs_override is not None:
        low_cap = center - abs_override
        high_cap = center + abs_override
    else:
        cap = abs(center) * cfg.relative_band_cap
        if cap == 0:
            cap = cfg.relative_band_cap
        low_cap = center - cap
        high_cap = center + cap

    if range_type == "lower_bound":
        return max(seg_low, low_cap) if seg_low is not None else low_cap, None
    if range_type == "upper_bound":
        return None, min(seg_high, high_cap) if seg_high is not None else high_cap
    if range_type == "all_range":
        return seg_low, seg_high
    low = max(seg_low, low_cap) if seg_low is not None else low_cap
    high = min(seg_high, high_cap) if seg_high is not None else high_cap
    return low, high


def suggestion_text(range_type: str, low: float | None, high: float | None) -> str:
    def fmt(v: float | None) -> str:
        if v is None:
            return ""
        if abs(v) >= 1000:
            return f"{v:,.2f}"
        if abs(v) >= 10:
            return f"{v:.2f}"
        return f"{v:.4f}"

    if range_type == "lower_bound":
        return f">= {fmt(low)}"
    if range_type == "upper_bound":
        return f"<= {fmt(high)}"
    if low is not None and high is not None:
        return f"between {fmt(low)} and {fmt(high)}"
    return "无建议"


def discover_for_feature(df: pd.DataFrame, feature: str, success: pd.Series, cfg: DiscoverConfig, base_rate: float) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any] | None]:
    bin_df = build_bin_stats(df, feature, success, cfg.quantile_bins)
    if bin_df.empty:
        return pd.DataFrame(), pd.DataFrame(), None
    bin_df = add_smoothed_metrics(bin_df, base_rate, cfg.smooth_window)
    seg_df = collect_segments(bin_df, cfg, base_rate)

    if seg_df.empty:
        best_bin = bin_df.sort_values(["smoothed_lift", "sample_count"], ascending=[False, False]).iloc[0]
        pseudo_seg = pd.Series({
            "feature": feature,
            "segment_start_bin_order": int(best_bin["bin_order"]),
            "segment_end_bin_order": int(best_bin["bin_order"]),
            "segment_bin_count": 1,
            "segment_low": best_bin["bin_low"] if pd.notna(best_bin["bin_low"]) else best_bin["value_min"],
            "segment_high": best_bin["bin_high"] if pd.notna(best_bin["bin_high"]) else best_bin["value_max"],
            "segment_sample_count": int(best_bin["sample_count"]),
            "segment_success_count": int(best_bin["success_count"]),
            "segment_success_rate": float(best_bin["success_rate"]),
            "segment_lift": float(best_bin["lift"]),
            "segment_margin": float(best_bin["success_margin"]),
            "weighted_center": float(best_bin["avg_value"]),
            "segment_score": float(best_bin["smoothed_lift"] * np.log1p(best_bin["sample_count"])),
        })
        range_type = "band"
        rec_low, rec_high = build_recommended_band(feature, pseudo_seg, range_type, cfg)
        summary = {
            "feature": feature,
            "feature_name": cfg.feature_alias.get(feature, feature),
            "base_success_rate": base_rate,
            "selection_method": "best_single_bin",
            "range_type": range_type,
            "observed_low": pseudo_seg["segment_low"],
            "observed_high": pseudo_seg["segment_high"],
            "center_value": pseudo_seg["weighted_center"],
            "recommended_low": rec_low,
            "recommended_high": rec_high,
            "recommended_rule": suggestion_text(range_type, rec_low, rec_high),
            "segment_success_rate": pseudo_seg["segment_success_rate"],
            "segment_lift": pseudo_seg["segment_lift"],
            "segment_margin": pseudo_seg["segment_margin"],
            "segment_sample_count": pseudo_seg["segment_sample_count"],
            "segment_success_count": pseudo_seg["segment_success_count"],
            "best_bin": best_bin["bin"],
        }
        return bin_df, seg_df, summary

    seg_df = seg_df.sort_values(["segment_score", "segment_sample_count"], ascending=[False, False]).reset_index(drop=True)
    best_seg = seg_df.iloc[0]
    range_type = classify_range_type(best_seg, bin_df)
    rec_low, rec_high = build_recommended_band(feature, best_seg, range_type, cfg)
    summary = {
        "feature": feature,
        "feature_name": cfg.feature_alias.get(feature, feature),
        "base_success_rate": base_rate,
        "selection_method": "best_segment",
        "range_type": range_type,
        "observed_low": best_seg["segment_low"],
        "observed_high": best_seg["segment_high"],
        "center_value": best_seg["weighted_center"],
        "recommended_low": rec_low,
        "recommended_high": rec_high,
        "recommended_rule": suggestion_text(range_type, rec_low, rec_high),
        "segment_success_rate": best_seg["segment_success_rate"],
        "segment_lift": best_seg["segment_lift"],
        "segment_margin": best_seg["segment_margin"],
        "segment_sample_count": best_seg["segment_sample_count"],
        "segment_success_count": best_seg["segment_success_count"],
        "best_bin": f"{int(best_seg['segment_start_bin_order'])}-{int(best_seg['segment_end_bin_order'])}",
    }
    return bin_df, seg_df, summary


def format_report(summary_df: pd.DataFrame, top_n: int, dataset_name: str, sample_filter: str, label_name: str) -> str:
    lines = [
        "# 参数区间发现报告（v1）",
        "",
        f"- 数据集: `{dataset_name}`",
        f"- 样本筛选: `{sample_filter}`",
        f"- 主成功标签: `{label_name}`",
        "",
    ]
    if summary_df.empty:
        lines.append("未发现可用的参数区间建议。")
        return "\n".join(lines)

    top_df = summary_df.sort_values(["segment_lift", "segment_sample_count"], ascending=[False, False]).head(top_n)
    lines += [
        "## 推荐优先查看的参数区间",
        "",
        top_df[[
            "feature_name",
            "range_type",
            "recommended_rule",
            "segment_success_rate",
            "segment_lift",
            "segment_sample_count",
        ]].to_markdown(index=False),
        "",
        "## 全部参数建议",
        "",
        summary_df[[
            "feature_name",
            "selection_method",
            "range_type",
            "observed_low",
            "observed_high",
            "center_value",
            "recommended_low",
            "recommended_high",
            "recommended_rule",
            "segment_success_rate",
            "segment_lift",
            "segment_sample_count",
        ]].to_markdown(index=False),
    ]
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        raise FileNotFoundError(f"未找到研究样本文件: {dataset_path}")
    cfg = load_settings(Path(args.research_config) if args.research_config else None)

    df = pd.read_parquet(dataset_path)
    filtered = apply_sample_filter(df, cfg.sample_filter)
    success = flag_series(filtered, cfg.primary_success_label)
    base_rate = float(success.mean()) if len(success) else 0.0
    if len(filtered) == 0:
        raise ValueError("筛选后样本为空")

    out_dir = dataset_path.parent
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_file = out_dir / f"p11_parameter_interval_summary_{stamp}.csv"
    bins_file = out_dir / f"p11_parameter_interval_bins_{stamp}.csv"
    segments_file = out_dir / f"p11_parameter_interval_segments_{stamp}.csv"
    markdown_file = out_dir / f"p11_parameter_interval_report_{stamp}.md"

    summary_rows: list[dict[str, Any]] = []
    all_bins: list[pd.DataFrame] = []
    all_segments: list[pd.DataFrame] = []

    for feature in cfg.feature_columns:
        if feature not in filtered.columns:
            continue
        bin_df, seg_df, summary = discover_for_feature(filtered, feature, success, cfg, base_rate)
        if not bin_df.empty:
            all_bins.append(bin_df)
        if not seg_df.empty:
            all_segments.append(seg_df)
        if summary is not None:
            summary_rows.append(summary)

    summary_df = pd.DataFrame(summary_rows)
    bins_df = pd.concat(all_bins, ignore_index=True) if all_bins else pd.DataFrame()
    segments_df = pd.concat(all_segments, ignore_index=True) if all_segments else pd.DataFrame()

    if not summary_df.empty:
        summary_df = summary_df.sort_values(["segment_lift", "segment_sample_count"], ascending=[False, False]).reset_index(drop=True)

    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")
    bins_df.to_csv(bins_file, index=False, encoding="utf-8-sig")
    segments_df.to_csv(segments_file, index=False, encoding="utf-8-sig")

    report = format_report(summary_df, 10, dataset_path.name, cfg.sample_filter, cfg.primary_success_label)
    markdown_file.write_text(report, encoding="utf-8")

    print(f"样本数: {len(filtered)}")
    print(f"基准成功率: {base_rate:.4f}")
    print(f"参数区间汇总: {summary_file}")
    print(f"分箱明细: {bins_file}")
    print(f"区间片段: {segments_file}")
    print(f"Markdown报告: {markdown_file}")


if __name__ == "__main__":
    main()
