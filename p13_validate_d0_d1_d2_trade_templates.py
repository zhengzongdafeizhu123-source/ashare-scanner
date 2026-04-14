from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_RESEARCH_CONFIG_FILE = PROJECT_DIR / "research_config.json"
SCAN_CONFIG_FILE = PROJECT_DIR / "scan_config.json"

DEFAULT_OFFICIAL_D0_LOGIC = {
    "enabled": True,
    "min_score": 3,
    "thresholds": {
        "br20_min": 1.02,
        "limit_up_space_min": 0.0,
        "limit_up_space_max": 5.60,
        "turnover_min": 9.67,
        "turnover_f_min": 15.126,
        "range_vol_min": 0.666,
        "range_vol_max": 1.05,
    },
}

POOL_NAMES = [
    "pool_score_ge_3",
    "pool_score_ge_4",
    "pool_br20_limit_up_space",
    "pool_br20_limit_up_space_turnover",
    "pool_candidate_score_ge_3",
    "pool_watch_score_ge_3",
]

ENTRY_TEMPLATES = [
    "d1_open_buy",
    "d1_breakout_buy",
    "d1_mid_pullback_buy",
    "d1_support1_buy",
    "d1_open_buy_gap_le_5",
]

EXIT_TEMPLATES = [
    "d2_open_exit",
    "d2_close_exit",
    "d2_target1_then_close",
    "d2_target1_then_open",
    "d2_fail_fast_open_else_close",
]

PATH_CLASS_SMOOTH = "smooth_followthrough"
PATH_CLASS_DELAYED = "delayed_payoff"
PATH_CLASS_D1_ONLY = "d1_only"
PATH_CLASS_FAILED = "failed_path"

TODO_LINES = [
    "TODO: add event/noise suspect flag in later iterations.",
    "TODO: rerun with robustness filters for extreme gap and extreme volume samples.",
]


@dataclass
class TradeTemplateConfig:
    sample_filter: str
    primary_success_label: str
    official_d0_logic_v2: dict[str, Any]


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def deep_merge(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    result = dict(a)
    for key, value in b.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="验证 D0-D1-D2 交易路径模板的研究框架（P13）")
    parser.add_argument("--dataset", required=True, help="P9 输出的研究样本 parquet 路径")
    parser.add_argument("--research-config", default="", help="研究配置文件路径，默认 research_config.json")
    return parser.parse_args()


def load_trade_template_config(path: Path | None) -> TradeTemplateConfig:
    research_cfg = load_json(path or DEFAULT_RESEARCH_CONFIG_FILE)
    scan_cfg = load_json(SCAN_CONFIG_FILE)
    official_cfg = deep_merge(DEFAULT_OFFICIAL_D0_LOGIC, scan_cfg.get("official_d0_logic_v2", {}))
    return TradeTemplateConfig(
        sample_filter=str(research_cfg.get("sample_filter", "hard_pass_or_watch")),
        primary_success_label=str(research_cfg.get("primary_success_label", "success_composite_flag")),
        official_d0_logic_v2=official_cfg,
    )


def derive_config_tag(config_path: Path | None, sample_filter: str) -> str:
    if config_path is not None:
        parts = [part for part in config_path.stem.split(".") if part]
        if parts:
            candidate = parts[-1].strip().lower()
            if candidate and candidate not in {"json", "research_config"}:
                return candidate
    alias = {
        "hard_pass_or_watch": "hpow",
        "candidate_or_watch": "cow",
        "hard_pass_only": "hardonly",
        "all": "all",
    }
    return alias.get(sample_filter, sample_filter.replace(" ", "_").lower())


def apply_sample_filter(df: pd.DataFrame, name: str) -> pd.DataFrame:
    if name == "all":
        return df.copy()
    if name == "hard_pass_only":
        return df[df["hard_pass"] == "是"].copy()
    if name == "candidate_or_watch":
        return df[df["research_bucket"].isin(["候选", "观察"])].copy()
    return df[df["research_bucket"].isin(["入围", "候选", "观察"])].copy()


def normalize_flag(series: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    return series.astype(str).eq("是")


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def ensure_required_columns(df: pd.DataFrame, sample_filter: str) -> None:
    required = {
        "股票代码",
        "股票名称",
        "setup_date",
        "d1_date",
        "d2_date",
        "research_bucket",
        "hard_pass",
        "br20",
        "d0_turnover",
        "d0_turnover_f",
        "d0_range_vol",
        "d0_limit_up_space_pct",
        "d0_close",
        "breakout_price",
        "support_price_1",
        "mid_price",
        "target_price_1",
        "target_price_2",
        "d1_open",
        "d1_high",
        "d1_low",
        "d1_close",
        "d1_gap_pct",
        "d1_close_ret_pct",
        "d2_open",
        "d2_high",
        "d2_low",
        "d2_close",
        "d2_open_ret_pct",
        "d2_high_ret_pct",
        "d2_close_ret_pct",
        "d2_target1_hit",
        "d1_stable_flag",
        "d2_sellable_flag",
        "success_composite_flag",
    }
    if sample_filter == "hard_pass_only":
        required.add("hard_pass")
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"P13 缺少必要列: {', '.join(missing)}")


def compute_official_d0_columns(df: pd.DataFrame, cfg: TradeTemplateConfig) -> pd.DataFrame:
    thresholds = cfg.official_d0_logic_v2.get("thresholds", {})
    work = df.copy()
    work["cond_br20_strong"] = to_numeric(work["br20"]) >= float(thresholds.get("br20_min", 1.02))

    limit_up_space = to_numeric(work["d0_limit_up_space_pct"])
    work["cond_limit_up_space_good"] = (
        limit_up_space >= float(thresholds.get("limit_up_space_min", 0.0))
    ) & (
        limit_up_space <= float(thresholds.get("limit_up_space_max", 5.60))
    )

    work["cond_turnover_good"] = to_numeric(work["d0_turnover"]) >= float(thresholds.get("turnover_min", 9.67))
    work["cond_turnover_f_good"] = to_numeric(work["d0_turnover_f"]) >= float(thresholds.get("turnover_f_min", 15.126))

    range_vol = to_numeric(work["d0_range_vol"])
    work["cond_range_vol_good"] = (
        range_vol >= float(thresholds.get("range_vol_min", 0.666))
    ) & (
        range_vol <= float(thresholds.get("range_vol_max", 1.05))
    )

    condition_columns = [
        "cond_br20_strong",
        "cond_limit_up_space_good",
        "cond_turnover_good",
        "cond_turnover_f_good",
        "cond_range_vol_good",
    ]
    work[condition_columns] = work[condition_columns].fillna(False)
    work["official_d0_score"] = work[condition_columns].sum(axis=1).astype(int)
    work["official_d0_tier"] = ""
    work.loc[work["official_d0_score"] == 5, "official_d0_tier"] = "A"
    work.loc[work["official_d0_score"] == 4, "official_d0_tier"] = "B"
    work.loc[work["official_d0_score"] == 3, "official_d0_tier"] = "C"
    return work


def classify_path(df: pd.DataFrame) -> pd.Series:
    d1_stable = normalize_flag(df["d1_stable_flag"])
    d2_sellable = normalize_flag(df["d2_sellable_flag"])
    path = pd.Series(PATH_CLASS_FAILED, index=df.index, dtype="object")
    path.loc[d1_stable & d2_sellable] = PATH_CLASS_SMOOTH
    path.loc[(~d1_stable) & d2_sellable] = PATH_CLASS_DELAYED
    path.loc[d1_stable & (~d2_sellable)] = PATH_CLASS_D1_ONLY
    return path


def compute_pool_mask(df: pd.DataFrame, pool_name: str) -> pd.Series:
    if pool_name == "pool_score_ge_3":
        return df["official_d0_score"] >= 3
    if pool_name == "pool_score_ge_4":
        return df["official_d0_score"] >= 4
    if pool_name == "pool_br20_limit_up_space":
        return df["cond_br20_strong"] & df["cond_limit_up_space_good"]
    if pool_name == "pool_br20_limit_up_space_turnover":
        return df["cond_br20_strong"] & df["cond_limit_up_space_good"] & df["cond_turnover_good"]
    if pool_name == "pool_candidate_score_ge_3":
        return (df["research_bucket"] == "候选") & (df["official_d0_score"] >= 3)
    if pool_name == "pool_watch_score_ge_3":
        return (df["research_bucket"] == "观察") & (df["official_d0_score"] >= 3)
    raise KeyError(f"未知 pool: {pool_name}")


def evaluate_entry_template(df: pd.DataFrame, entry_template: str) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    result["entry_triggered_flag"] = False
    result["executable_flag"] = False
    result["entry_price"] = pd.NA
    result["entry_note"] = ""

    d1_open = to_numeric(df["d1_open"])
    d1_high = to_numeric(df["d1_high"])
    d1_low = to_numeric(df["d1_low"])
    d1_gap_pct = to_numeric(df["d1_gap_pct"])
    breakout_price = to_numeric(df["breakout_price"])
    mid_price = to_numeric(df["mid_price"])
    support_price_1 = to_numeric(df["support_price_1"])

    if entry_template == "d1_open_buy":
        mask = d1_open.notna()
        result.loc[mask, "entry_triggered_flag"] = True
        result.loc[mask, "executable_flag"] = True
        result.loc[mask, "entry_price"] = d1_open[mask]
        result.loc[~mask, "entry_note"] = "missing_d1_open"
        return result

    if entry_template == "d1_breakout_buy":
        mask = breakout_price.notna() & d1_high.notna() & (d1_high >= breakout_price)
        result.loc[mask, "entry_triggered_flag"] = True
        result.loc[mask, "executable_flag"] = True
        result.loc[mask, "entry_price"] = breakout_price[mask]
        result.loc[breakout_price.isna(), "entry_note"] = "missing_breakout_price"
        result.loc[result["entry_note"].eq("") & ~mask, "entry_note"] = "not_triggered"
        return result

    if entry_template == "d1_mid_pullback_buy":
        mask = mid_price.notna() & d1_low.notna() & d1_high.notna() & (d1_low <= mid_price) & (mid_price <= d1_high)
        result.loc[mask, "entry_triggered_flag"] = True
        result.loc[mask, "executable_flag"] = True
        result.loc[mask, "entry_price"] = mid_price[mask]
        result.loc[mid_price.isna(), "entry_note"] = "missing_mid_price"
        result.loc[result["entry_note"].eq("") & ~mask, "entry_note"] = "not_triggered"
        return result

    if entry_template == "d1_support1_buy":
        mask = support_price_1.notna() & d1_low.notna() & d1_high.notna() & (d1_low <= support_price_1) & (support_price_1 <= d1_high)
        result.loc[mask, "entry_triggered_flag"] = True
        result.loc[mask, "executable_flag"] = True
        result.loc[mask, "entry_price"] = support_price_1[mask]
        result.loc[support_price_1.isna(), "entry_note"] = "missing_support1_price"
        result.loc[result["entry_note"].eq("") & ~mask, "entry_note"] = "not_triggered"
        return result

    if entry_template == "d1_open_buy_gap_le_5":
        mask = d1_open.notna() & d1_gap_pct.notna() & (d1_gap_pct <= 5.0)
        result.loc[mask, "entry_triggered_flag"] = True
        result.loc[mask, "executable_flag"] = True
        result.loc[mask, "entry_price"] = d1_open[mask]
        result.loc[d1_gap_pct.isna(), "entry_note"] = "missing_gap_pct"
        result.loc[result["entry_note"].eq("") & ~mask, "entry_note"] = "filtered_out"
        return result

    raise KeyError(f"未知 entry_template: {entry_template}")


def evaluate_exit_template(df: pd.DataFrame, entry_df: pd.DataFrame, exit_template: str) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    result["exit_price"] = pd.NA
    result["exit_day"] = ""
    result["exit_note"] = ""
    result["template_executable_flag"] = False
    result["tradable_flag"] = False

    entry_executable = entry_df["executable_flag"].fillna(False).astype(bool)

    d2_open = to_numeric(df["d2_open"])
    d2_close = to_numeric(df["d2_close"])
    d2_high = to_numeric(df["d2_high"])
    d2_open_ret_pct = to_numeric(df["d2_open_ret_pct"])
    target_price_1 = to_numeric(df["target_price_1"])
    mid_price = to_numeric(df["mid_price"])

    if exit_template == "d2_open_exit":
        mask = entry_executable & d2_open.notna()
        result.loc[mask, "exit_price"] = d2_open[mask]
        result.loc[mask, "exit_day"] = "D2"
        result.loc[mask, "template_executable_flag"] = True
        result.loc[mask, "tradable_flag"] = True
        result.loc[entry_executable & ~mask, "exit_note"] = "missing_d2_open"
        return result

    if exit_template == "d2_close_exit":
        mask = entry_executable & d2_close.notna()
        result.loc[mask, "exit_price"] = d2_close[mask]
        result.loc[mask, "exit_day"] = "D2"
        result.loc[mask, "template_executable_flag"] = True
        result.loc[mask, "tradable_flag"] = True
        result.loc[entry_executable & ~mask, "exit_note"] = "missing_d2_close"
        return result

    if exit_template == "d2_target1_then_close":
        target_hit = entry_executable & d2_high.notna() & target_price_1.notna() & (d2_high >= target_price_1)
        fallback = entry_executable & ~target_hit & d2_close.notna()
        result.loc[target_hit, "exit_price"] = target_price_1[target_hit]
        result.loc[target_hit, "exit_day"] = "D2"
        result.loc[target_hit, "exit_note"] = "target1_hit"
        result.loc[target_hit, "template_executable_flag"] = True
        result.loc[target_hit, "tradable_flag"] = True
        result.loc[fallback, "exit_price"] = d2_close[fallback]
        result.loc[fallback, "exit_day"] = "D2"
        result.loc[fallback, "exit_note"] = "fallback_d2_close"
        result.loc[fallback, "template_executable_flag"] = True
        result.loc[fallback, "tradable_flag"] = True
        result.loc[entry_executable & ~(target_hit | fallback), "exit_note"] = "missing_d2_close_or_target1"
        return result

    if exit_template == "d2_target1_then_open":
        target_hit = entry_executable & d2_high.notna() & target_price_1.notna() & (d2_high >= target_price_1)
        fallback = entry_executable & ~target_hit & d2_open.notna()
        result.loc[target_hit, "exit_price"] = target_price_1[target_hit]
        result.loc[target_hit, "exit_day"] = "D2"
        result.loc[target_hit, "exit_note"] = "target1_hit"
        result.loc[target_hit, "template_executable_flag"] = True
        result.loc[target_hit, "tradable_flag"] = True
        result.loc[fallback, "exit_price"] = d2_open[fallback]
        result.loc[fallback, "exit_day"] = "D2"
        result.loc[fallback, "exit_note"] = "fallback_d2_open"
        result.loc[fallback, "template_executable_flag"] = True
        result.loc[fallback, "tradable_flag"] = True
        result.loc[entry_executable & ~(target_hit | fallback), "exit_note"] = "missing_d2_open_or_target1"
        return result

    if exit_template == "d2_fail_fast_open_else_close":
        fail_fast = entry_executable & d2_open.notna() & ((mid_price.notna() & (d2_open < mid_price)) | (d2_open_ret_pct < 0))
        fallback = entry_executable & ~fail_fast & d2_close.notna()
        result.loc[fail_fast, "exit_price"] = d2_open[fail_fast]
        result.loc[fail_fast, "exit_day"] = "D2"
        result.loc[fail_fast, "exit_note"] = "d2_fail_fast_open"
        result.loc[fail_fast, "template_executable_flag"] = True
        result.loc[fail_fast, "tradable_flag"] = True
        result.loc[fallback, "exit_price"] = d2_close[fallback]
        result.loc[fallback, "exit_day"] = "D2"
        result.loc[fallback, "exit_note"] = "hold_to_d2_close"
        result.loc[fallback, "template_executable_flag"] = True
        result.loc[fallback, "tradable_flag"] = True
        result.loc[entry_executable & ~(fail_fast | fallback), "exit_note"] = "missing_d2_open_or_d2_close"
        return result

    raise KeyError(f"未知 exit_template: {exit_template}")


def compute_trade_metrics(df: pd.DataFrame, entry_df: pd.DataFrame, exit_df: pd.DataFrame) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)
    result["entry_day"] = "D1"
    result["entry_triggered_flag"] = entry_df["entry_triggered_flag"].fillna(False).astype(bool)
    result["executable_flag"] = exit_df["template_executable_flag"].fillna(False).astype(bool)
    result["tradable_flag"] = exit_df["tradable_flag"].fillna(False).astype(bool)
    result["entry_price"] = to_numeric(entry_df["entry_price"])
    result["exit_price"] = to_numeric(exit_df["exit_price"])
    result["exit_day"] = exit_df["exit_day"].astype(str)
    result["entry_note"] = entry_df["entry_note"].astype(str)
    result["exit_note"] = exit_df["exit_note"].astype(str)

    d0_close = to_numeric(df["d0_close"])
    d1_high = to_numeric(df["d1_high"])
    d1_low = to_numeric(df["d1_low"])
    d2_high = to_numeric(df["d2_high"])
    d2_low = to_numeric(df["d2_low"])

    entry_price = result["entry_price"]
    exit_price = result["exit_price"]

    result["entry_ret_from_d0_close_pct"] = (entry_price / d0_close - 1.0) * 100.0
    result["realized_ret_pct"] = (exit_price / entry_price - 1.0) * 100.0

    max_high = d1_high.copy()
    min_low = d1_low.copy()
    exit_on_d2 = result["exit_day"].eq("D2")
    max_high.loc[exit_on_d2] = pd.concat([d1_high.loc[exit_on_d2], d2_high.loc[exit_on_d2]], axis=1).max(axis=1)
    min_low.loc[exit_on_d2] = pd.concat([d1_low.loc[exit_on_d2], d2_low.loc[exit_on_d2]], axis=1).min(axis=1)

    result["mfe_pct"] = (max_high / entry_price - 1.0) * 100.0
    result["mae_pct"] = (min_low / entry_price - 1.0) * 100.0
    result["d2_fail_fast_open_flag"] = result["exit_note"].eq("d2_fail_fast_open")
    return result


def add_calendar_fields(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    d1_dates = pd.to_datetime(work["d1_date"], errors="coerce")
    work["weekday_d1"] = d1_dates.dt.day_name()
    work["is_friday_entry"] = work["weekday_d1"].eq("Friday")
    return work


def safe_mean(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() == 0:
        return None
    return float(numeric.mean())


def safe_median(series: pd.Series) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().sum() == 0:
        return None
    return float(numeric.median())


def build_summary_row(
    scoped_df: pd.DataFrame,
    evaluated_df: pd.DataFrame,
    pool_name: str,
    entry_template: str,
    exit_template: str,
) -> dict[str, Any]:
    sample_count = len(scoped_df)
    trigger_count = int(evaluated_df["entry_triggered_flag"].fillna(False).sum())
    executable_mask = (
        evaluated_df["executable_flag"].fillna(False).astype(bool)
        & evaluated_df["tradable_flag"].fillna(False).astype(bool)
    )
    executable_df = evaluated_df[executable_mask].copy()
    executable_count = len(executable_df)

    trigger_ratio = trigger_count / sample_count if sample_count else 0.0
    executable_ratio = executable_count / sample_count if sample_count else 0.0
    win_rate = float((to_numeric(executable_df["realized_ret_pct"]) > 0).mean()) if executable_count else 0.0
    target1_hit_rate = float(normalize_flag(executable_df["d2_target1_hit"]).mean()) if executable_count else 0.0
    d2_fail_fast_open_rate = float(executable_df["d2_fail_fast_open_flag"].fillna(False).mean()) if executable_count else 0.0
    friday_entry_count = int(executable_df["is_friday_entry"].fillna(False).sum()) if executable_count else 0
    friday_entry_ratio = friday_entry_count / executable_count if executable_count else 0.0
    delayed_payoff_ratio = float(executable_df["path_class"].eq(PATH_CLASS_DELAYED).mean()) if executable_count else 0.0
    smooth_followthrough_ratio = float(executable_df["path_class"].eq(PATH_CLASS_SMOOTH).mean()) if executable_count else 0.0

    notes: list[str] = []
    if trigger_ratio < 0.05:
        notes.append("low_trigger")
    if executable_ratio < 0.05:
        notes.append("low_executable")
    if executable_count < 20:
        notes.append("small_exec_sample")
    if not notes:
        notes.append("baseline_template_ready")

    return {
        "pool_name": pool_name,
        "tradable_flag": True,
        "entry_day": "D1",
        "exit_day": "D2",
        "entry_template": entry_template,
        "exit_template": exit_template,
        "sample_count": sample_count,
        "trigger_count": trigger_count,
        "trigger_ratio": round(trigger_ratio, 6),
        "executable_count": executable_count,
        "executable_ratio": round(executable_ratio, 6),
        "avg_entry_ret_from_d0_close_pct": round(safe_mean(executable_df["entry_ret_from_d0_close_pct"]), 6) if executable_count else None,
        "avg_realized_ret_pct": round(safe_mean(executable_df["realized_ret_pct"]), 6) if executable_count else None,
        "median_realized_ret_pct": round(safe_median(executable_df["realized_ret_pct"]), 6) if executable_count else None,
        "win_rate": round(win_rate, 6),
        "avg_mfe_pct": round(safe_mean(executable_df["mfe_pct"]), 6) if executable_count else None,
        "avg_mae_pct": round(safe_mean(executable_df["mae_pct"]), 6) if executable_count else None,
        "avg_d1_close_ret_pct": round(safe_mean(executable_df["d1_close_ret_pct"]), 6) if executable_count else None,
        "avg_d2_high_ret_pct": round(safe_mean(executable_df["d2_high_ret_pct"]), 6) if executable_count else None,
        "avg_d2_close_ret_pct": round(safe_mean(executable_df["d2_close_ret_pct"]), 6) if executable_count else None,
        "target1_hit_rate": round(target1_hit_rate, 6),
        "d2_fail_fast_open_rate": round(d2_fail_fast_open_rate, 6),
        "friday_entry_count": friday_entry_count,
        "friday_entry_ratio": round(friday_entry_ratio, 6),
        "delayed_payoff_ratio": round(delayed_payoff_ratio, 6),
        "smooth_followthrough_ratio": round(smooth_followthrough_ratio, 6),
        "note": "; ".join(notes),
    }


def weighted_average(df: pd.DataFrame, value_col: str, weight_col: str) -> float | None:
    sub = df[[value_col, weight_col]].copy()
    sub[value_col] = pd.to_numeric(sub[value_col], errors="coerce")
    sub[weight_col] = pd.to_numeric(sub[weight_col], errors="coerce")
    sub = sub[sub[value_col].notna() & sub[weight_col].notna() & (sub[weight_col] > 0)]
    if sub.empty:
        return None
    return float((sub[value_col] * sub[weight_col]).sum() / sub[weight_col].sum())


def validate_t1_outputs(summary_df: pd.DataFrame, detail_df: pd.DataFrame) -> None:
    if not summary_df.empty:
        if summary_df["exit_template"].isin(["d1_close_exit", "d1_fail_fast_else_d2_close"]).any():
            raise ValueError("P13 ä¸» summary ä»åŒ…å« D1 å½“æ—¥å–å‡ºæ¨¡æ¿")
        if not summary_df["tradable_flag"].fillna(False).all():
            raise ValueError("P13 ä¸» summary å­˜åœ¨ tradable_flag=false çš„è¡Œ")
        if not summary_df["entry_day"].eq("D1").all():
            raise ValueError("P13 ä¸» summary å­˜åœ¨éž D1 entry_day")
        if not summary_df["exit_day"].eq("D2").all():
            raise ValueError("P13 ä¸» summary å­˜åœ¨éž D2 exit_day")
    if not detail_df.empty:
        tradable_detail = detail_df[detail_df["tradable_flag"].fillna(False).astype(bool)].copy()
        if tradable_detail["exit_template"].isin(["d1_close_exit", "d1_fail_fast_else_d2_close"]).any():
            raise ValueError("P13 ä¸» detail ä»åŒ…å« D1 å½“æ—¥å–å‡ºæ¨¡æ¿")
        if not tradable_detail["entry_day"].eq("D1").all():
            raise ValueError("P13 ä¸» detail å­˜åœ¨éž D1 entry_day")
        if not tradable_detail["exit_day"].eq("D2").all():
            raise ValueError("P13 ä¸» detail å­˜åœ¨éž D2 exit_day")


def build_report(summary_df: pd.DataFrame, detail_df: pd.DataFrame, cfg: TradeTemplateConfig, dataset_name: str) -> str:
    summary_nonzero = summary_df[
        summary_df["tradable_flag"].fillna(False).astype(bool)
        & summary_df["entry_day"].eq("D1")
        & summary_df["exit_day"].eq("D2")
        & (summary_df["executable_count"] > 0)
    ].copy()

    pool_candidates = summary_nonzero.copy()
    pool_candidates["_pool_score"] = (
        pool_candidates["executable_ratio"].fillna(0.0)
        * pool_candidates["win_rate"].fillna(0.0)
        * pool_candidates["avg_realized_ret_pct"].clip(lower=0).fillna(0.0)
    )
    best_pool_row = pool_candidates.sort_values(
        ["_pool_score", "executable_count", "avg_realized_ret_pct"],
        ascending=[False, False, False],
    ).iloc[0] if not pool_candidates.empty else None

    entry_view = summary_nonzero.drop_duplicates(subset=["pool_name", "entry_template"]).copy()
    entry_group = entry_view.groupby("entry_template", dropna=False).agg(
        sample_count=("sample_count", "sum"),
        trigger_count=("trigger_count", "sum"),
        executable_count=("executable_count", "sum"),
    ).reset_index()
    if not entry_group.empty:
        entry_group["trigger_ratio"] = entry_group["trigger_count"] / entry_group["sample_count"].where(entry_group["sample_count"] > 0, 1)
        entry_group["executable_ratio"] = entry_group["executable_count"] / entry_group["sample_count"].where(entry_group["sample_count"] > 0, 1)
        best_entry_row = entry_group.sort_values(
            ["executable_ratio", "trigger_ratio", "executable_count"],
            ascending=[False, False, False],
        ).iloc[0]
    else:
        best_entry_row = None

    exit_group = summary_nonzero.groupby("exit_template", dropna=False).apply(
        lambda g: pd.Series({
            "weighted_avg_realized_ret_pct": weighted_average(g, "avg_realized_ret_pct", "executable_count"),
            "weighted_win_rate": weighted_average(g, "win_rate", "executable_count"),
            "total_exec": g["executable_count"].sum(),
        })
    ).reset_index()
    if not exit_group.empty:
        best_exit_row = exit_group.sort_values(
            ["weighted_avg_realized_ret_pct", "weighted_win_rate", "total_exec"],
            ascending=[False, False, False],
        ).iloc[0]
    else:
        best_exit_row = None

    fragile_rows = summary_nonzero[
        (summary_nonzero["avg_realized_ret_pct"].fillna(0.0) > 0)
        & (
            (summary_nonzero["trigger_ratio"] < 0.05)
            | (summary_nonzero["executable_ratio"] < 0.05)
        )
    ].sort_values(["avg_realized_ret_pct", "executable_ratio"], ascending=[False, True]).head(5)

    executable_detail = detail_df[
        detail_df["tradable_flag"].fillna(False).astype(bool)
        & detail_df["executable_flag"].fillna(False).astype(bool)
        & detail_df["entry_day"].eq("D1")
        & detail_df["exit_day"].eq("D2")
    ].copy()
    delayed_ratio = float(executable_detail["path_class"].eq(PATH_CLASS_DELAYED).mean()) if not executable_detail.empty else 0.0
    delayed_detail = executable_detail[executable_detail["path_class"].eq(PATH_CLASS_DELAYED)].copy()
    if not delayed_detail.empty:
        delayed_templates = delayed_detail.groupby(["entry_template", "exit_template"], dropna=False).agg(
            executable_count=("股票代码", "count"),
            avg_realized_ret_pct=("realized_ret_pct", "mean"),
            win_rate=("realized_ret_pct", lambda s: (pd.to_numeric(s, errors="coerce") > 0).mean()),
        ).reset_index().sort_values(
            ["avg_realized_ret_pct", "win_rate", "executable_count"],
            ascending=[False, False, False],
        )
        best_delayed_row = delayed_templates.iloc[0]
    else:
        delayed_templates = pd.DataFrame()
        best_delayed_row = None

    friday_stats = executable_detail.groupby("is_friday_entry", dropna=False).agg(
        count=("股票代码", "count"),
        avg_realized_ret_pct=("realized_ret_pct", "mean"),
        win_rate=("realized_ret_pct", lambda s: (pd.to_numeric(s, errors="coerce") > 0).mean()),
    ).reset_index()

    answer_lines = []
    if best_pool_row is not None:
        answer_lines.append(
            f"- 更适合继续研究的 D0 pool 暂时是 `{best_pool_row['pool_name']}`，对应 entry/exit 为 `{best_pool_row['entry_template']} -> {best_pool_row['exit_template']}`，executable_ratio=`{best_pool_row['executable_ratio']:.4f}`，avg_realized_ret_pct=`{best_pool_row['avg_realized_ret_pct']:.4f}`。"
        )
    if best_entry_row is not None:
        answer_lines.append(
            f"- 最容易触发且可执行的 entry_template 是 `{best_entry_row['entry_template']}`，trigger_ratio=`{best_entry_row['trigger_ratio']:.4f}`，executable_ratio=`{best_entry_row['executable_ratio']:.4f}`。"
        )
    if best_exit_row is not None and pd.notna(best_exit_row["weighted_avg_realized_ret_pct"]):
        answer_lines.append(
            f"- realized_ret 表现最好的 exit_template 是 `{best_exit_row['exit_template']}`，加权 avg_realized_ret_pct=`{best_exit_row['weighted_avg_realized_ret_pct']:.4f}`。"
        )
    if not fragile_rows.empty:
        top_fragile = fragile_rows.iloc[0]
        answer_lines.append(
            f"- 需要警惕的高收益低覆盖模板是 `{top_fragile['pool_name']} | {top_fragile['entry_template']} | {top_fragile['exit_template']}`，trigger_ratio=`{top_fragile['trigger_ratio']:.4f}`，executable_ratio=`{top_fragile['executable_ratio']:.4f}`。"
        )
    answer_lines.append(f"- delayed_payoff 在可执行样本中的占比约为 `{delayed_ratio:.4f}`。")
    if best_delayed_row is not None:
        answer_lines.append(
            f"- delayed_payoff 路径下当前更可行的模板是 `{best_delayed_row['entry_template']} -> {best_delayed_row['exit_template']}`，avg_realized_ret_pct=`{best_delayed_row['avg_realized_ret_pct']:.4f}`，executable_count=`{int(best_delayed_row['executable_count'])}`。"
        )
    if len(friday_stats) >= 2:
        friday_row = friday_stats[friday_stats["is_friday_entry"] == True]
        non_friday_row = friday_stats[friday_stats["is_friday_entry"] == False]
        if not friday_row.empty and not non_friday_row.empty:
            friday = friday_row.iloc[0]
            non_friday = non_friday_row.iloc[0]
            answer_lines.append(
                f"- 周五 entry 的 avg_realized_ret_pct=`{friday['avg_realized_ret_pct']:.4f}`，非周五=`{non_friday['avg_realized_ret_pct']:.4f}`；win_rate 分别为 `{friday['win_rate']:.4f}` / `{non_friday['win_rate']:.4f}`。"
            )
    if not answer_lines:
        answer_lines.append("- 当前样本不足以形成稳定结论。")

    lines = [
        "# P13 D0-D1-D2 交易路径模板验证报告",
        "",
        f"- 数据集: `{dataset_name}`",
        f"- 样本筛选: `{cfg.sample_filter}`",
        f"- 主成功标签: `{cfg.primary_success_label}`",
        "",
        "## 框架说明",
        "",
        "- 本报告聚焦 D0 setup -> D1 entry -> D1/D2 exit 的真实可执行路径，而不是继续比较单纯的 D0 条件强弱。",
        "- official_d0_score 在 P13 内按与 P12 / p7 相同的 official_d0_logic_v2 阈值重算。",
        "- 本次不做噪声排除，只先建立基础交易路径框架。",
        "",
        "## Pool × Template Top",
        "",
        summary_nonzero.sort_values(
            ["avg_realized_ret_pct", "executable_ratio", "win_rate"],
            ascending=[False, False, False],
        ).head(12).to_markdown(index=False) if not summary_nonzero.empty else "无可执行模板结果。",
        "",
        "## Entry 可执行性",
        "",
        entry_group.to_markdown(index=False) if not entry_group.empty else "无 entry 聚合结果。",
        "",
        "## Exit 兑现质量",
        "",
        exit_group.to_markdown(index=False) if not exit_group.empty else "无 exit 聚合结果。",
        "",
        "## Delayed Payoff 路径",
        "",
        delayed_templates.head(10).to_markdown(index=False) if not delayed_templates.empty else "无 delayed payoff 可执行结果。",
        "",
        "## Friday Entry 基础统计",
        "",
        friday_stats.to_markdown(index=False) if not friday_stats.empty else "无周五 entry 对比结果。",
        "",
        "## 初步结论",
        "",
    ]
    lines[8] = "- æœ¬æŠ¥å‘Šèšç„¦ D0 setup -> D1 entry -> D2 exit çš„çœŸå®žå¯æ‰§è¡Œè·¯å¾„ï¼Œè€Œä¸æ˜¯ç»§ç»­æ¯”è¾ƒå•çº¯çš„ D0 æ¡ä»¶å¼ºå¼±ã€‚"
    lines = lines[:10] + [
        "- æœ¬æŠ¥å‘Šä¸»ç»“æžœå·²æŒ‰ A è‚¡çŽ°è´§ T+1 çº¦æŸè¿‡æ»¤ã€‚",
        "- æ‰€æœ‰ä¸»ç»“æžœå‡ä¸º D1 ä¹°å…¥ã€D2 å–å‡ºçš„å¯æ‰§è¡Œæ¨¡æ¿ã€‚",
        "- D1 å½“æ—¥å–å‡ºæ¨¡æ¿ä¸çº³å…¥ä¸»ç»“è®ºã€‚",
    ] + lines[10:]
    lines.extend(answer_lines)
    lines.extend(["", "## TODO", ""])
    lines.extend([f"- {line}" for line in TODO_LINES])
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        raise FileNotFoundError(f"未找到研究样本文件: {dataset_path}")

    config_path = Path(args.research_config) if args.research_config else None
    cfg = load_trade_template_config(config_path)

    df = pd.read_parquet(dataset_path)
    ensure_required_columns(df, cfg.sample_filter)
    filtered = apply_sample_filter(df, cfg.sample_filter)
    if filtered.empty:
        raise ValueError("样本过滤后为空，无法做 P13 路径验证")

    work = compute_official_d0_columns(filtered, cfg)
    work = add_calendar_fields(work)
    work["path_class"] = classify_path(work)

    summary_rows: list[dict[str, Any]] = []
    detail_frames: list[pd.DataFrame] = []

    base_detail_columns = [
        "股票代码",
        "股票名称",
        "setup_date",
        "research_bucket",
        "official_d0_score",
        "official_d0_tier",
        "path_class",
        "d1_gap_pct",
        "d1_close_ret_pct",
        "d2_open_ret_pct",
        "d2_high_ret_pct",
        "d2_close_ret_pct",
        "d2_target1_hit",
        "weekday_d1",
        "is_friday_entry",
    ]

    for pool_name in POOL_NAMES:
        pool_df = work[compute_pool_mask(work, pool_name)].copy()
        if pool_df.empty:
            continue

        for entry_template in ENTRY_TEMPLATES:
            entry_df = evaluate_entry_template(pool_df, entry_template)
            for exit_template in EXIT_TEMPLATES:
                exit_df = evaluate_exit_template(pool_df, entry_df, exit_template)
                metrics_df = compute_trade_metrics(pool_df, entry_df, exit_df)
                evaluated_df = pd.concat([pool_df.reset_index(drop=True), metrics_df.reset_index(drop=True)], axis=1)
                summary_rows.append(build_summary_row(pool_df, evaluated_df, pool_name, entry_template, exit_template))

                detail_mask = evaluated_df["entry_triggered_flag"].fillna(False).astype(bool) | evaluated_df["executable_flag"].fillna(False).astype(bool)
                detail_df = evaluated_df.loc[detail_mask, base_detail_columns + [
                    "tradable_flag",
                    "entry_day",
                    "exit_day",
                    "entry_triggered_flag",
                    "executable_flag",
                    "entry_price",
                    "exit_price",
                    "entry_ret_from_d0_close_pct",
                    "realized_ret_pct",
                    "mfe_pct",
                    "mae_pct",
                ]].copy()
                if detail_df.empty:
                    continue
                detail_df["pool_name"] = pool_name
                detail_df["entry_template"] = entry_template
                detail_df["exit_template"] = exit_template
                detail_frames.append(detail_df)

    summary_df = pd.DataFrame(summary_rows)
    if detail_frames:
        detail_df = pd.concat(detail_frames, ignore_index=True)
    else:
        detail_df = pd.DataFrame()

    ordered_detail_columns = [
        "股票代码",
        "股票名称",
        "setup_date",
        "research_bucket",
        "official_d0_score",
        "official_d0_tier",
        "pool_name",
        "path_class",
        "tradable_flag",
        "entry_day",
        "exit_day",
        "entry_template",
        "exit_template",
        "entry_triggered_flag",
        "executable_flag",
        "entry_price",
        "exit_price",
        "entry_ret_from_d0_close_pct",
        "realized_ret_pct",
        "mfe_pct",
        "mae_pct",
        "d1_gap_pct",
        "d1_close_ret_pct",
        "d2_open_ret_pct",
        "d2_high_ret_pct",
        "d2_close_ret_pct",
        "weekday_d1",
        "is_friday_entry",
    ]
    if detail_df.empty:
        detail_df = pd.DataFrame(columns=ordered_detail_columns)
    else:
        detail_df = detail_df[ordered_detail_columns]

    validate_t1_outputs(summary_df, detail_df)

    out_dir = dataset_path.parent
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    config_tag = derive_config_tag(config_path, cfg.sample_filter)
    summary_file = out_dir / f"p13_trade_template_summary_{config_tag}_{stamp}.csv"
    detail_file = out_dir / f"p13_trade_template_detail_{config_tag}_{stamp}.csv"
    report_file = out_dir / f"p13_trade_template_report_{config_tag}_{stamp}.md"

    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")
    detail_df.to_csv(detail_file, index=False, encoding="utf-8-sig")
    report_file.write_text(build_report(summary_df, detail_df, cfg, dataset_path.name), encoding="utf-8")

    print(f"P13 summary: {summary_file}")
    print(f"P13 detail: {detail_file}")
    print(f"P13 report: {report_file}")


if __name__ == "__main__":
    main()
