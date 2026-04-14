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

DEFAULT_PATTERN_DISCOVERY = {
    "shortlist": {
        "min_family_sample_count": 3000,
        "min_executable_count": 150,
        "min_executable_ratio": 0.35,
        "min_avg_realized_ret_pct": 0.20,
        "min_win_rate": 0.45,
        "max_templates_per_family": 3,
    },
    "validation": {
        "min_actual_to_expected_ratio": 0.45,
        "min_return_retention_ratio": 0.35,
    },
    "pattern_thresholds": {
        "strong_br20_min": 1.06,
        "moderate_br20_min": 0.99,
        "tight_space_max": 2.0,
        "hot_turnover_f_min": 22.0,
        "orderly_range_max": 0.95,
        "expansive_range_max": 1.20,
    },
}

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
    "TODO: add event/noise suspect flags after the pattern families are stabilized.",
    "TODO: rerun the survivors with robustness filters for extreme gap and extreme volume samples.",
]

FAMILY_DESCRIPTIONS = {
    "family_hardpass_high_score": "Hard-pass high-score continuation family; strongest D0 quality bucket.",
    "family_orderly_breakout": "Orderly breakout family; strong trend, good space, confirmed turnover, not overly crowded.",
    "family_crowded_momentum": "Crowded momentum family; strong trend close to limit-up with elevated turnover_f.",
    "family_repair_pullback": "Repair / pullback family; moderate trend, still in good space, seeks delayed or pullback entries.",
    "family_candidate_continuation": "Candidate continuation family; candidate bucket with score>=3 and strong continuation structure.",
    "family_watch_repair": "Watch repair family; watch bucket with score>=3, more likely to require path repair.",
}


@dataclass
class PatternDiscoveryConfig:
    sample_filter: str
    primary_success_label: str
    official_d0_logic_v2: dict[str, Any]
    min_family_sample_count: int
    min_executable_count: int
    min_executable_ratio: float
    min_avg_realized_ret_pct: float
    min_win_rate: float
    max_templates_per_family: int
    min_actual_to_expected_ratio: float
    min_return_retention_ratio: float
    pattern_thresholds: dict[str, float]


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
    parser = argparse.ArgumentParser(
        description="P14: discover interpretable D0 pattern families on a long library and validate them on shorter libraries"
    )
    parser.add_argument("--dataset", "--discovery-dataset", dest="discovery_dataset", required=True, help="Long-window P9 parquet path used as the discovery library")
    parser.add_argument("--validation-datasets", default="", help="Optional comma-separated P9 parquet paths used as validation libraries")
    parser.add_argument("--research-config", default="", help="Research config path, defaults to research_config.json")
    parser.add_argument("--output-dir", default="", help="Optional output directory, defaults to the discovery dataset directory")
    return parser.parse_args()


def load_pattern_discovery_config(path: Path | None) -> PatternDiscoveryConfig:
    research_cfg = load_json(path or DEFAULT_RESEARCH_CONFIG_FILE)
    scan_cfg = load_json(SCAN_CONFIG_FILE)
    pattern_cfg = deep_merge(DEFAULT_PATTERN_DISCOVERY, research_cfg.get("pattern_family_discovery", {}))
    official_cfg = deep_merge(DEFAULT_OFFICIAL_D0_LOGIC, scan_cfg.get("official_d0_logic_v2", {}))
    shortlist_cfg = pattern_cfg.get("shortlist", {})
    validation_cfg = pattern_cfg.get("validation", {})
    threshold_cfg = pattern_cfg.get("pattern_thresholds", {})
    return PatternDiscoveryConfig(
        sample_filter=str(research_cfg.get("sample_filter", "hard_pass_or_watch")),
        primary_success_label=str(research_cfg.get("primary_success_label", "success_composite_flag")),
        official_d0_logic_v2=official_cfg,
        min_family_sample_count=int(shortlist_cfg.get("min_family_sample_count", 3000)),
        min_executable_count=int(shortlist_cfg.get("min_executable_count", 150)),
        min_executable_ratio=float(shortlist_cfg.get("min_executable_ratio", 0.35)),
        min_avg_realized_ret_pct=float(shortlist_cfg.get("min_avg_realized_ret_pct", 0.20)),
        min_win_rate=float(shortlist_cfg.get("min_win_rate", 0.45)),
        max_templates_per_family=int(shortlist_cfg.get("max_templates_per_family", 3)),
        min_actual_to_expected_ratio=float(validation_cfg.get("min_actual_to_expected_ratio", 0.45)),
        min_return_retention_ratio=float(validation_cfg.get("min_return_retention_ratio", 0.35)),
        pattern_thresholds={str(k): float(v) for k, v in threshold_cfg.items()},
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


def parse_validation_dataset_paths(raw: str) -> list[Path]:
    if not raw.strip():
        return []
    parts = [part.strip() for part in raw.replace(";", ",").split(",") if part.strip()]
    return [Path(part) for part in parts]


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


def rounded(value: float | None) -> float | None:
    return round(value, 6) if value is not None else None


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
        raise ValueError(f"P14 missing required columns: {', '.join(missing)}")


def compute_official_d0_columns(df: pd.DataFrame, cfg: PatternDiscoveryConfig) -> pd.DataFrame:
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


def add_calendar_fields(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    d1_dates = pd.to_datetime(work["d1_date"], errors="coerce")
    work["weekday_d1"] = d1_dates.dt.day_name()
    work["is_friday_entry"] = work["weekday_d1"].eq("Friday")
    return work


def classify_path(df: pd.DataFrame) -> pd.Series:
    d1_stable = normalize_flag(df["d1_stable_flag"])
    d2_sellable = normalize_flag(df["d2_sellable_flag"])
    path = pd.Series(PATH_CLASS_FAILED, index=df.index, dtype="object")
    path.loc[d1_stable & d2_sellable] = PATH_CLASS_SMOOTH
    path.loc[(~d1_stable) & d2_sellable] = PATH_CLASS_DELAYED
    path.loc[d1_stable & (~d2_sellable)] = PATH_CLASS_D1_ONLY
    return path


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

    raise KeyError(f"Unknown entry_template: {entry_template}")


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

    raise KeyError(f"Unknown exit_template: {exit_template}")


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


def build_pattern_masks(df: pd.DataFrame, cfg: PatternDiscoveryConfig) -> dict[str, pd.Series]:
    t = cfg.pattern_thresholds
    br20 = to_numeric(df["br20"])
    limit_up_space = to_numeric(df["d0_limit_up_space_pct"])
    turnover_f = to_numeric(df["d0_turnover_f"])
    range_vol = to_numeric(df["d0_range_vol"])

    hard_pass = df["hard_pass"].astype(str).eq("是")
    candidate = df["research_bucket"].astype(str).eq("候选")
    watch = df["research_bucket"].astype(str).eq("观察")
    turnover_f_min = float(cfg.official_d0_logic_v2["thresholds"].get("turnover_f_min", 15.126))
    range_vol_min = float(cfg.official_d0_logic_v2["thresholds"].get("range_vol_min", 0.666))

    families: dict[str, pd.Series] = {}

    families["family_hardpass_high_score"] = (
        hard_pass
        & (df["official_d0_score"] >= 4)
        & (br20 >= t["strong_br20_min"])
        & df["cond_limit_up_space_good"]
    )

    families["family_orderly_breakout"] = (
        (df["official_d0_score"] >= 3)
        & (br20 >= t["strong_br20_min"])
        & df["cond_limit_up_space_good"]
        & df["cond_turnover_good"]
        & turnover_f.between(turnover_f_min, t["hot_turnover_f_min"], inclusive="left")
        & range_vol.between(range_vol_min, t["orderly_range_max"], inclusive="both")
    )

    families["family_crowded_momentum"] = (
        (df["official_d0_score"] >= 3)
        & (br20 >= t["strong_br20_min"])
        & limit_up_space.between(0.0, t["tight_space_max"], inclusive="both")
        & (turnover_f >= t["hot_turnover_f_min"])
        & range_vol.between(range_vol_min, t["expansive_range_max"], inclusive="both")
    )

    families["family_repair_pullback"] = (
        df["research_bucket"].astype(str).isin(["候选", "观察"])
        & br20.between(t["moderate_br20_min"], t["strong_br20_min"], inclusive="left")
        & df["cond_limit_up_space_good"]
        & df["cond_turnover_good"]
        & range_vol.between(range_vol_min, t["expansive_range_max"], inclusive="both")
    )

    families["family_candidate_continuation"] = (
        candidate
        & (df["official_d0_score"] >= 3)
        & (br20 >= t["strong_br20_min"])
        & df["cond_limit_up_space_good"]
    )

    families["family_watch_repair"] = (
        watch
        & (df["official_d0_score"] >= 3)
        & df["cond_turnover_good"]
        & range_vol.between(range_vol_min, t["expansive_range_max"], inclusive="both")
    )

    return {name: mask.fillna(False).astype(bool) for name, mask in families.items()}


def build_family_summary_rows(df: pd.DataFrame, family_masks: dict[str, pd.Series]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    total_count = len(df)
    for family_name, mask in family_masks.items():
        sub = df[mask].copy()
        sample_count = len(sub)
        sample_ratio = sample_count / total_count if total_count else 0.0
        rows.append({
            "family_name": family_name,
            "family_description": FAMILY_DESCRIPTIONS.get(family_name, ""),
            "sample_count": sample_count,
            "sample_ratio": rounded(sample_ratio),
            "hard_pass_ratio": rounded(float(sub["hard_pass"].astype(str).eq("是").mean())) if sample_count else 0.0,
            "score_ge_4_ratio": rounded(float((sub["official_d0_score"] >= 4).mean())) if sample_count else 0.0,
            "avg_official_d0_score": rounded(safe_mean(sub["official_d0_score"])) if sample_count else None,
            "d1_stable_rate": rounded(float(normalize_flag(sub["d1_stable_flag"]).mean())) if sample_count else 0.0,
            "d2_sellable_rate": rounded(float(normalize_flag(sub["d2_sellable_flag"]).mean())) if sample_count else 0.0,
            "success_composite_rate": rounded(float(normalize_flag(sub["success_composite_flag"]).mean())) if sample_count else 0.0,
            "smooth_followthrough_ratio": rounded(float(sub["path_class"].eq(PATH_CLASS_SMOOTH).mean())) if sample_count else 0.0,
            "delayed_payoff_ratio": rounded(float(sub["path_class"].eq(PATH_CLASS_DELAYED).mean())) if sample_count else 0.0,
            "d1_only_ratio": rounded(float(sub["path_class"].eq(PATH_CLASS_D1_ONLY).mean())) if sample_count else 0.0,
            "failed_path_ratio": rounded(float(sub["path_class"].eq(PATH_CLASS_FAILED).mean())) if sample_count else 0.0,
            "avg_d1_close_ret_pct": rounded(safe_mean(sub["d1_close_ret_pct"])) if sample_count else None,
            "avg_d2_high_ret_pct": rounded(safe_mean(sub["d2_high_ret_pct"])) if sample_count else None,
            "avg_d2_close_ret_pct": rounded(safe_mean(sub["d2_close_ret_pct"])) if sample_count else None,
            "note": "candidate_pattern_family" if sample_count else "empty_family",
        })
    return rows


def build_route_summary_row(
    family_name: str,
    family_df: pd.DataFrame,
    evaluated_df: pd.DataFrame,
    entry_template: str,
    exit_template: str,
    cfg: PatternDiscoveryConfig,
) -> dict[str, Any]:
    sample_count = len(family_df)
    trigger_count = int(evaluated_df["entry_triggered_flag"].fillna(False).sum())
    executable_mask = (
        evaluated_df["executable_flag"].fillna(False).astype(bool)
        & evaluated_df["tradable_flag"].fillna(False).astype(bool)
    )
    executable_df = evaluated_df[executable_mask].copy()
    executable_count = len(executable_df)

    trigger_ratio = trigger_count / sample_count if sample_count else 0.0
    executable_ratio = executable_count / sample_count if sample_count else 0.0
    avg_realized = safe_mean(executable_df["realized_ret_pct"]) if executable_count else None
    win_rate = float((to_numeric(executable_df["realized_ret_pct"]) > 0).mean()) if executable_count else 0.0
    delayed_ratio = float(executable_df["path_class"].eq(PATH_CLASS_DELAYED).mean()) if executable_count else 0.0
    smooth_ratio = float(executable_df["path_class"].eq(PATH_CLASS_SMOOTH).mean()) if executable_count else 0.0
    friday_ratio = float(executable_df["is_friday_entry"].fillna(False).mean()) if executable_count else 0.0

    shortlisted = (
        sample_count >= cfg.min_family_sample_count
        and executable_count >= cfg.min_executable_count
        and executable_ratio >= cfg.min_executable_ratio
        and (avg_realized or 0.0) >= cfg.min_avg_realized_ret_pct
        and win_rate >= cfg.min_win_rate
    )

    reasons: list[str] = []
    if sample_count < cfg.min_family_sample_count:
        reasons.append("family_sample_too_small")
    if executable_count < cfg.min_executable_count:
        reasons.append("exec_count_too_small")
    if executable_ratio < cfg.min_executable_ratio:
        reasons.append("exec_ratio_too_low")
    if (avg_realized or 0.0) < cfg.min_avg_realized_ret_pct:
        reasons.append("avg_ret_too_low")
    if win_rate < cfg.min_win_rate:
        reasons.append("win_rate_too_low")
    if not reasons:
        reasons.append("candidate_shortlist")

    return {
        "family_name": family_name,
        "family_description": FAMILY_DESCRIPTIONS.get(family_name, ""),
        "sample_count": sample_count,
        "entry_template": entry_template,
        "exit_template": exit_template,
        "trigger_count": trigger_count,
        "trigger_ratio": rounded(trigger_ratio),
        "executable_count": executable_count,
        "executable_ratio": rounded(executable_ratio),
        "avg_entry_ret_from_d0_close_pct": rounded(safe_mean(executable_df["entry_ret_from_d0_close_pct"])) if executable_count else None,
        "avg_realized_ret_pct": rounded(avg_realized),
        "median_realized_ret_pct": rounded(safe_median(executable_df["realized_ret_pct"])) if executable_count else None,
        "win_rate": rounded(win_rate),
        "avg_mfe_pct": rounded(safe_mean(executable_df["mfe_pct"])) if executable_count else None,
        "avg_mae_pct": rounded(safe_mean(executable_df["mae_pct"])) if executable_count else None,
        "delayed_payoff_ratio": rounded(delayed_ratio),
        "smooth_followthrough_ratio": rounded(smooth_ratio),
        "friday_entry_ratio": rounded(friday_ratio),
        "shortlist_flag": shortlisted,
        "shortlist_reason": "; ".join(reasons),
    }


def evaluate_routes_for_dataset(df: pd.DataFrame, family_masks: dict[str, pd.Series], cfg: PatternDiscoveryConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    family_summary_df = pd.DataFrame(build_family_summary_rows(df, family_masks))
    route_rows: list[dict[str, Any]] = []
    for family_name, mask in family_masks.items():
        family_df = df[mask].copy()
        if family_df.empty:
            continue
        for entry_template in ENTRY_TEMPLATES:
            entry_df = evaluate_entry_template(family_df, entry_template)
            for exit_template in EXIT_TEMPLATES:
                exit_df = evaluate_exit_template(family_df, entry_df, exit_template)
                metrics_df = compute_trade_metrics(family_df, entry_df, exit_df)
                evaluated_df = pd.concat([family_df.reset_index(drop=True), metrics_df.reset_index(drop=True)], axis=1)
                route_rows.append(build_route_summary_row(family_name, family_df, evaluated_df, entry_template, exit_template, cfg))
    route_summary_df = pd.DataFrame(route_rows)
    if not route_summary_df.empty:
        route_summary_df = route_summary_df.sort_values(
            ["family_name", "avg_realized_ret_pct", "win_rate", "executable_count"],
            ascending=[True, False, False, False],
        ).reset_index(drop=True)
        route_summary_df["family_rank"] = route_summary_df.groupby("family_name").cumcount() + 1
        route_summary_df["shortlist_flag"] = route_summary_df["shortlist_flag"].fillna(False).astype(bool)
        family_cap_mask = route_summary_df["family_rank"] <= cfg.max_templates_per_family
        route_summary_df["shortlist_flag"] = route_summary_df["shortlist_flag"] & family_cap_mask
        route_summary_df.loc[
            ~family_cap_mask & route_summary_df["shortlist_reason"].str.contains("candidate_shortlist", na=False),
            "shortlist_reason",
        ] = "rank_capped"
    return family_summary_df, route_summary_df


def trading_day_count(df: pd.DataFrame) -> int:
    setup_dates = pd.to_datetime(df["setup_date"], errors="coerce").dropna().dt.normalize().drop_duplicates()
    return int(len(setup_dates))


def validation_row(
    dataset_name: str,
    route_row: pd.Series,
    route_eval: pd.Series,
    discovery_days: int,
    validation_days: int,
    cfg: PatternDiscoveryConfig,
) -> dict[str, Any]:
    discovery_exec = int(route_row.get("executable_count", 0) or 0)
    expected_exec = (discovery_exec * validation_days / discovery_days) if discovery_days > 0 else None
    actual_exec = int(route_eval.get("executable_count", 0) or 0)
    actual_to_expected = (actual_exec / expected_exec) if expected_exec and expected_exec > 0 else None
    discovery_avg_ret = float(route_row.get("avg_realized_ret_pct") or 0.0)
    validation_avg_ret = float(route_eval.get("avg_realized_ret_pct") or 0.0)
    same_sign_flag = (discovery_avg_ret > 0 and validation_avg_ret > 0) or (discovery_avg_ret < 0 and validation_avg_ret < 0) or (discovery_avg_ret == 0 and validation_avg_ret == 0)
    return_retention = (validation_avg_ret / discovery_avg_ret) if discovery_avg_ret > 0 else None
    validation_pass = (
        actual_exec > 0
        and same_sign_flag
        and (actual_to_expected is None or actual_to_expected >= cfg.min_actual_to_expected_ratio)
        and (return_retention is None or return_retention >= cfg.min_return_retention_ratio)
    )
    return {
        "validation_dataset": dataset_name,
        "family_name": str(route_row["family_name"]),
        "entry_template": str(route_row["entry_template"]),
        "exit_template": str(route_row["exit_template"]),
        "discovery_executable_count": discovery_exec,
        "validation_executable_count": actual_exec,
        "discovery_setup_days": discovery_days,
        "validation_setup_days": validation_days,
        "expected_executable_count": rounded(expected_exec),
        "actual_to_expected_ratio": rounded(actual_to_expected),
        "discovery_avg_realized_ret_pct": rounded(discovery_avg_ret),
        "validation_avg_realized_ret_pct": rounded(validation_avg_ret),
        "validation_win_rate": rounded(float(route_eval.get("win_rate") or 0.0)),
        "validation_avg_mae_pct": rounded(float(route_eval.get("avg_mae_pct")) if pd.notna(route_eval.get("avg_mae_pct")) else None),
        "validation_delayed_payoff_ratio": rounded(float(route_eval.get("delayed_payoff_ratio") or 0.0)),
        "same_sign_flag": same_sign_flag,
        "return_retention_ratio": rounded(return_retention),
        "validation_pass_flag": validation_pass,
    }


def evaluate_validation_datasets(
    validation_paths: list[Path],
    discovery_routes: pd.DataFrame,
    discovery_days: int,
    cfg: PatternDiscoveryConfig,
) -> pd.DataFrame:
    shortlisted = discovery_routes[discovery_routes["shortlist_flag"].fillna(False).astype(bool)].copy()
    if shortlisted.empty or not validation_paths:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for path in validation_paths:
        if not path.exists():
            raise FileNotFoundError(f"Validation dataset not found: {path}")
        df = pd.read_parquet(path)
        ensure_required_columns(df, cfg.sample_filter)
        filtered = apply_sample_filter(df, cfg.sample_filter)
        if filtered.empty:
            continue
        work = compute_official_d0_columns(filtered, cfg)
        work = add_calendar_fields(work)
        work["path_class"] = classify_path(work)
        family_masks = build_pattern_masks(work, cfg)
        _, route_summary_df = evaluate_routes_for_dataset(work, family_masks, cfg)
        validation_days = trading_day_count(work)
        lookup = route_summary_df.set_index(["family_name", "entry_template", "exit_template"])
        for _, route_row in shortlisted.iterrows():
            key = (str(route_row["family_name"]), str(route_row["entry_template"]), str(route_row["exit_template"]))
            if key not in lookup.index:
                continue
            route_eval = lookup.loc[key]
            rows.append(validation_row(path.name, route_row, route_eval, discovery_days, validation_days, cfg))
    return pd.DataFrame(rows)


def markdown_table(df: pd.DataFrame, columns: list[str], limit: int | None = None) -> str:
    if df.empty:
        return "_No rows._"
    work = df.loc[:, [column for column in columns if column in df.columns]].copy()
    if limit is not None:
        work = work.head(limit)
    return work.to_markdown(index=False)


def build_report(
    discovery_dataset: Path,
    validation_paths: list[Path],
    cfg: PatternDiscoveryConfig,
    family_summary_df: pd.DataFrame,
    route_summary_df: pd.DataFrame,
    validation_df: pd.DataFrame,
) -> str:
    shortlisted = route_summary_df[route_summary_df["shortlist_flag"].fillna(False).astype(bool)].copy()
    top_shortlisted = shortlisted.sort_values(
        ["avg_realized_ret_pct", "win_rate", "executable_count"],
        ascending=[False, False, False],
    )

    report_lines = [
        "# P14 Pattern Family Discovery",
        "",
        "## Run Scope",
        "",
        f"- Discovery dataset: `{discovery_dataset.name}`",
        f"- Sample filter: `{cfg.sample_filter}`",
        f"- Validation datasets: {', '.join(f'`{path.name}`' for path in validation_paths) if validation_paths else 'None'}",
        f"- Discovery families: `{len(family_summary_df)}`",
        f"- Discovery routes: `{len(route_summary_df)}`",
        f"- Shortlisted routes: `{len(shortlisted)}`",
        "",
        "## Why P14 Exists",
        "",
        "- P14 is not trying to find one universal route.",
        "- It treats the long library as a discovery set for a small number of interpretable D0 pattern families.",
        "- It then asks whether the strongest family-level routes still survive on shorter libraries.",
        "",
        "## Pattern Families",
        "",
        markdown_table(
            family_summary_df.sort_values("sample_count", ascending=False),
            [
                "family_name",
                "sample_count",
                "sample_ratio",
                "avg_official_d0_score",
                "hard_pass_ratio",
                "success_composite_rate",
                "smooth_followthrough_ratio",
                "delayed_payoff_ratio",
            ],
        ),
        "",
        "## Discovery Route Shortlist",
        "",
        markdown_table(
            top_shortlisted,
            [
                "family_name",
                "entry_template",
                "exit_template",
                "sample_count",
                "executable_count",
                "executable_ratio",
                "avg_realized_ret_pct",
                "win_rate",
                "avg_mae_pct",
                "delayed_payoff_ratio",
            ],
            limit=20,
        ),
        "",
    ]

    if not validation_df.empty:
        survivors = validation_df[validation_df["validation_pass_flag"].fillna(False).astype(bool)].copy()
        report_lines.extend(
            [
                "## Validation Results",
                "",
                markdown_table(
                    validation_df.sort_values(
                        ["validation_pass_flag", "validation_avg_realized_ret_pct", "actual_to_expected_ratio"],
                        ascending=[False, False, False],
                    ),
                    [
                        "validation_dataset",
                        "family_name",
                        "entry_template",
                        "exit_template",
                        "validation_pass_flag",
                        "validation_executable_count",
                        "expected_executable_count",
                        "actual_to_expected_ratio",
                        "validation_avg_realized_ret_pct",
                        "return_retention_ratio",
                    ],
                    limit=40,
                ),
                "",
                "## Survivor Notes",
                "",
                f"- Validation survivors: `{len(survivors)}`",
                "- A survivor here means: executable count did not collapse too hard, return direction did not flip, and return retention did not fully break.",
                "",
            ]
        )
    else:
        report_lines.extend(
            [
                "## Validation Results",
                "",
                "_No validation datasets were provided, so this run only produced discovery outputs._",
                "",
            ]
        )

    report_lines.extend(
        [
            "## Research Guidance",
            "",
            "- Use the long library to nominate candidate pattern families, not to declare a final production route.",
            "- Treat the shorter libraries as validation windows and watch for route sign flips, sample collapse, or MAE blow-ups.",
            "- If a family only works in one window, keep it exploratory.",
            "",
            "## TODO",
            "",
            *[f"- {line}" for line in TODO_LINES],
            "",
        ]
    )
    return "\n".join(report_lines)


def main() -> None:
    args = parse_args()
    research_config_path = Path(args.research_config) if args.research_config else None
    cfg = load_pattern_discovery_config(research_config_path)
    discovery_dataset = Path(args.discovery_dataset)
    if not discovery_dataset.exists():
        raise FileNotFoundError(f"Discovery dataset not found: {discovery_dataset}")

    validation_paths = parse_validation_dataset_paths(args.validation_datasets)
    output_dir = Path(args.output_dir) if args.output_dir else discovery_dataset.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(discovery_dataset)
    ensure_required_columns(df, cfg.sample_filter)
    filtered = apply_sample_filter(df, cfg.sample_filter)
    if filtered.empty:
        raise RuntimeError(f"No samples left after sample_filter={cfg.sample_filter}")

    work = compute_official_d0_columns(filtered, cfg)
    work = add_calendar_fields(work)
    work["path_class"] = classify_path(work)

    family_masks = build_pattern_masks(work, cfg)
    family_summary_df, route_summary_df = evaluate_routes_for_dataset(work, family_masks, cfg)
    discovery_days = trading_day_count(work)
    validation_df = evaluate_validation_datasets(validation_paths, route_summary_df, discovery_days, cfg)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = derive_config_tag(research_config_path, cfg.sample_filter)
    family_summary_path = output_dir / f"p14_pattern_family_summary_{tag}_{stamp}.csv"
    route_summary_path = output_dir / f"p14_pattern_route_discovery_{tag}_{stamp}.csv"
    validation_path = output_dir / f"p14_pattern_route_validation_{tag}_{stamp}.csv"
    report_path = output_dir / f"p14_pattern_family_report_{tag}_{stamp}.md"

    family_summary_df.to_csv(family_summary_path, index=False, encoding="utf-8-sig")
    route_summary_df.to_csv(route_summary_path, index=False, encoding="utf-8-sig")
    if not validation_df.empty:
        validation_df.to_csv(validation_path, index=False, encoding="utf-8-sig")
    report_path.write_text(
        build_report(discovery_dataset, validation_paths, cfg, family_summary_df, route_summary_df, validation_df),
        encoding="utf-8",
    )

    print(f"Discovery dataset: {discovery_dataset}")
    print(f"Sample filter: {cfg.sample_filter}")
    print(f"Family summary: {family_summary_path}")
    print(f"Route discovery: {route_summary_path}")
    if not validation_df.empty:
        print(f"Route validation: {validation_path}")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
