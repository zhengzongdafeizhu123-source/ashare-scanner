from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from p14_discover_pattern_families import (
    FAMILY_DESCRIPTIONS,
    add_calendar_fields,
    apply_sample_filter,
    build_pattern_masks,
    classify_path,
    compute_official_d0_columns,
    compute_trade_metrics,
    derive_config_tag,
    ensure_required_columns,
    evaluate_entry_template,
    evaluate_exit_template,
    load_pattern_discovery_config,
    rounded,
    safe_mean,
    safe_median,
)


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


PROJECT_DIR = Path(__file__).resolve().parent
SCAN_CONFIG_FILE = PROJECT_DIR / "scan_config.json"

OUTCOME_SUCCESS = "success"
OUTCOME_FAILURE = "failure"
OUTCOME_FLAT = "flat"
OUTCOME_UNTRIGGERED = "untriggered"
OUTCOME_NON_EXECUTABLE = "non_executable"
SCOPE_FULL = "full_filtered_universe"
SCOPE_RECENT_TOP = "recent_top_ranked_subset"

PRIMARY_FAMILY_ORDER = [
    "family_hardpass_high_score",
    "family_hardpass_space_turnover",
    "family_hardpass_core",
    "family_candidate_continuation",
    "family_candidate_secondary_core",
    "family_watch_repair",
    "family_orderly_breakout",
    "family_crowded_momentum",
    "family_repair_pullback",
]

POOL_BUCKET_MAP = {
    "family_hardpass_high_score": "主线",
    "family_hardpass_space_turnover": "主线",
    "family_hardpass_core": "主线",
    "family_candidate_continuation": "主线",
    "family_candidate_secondary_core": "副池",
    "family_watch_repair": "其他",
    "family_orderly_breakout": "其他",
    "family_crowded_momentum": "其他",
    "family_repair_pullback": "其他",
}

NUMERIC_COMPARE_FIELDS = [
    "official_d0_score",
    "br20",
    "d0_limit_up_space_pct",
    "d0_turnover",
    "d0_turnover_f",
    "d0_range_vol",
    "d1_gap_pct",
    "d1_close_ret_pct",
    "d2_open_ret_pct",
    "d2_high_ret_pct",
    "d2_close_ret_pct",
    "d1_breakout_close_gap_pct",
    "d1_breakout_drawdown_pct",
    "mfe_pct",
    "mae_pct",
]

CATEGORICAL_COMPARE_FIELDS = [
    "research_bucket",
    "hard_pass",
    "official_d0_tier",
    "p15_primary_family",
    "p15_pool_bucket",
    "weekday_d1",
    "postmortem_rank_band",
    "is_friday_entry",
    "d1_close_above_breakout_flag",
    "d1_breakout_reversal_flag",
    "d1_breakout_close_gap_bucket",
    "d1_breakout_drawdown_bucket",
    "d1_breakout_reversal_subtype",
]

DETAIL_COLUMNS = [
    "股票代码",
    "股票名称",
    "setup_date",
    "postmortem_rank",
    "research_bucket",
    "hard_pass",
    "official_d0_score",
    "official_d0_tier",
    "br20",
    "d0_limit_up_space_pct",
    "d0_turnover",
    "d0_turnover_f",
    "d0_range_vol",
    "breakout_price",
    "d1_open",
    "d1_high",
    "d1_low",
    "d1_close",
    "d1_gap_pct",
    "d2_open",
    "d2_high",
    "d2_low",
    "d2_close",
    "p15_primary_family",
    "p15_pool_bucket",
    "research_mainline_priority",
    "research_family_win_rate",
    "path_class",
    "weekday_d1",
    "postmortem_rank_band",
    "is_friday_entry",
    "d1_close_above_breakout_flag",
    "d1_breakout_reversal_flag",
    "d1_breakout_close_gap_pct",
    "d1_breakout_close_gap_bucket",
    "d1_breakout_drawdown_pct",
    "d1_breakout_drawdown_bucket",
    "d1_breakout_reversal_subtype",
    "entry_triggered_flag",
    "executable_flag",
    "tradable_flag",
    "entry_price",
    "exit_price",
    "entry_note",
    "exit_note",
    "realized_ret_pct",
    "mfe_pct",
    "mae_pct",
    "outcome_bucket",
]

REPORT_LINES = [
    "P15 is an explanatory failure-condition study, not an auto-rule engine.",
    "Success pool: D1 breakout triggered and executable, D2 exit realized_ret_pct > 0.",
    "Failure pool: D1 breakout triggered and executable, D2 exit realized_ret_pct < 0.",
    "Untriggered and non-executable samples stay outside the failure pool.",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="P15: analyze success-vs-failure pools for current D0-D1-D2 routes")
    parser.add_argument("--dataset", required=True, help="P9 research dataset parquet path")
    parser.add_argument("--research-config", default="", help="Research config path, defaults to research_config.json")
    parser.add_argument("--entry-template", default="d1_breakout_buy", help="Entry template to study")
    parser.add_argument("--exit-template", default="d2_close_exit", help="Exit template to study")
    parser.add_argument("--scope", choices=[SCOPE_RECENT_TOP, SCOPE_FULL], default=SCOPE_RECENT_TOP, help="Study recent daily top-ranked subset or the full filtered universe")
    parser.add_argument("--recent-setup-days", type=int, default=40, help="Recent setup_date windows to keep when scope=recent_top_ranked_subset")
    parser.add_argument("--top-n-per-day", type=int, default=20, help="Top N ranked names per setup_date when scope=recent_top_ranked_subset")
    parser.add_argument("--output-dir", default="", help="Optional output directory, defaults to dataset directory")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def assign_primary_family(df: pd.DataFrame, family_masks: dict[str, pd.Series]) -> pd.DataFrame:
    work = df.copy()
    work["p15_primary_family"] = "unclassified"
    for family_name in PRIMARY_FAMILY_ORDER:
        mask = family_masks.get(family_name)
        if mask is None:
            continue
        assign_mask = work["p15_primary_family"].eq("unclassified") & mask.fillna(False).astype(bool)
        work.loc[assign_mask, "p15_primary_family"] = family_name
    work["p15_pool_bucket"] = work["p15_primary_family"].map(POOL_BUCKET_MAP).fillna("其他")
    work["p15_primary_family_description"] = work["p15_primary_family"].map(FAMILY_DESCRIPTIONS).fillna("")
    return work


def add_shortlist_ranking_fields(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    scan_cfg = load_json(SCAN_CONFIG_FILE)
    official_cfg = scan_cfg.get("official_d0_logic_v2", {})
    mainline_cfg = scan_cfg.get("research_mainline_v1", {})
    families_cfg = mainline_cfg.get("families", {})
    min_score = int(official_cfg.get("min_score", 3))

    family_key = work["p15_primary_family"].astype(str).str.replace(r"^family_", "", regex=True)
    priority_map = {str(key): int((value or {}).get("priority", 0)) for key, value in families_cfg.items()}
    win_rate_map = {str(key): float((value or {}).get("win_rate", 0.0)) for key, value in families_cfg.items()}
    pool_bucket_map = {str(key): str((value or {}).get("pool_bucket", "其他")) for key, value in families_cfg.items()}

    work["research_mainline_priority"] = family_key.map(priority_map).fillna(0).astype(int)
    work["research_family_win_rate"] = family_key.map(win_rate_map).fillna(0.0)
    work["research_pool_bucket"] = family_key.map(pool_bucket_map).fillna(work["p15_pool_bucket"]).astype(str)
    work["_research_pool_sort"] = work["research_pool_bucket"].map({"主线": 2, "副池": 1, "其他": 0}).fillna(0).astype(int)
    work["_research_mainline_sort"] = work["research_mainline_priority"].fillna(0).astype(int)
    work["_research_win_rate_sort"] = pd.to_numeric(work["research_family_win_rate"], errors="coerce").fillna(0.0)
    work["_official_d0_flag_sort"] = (pd.to_numeric(work["official_d0_score"], errors="coerce").fillna(0) >= min_score).astype(int)
    work["_official_d0_tier_sort"] = work["official_d0_tier"].map({"A": 0, "B": 1, "C": 2}).fillna(9).astype(int)
    work["_label_rank_sort"] = work["research_bucket"].map({"候选": 0, "入围": 1, "观察": 2}).fillna(9).astype(int)
    return work


def select_scope_subset(df: pd.DataFrame, scope: str, recent_setup_days: int, top_n_per_day: int) -> pd.DataFrame:
    work = df.copy()
    work["setup_date"] = pd.to_datetime(work["setup_date"], errors="coerce")
    work = work[work["setup_date"].notna()].copy()
    if work.empty:
        return work

    if scope == SCOPE_FULL:
        work["postmortem_rank"] = pd.NA
        work["scope_mode"] = SCOPE_FULL
        return work

    unique_dates = work["setup_date"].dt.normalize().drop_duplicates().sort_values()
    if recent_setup_days > 0:
        selected_dates = set(unique_dates.iloc[-recent_setup_days:].tolist())
        work = work[work["setup_date"].dt.normalize().isin(selected_dates)].copy()

    work = work.sort_values(
        by=[
            "setup_date",
            "_research_pool_sort",
            "_research_win_rate_sort",
            "_research_mainline_sort",
            "_official_d0_flag_sort",
            "_official_d0_tier_sort",
            "official_d0_score",
            "_label_rank_sort",
            "br20",
            "d0_turnover",
            "d0_turnover_f",
            "股票代码",
        ],
        ascending=[True, False, False, False, False, True, False, True, False, False, False, True],
        na_position="last",
    ).copy()
    work["postmortem_rank"] = work.groupby(work["setup_date"].dt.normalize()).cumcount() + 1
    if top_n_per_day > 0:
        work = work[work["postmortem_rank"] <= top_n_per_day].copy()
    work["scope_mode"] = SCOPE_RECENT_TOP
    return work


def build_rank_band(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    rank_band = pd.Series("missing", index=series.index, dtype="object")
    rank_band.loc[numeric.between(1, 5, inclusive="both")] = "01_05"
    rank_band.loc[numeric.between(6, 10, inclusive="both")] = "06_10"
    rank_band.loc[numeric.between(11, 15, inclusive="both")] = "11_15"
    rank_band.loc[numeric.between(16, 20, inclusive="both")] = "16_20"
    rank_band.loc[numeric >= 21] = "21_plus"
    return rank_band


def bucket_breakout_gap(close_gap_pct: pd.Series, reversal_flag: pd.Series) -> pd.Series:
    bucket = pd.Series("not_applicable", index=close_gap_pct.index, dtype="object")
    active = reversal_flag.fillna(False).astype(bool)
    numeric = pd.to_numeric(close_gap_pct, errors="coerce")
    bucket.loc[active] = "missing"
    bucket.loc[active & (numeric >= -1.5)] = "gap_-1.5_to_0"
    bucket.loc[active & (numeric < -1.5) & (numeric >= -3.0)] = "gap_-3.0_to_-1.5"
    bucket.loc[active & (numeric < -3.0)] = "gap_below_-3.0"
    return bucket


def bucket_breakout_drawdown(drawdown_pct: pd.Series, reversal_flag: pd.Series) -> pd.Series:
    bucket = pd.Series("not_applicable", index=drawdown_pct.index, dtype="object")
    active = reversal_flag.fillna(False).astype(bool)
    numeric = pd.to_numeric(drawdown_pct, errors="coerce")
    bucket.loc[active] = "missing"
    bucket.loc[active & (numeric > -4.0)] = "dd_above_-4.0"
    bucket.loc[active & (numeric <= -4.0) & (numeric > -6.0)] = "dd_-6.0_to_-4.0"
    bucket.loc[active & (numeric <= -6.0)] = "dd_below_-6.0"
    return bucket


def add_breakout_behavior_fields(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    breakout_price = pd.to_numeric(work["breakout_price"], errors="coerce")
    d1_close = pd.to_numeric(work["d1_close"], errors="coerce")
    d1_low = pd.to_numeric(work["d1_low"], errors="coerce")
    d1_high = pd.to_numeric(work["d1_high"], errors="coerce")

    breakout_touched = breakout_price.notna() & d1_high.notna() & (d1_high >= breakout_price)
    work["d1_close_above_breakout_flag"] = breakout_price.notna() & d1_close.notna() & (d1_close >= breakout_price)
    work["d1_breakout_reversal_flag"] = breakout_touched & d1_close.notna() & (d1_close < breakout_price)
    work["d1_breakout_close_gap_pct"] = (d1_close / breakout_price - 1.0) * 100.0
    work["d1_breakout_drawdown_pct"] = (d1_low / breakout_price - 1.0) * 100.0
    work["d1_breakout_close_gap_bucket"] = bucket_breakout_gap(work["d1_breakout_close_gap_pct"], work["d1_breakout_reversal_flag"])
    work["d1_breakout_drawdown_bucket"] = bucket_breakout_drawdown(work["d1_breakout_drawdown_pct"], work["d1_breakout_reversal_flag"])

    reversal_subtype = pd.Series("no_reversal", index=work.index, dtype="object")
    reversal_flag = work["d1_breakout_reversal_flag"].fillna(False).astype(bool)
    close_gap = pd.to_numeric(work["d1_breakout_close_gap_pct"], errors="coerce")
    drawdown = pd.to_numeric(work["d1_breakout_drawdown_pct"], errors="coerce")

    # These are explanatory postmortem buckets, not trading rules.
    reversal_subtype.loc[reversal_flag] = "reversal_unbucketed"
    reversal_subtype.loc[reversal_flag & (close_gap >= -1.5)] = "reversal_near_line"
    reversal_subtype.loc[reversal_flag & (close_gap < -1.5) & (drawdown > -6.0)] = "reversal_weak_close"
    reversal_subtype.loc[reversal_flag & (drawdown <= -6.0)] = "reversal_deep_flush"
    work["d1_breakout_reversal_subtype"] = reversal_subtype
    return work


def classify_outcome_bucket(evaluated_df: pd.DataFrame) -> pd.Series:
    outcome = pd.Series(OUTCOME_NON_EXECUTABLE, index=evaluated_df.index, dtype="object")
    triggered = evaluated_df["entry_triggered_flag"].fillna(False).astype(bool)
    executable = (
        evaluated_df["executable_flag"].fillna(False).astype(bool)
        & evaluated_df["tradable_flag"].fillna(False).astype(bool)
    )
    realized = pd.to_numeric(evaluated_df["realized_ret_pct"], errors="coerce")

    outcome.loc[~triggered] = OUTCOME_UNTRIGGERED
    outcome.loc[triggered & executable & (realized > 0)] = OUTCOME_SUCCESS
    outcome.loc[triggered & executable & (realized < 0)] = OUTCOME_FAILURE
    outcome.loc[triggered & executable & (realized == 0)] = OUTCOME_FLAT
    return outcome


def build_outcome_summary(evaluated_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    total = len(evaluated_df)
    for bucket in [OUTCOME_SUCCESS, OUTCOME_FAILURE, OUTCOME_FLAT, OUTCOME_UNTRIGGERED, OUTCOME_NON_EXECUTABLE]:
        sub = evaluated_df[evaluated_df["outcome_bucket"] == bucket].copy()
        rows.append(
            {
                "outcome_bucket": bucket,
                "sample_count": len(sub),
                "sample_ratio": rounded(len(sub) / total if total else 0.0),
                "avg_realized_ret_pct": rounded(safe_mean(sub["realized_ret_pct"])) if not sub.empty else None,
                "median_realized_ret_pct": rounded(safe_median(sub["realized_ret_pct"])) if not sub.empty else None,
                "avg_mfe_pct": rounded(safe_mean(sub["mfe_pct"])) if not sub.empty else None,
                "avg_mae_pct": rounded(safe_mean(sub["mae_pct"])) if not sub.empty else None,
            }
        )
    return pd.DataFrame(rows)


def build_numeric_comparison(evaluated_df: pd.DataFrame) -> pd.DataFrame:
    sf = evaluated_df[evaluated_df["outcome_bucket"].isin([OUTCOME_SUCCESS, OUTCOME_FAILURE])].copy()
    success_df = sf[sf["outcome_bucket"] == OUTCOME_SUCCESS]
    failure_df = sf[sf["outcome_bucket"] == OUTCOME_FAILURE]

    rows: list[dict[str, Any]] = []
    for field in NUMERIC_COMPARE_FIELDS:
        if field not in sf.columns:
            continue
        success_series = pd.to_numeric(success_df[field], errors="coerce")
        failure_series = pd.to_numeric(failure_df[field], errors="coerce")
        rows.append(
            {
                "field_name": field,
                "success_count": int(success_series.notna().sum()),
                "failure_count": int(failure_series.notna().sum()),
                "success_mean": rounded(safe_mean(success_series)),
                "failure_mean": rounded(safe_mean(failure_series)),
                "mean_gap_success_minus_failure": rounded((safe_mean(success_series) or 0.0) - (safe_mean(failure_series) or 0.0)),
                "success_median": rounded(safe_median(success_series)),
                "failure_median": rounded(safe_median(failure_series)),
                "success_q25": rounded(float(success_series.quantile(0.25))) if success_series.notna().any() else None,
                "failure_q25": rounded(float(failure_series.quantile(0.25))) if failure_series.notna().any() else None,
                "success_q75": rounded(float(success_series.quantile(0.75))) if success_series.notna().any() else None,
                "failure_q75": rounded(float(failure_series.quantile(0.75))) if failure_series.notna().any() else None,
            }
        )
    return pd.DataFrame(rows)


def build_categorical_comparison(evaluated_df: pd.DataFrame) -> pd.DataFrame:
    sf = evaluated_df[evaluated_df["outcome_bucket"].isin([OUTCOME_SUCCESS, OUTCOME_FAILURE])].copy()
    rows: list[dict[str, Any]] = []
    for field in CATEGORICAL_COMPARE_FIELDS:
        if field not in sf.columns:
            continue
        work = sf[[field, "outcome_bucket", "realized_ret_pct"]].copy()
        work[field] = work[field].astype(str).replace({"": "missing", "nan": "missing", "None": "missing"})
        for value, sub in work.groupby(field, dropna=False):
            success_count = int((sub["outcome_bucket"] == OUTCOME_SUCCESS).sum())
            failure_count = int((sub["outcome_bucket"] == OUTCOME_FAILURE).sum())
            total = success_count + failure_count
            rows.append(
                {
                    "field_name": field,
                    "field_value": value,
                    "success_count": success_count,
                    "failure_count": failure_count,
                    "total_count": total,
                    "success_rate": rounded(success_count / total if total else 0.0),
                    "failure_rate": rounded(failure_count / total if total else 0.0),
                    "avg_realized_ret_pct": rounded(safe_mean(sub["realized_ret_pct"])),
                }
            )
    return pd.DataFrame(rows)


def build_group_summary(evaluated_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for field in [
        "research_bucket",
        "hard_pass",
        "official_d0_tier",
        "p15_pool_bucket",
        "p15_primary_family",
        "weekday_d1",
        "postmortem_rank",
        "postmortem_rank_band",
        "d1_breakout_reversal_subtype",
    ]:
        if field not in evaluated_df.columns:
            continue
        work = evaluated_df[[field, "outcome_bucket", "realized_ret_pct", "mae_pct"]].copy()
        work[field] = work[field].astype(str).replace({"": "missing", "nan": "missing", "None": "missing"})
        for value, sub in work.groupby(field, dropna=False):
            success_count = int((sub["outcome_bucket"] == OUTCOME_SUCCESS).sum())
            failure_count = int((sub["outcome_bucket"] == OUTCOME_FAILURE).sum())
            flat_count = int((sub["outcome_bucket"] == OUTCOME_FLAT).sum())
            executable = success_count + failure_count + flat_count
            rows.append(
                {
                    "group_field": field,
                    "group_value": value,
                    "sample_count": len(sub),
                    "success_count": success_count,
                    "failure_count": failure_count,
                    "flat_count": flat_count,
                    "untriggered_count": int((sub["outcome_bucket"] == OUTCOME_UNTRIGGERED).sum()),
                    "non_executable_count": int((sub["outcome_bucket"] == OUTCOME_NON_EXECUTABLE).sum()),
                    "success_rate_among_executable": rounded(success_count / executable if executable else 0.0),
                    "failure_rate_among_executable": rounded(failure_count / executable if executable else 0.0),
                    "avg_realized_ret_pct": rounded(safe_mean(sub["realized_ret_pct"])),
                    "avg_mae_pct": rounded(safe_mean(sub["mae_pct"])),
                }
            )
    return pd.DataFrame(rows)


def build_recent_failure_slice_summary(evaluated_df: pd.DataFrame) -> pd.DataFrame:
    sf = evaluated_df[evaluated_df["outcome_bucket"].isin([OUTCOME_SUCCESS, OUTCOME_FAILURE, OUTCOME_FLAT, OUTCOME_UNTRIGGERED])].copy()
    if sf.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for field in ["p15_pool_bucket", "p15_primary_family", "weekday_d1", "postmortem_rank_band"]:
        if field not in sf.columns:
            continue
        work = sf[[field, "outcome_bucket", "realized_ret_pct", "d1_breakout_reversal_flag"]].copy()
        work[field] = work[field].astype(str).replace({"": "missing", "nan": "missing", "None": "missing"})
        for value, sub in work.groupby(field, dropna=False):
            success_count = int((sub["outcome_bucket"] == OUTCOME_SUCCESS).sum())
            failure_count = int((sub["outcome_bucket"] == OUTCOME_FAILURE).sum())
            flat_count = int((sub["outcome_bucket"] == OUTCOME_FLAT).sum())
            executable_total = success_count + failure_count + flat_count
            untriggered_count = int((sub["outcome_bucket"] == OUTCOME_UNTRIGGERED).sum())
            executable_df = sub[sub["outcome_bucket"].isin([OUTCOME_SUCCESS, OUTCOME_FAILURE, OUTCOME_FLAT])].copy()
            reversal_count = int(executable_df["d1_breakout_reversal_flag"].fillna(False).astype(bool).sum())
            rows.append(
                {
                    "group_field": field,
                    "group_value": value,
                    "sample_count": len(sub),
                    "executable_total": executable_total,
                    "untriggered_count": untriggered_count,
                    "success_count": success_count,
                    "failure_count": failure_count,
                    "flat_count": flat_count,
                    "failure_rate_among_executable": rounded(failure_count / executable_total if executable_total else 0.0),
                    "success_rate_among_executable": rounded(success_count / executable_total if executable_total else 0.0),
                    "untriggered_ratio": rounded(untriggered_count / len(sub) if len(sub) else 0.0),
                    "reversal_ratio_among_executable": rounded(reversal_count / executable_total if executable_total else 0.0),
                    "avg_realized_ret_pct": rounded(safe_mean(executable_df["realized_ret_pct"])),
                }
            )
    return pd.DataFrame(rows)


def build_daily_postmortem_summary(evaluated_df: pd.DataFrame) -> pd.DataFrame:
    if "setup_date" not in evaluated_df.columns or evaluated_df.empty:
        return pd.DataFrame()
    work = evaluated_df.copy()
    work["setup_date"] = pd.to_datetime(work["setup_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    rows: list[dict[str, Any]] = []
    for setup_date, sub in work.groupby("setup_date", dropna=False):
        success_count = int((sub["outcome_bucket"] == OUTCOME_SUCCESS).sum())
        failure_count = int((sub["outcome_bucket"] == OUTCOME_FAILURE).sum())
        flat_count = int((sub["outcome_bucket"] == OUTCOME_FLAT).sum())
        executable = success_count + failure_count + flat_count
        rows.append(
            {
                "setup_date": setup_date,
                "sample_count": len(sub),
                "success_count": success_count,
                "failure_count": failure_count,
                "flat_count": flat_count,
                "untriggered_count": int((sub["outcome_bucket"] == OUTCOME_UNTRIGGERED).sum()),
                "success_rate_among_executable": rounded(success_count / executable if executable else 0.0),
                "failure_rate_among_executable": rounded(failure_count / executable if executable else 0.0),
                "avg_realized_ret_pct": rounded(safe_mean(sub["realized_ret_pct"])),
            }
        )
    return pd.DataFrame(rows).sort_values("setup_date").reset_index(drop=True)


def build_breakout_reversal_summary(evaluated_df: pd.DataFrame) -> pd.DataFrame:
    sf = evaluated_df[evaluated_df["outcome_bucket"].isin([OUTCOME_SUCCESS, OUTCOME_FAILURE])].copy()
    if sf.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []

    def _append_rows(group_field: str) -> None:
        if group_field not in sf.columns:
            return
        work = sf[[group_field, "d1_breakout_reversal_flag", "outcome_bucket", "realized_ret_pct", "mae_pct"]].copy()
        work[group_field] = work[group_field].astype(str).replace({"": "missing", "nan": "missing", "None": "missing"})
        for value, sub in work.groupby(group_field, dropna=False):
            reversal = sub[sub["d1_breakout_reversal_flag"].fillna(False).astype(bool)].copy()
            non_reversal = sub[~sub["d1_breakout_reversal_flag"].fillna(False).astype(bool)].copy()
            executable_total = len(sub)
            reversal_total = len(reversal)
            reversal_failure = int((reversal["outcome_bucket"] == OUTCOME_FAILURE).sum())
            reversal_success = int((reversal["outcome_bucket"] == OUTCOME_SUCCESS).sum())
            non_reversal_failure = int((non_reversal["outcome_bucket"] == OUTCOME_FAILURE).sum())
            non_reversal_success = int((non_reversal["outcome_bucket"] == OUTCOME_SUCCESS).sum())
            rows.append(
                {
                    "group_field": group_field,
                    "group_value": value,
                    "executable_total": executable_total,
                    "reversal_count": reversal_total,
                    "reversal_ratio": rounded(reversal_total / executable_total if executable_total else 0.0),
                    "reversal_success_count": reversal_success,
                    "reversal_failure_count": reversal_failure,
                    "reversal_failure_rate": rounded(reversal_failure / reversal_total if reversal_total else 0.0),
                    "non_reversal_success_count": non_reversal_success,
                    "non_reversal_failure_count": non_reversal_failure,
                    "non_reversal_failure_rate": rounded(non_reversal_failure / len(non_reversal) if len(non_reversal) else 0.0),
                    "reversal_avg_realized_ret_pct": rounded(safe_mean(reversal["realized_ret_pct"])),
                    "non_reversal_avg_realized_ret_pct": rounded(safe_mean(non_reversal["realized_ret_pct"])),
                    "reversal_avg_mae_pct": rounded(safe_mean(reversal["mae_pct"])),
                    "non_reversal_avg_mae_pct": rounded(safe_mean(non_reversal["mae_pct"])),
                }
            )

    for group_field in ["p15_pool_bucket", "p15_primary_family", "postmortem_rank"]:
        _append_rows(group_field)

    return pd.DataFrame(rows)


def build_breakout_reversal_subtype_summary(evaluated_df: pd.DataFrame) -> pd.DataFrame:
    sf = evaluated_df[
        evaluated_df["outcome_bucket"].isin([OUTCOME_SUCCESS, OUTCOME_FAILURE])
        & evaluated_df["d1_breakout_reversal_flag"].fillna(False).astype(bool)
    ].copy()
    if sf.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for field in ["d1_breakout_reversal_subtype", "p15_primary_family", "p15_pool_bucket", "weekday_d1", "postmortem_rank_band"]:
        if field not in sf.columns:
            continue
        work = sf[[field, "outcome_bucket", "realized_ret_pct", "mae_pct", "d1_breakout_close_gap_pct", "d1_breakout_drawdown_pct"]].copy()
        work[field] = work[field].astype(str).replace({"": "missing", "nan": "missing", "None": "missing"})
        for value, sub in work.groupby(field, dropna=False):
            success_count = int((sub["outcome_bucket"] == OUTCOME_SUCCESS).sum())
            failure_count = int((sub["outcome_bucket"] == OUTCOME_FAILURE).sum())
            total = success_count + failure_count
            rows.append(
                {
                    "group_field": field,
                    "group_value": value,
                    "sample_count": len(sub),
                    "success_count": success_count,
                    "failure_count": failure_count,
                    "failure_rate": rounded(failure_count / total if total else 0.0),
                    "success_rate": rounded(success_count / total if total else 0.0),
                    "avg_realized_ret_pct": rounded(safe_mean(sub["realized_ret_pct"])),
                    "avg_mae_pct": rounded(safe_mean(sub["mae_pct"])),
                    "avg_breakout_close_gap_pct": rounded(safe_mean(sub["d1_breakout_close_gap_pct"])),
                    "avg_breakout_drawdown_pct": rounded(safe_mean(sub["d1_breakout_drawdown_pct"])),
                }
            )
    return pd.DataFrame(rows)


def markdown_table(df: pd.DataFrame, columns: list[str], limit: int | None = None) -> str:
    if df.empty:
        return "_No rows._"
    work = df.loc[:, [column for column in columns if column in df.columns]].copy()
    if limit is not None:
        work = work.head(limit)
    return work.to_markdown(index=False)


def build_report(
    dataset_path: Path,
    cfg_tag: str,
    scope: str,
    recent_setup_days: int,
    top_n_per_day: int,
    entry_template: str,
    exit_template: str,
    outcome_summary_df: pd.DataFrame,
    daily_summary_df: pd.DataFrame,
    group_summary_df: pd.DataFrame,
    failure_slice_summary_df: pd.DataFrame,
    reversal_summary_df: pd.DataFrame,
    reversal_subtype_summary_df: pd.DataFrame,
    numeric_comparison_df: pd.DataFrame,
    categorical_comparison_df: pd.DataFrame,
) -> str:
    return "\n".join(
        [
            "# P15 Success vs Failure Pools",
            "",
            "## Run Scope",
            "",
            f"- Dataset: `{dataset_path.name}`",
            f"- Config tag: `{cfg_tag}`",
            f"- Scope mode: `{scope}`",
            f"- Recent setup days: `{recent_setup_days}`",
            f"- Top N per day: `{top_n_per_day}`",
            f"- Entry template: `{entry_template}`",
            f"- Exit template: `{exit_template}`",
            "",
            "## Working Notes",
            "",
            *[f"- {line}" for line in REPORT_LINES],
            "",
            "## Outcome Summary",
            "",
            markdown_table(outcome_summary_df, ["outcome_bucket", "sample_count", "sample_ratio", "avg_realized_ret_pct", "avg_mfe_pct", "avg_mae_pct"]),
            "",
            "## Daily Postmortem Summary",
            "",
            markdown_table(daily_summary_df, ["setup_date", "sample_count", "success_count", "failure_count", "success_rate_among_executable", "avg_realized_ret_pct"], 30),
            "",
            "## Breakout Reversal Summary",
            "",
            markdown_table(reversal_summary_df.sort_values(["group_field", "reversal_failure_rate"], ascending=[True, False]), ["group_field", "group_value", "executable_total", "reversal_count", "reversal_ratio", "reversal_failure_rate", "non_reversal_failure_rate", "reversal_avg_realized_ret_pct", "non_reversal_avg_realized_ret_pct"], 30),
            "",
            "## Breakout Reversal Subtypes",
            "",
            markdown_table(reversal_subtype_summary_df.sort_values(["group_field", "failure_rate"], ascending=[True, False]), ["group_field", "group_value", "sample_count", "failure_rate", "avg_realized_ret_pct", "avg_breakout_close_gap_pct", "avg_breakout_drawdown_pct"], 30),
            "",
            "## Recent Failure Slices",
            "",
            markdown_table(failure_slice_summary_df.sort_values(["group_field", "failure_rate_among_executable"], ascending=[True, False]), ["group_field", "group_value", "sample_count", "executable_total", "failure_rate_among_executable", "untriggered_ratio", "reversal_ratio_among_executable", "avg_realized_ret_pct"], 30),
            "",
            "## Group Summary",
            "",
            markdown_table(group_summary_df.sort_values(["group_field", "success_rate_among_executable"], ascending=[True, False]), ["group_field", "group_value", "sample_count", "success_count", "failure_count", "success_rate_among_executable", "avg_realized_ret_pct"], 20),
            "",
            "## Numeric Gaps",
            "",
            markdown_table(numeric_comparison_df.sort_values("mean_gap_success_minus_failure", ascending=False), ["field_name", "success_mean", "failure_mean", "mean_gap_success_minus_failure", "success_median", "failure_median"], 20),
            "",
            "## Categorical Gaps",
            "",
            markdown_table(categorical_comparison_df.sort_values(["field_name", "success_rate"], ascending=[True, False]), ["field_name", "field_value", "total_count", "success_count", "failure_count", "success_rate"], 30),
            "",
        ]
    )


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")

    research_config_path = Path(args.research_config) if args.research_config else None
    cfg = load_pattern_discovery_config(research_config_path)
    output_dir = Path(args.output_dir) if args.output_dir else dataset_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(dataset_path)
    ensure_required_columns(df, cfg.sample_filter)
    filtered = apply_sample_filter(df, cfg.sample_filter)
    if filtered.empty:
        raise RuntimeError(f"No samples left after sample_filter={cfg.sample_filter}")

    work = compute_official_d0_columns(filtered, cfg)
    work = add_calendar_fields(work)
    work["path_class"] = classify_path(work)
    family_masks = build_pattern_masks(work, cfg)
    work = assign_primary_family(work, family_masks)
    work = add_breakout_behavior_fields(work)
    work = add_shortlist_ranking_fields(work)
    work = select_scope_subset(work, args.scope, args.recent_setup_days, args.top_n_per_day)
    if work.empty:
        raise RuntimeError("No samples left after applying scope selection")
    work["postmortem_rank_band"] = build_rank_band(work["postmortem_rank"])

    entry_df = evaluate_entry_template(work, args.entry_template)
    exit_df = evaluate_exit_template(work, entry_df, args.exit_template)
    metrics_df = compute_trade_metrics(work, entry_df, exit_df)
    evaluated_df = pd.concat([work.reset_index(drop=True), metrics_df.reset_index(drop=True)], axis=1)
    evaluated_df["outcome_bucket"] = classify_outcome_bucket(evaluated_df)

    success_df = evaluated_df[evaluated_df["outcome_bucket"] == OUTCOME_SUCCESS].copy()
    failure_df = evaluated_df[evaluated_df["outcome_bucket"] == OUTCOME_FAILURE].copy()
    nontrade_df = evaluated_df[evaluated_df["outcome_bucket"].isin([OUTCOME_UNTRIGGERED, OUTCOME_NON_EXECUTABLE, OUTCOME_FLAT])].copy()

    outcome_summary_df = build_outcome_summary(evaluated_df)
    numeric_comparison_df = build_numeric_comparison(evaluated_df)
    categorical_comparison_df = build_categorical_comparison(evaluated_df)
    group_summary_df = build_group_summary(evaluated_df)
    failure_slice_summary_df = build_recent_failure_slice_summary(evaluated_df)
    daily_summary_df = build_daily_postmortem_summary(evaluated_df)
    reversal_summary_df = build_breakout_reversal_summary(evaluated_df)
    reversal_subtype_summary_df = build_breakout_reversal_subtype_summary(evaluated_df)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    cfg_tag = derive_config_tag(research_config_path, cfg.sample_filter)
    scope_tag = "recenttop" if args.scope == SCOPE_RECENT_TOP else "full"
    run_tag = f"{cfg_tag}_{scope_tag}_{args.entry_template}_{args.exit_template}_{stamp}"

    scope_path = output_dir / f"p15_scope_subset_details_{run_tag}.csv"
    success_path = output_dir / f"p15_success_pool_details_{run_tag}.csv"
    failure_path = output_dir / f"p15_failure_pool_details_{run_tag}.csv"
    nontrade_path = output_dir / f"p15_nontrade_pool_details_{run_tag}.csv"
    outcome_summary_path = output_dir / f"p15_outcome_summary_{run_tag}.csv"
    numeric_comparison_path = output_dir / f"p15_numeric_feature_comparison_{run_tag}.csv"
    categorical_comparison_path = output_dir / f"p15_categorical_feature_comparison_{run_tag}.csv"
    group_summary_path = output_dir / f"p15_group_summary_{run_tag}.csv"
    failure_slice_summary_path = output_dir / f"p15_recent_failure_slice_summary_{run_tag}.csv"
    daily_summary_path = output_dir / f"p15_daily_postmortem_summary_{run_tag}.csv"
    reversal_summary_path = output_dir / f"p15_breakout_reversal_summary_{run_tag}.csv"
    reversal_subtype_summary_path = output_dir / f"p15_breakout_reversal_subtype_summary_{run_tag}.csv"
    report_path = output_dir / f"p15_success_failure_report_{run_tag}.md"

    evaluated_df.loc[:, [col for col in DETAIL_COLUMNS if col in evaluated_df.columns]].to_csv(scope_path, index=False, encoding="utf-8-sig")
    success_df.loc[:, [col for col in DETAIL_COLUMNS if col in success_df.columns]].to_csv(success_path, index=False, encoding="utf-8-sig")
    failure_df.loc[:, [col for col in DETAIL_COLUMNS if col in failure_df.columns]].to_csv(failure_path, index=False, encoding="utf-8-sig")
    nontrade_df.loc[:, [col for col in DETAIL_COLUMNS if col in nontrade_df.columns]].to_csv(nontrade_path, index=False, encoding="utf-8-sig")
    outcome_summary_df.to_csv(outcome_summary_path, index=False, encoding="utf-8-sig")
    numeric_comparison_df.to_csv(numeric_comparison_path, index=False, encoding="utf-8-sig")
    categorical_comparison_df.to_csv(categorical_comparison_path, index=False, encoding="utf-8-sig")
    group_summary_df.to_csv(group_summary_path, index=False, encoding="utf-8-sig")
    failure_slice_summary_df.to_csv(failure_slice_summary_path, index=False, encoding="utf-8-sig")
    daily_summary_df.to_csv(daily_summary_path, index=False, encoding="utf-8-sig")
    reversal_summary_df.to_csv(reversal_summary_path, index=False, encoding="utf-8-sig")
    reversal_subtype_summary_df.to_csv(reversal_subtype_summary_path, index=False, encoding="utf-8-sig")
    report_path.write_text(
        build_report(
            dataset_path,
            cfg_tag,
            args.scope,
            args.recent_setup_days,
            args.top_n_per_day,
            args.entry_template,
            args.exit_template,
            outcome_summary_df,
            daily_summary_df,
            group_summary_df,
            failure_slice_summary_df,
            reversal_summary_df,
            reversal_subtype_summary_df,
            numeric_comparison_df,
            categorical_comparison_df,
        ),
        encoding="utf-8",
    )

    print(f"Dataset: {dataset_path}")
    print(f"Sample filter: {cfg.sample_filter}")
    print(f"Scope mode: {args.scope}")
    print(f"Recent setup days: {args.recent_setup_days}")
    print(f"Top N per day: {args.top_n_per_day}")
    print(f"Entry template: {args.entry_template}")
    print(f"Exit template: {args.exit_template}")
    print(f"Scope subset: {scope_path}")
    print(f"Success pool: {success_path}")
    print(f"Failure pool: {failure_path}")
    print(f"Nontrade pool: {nontrade_path}")
    print(f"Outcome summary: {outcome_summary_path}")
    print(f"Numeric comparison: {numeric_comparison_path}")
    print(f"Categorical comparison: {categorical_comparison_path}")
    print(f"Group summary: {group_summary_path}")
    print(f"Recent failure slices: {failure_slice_summary_path}")
    print(f"Daily postmortem summary: {daily_summary_path}")
    print(f"Breakout reversal summary: {reversal_summary_path}")
    print(f"Breakout reversal subtype summary: {reversal_subtype_summary_path}")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
