from __future__ import annotations

import argparse
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
    compute_official_d0_columns,
    derive_config_tag,
    ensure_required_columns,
    load_pattern_discovery_config,
    rounded,
    safe_mean,
    safe_median,
)
from p15_analyze_success_failure_pools import (
    assign_primary_family,
    add_shortlist_ranking_fields,
    select_scope_subset,
)


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


PROJECT_DIR = Path(__file__).resolve().parent
SCOPE_FULL = "full_filtered_universe"
SCOPE_RECENT_TOP = "recent_top_ranked_subset"

MAINLINE_FAMILIES = [
    "family_hardpass_high_score",
    "family_hardpass_space_turnover",
    "family_hardpass_core",
    "family_candidate_continuation",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="P16: analyze confirm-close D1 entry viability using D2 open as executable entry"
    )
    parser.add_argument("--dataset", required=True, help="P9 research dataset parquet path")
    parser.add_argument("--research-config", default="", help="Research config path, defaults to research_config.json")
    parser.add_argument(
        "--scope",
        choices=[SCOPE_RECENT_TOP, SCOPE_FULL],
        default=SCOPE_RECENT_TOP,
        help="Study recent daily top-ranked subset or the full filtered universe",
    )
    parser.add_argument("--recent-setup-days", type=int, default=40, help="Recent setup_date windows to keep when scope=recent_top_ranked_subset")
    parser.add_argument("--top-n-per-day", type=int, default=20, help="Top N ranked names per setup_date when scope=recent_top_ranked_subset")
    parser.add_argument("--output-dir", default="", help="Optional output directory, defaults to dataset directory")
    parser.add_argument("--min-close-gap-pct", type=float, default=0.5, help="Minimum D1 close buffer above breakout_price, in percent")
    parser.add_argument("--min-close-position-pct", type=float, default=35.0, help="Minimum D1 close position within the daily range, where 0 is daily low and 100 is daily high")
    parser.add_argument("--max-d2-open-gap-pct", type=float, default=2.0, help="Maximum allowed D2 open over breakout_price for confirm-close entry")
    parser.add_argument("--min-d2-space-pct", type=float, default=2.0, help="Minimum D2 high space from D2 open to count as enough room")
    parser.add_argument("--max-d2-drawdown-pct", type=float, default=3.0, help="Maximum tolerated D2 intraday drawdown from D2 open")
    return parser.parse_args()


def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def restrict_to_mainline_families(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["p16_primary_family"].isin(MAINLINE_FAMILIES)].copy()


def evaluate_confirm_close_entry(
    df: pd.DataFrame,
    *,
    min_close_gap_pct: float,
    min_close_position_pct: float,
    max_d2_open_gap_pct: float,
) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)

    breakout_price = to_numeric(df["breakout_price"])
    d1_high = to_numeric(df["d1_high"])
    d1_low = to_numeric(df["d1_low"])
    d1_close = to_numeric(df["d1_close"])
    d2_open = to_numeric(df["d2_open"])

    result["breakout_touched_flag"] = breakout_price.notna() & d1_high.notna() & (d1_high >= breakout_price)
    result["d1_close_above_breakout_flag"] = breakout_price.notna() & d1_close.notna() & (d1_close >= breakout_price)
    result["d1_close_gap_pct"] = (d1_close / breakout_price - 1.0) * 100.0

    day_range = d1_high - d1_low
    close_position_pct = ((d1_close - d1_low) / day_range) * 100.0
    close_position_pct = close_position_pct.where(day_range > 0)
    result["d1_close_position_pct"] = close_position_pct

    result["close_gap_ok_flag"] = result["d1_close_gap_pct"] >= float(min_close_gap_pct)
    result["close_position_ok_flag"] = result["d1_close_position_pct"] >= float(min_close_position_pct)
    result["d2_open_gap_from_breakout_pct"] = (d2_open / breakout_price - 1.0) * 100.0
    result["d2_open_gap_ok_flag"] = result["d2_open_gap_from_breakout_pct"] <= float(max_d2_open_gap_pct)

    confirm_ok = (
        result["breakout_touched_flag"].fillna(False).astype(bool)
        & result["d1_close_above_breakout_flag"].fillna(False).astype(bool)
        & result["close_gap_ok_flag"].fillna(False).astype(bool)
        & result["close_position_ok_flag"].fillna(False).astype(bool)
    )
    result["confirm_valid_flag"] = confirm_ok

    result["entry_triggered_flag"] = confirm_ok
    result["entry_day"] = "D2"
    result["entry_price"] = d2_open
    result["executable_flag"] = confirm_ok & d2_open.notna() & result["d2_open_gap_ok_flag"].fillna(False).astype(bool)
    result["tradable_flag"] = result["executable_flag"]
    result["entry_note"] = ""

    result.loc[~result["breakout_touched_flag"], "entry_note"] = "d1_no_breakout_touch"
    result.loc[result["entry_note"].eq("") & ~result["d1_close_above_breakout_flag"], "entry_note"] = "d1_close_lost_breakout"
    result.loc[result["entry_note"].eq("") & ~result["close_gap_ok_flag"], "entry_note"] = "d1_close_buffer_too_thin"
    result.loc[result["entry_note"].eq("") & ~result["close_position_ok_flag"], "entry_note"] = "d1_close_too_low_in_range"
    result.loc[result["entry_note"].eq("") & result["confirm_valid_flag"] & d2_open.isna(), "entry_note"] = "missing_d2_open"
    result.loc[result["entry_note"].eq("") & result["confirm_valid_flag"] & ~result["d2_open_gap_ok_flag"], "entry_note"] = "d2_open_chased_too_high"
    result.loc[result["entry_note"].eq("") & result["executable_flag"], "entry_note"] = "confirm_valid_enter_d2_open"
    return result


def compute_confirm_trade_metrics(df: pd.DataFrame, entry_df: pd.DataFrame) -> pd.DataFrame:
    result = pd.DataFrame(index=df.index)

    d2_open = to_numeric(df["d2_open"])
    d2_high = to_numeric(df["d2_high"])
    d2_low = to_numeric(df["d2_low"])
    d2_close = to_numeric(df["d2_close"])

    entry_price = to_numeric(entry_df["entry_price"])
    result["d2_close_price"] = d2_close
    result["d2_open_entry_close_ret_pct"] = (d2_close / entry_price - 1.0) * 100.0
    result["d2_open_entry_high_space_pct"] = (d2_high / entry_price - 1.0) * 100.0
    result["d2_open_entry_low_drawdown_pct"] = (d2_low / entry_price - 1.0) * 100.0
    return result


def add_actionability_flags(
    evaluated_df: pd.DataFrame,
    *,
    min_d2_space_pct: float,
    max_d2_drawdown_pct: float,
) -> pd.DataFrame:
    work = evaluated_df.copy()
    executable = work["executable_flag"].fillna(False).astype(bool) & work["tradable_flag"].fillna(False).astype(bool)
    work["d2_space_enough_flag"] = executable & (to_numeric(work["d2_open_entry_high_space_pct"]) >= float(min_d2_space_pct))
    work["d2_drawdown_acceptable_flag"] = executable & (to_numeric(work["d2_open_entry_low_drawdown_pct"]) >= -float(max_d2_drawdown_pct))
    work["d2_close_profitable_flag"] = executable & (to_numeric(work["d2_open_entry_close_ret_pct"]) > 0)
    work["d2_actionable_flag"] = work["d2_space_enough_flag"] & work["d2_drawdown_acceptable_flag"]
    return work


def build_overall_summary(df: pd.DataFrame) -> pd.DataFrame:
    executable = df[df["executable_flag"].fillna(False).astype(bool) & df["tradable_flag"].fillna(False).astype(bool)].copy()
    rows = [
        {"metric": "sample_count", "value": len(df)},
        {"metric": "confirm_valid_count", "value": int(df["confirm_valid_flag"].fillna(False).sum())},
        {"metric": "confirm_valid_ratio", "value": rounded(float(df["confirm_valid_flag"].fillna(False).mean()) if len(df) else 0.0)},
        {"metric": "d2_open_gap_ok_count", "value": int(df["d2_open_gap_ok_flag"].fillna(False).sum())},
        {"metric": "d2_open_gap_ok_ratio", "value": rounded(float(df["d2_open_gap_ok_flag"].fillna(False).mean()) if len(df) else 0.0)},
        {"metric": "executable_count", "value": len(executable)},
        {"metric": "executable_ratio", "value": rounded(len(executable) / len(df) if len(df) else 0.0)},
        {"metric": "avg_d2_high_space_pct", "value": rounded(safe_mean(executable["d2_open_entry_high_space_pct"])) if len(executable) else None},
        {"metric": "median_d2_high_space_pct", "value": rounded(safe_median(executable["d2_open_entry_high_space_pct"])) if len(executable) else None},
        {"metric": "avg_d2_close_ret_pct", "value": rounded(safe_mean(executable["d2_open_entry_close_ret_pct"])) if len(executable) else None},
        {"metric": "median_d2_close_ret_pct", "value": rounded(safe_median(executable["d2_open_entry_close_ret_pct"])) if len(executable) else None},
        {"metric": "avg_d2_low_drawdown_pct", "value": rounded(safe_mean(executable["d2_open_entry_low_drawdown_pct"])) if len(executable) else None},
        {"metric": "space_enough_ratio", "value": rounded(float(executable["d2_space_enough_flag"].mean())) if len(executable) else 0.0},
        {"metric": "drawdown_acceptable_ratio", "value": rounded(float(executable["d2_drawdown_acceptable_flag"].mean())) if len(executable) else 0.0},
        {"metric": "actionable_ratio", "value": rounded(float(executable["d2_actionable_flag"].mean())) if len(executable) else 0.0},
        {"metric": "d2_close_profitable_ratio", "value": rounded(float(executable["d2_close_profitable_flag"].mean())) if len(executable) else 0.0},
    ]
    return pd.DataFrame(rows)


def build_family_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family_name in MAINLINE_FAMILIES:
        sub = df[df["p16_primary_family"] == family_name].copy()
        executable = sub[sub["executable_flag"].fillna(False).astype(bool) & sub["tradable_flag"].fillna(False).astype(bool)].copy()
        rows.append(
            {
                "family_name": family_name,
                "family_description": FAMILY_DESCRIPTIONS.get(family_name, ""),
                "sample_count": len(sub),
                "confirm_valid_count": int(sub["confirm_valid_flag"].fillna(False).sum()),
                "confirm_valid_ratio": rounded(float(sub["confirm_valid_flag"].fillna(False).mean())) if len(sub) else 0.0,
                "d2_open_gap_ok_ratio": rounded(float(sub["d2_open_gap_ok_flag"].fillna(False).mean())) if len(sub) else 0.0,
                "executable_count": len(executable),
                "executable_ratio": rounded(len(executable) / len(sub) if len(sub) else 0.0),
                "avg_d2_high_space_pct": rounded(safe_mean(executable["d2_open_entry_high_space_pct"])) if len(executable) else None,
                "avg_d2_close_ret_pct": rounded(safe_mean(executable["d2_open_entry_close_ret_pct"])) if len(executable) else None,
                "avg_d2_low_drawdown_pct": rounded(safe_mean(executable["d2_open_entry_low_drawdown_pct"])) if len(executable) else None,
                "space_enough_ratio": rounded(float(executable["d2_space_enough_flag"].mean())) if len(executable) else 0.0,
                "drawdown_acceptable_ratio": rounded(float(executable["d2_drawdown_acceptable_flag"].mean())) if len(executable) else 0.0,
                "actionable_ratio": rounded(float(executable["d2_actionable_flag"].mean())) if len(executable) else 0.0,
                "d2_close_profitable_ratio": rounded(float(executable["d2_close_profitable_flag"].mean())) if len(executable) else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values(["actionable_ratio", "avg_d2_high_space_pct", "executable_count"], ascending=[False, False, False])


def build_detail_frame(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
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
        "p16_primary_family",
        "research_pool_bucket",
        "research_mainline_priority",
        "breakout_price",
        "d1_open",
        "d1_high",
        "d1_low",
        "d1_close",
        "d2_open",
        "d2_high",
        "d2_low",
        "d2_close",
        "confirm_valid_flag",
        "breakout_touched_flag",
        "d1_close_above_breakout_flag",
        "d1_close_gap_pct",
        "d1_close_position_pct",
        "close_gap_ok_flag",
        "close_position_ok_flag",
        "d2_open_gap_ok_flag",
        "entry_triggered_flag",
        "executable_flag",
        "tradable_flag",
        "entry_day",
        "entry_price",
        "entry_note",
        "d2_open_gap_from_breakout_pct",
        "d2_open_entry_high_space_pct",
        "d2_open_entry_close_ret_pct",
        "d2_open_entry_low_drawdown_pct",
        "d2_space_enough_flag",
        "d2_drawdown_acceptable_flag",
        "d2_actionable_flag",
        "d2_close_profitable_flag",
    ]
    present = [column for column in columns if column in df.columns]
    return df[present].copy()


def build_report_lines(
    *,
    args: argparse.Namespace,
    cfg_tag: str,
    scope_tag: str,
    overall_summary: pd.DataFrame,
    family_summary: pd.DataFrame,
) -> str:
    metric_map = overall_summary.set_index("metric")["value"].to_dict()
    best_family = family_summary.iloc[0] if not family_summary.empty else None
    lines = [
        "# P16 Confirm-Close Entry Report",
        "",
        "- P16 studies a stricter execution reality than `d1_breakout_buy`.",
        "- Confirmation happens on `D1`; executable entry is assumed at `D2 open`.",
        f"- Config tag: `{cfg_tag}`",
        f"- Scope: `{scope_tag}`",
        f"- Min D1 close gap over breakout: `{args.min_close_gap_pct:.2f}%`",
        f"- Min D1 close position in range: `{args.min_close_position_pct:.1f}`",
        f"- Max D2 open over breakout: `+{args.max_d2_open_gap_pct:.2f}%`",
        f"- Min D2 high space from entry: `{args.min_d2_space_pct:.2f}%`",
        f"- Max tolerated D2 drawdown: `-{args.max_d2_drawdown_pct:.2f}%`",
        "",
        "## Key Numbers",
        "",
        f"- sample_count = `{metric_map.get('sample_count')}`",
        f"- confirm_valid_ratio = `{metric_map.get('confirm_valid_ratio')}`",
        f"- d2_open_gap_ok_ratio = `{metric_map.get('d2_open_gap_ok_ratio')}`",
        f"- executable_ratio = `{metric_map.get('executable_ratio')}`",
        f"- avg_d2_high_space_pct = `{metric_map.get('avg_d2_high_space_pct')}`",
        f"- avg_d2_close_ret_pct = `{metric_map.get('avg_d2_close_ret_pct')}`",
        f"- avg_d2_low_drawdown_pct = `{metric_map.get('avg_d2_low_drawdown_pct')}`",
        f"- actionable_ratio = `{metric_map.get('actionable_ratio')}`",
        f"- d2_close_profitable_ratio = `{metric_map.get('d2_close_profitable_ratio')}`",
    ]
    if best_family is not None:
        lines.extend(
            [
                "",
                "## Best Family Snapshot",
                "",
                f"- best_family = `{best_family['family_name']}`",
                f"- executable_count = `{best_family['executable_count']}`",
                f"- avg_d2_high_space_pct = `{best_family['avg_d2_high_space_pct']}`",
                f"- actionable_ratio = `{best_family['actionable_ratio']}`",
                f"- d2_close_profitable_ratio = `{best_family['d2_close_profitable_ratio']}`",
            ]
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- P16 does not reuse `D1 close` as an idealized fill.",
            "- P16 now also rejects confirmation setups whose `D2 open` is already too stretched above breakout.",
            "- P16 is not a replacement for P14 mainlines; it is an executable-reality comparison layer.",
            "- P16 currently measures D2 room and pain; it does not yet model D3.",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset).expanduser().resolve()
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found: {dataset_path}")

    config_path = Path(args.research_config).expanduser().resolve() if args.research_config else None
    cfg = load_pattern_discovery_config(config_path)
    cfg_tag = derive_config_tag(config_path, cfg.sample_filter)

    raw = pd.read_parquet(dataset_path)
    ensure_required_columns(raw, cfg.sample_filter)
    filtered = apply_sample_filter(raw, cfg.sample_filter)
    work = compute_official_d0_columns(filtered, cfg)
    work = add_calendar_fields(work)

    family_masks = build_pattern_masks(work, cfg)
    work = assign_primary_family(work, family_masks)
    work["p16_primary_family"] = work["p15_primary_family"]
    work = restrict_to_mainline_families(work)
    work = add_shortlist_ranking_fields(work)
    scoped = select_scope_subset(work, args.scope, args.recent_setup_days, args.top_n_per_day)

    entry_df = evaluate_confirm_close_entry(
        scoped,
        min_close_gap_pct=args.min_close_gap_pct,
        min_close_position_pct=args.min_close_position_pct,
        max_d2_open_gap_pct=args.max_d2_open_gap_pct,
    )
    metrics_df = compute_confirm_trade_metrics(scoped, entry_df)
    evaluated = pd.concat([scoped, entry_df, metrics_df], axis=1)
    evaluated = add_actionability_flags(
        evaluated,
        min_d2_space_pct=args.min_d2_space_pct,
        max_d2_drawdown_pct=args.max_d2_drawdown_pct,
    )

    output_dir = Path(args.output_dir).expanduser().resolve() if args.output_dir else dataset_path.parent / "tmp_p16_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    scope_tag = "recenttop" if args.scope == SCOPE_RECENT_TOP else "full"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_tag = f"{cfg_tag}_{scope_tag}_confirmclose_d2open_{stamp}"

    overall_summary = build_overall_summary(evaluated)
    family_summary = build_family_summary(evaluated)
    detail_df = build_detail_frame(evaluated)
    report_text = build_report_lines(
        args=args,
        cfg_tag=cfg_tag,
        scope_tag=scope_tag,
        overall_summary=overall_summary,
        family_summary=family_summary,
    )

    overall_path = output_dir / f"p16_overall_summary_{run_tag}.csv"
    family_path = output_dir / f"p16_family_summary_{run_tag}.csv"
    detail_path = output_dir / f"p16_confirm_detail_{run_tag}.csv"
    report_path = output_dir / f"p16_confirm_report_{run_tag}.md"

    overall_summary.to_csv(overall_path, index=False, encoding="utf-8-sig")
    family_summary.to_csv(family_path, index=False, encoding="utf-8-sig")
    detail_df.to_csv(detail_path, index=False, encoding="utf-8-sig")
    report_path.write_text(report_text, encoding="utf-8")

    print(f"P16 completed: {run_tag}")
    print(f"Overall summary: {overall_path}")
    print(f"Family summary: {family_path}")
    print(f"Detail file: {detail_path}")
    print(f"Report: {report_path}")


if __name__ == "__main__":
    main()
