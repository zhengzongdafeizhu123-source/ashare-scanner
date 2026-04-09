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

DEFAULT_COMBO_VALIDATION = {
    "bucket_scopes": ["all", "non_abandon", "候选", "观察", "入围"],
    "trusted_hint": {
        "min_sample_ratio": 0.03,
        "min_lift": 1.05,
    },
    "official_d0_logic_v2": DEFAULT_OFFICIAL_D0_LOGIC,
}

CONDITION_SPECS = [
    ("cond_br20_strong", "br20"),
    ("cond_limit_up_space_good", "d0_limit_up_space_pct"),
    ("cond_turnover_good", "d0_turnover"),
    ("cond_turnover_f_good", "d0_turnover_f"),
    ("cond_range_vol_good", "d0_range_vol"),
]

KEY_COMBO_RULES = [
    ("combo_br20_turnover", ["cond_br20_strong", "cond_turnover_good"], "BR20 + turnover"),
    ("combo_br20_turnover_f", ["cond_br20_strong", "cond_turnover_f_good"], "BR20 + turnover_f"),
    ("combo_br20_limit_up_space", ["cond_br20_strong", "cond_limit_up_space_good"], "BR20 + limit_up_space"),
    ("combo_br20_range_vol", ["cond_br20_strong", "cond_range_vol_good"], "BR20 + range_vol"),
    ("combo_br20_turnover_turnover_f", ["cond_br20_strong", "cond_turnover_good", "cond_turnover_f_good"], "BR20 + turnover + turnover_f"),
    ("combo_br20_limit_up_space_turnover", ["cond_br20_strong", "cond_limit_up_space_good", "cond_turnover_good"], "BR20 + limit_up_space + turnover"),
    (
        "combo_br20_limit_up_space_turnover_range_vol",
        ["cond_br20_strong", "cond_limit_up_space_good", "cond_turnover_good", "cond_range_vol_good"],
        "BR20 + limit_up_space + turnover + range_vol",
    ),
    ("combo_all_5", [name for name, _ in CONDITION_SPECS], "all 5 conditions"),
]

SUCCESS_LABELS = ["d1_stable_flag", "d2_sellable_flag", "success_composite_flag"]
BALANCED_ALLOWED_SCORE_RULES = {"score_ge_3", "score_ge_4"}


@dataclass
class ComboValidationConfig:
    sample_filter: str
    primary_success_label: str
    official_d0_logic_v2: dict[str, Any]
    bucket_scopes: list[str]
    trusted_min_sample_ratio: float
    trusted_min_lift: float


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
    parser = argparse.ArgumentParser(description="验证 official D0 五条件在研究样本中的组合效果")
    parser.add_argument("--dataset", required=True, help="P9 输出的研究样本 parquet 路径")
    parser.add_argument("--research-config", default="", help="研究配置文件路径，默认 research_config.json")
    return parser.parse_args()


def load_combo_validation_config(path: Path | None) -> ComboValidationConfig:
    research_cfg = load_json(path or DEFAULT_RESEARCH_CONFIG_FILE)
    scan_cfg = load_json(SCAN_CONFIG_FILE)
    combo_cfg = deep_merge(DEFAULT_COMBO_VALIDATION, research_cfg.get("combo_validation", {}))
    official_cfg = deep_merge(
        combo_cfg.get("official_d0_logic_v2", DEFAULT_OFFICIAL_D0_LOGIC),
        scan_cfg.get("official_d0_logic_v2", {}),
    )
    trusted_cfg = combo_cfg.get("trusted_hint", {})
    return ComboValidationConfig(
        sample_filter=str(research_cfg.get("sample_filter", "hard_pass_or_watch")),
        primary_success_label=str(research_cfg.get("primary_success_label", "success_composite_flag")),
        official_d0_logic_v2=official_cfg,
        bucket_scopes=[str(x) for x in combo_cfg.get("bucket_scopes", DEFAULT_COMBO_VALIDATION["bucket_scopes"])],
        trusted_min_sample_ratio=float(trusted_cfg.get("min_sample_ratio", 0.03)),
        trusted_min_lift=float(trusted_cfg.get("min_lift", 1.05)),
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


def ensure_required_columns(df: pd.DataFrame, sample_filter: str) -> None:
    required = {
        "股票代码",
        "股票名称",
        "setup_date",
        "research_bucket",
        "br20",
        "d0_limit_up_space_pct",
        "d0_turnover",
        "d0_turnover_f",
        "d0_range_vol",
        "d1_stable_flag",
        "d2_sellable_flag",
        "success_composite_flag",
        "d1_close_ret_pct",
        "d2_close_ret_pct",
        "d2_high_ret_pct",
    }
    if sample_filter in {"hard_pass_only", "hard_pass_or_watch"}:
        required.add("hard_pass")
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"P12 缺少必要列: {', '.join(missing)}")


def bucket_scope_mask(df: pd.DataFrame, scope: str) -> pd.Series:
    if scope == "all":
        return pd.Series(True, index=df.index)
    if scope == "non_abandon":
        return df["research_bucket"].isin(["入围", "候选", "观察"])
    return df["research_bucket"] == scope


def compute_condition_columns(df: pd.DataFrame, cfg: ComboValidationConfig) -> pd.DataFrame:
    thresholds = cfg.official_d0_logic_v2.get("thresholds", {})
    work = df.copy()
    work["cond_br20_strong"] = pd.to_numeric(work["br20"], errors="coerce") >= float(thresholds.get("br20_min", 1.02))

    limit_up_space = pd.to_numeric(work["d0_limit_up_space_pct"], errors="coerce")
    work["cond_limit_up_space_good"] = (
        limit_up_space >= float(thresholds.get("limit_up_space_min", 0.0))
    ) & (
        limit_up_space <= float(thresholds.get("limit_up_space_max", 5.60))
    )

    work["cond_turnover_good"] = pd.to_numeric(work["d0_turnover"], errors="coerce") >= float(thresholds.get("turnover_min", 9.67))
    work["cond_turnover_f_good"] = pd.to_numeric(work["d0_turnover_f"], errors="coerce") >= float(thresholds.get("turnover_f_min", 15.126))

    range_vol = pd.to_numeric(work["d0_range_vol"], errors="coerce")
    work["cond_range_vol_good"] = (
        range_vol >= float(thresholds.get("range_vol_min", 0.666))
    ) & (
        range_vol <= float(thresholds.get("range_vol_max", 1.05))
    )

    condition_names = [name for name, _ in CONDITION_SPECS]
    work[condition_names] = work[condition_names].fillna(False)
    work["official_d0_score"] = work[condition_names].sum(axis=1).astype(int)
    work["official_d0_tier"] = ""
    work.loc[work["official_d0_score"] == 5, "official_d0_tier"] = "A"
    work.loc[work["official_d0_score"] == 4, "official_d0_tier"] = "B"
    work.loc[work["official_d0_score"] == 3, "official_d0_tier"] = "C"
    work["official_d0_hit_rules"] = work[condition_names].apply(
        lambda row: " | ".join([name for name in condition_names if bool(row[name])]),
        axis=1,
    )
    work["official_d0_miss_rules"] = work[condition_names].apply(
        lambda row: " | ".join([name for name in condition_names if not bool(row[name])]),
        axis=1,
    )
    return work


def make_rule_mask(df: pd.DataFrame, rule_name: str) -> pd.Series:
    if rule_name in {name for name, _ in CONDITION_SPECS}:
        return df[rule_name].astype(bool)
    if rule_name.startswith("score_eq_"):
        return df["official_d0_score"] == int(rule_name.split("_")[-1])
    if rule_name == "score_ge_3":
        return df["official_d0_score"] >= 3
    if rule_name == "score_ge_4":
        return df["official_d0_score"] >= 4
    if rule_name == "score_eq_5":
        return df["official_d0_score"] == 5
    if rule_name == "combo_any_3":
        return df["official_d0_score"] >= 3
    if rule_name == "combo_any_4":
        return df["official_d0_score"] >= 4

    combo_lookup = {name: members for name, members, _ in KEY_COMBO_RULES}
    members = combo_lookup.get(rule_name)
    if members is None:
        raise KeyError(f"未知规则: {rule_name}")
    mask = pd.Series(True, index=df.index)
    for member in members:
        mask = mask & df[member].astype(bool)
    return mask


def avg_metric(sub: pd.DataFrame, column: str) -> float | None:
    numeric = pd.to_numeric(sub[column], errors="coerce")
    if numeric.notna().sum() == 0:
        return None
    return float(numeric.mean())


def build_summary_row(
    scoped_df: pd.DataFrame,
    mask: pd.Series,
    rule_group: str,
    rule_name: str,
    bucket_scope: str,
    condition_count: int,
    note: str,
    cfg: ComboValidationConfig,
) -> dict[str, Any]:
    scoped_total = len(scoped_df)
    base_success = normalize_flag(scoped_df["success_composite_flag"])
    base_rate = float(base_success.mean()) if scoped_total else 0.0

    sub = scoped_df[mask].copy()
    sample_count = len(sub)
    sample_ratio = sample_count / scoped_total if scoped_total else 0.0

    if sample_count:
        d1_rate = float(normalize_flag(sub["d1_stable_flag"]).mean())
        d2_rate = float(normalize_flag(sub["d2_sellable_flag"]).mean())
        composite_rate = float(normalize_flag(sub["success_composite_flag"]).mean())
    else:
        d1_rate = 0.0
        d2_rate = 0.0
        composite_rate = 0.0

    lift = (composite_rate / base_rate) if base_rate > 0 else None
    trusted_hint = bool(
        sample_ratio >= cfg.trusted_min_sample_ratio and (lift or 0.0) >= cfg.trusted_min_lift
    )

    return {
        "rule_group": rule_group,
        "rule_name": rule_name,
        "bucket_scope": bucket_scope,
        "condition_count": condition_count,
        "sample_count": sample_count,
        "sample_ratio": round(sample_ratio, 6),
        "base_success_rate": round(base_rate, 6),
        "d1_stable_rate": round(d1_rate, 6),
        "d2_sellable_rate": round(d2_rate, 6),
        "success_composite_rate": round(composite_rate, 6),
        "composite_lift_vs_base": round(lift, 6) if lift is not None else None,
        "avg_d1_close_ret_pct": round(avg_metric(sub, "d1_close_ret_pct"), 6) if sample_count else None,
        "avg_d2_close_ret_pct": round(avg_metric(sub, "d2_close_ret_pct"), 6) if sample_count else None,
        "avg_d2_high_ret_pct": round(avg_metric(sub, "d2_high_ret_pct"), 6) if sample_count else None,
        "trusted_hint": trusted_hint,
        "note": note,
    }


def summarize_rules(df: pd.DataFrame, cfg: ComboValidationConfig) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    single_condition_notes = {
        "cond_br20_strong": "single official D0 condition",
        "cond_limit_up_space_good": "single official D0 condition",
        "cond_turnover_good": "single official D0 condition",
        "cond_turnover_f_good": "single official D0 condition",
        "cond_range_vol_good": "single official D0 condition",
    }
    combo_notes = {
        "combo_any_3": "umbrella rule: any 3 conditions hit",
        "combo_any_4": "umbrella rule: any 4 conditions hit",
        "combo_all_5": "all five official conditions hit",
    }
    combo_notes.update({name: label for name, _, label in KEY_COMBO_RULES if name not in combo_notes})

    for scope in cfg.bucket_scopes:
        scoped_df = df[bucket_scope_mask(df, scope)].copy()
        if scoped_df.empty:
            continue

        for rule_name, _ in CONDITION_SPECS:
            rows.append(
                build_summary_row(
                    scoped_df,
                    make_rule_mask(scoped_df, rule_name),
                    "single_condition",
                    rule_name,
                    scope,
                    1,
                    single_condition_notes[rule_name],
                    cfg,
                )
            )

        for score in range(6):
            rule_name = f"score_eq_{score}"
            rows.append(
                build_summary_row(
                    scoped_df,
                    make_rule_mask(scoped_df, rule_name),
                    "score_bucket",
                    rule_name,
                    scope,
                    score,
                    f"exact score bucket {score}",
                    cfg,
                )
            )

        for rule_name in ["score_ge_3", "score_ge_4", "score_eq_5"]:
            condition_count = 3 if rule_name == "score_ge_3" else 4 if rule_name == "score_ge_4" else 5
            rows.append(
                build_summary_row(
                    scoped_df,
                    make_rule_mask(scoped_df, rule_name),
                    "score_bucket",
                    rule_name,
                    scope,
                    condition_count,
                    "score threshold summary",
                    cfg,
                )
            )

        for rule_name, members, _ in KEY_COMBO_RULES:
            rows.append(
                build_summary_row(
                    scoped_df,
                    make_rule_mask(scoped_df, rule_name),
                    "combo_rule",
                    rule_name,
                    scope,
                    len(members),
                    combo_notes[rule_name],
                    cfg,
                )
            )

        for rule_name, condition_count in [("combo_any_3", 3), ("combo_any_4", 4)]:
            rows.append(
                build_summary_row(
                    scoped_df,
                    make_rule_mask(scoped_df, rule_name),
                    "combo_rule",
                    rule_name,
                    scope,
                    condition_count,
                    combo_notes[rule_name],
                    cfg,
                )
            )

    return pd.DataFrame(rows)


def build_detail_df(df: pd.DataFrame) -> pd.DataFrame:
    detail_columns = [
        "股票代码",
        "股票名称",
        "setup_date",
        "research_bucket",
        "hard_pass",
        "official_d0_score",
        "official_d0_tier",
        "official_d0_hit_rules",
        "official_d0_miss_rules",
        "cond_br20_strong",
        "cond_limit_up_space_good",
        "cond_turnover_good",
        "cond_turnover_f_good",
        "cond_range_vol_good",
        "success_composite_flag",
        "d1_stable_flag",
        "d2_sellable_flag",
        "d1_close_ret_pct",
        "d2_close_ret_pct",
        "d2_high_ret_pct",
    ]
    existing = [column for column in detail_columns if column in df.columns]
    return df[existing].copy()


def summary_table(summary_df: pd.DataFrame, rule_group: str, scope: str, top_n: int = 10) -> pd.DataFrame:
    sub = summary_df[
        (summary_df["rule_group"] == rule_group)
        & (summary_df["bucket_scope"] == scope)
    ].copy()
    if sub.empty:
        return sub
    return sub.sort_values(
        ["trusted_hint", "composite_lift_vs_base", "sample_ratio"],
        ascending=[False, False, False],
    ).head(top_n)


def compare_candidate_watch(summary_df: pd.DataFrame) -> pd.DataFrame:
    sub = summary_df[
        (summary_df["rule_group"] == "combo_rule")
        & (summary_df["bucket_scope"].isin(["候选", "观察"]))
    ][
        [
            "rule_name",
            "bucket_scope",
            "sample_count",
            "sample_ratio",
            "base_success_rate",
            "success_composite_rate",
            "composite_lift_vs_base",
        ]
    ].copy()
    if sub.empty:
        return sub
    return sub.sort_values(["rule_name", "bucket_scope"]).reset_index(drop=True)


def choose_balanced_rule(summary_df: pd.DataFrame, scope: str) -> pd.Series | None:
    sub = summary_df[
        (summary_df["bucket_scope"] == scope)
        & (summary_df["sample_count"] > 0)
        & (
            (
                (summary_df["rule_group"] == "score_bucket")
                & (summary_df["rule_name"].isin(BALANCED_ALLOWED_SCORE_RULES))
            )
            | (summary_df["rule_group"] == "combo_rule")
        )
    ].copy()
    if sub.empty:
        return None
    trusted = sub[sub["trusted_hint"] == True].copy()
    target = trusted if not trusted.empty else sub
    target["_balance_score"] = target["sample_ratio"].fillna(0.0) * target["composite_lift_vs_base"].fillna(0.0)
    target = target.sort_values(["_balance_score", "sample_ratio"], ascending=[False, False])
    return target.iloc[0]


def choose_small_sample_winner(summary_df: pd.DataFrame, scope: str) -> pd.Series | None:
    sub = summary_df[
        (summary_df["bucket_scope"] == scope)
        & (summary_df["sample_count"] > 0)
        & (summary_df["sample_ratio"] < 0.03)
    ].copy()
    if sub.empty:
        return None
    sub = sub.sort_values(["composite_lift_vs_base", "sample_count"], ascending=[False, False])
    return sub.iloc[0]


def scope_base_rate(summary_df: pd.DataFrame, scope: str) -> float | None:
    sub = summary_df[summary_df["bucket_scope"] == scope].copy()
    if sub.empty:
        return None
    values = pd.to_numeric(sub["base_success_rate"], errors="coerce").dropna()
    if values.empty:
        return None
    return float(values.iloc[0])


def build_markdown_report(summary_df: pd.DataFrame, cfg: ComboValidationConfig, dataset_name: str) -> str:
    scope_labels = {
        "all": "全样本",
        "non_abandon": "non_abandon",
        "候选": "候选",
        "观察": "观察",
        "入围": "入围",
    }
    baseline_lines: list[str] = []
    for scope in ["all", "non_abandon", "候选", "观察", "入围"]:
        base_rate = scope_base_rate(summary_df, scope)
        if base_rate is not None:
            baseline_lines.append(f"- `{scope_labels.get(scope, scope)}` baseline = `{base_rate:.4f}`")
    if not baseline_lines:
        baseline_lines = ["基准成功率未计算"]

    single_top = summary_table(summary_df, "single_condition", "all", top_n=5)
    score_all = summary_df[
        (summary_df["rule_group"] == "score_bucket")
        & (summary_df["bucket_scope"] == "all")
    ].copy()
    combo_top = summary_table(summary_df, "combo_rule", "all", top_n=8)
    candidate_watch = compare_candidate_watch(summary_df)

    balanced_all = choose_balanced_rule(summary_df, "all")
    balanced_candidate = choose_balanced_rule(summary_df, "候选")
    small_sample = choose_small_sample_winner(summary_df, "all")

    conclusion_lines = []
    score_ge_3 = summary_df[
        (summary_df["rule_name"] == "score_ge_3")
        & (summary_df["bucket_scope"] == "all")
    ]
    if not score_ge_3.empty:
        row = score_ge_3.iloc[0]
        conclusion_lines.append(
            f"- `score>=3` 在全样本上的 success_composite_rate 为 `{row['success_composite_rate']:.4f}`，base_success_rate 为 `{row['base_success_rate']:.4f}`，lift 为 `{row['composite_lift_vs_base']:.4f}`。"
        )

    score_ge_4 = summary_df[
        (summary_df["rule_name"] == "score_ge_4")
        & (summary_df["bucket_scope"] == "all")
    ]
    if not score_ge_4.empty:
        row = score_ge_4.iloc[0]
        conclusion_lines.append(
            f"- `score>=4` 的样本占比为 `{row['sample_ratio']:.4f}`，success_composite_rate 为 `{row['success_composite_rate']:.4f}`，base_success_rate 为 `{row['base_success_rate']:.4f}`，可用来判断是否值得做更强池。"
        )

    if balanced_all is not None:
        conclusion_lines.append(
            f"- 全样本里样本量与提升更平衡的规则是 `{balanced_all['rule_name']}`，sample_ratio=`{balanced_all['sample_ratio']:.4f}`，success/base=`{balanced_all['success_composite_rate']:.4f}`/`{balanced_all['base_success_rate']:.4f}`，lift=`{balanced_all['composite_lift_vs_base']:.4f}`。"
        )

    if balanced_candidate is not None:
        conclusion_lines.append(
            f"- 候选池里更平衡的规则是 `{balanced_candidate['rule_name']}`，sample_ratio=`{balanced_candidate['sample_ratio']:.4f}`，success/base=`{balanced_candidate['success_composite_rate']:.4f}`/`{balanced_candidate['base_success_rate']:.4f}`，lift=`{balanced_candidate['composite_lift_vs_base']:.4f}`。"
        )

    if small_sample is not None:
        conclusion_lines.append(
            f"- 需要警惕的小样本高光规则是 `{small_sample['rule_name']}`，它的 sample_ratio 只有 `{small_sample['sample_ratio']:.4f}`。"
        )

    conclusion_section = conclusion_lines if conclusion_lines else ["- 当前结果不足以形成明确结论。"]

    lines = [
        "# Official D0 组合验证报告（P12）",
        "",
        f"- 数据集: `{dataset_name}`",
        f"- 样本筛选: `{cfg.sample_filter}`",
        f"- 主成功标签: `{cfg.primary_success_label}`",
        f"- 阈值来源: `scan_config.official_d0_logic_v2` 优先，缺失时回退 `research_config.combo_validation.official_d0_logic_v2`",
        "",
        "## 基准成功率",
        "",
        *baseline_lines,
        "",
        "## 单条件结果 Top",
        "",
        single_top.to_markdown(index=False) if not single_top.empty else "无单条件结果。",
        "",
        "## Score 分层结果",
        "",
        score_all.to_markdown(index=False) if not score_all.empty else "无 score 分层结果。",
        "",
        "## 关键组合结果 Top",
        "",
        combo_top.to_markdown(index=False) if not combo_top.empty else "无关键组合结果。",
        "",
        "## 候选 / 观察 差异",
        "",
        candidate_watch.to_markdown(index=False) if not candidate_watch.empty else "无候选/观察交叉结果。",
        "",
        "## 初步结论",
        "",
    ]
    lines.extend(conclusion_section)
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    dataset_path = Path(args.dataset)
    if not dataset_path.exists():
        raise FileNotFoundError(f"未找到研究样本文件: {dataset_path}")

    config_path = Path(args.research_config) if args.research_config else None
    cfg = load_combo_validation_config(config_path)
    df = pd.read_parquet(dataset_path)
    ensure_required_columns(df, cfg.sample_filter)
    filtered = apply_sample_filter(df, cfg.sample_filter)
    if filtered.empty:
        raise ValueError("样本过滤后为空，无法做 P12 组合验证")

    work = compute_condition_columns(filtered, cfg)
    summary_df = summarize_rules(work, cfg)
    detail_df = build_detail_df(work)

    out_dir = dataset_path.parent
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    config_tag = derive_config_tag(config_path, cfg.sample_filter)
    summary_file = out_dir / f"p12_combo_validation_summary_{config_tag}_{stamp}.csv"
    detail_file = out_dir / f"p12_combo_validation_detail_{config_tag}_{stamp}.csv"
    report_file = out_dir / f"p12_combo_validation_report_{config_tag}_{stamp}.md"

    summary_df.to_csv(summary_file, index=False, encoding="utf-8-sig")
    detail_df.to_csv(detail_file, index=False, encoding="utf-8-sig")
    report_file.write_text(build_markdown_report(summary_df, cfg, dataset_path.name), encoding="utf-8")

    print(f"P12 summary: {summary_file}")
    print(f"P12 detail: {detail_file}")
    print(f"P12 report: {report_file}")


if __name__ == "__main__":
    main()
