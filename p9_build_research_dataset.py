from __future__ import annotations

import argparse
import gc
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shutil
import sys

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    pa = None
    pq = None

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_BASE_DIR = Path(r"W:\AshareScanner")
APP_CONFIG_FILE = PROJECT_DIR / "app_config.json"
SCAN_CONFIG_FILE = PROJECT_DIR / "scan_config.json"
DEFAULT_RESEARCH_CONFIG_FILE = PROJECT_DIR / "research_config.json"
PREVIEW_ROW_LIMIT = 200_000
DEFAULT_BATCH_SIZE_SYMBOLS = 200
PACK_REQUIRED_COLS = ["股票代码", "股票名称", "日期", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"]
NULLABLE_INT_COLUMNS = ["d0_hit_count", "d1_stable_score", "list_age_days"]
RAW_MAP_SPECS = {
    "daily_basic": {
        "path_parts": ("daily_basic", "daily_basic.parquet"),
        "value_fields": ["turnover_rate_f", "volume_ratio", "total_mv", "circ_mv", "free_share"],
    },
    "adj_factor": {
        "path_parts": ("adj_factor", "adj_factor.parquet"),
        "value_fields": ["adj_factor"],
    },
    "stk_limit": {
        "path_parts": ("stk_limit", "stk_limit.parquet"),
        "value_fields": ["up_limit", "down_limit"],
    },
    "moneyflow": {
        "path_parts": ("moneyflow", "moneyflow.parquet"),
        "value_fields": [
            "buy_lg_amount",
            "sell_lg_amount",
            "buy_elg_amount",
            "sell_elg_amount",
            "buy_sm_amount",
            "sell_sm_amount",
            "buy_md_amount",
            "sell_md_amount",
        ],
    },
}


@dataclass
class ResearchConfig:
    start_date: str | None
    end_date: str | None
    output_prefix: str
    include_all_rows: bool
    min_future_days: int
    success_labels: dict


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


def resolve_base_dir() -> Path:
    cfg = load_json(APP_CONFIG_FILE)
    return Path(cfg["base_dir"]) if cfg.get("base_dir") else DEFAULT_BASE_DIR


def load_scan_config() -> dict:
    default_cfg = {
        "hard_filters": {
            "volatility_window": 90,
            "volatility_max": 0.35,
            "require_bullish": True,
            "volume_multiplier_min": 2.5,
            "volume_multiplier_max": 5.0,
            "turnover_min": 8.0,
            "cold_volume_window": 60,
            "cold_volume_ratio": 0.8,
            "min_history_bars": 90,
        },
        "label_rules": {
            "candidate": {"vr5_min": 1.8, "clv_min": 0.3, "br20_min": 0.98},
            "watch": {"vr5_min": 1.2, "clv_min": 0.0, "br20_min": 0.95},
        },
    }
    cfg = load_json(SCAN_CONFIG_FILE)
    return deep_merge(default_cfg, cfg)


def load_research_config(path: Path | None) -> ResearchConfig:
    defaults = {
        "success_labels": {
            "d1_stable": {
                "min_rules_hit": 2,
                "require_close_ge_d0": True,
                "require_close_ge_mid": True,
                "require_bullish_close": True,
            },
            "d2_sellable": {
                "min_d2_high_ret_pct": 2.0,
                "allow_target1_hit": True,
            },
            "composite": {
                "mode": "d1_stable_and_d2_sellable"
            },
        }
    }
    cfg = deep_merge(defaults, load_json(path or DEFAULT_RESEARCH_CONFIG_FILE))
    return ResearchConfig(
        start_date=cfg.get("start_date"),
        end_date=cfg.get("end_date"),
        output_prefix=str(cfg.get("output_prefix", "p9_research_dataset")),
        include_all_rows=bool(cfg.get("include_all_rows", False)),
        min_future_days=int(cfg.get("min_future_days", 2)),
        success_labels=cfg.get("success_labels", defaults["success_labels"]),
    )


def calc_clv(high_price: float, low_price: float, close_price: float) -> float:
    denominator = high_price - low_price
    if denominator == 0:
        return 0.0
    return ((close_price - low_price) - (high_price - close_price)) / denominator


def get_label(vr5: float, clv: float, br20: float, label_rules: dict) -> tuple[str, int, bool, bool]:
    candidate_cfg = label_rules["candidate"]
    watch_cfg = label_rules["watch"]
    is_candidate = (
        vr5 >= float(candidate_cfg["vr5_min"])
        and clv >= float(candidate_cfg["clv_min"])
        and br20 >= float(candidate_cfg["br20_min"])
    )
    is_watch = (
        vr5 >= float(watch_cfg["vr5_min"])
        and clv >= float(watch_cfg["clv_min"])
        and br20 >= float(watch_cfg["br20_min"])
    )
    if is_candidate:
        return "候选", 3, True, True
    if is_watch:
        return "观察", 2, False, True
    return "放弃", 1, False, False


def true_range(high_s: pd.Series, low_s: pd.Series, close_s: pd.Series) -> pd.Series:
    prev_close = close_s.shift(1)
    a = (high_s - low_s).abs()
    b = (high_s - prev_close).abs()
    c = (low_s - prev_close).abs()
    return pd.concat([a, b, c], axis=1).max(axis=1)


def compute_atr14(df: pd.DataFrame, idx: int) -> float:
    window = df.iloc[max(0, idx - 13):idx + 1]
    if len(window) < 14:
        return float("nan")
    tr = true_range(window["最高"], window["最低"], window["收盘"])
    return float(tr.mean())


def classify_research_bucket(hard_pass: bool, label: str) -> str:
    if hard_pass:
        return "入围"
    return label


def safe_pct(a: float, b: float) -> float:
    if b == 0:
        return float("nan")
    return (a / b - 1.0) * 100.0


def parse_args():
    parser = argparse.ArgumentParser(description="构建 D0-D1-D2 历史研究样本库（分批省内存版本）")
    parser.add_argument("--research-config", default="", help="研究配置文件路径，默认 research_config.json")
    parser.add_argument("--batch-size-symbols", type=int, default=DEFAULT_BATCH_SIZE_SYMBOLS, help="每批处理股票数量，默认 200")
    return parser.parse_args()


def read_parquet_safe(path: Path, columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        if columns is None:
            return pd.read_parquet(path)
        return pd.read_parquet(path, columns=columns)
    except Exception:
        return pd.DataFrame()


def read_parquet_subset(path: Path, columns: list[str] | None = None, filters: list[tuple[str, str, object]] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        kwargs = {}
        if columns is not None:
            kwargs["columns"] = columns
        if filters:
            kwargs["filters"] = filters
        return pd.read_parquet(path, **kwargs)
    except TypeError:
        try:
            df = pd.read_parquet(path, columns=columns)
        except Exception:
            return pd.DataFrame()
        if not filters or df.empty:
            return df
        for col, op, value in filters:
            if col not in df.columns:
                continue
            if op == "in":
                df = df[df[col].isin(value)]
            elif op == ">=":
                df = df[df[col] >= value]
            elif op == "<=":
                df = df[df[col] <= value]
        return df
    except Exception:
        return pd.DataFrame()


def load_stock_basic_map(base_dir: Path) -> pd.DataFrame:
    root = base_dir / "data" / "research_raw"
    stock_basic = read_parquet_safe(root / "stock_basic" / "stock_basic_latest.parquet")
    if stock_basic.empty:
        return pd.DataFrame()
    if "symbol" in stock_basic.columns:
        stock_basic["股票代码"] = stock_basic["symbol"].astype(str).str.zfill(6)
    elif "股票代码" in stock_basic.columns:
        stock_basic["股票代码"] = stock_basic["股票代码"].astype(str).str.zfill(6)
    else:
        return pd.DataFrame()
    return stock_basic.drop_duplicates(subset=["股票代码"]).set_index("股票代码")


def build_symbol_batches(symbols: list[str], batch_size: int) -> list[list[str]]:
    actual_size = max(1, int(batch_size))
    return [symbols[i : i + actual_size] for i in range(0, len(symbols), actual_size)]


def normalize_pack_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    work["股票代码"] = work["股票代码"].astype(str).str.zfill(6)
    work["股票名称"] = work["股票名称"].astype(str).str.strip()
    work["日期"] = pd.to_datetime(work["日期"], errors="coerce")
    for col in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "换手率"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work = work.dropna(subset=PACK_REQUIRED_COLS).sort_values(["股票代码", "日期"]).reset_index(drop=True)
    return work


def load_pack_symbols(pack_file: Path) -> list[str]:
    df_codes = read_parquet_subset(pack_file, columns=["股票代码"])
    if df_codes.empty:
        return []
    return df_codes["股票代码"].astype(str).str.zfill(6).drop_duplicates().tolist()


def build_ts_code_map(stock_basic_map: pd.DataFrame, symbols: list[str]) -> dict[str, str]:
    rows = {}
    if stock_basic_map.empty:
        return rows
    for symbol in symbols:
        if symbol not in stock_basic_map.index:
            continue
        value = stock_basic_map.loc[symbol]
        if isinstance(value, pd.DataFrame):
            value = value.iloc[0]
        ts_code = str(value.get("ts_code", "")).strip()
        if ts_code:
            rows[symbol] = ts_code
    return rows


def batch_key_map(df: pd.DataFrame, value_fields: list[str]) -> dict[tuple[str, str], dict]:
    if df.empty or "股票代码" not in df.columns or "trade_date" not in df.columns:
        return {}
    keep_cols = ["股票代码", "trade_date"] + [col for col in value_fields if col in df.columns]
    work = df[keep_cols].copy()
    work["股票代码"] = work["股票代码"].astype(str).str.zfill(6)
    work["trade_date"] = pd.to_datetime(work["trade_date"].astype(str), errors="coerce").dt.strftime("%Y-%m-%d")
    work = work.dropna(subset=["trade_date"])
    rows = {}
    for _, row in work.iterrows():
        rows[(str(row["股票代码"]).zfill(6), str(row["trade_date"]))] = row.to_dict()
    return rows


def load_batch_research_raw_maps(base_dir: Path, stock_basic_map: pd.DataFrame, symbols: list[str], start_dt, end_dt) -> dict:
    root = base_dir / "data" / "research_raw"
    ts_code_map = build_ts_code_map(stock_basic_map, symbols)
    ts_codes = sorted(set(ts_code_map.values()))
    batch_maps = {"stock_basic": stock_basic_map}
    if not ts_codes:
        for name in RAW_MAP_SPECS:
            batch_maps[name] = {}
        return batch_maps

    start_key = pd.Timestamp(start_dt).strftime("%Y%m%d") if start_dt is not None else ""
    end_key = pd.Timestamp(end_dt).strftime("%Y%m%d") if end_dt is not None else ""
    base_filters: list[tuple[str, str, object]] = [("ts_code", "in", ts_codes)]
    if start_key:
        base_filters.append(("trade_date", ">=", start_key))
    if end_key:
        base_filters.append(("trade_date", "<=", end_key))

    for name, spec in RAW_MAP_SPECS.items():
        path = root.joinpath(*spec["path_parts"])
        cols = ["ts_code", "trade_date"] + spec["value_fields"]
        df = read_parquet_subset(path, columns=cols, filters=base_filters)
        if not df.empty and "ts_code" in df.columns:
            df["股票代码"] = df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
        batch_maps[name] = batch_key_map(df, spec["value_fields"])
    return batch_maps


def compute_success_labels(d0_close: float, d1_open: float, d1_close: float, d2_high_ret_pct: float,
                           target1_hit: bool, mid_price: float, cfg: dict) -> dict:
    d1_cfg = cfg.get("d1_stable", {})
    d2_cfg = cfg.get("d2_sellable", {})

    d1_rule_close_ge_d0 = d1_close >= d0_close if d1_cfg.get("require_close_ge_d0", True) else True
    d1_rule_close_ge_mid = d1_close >= mid_price if d1_cfg.get("require_close_ge_mid", True) else True
    d1_rule_bullish_close = d1_close >= d1_open if d1_cfg.get("require_bullish_close", True) else True
    d1_stable_score = int(d1_rule_close_ge_d0) + int(d1_rule_close_ge_mid) + int(d1_rule_bullish_close)
    d1_stable_flag = d1_stable_score >= int(d1_cfg.get("min_rules_hit", 2))

    min_d2_high_ret_pct = float(d2_cfg.get("min_d2_high_ret_pct", 2.0))
    d2_rule_high_ret_hit = pd.notna(d2_high_ret_pct) and d2_high_ret_pct >= min_d2_high_ret_pct
    d2_rule_target1_hit = bool(target1_hit) if d2_cfg.get("allow_target1_hit", True) else False
    d2_sellable_flag = bool(d2_rule_high_ret_hit or d2_rule_target1_hit)

    success_composite_flag = bool(d1_stable_flag and d2_sellable_flag)

    return {
        "d1_rule_close_ge_d0": "是" if d1_rule_close_ge_d0 else "否",
        "d1_rule_close_ge_mid": "是" if d1_rule_close_ge_mid else "否",
        "d1_rule_bullish_close": "是" if d1_rule_bullish_close else "否",
        "d1_stable_score": d1_stable_score,
        "d1_stable_flag": "是" if d1_stable_flag else "否",
        "d2_rule_high_ret_hit": "是" if d2_rule_high_ret_hit else "否",
        "d2_rule_target1_hit": "是" if d2_rule_target1_hit else "否",
        "d2_sellable_flag": "是" if d2_sellable_flag else "否",
        "success_composite_flag": "是" if success_composite_flag else "否",
    }


class DatasetSink:
    def __init__(self, final_path: Path, temp_dir: Path):
        self.final_path = final_path
        self.temp_dir = temp_dir
        self.batch_counter = 0
        self.batch_files: list[Path] = []
        self.writer = None

    def write(self, df: pd.DataFrame) -> None:
        if df.empty:
            return
        if pa is not None and pq is not None:
            table = pa.Table.from_pandas(df, preserve_index=False)
            if self.writer is None:
                self.writer = pq.ParquetWriter(self.final_path, table.schema)
            self.writer.write_table(table)
            return
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        batch_file = self.temp_dir / f"dataset_batch_{self.batch_counter:04d}.parquet"
        df.to_parquet(batch_file, index=False)
        self.batch_files.append(batch_file)
        self.batch_counter += 1

    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None

    def finalize_fallback(self) -> None:
        self.close()
        if self.final_path.exists() or not self.batch_files:
            return
        frames = [pd.read_parquet(path) for path in self.batch_files]
        pd.concat(frames, ignore_index=True).to_parquet(self.final_path, index=False)


class PreviewCsvWriter:
    def __init__(self, path: Path, limit: int):
        self.path = path
        self.limit = limit
        self.written_rows = 0
        self.header_written = False
        self.has_data = False

    def write(self, df: pd.DataFrame) -> None:
        if self.written_rows >= self.limit or df.empty:
            return
        chunk = df.head(self.limit - self.written_rows)
        if chunk.empty:
            return
        chunk.to_csv(
            self.path,
            mode="a",
            header=not self.header_written,
            index=False,
            encoding="utf-8-sig" if not self.header_written else "utf-8",
        )
        self.header_written = True
        self.has_data = True
        self.written_rows += len(chunk)

    def finalize_empty(self) -> None:
        if self.has_data:
            return
        pd.DataFrame().to_csv(self.path, index=False, encoding="utf-8-sig")


def process_stock_df(
    df: pd.DataFrame,
    raw_maps: dict,
    hf: dict,
    lr: dict,
    min_future_days: int,
    start_dt,
    end_dt,
    success_label_cfg: dict,
) -> tuple[list[dict], dict | None]:
    if df.empty:
        return [], None

    symbol = str(df.iloc[0]["股票代码"]).zfill(6)
    sb_row = raw_maps["stock_basic"].loc[symbol] if symbol in raw_maps["stock_basic"].index else None
    if isinstance(sb_row, pd.DataFrame):
        sb_row = sb_row.iloc[0]

    vol_window = int(hf["volatility_window"])
    vol_max = float(hf["volatility_max"])
    require_bullish = bool(hf["require_bullish"])
    vol_mult_min = float(hf["volume_multiplier_min"])
    vol_mult_max = float(hf["volume_multiplier_max"])
    turnover_min = float(hf["turnover_min"])
    cold_window = int(hf["cold_volume_window"])
    cold_ratio = float(hf["cold_volume_ratio"])
    min_history = int(hf["min_history_bars"])
    start_bar = max(min_history, vol_window, cold_window + 1, 21, 14)

    if len(df) < start_bar + min_future_days:
        return [], {"股票代码": symbol, "原因": "历史长度不足以生成研究样本"}

    event_rows = []
    for idx in range(start_bar, len(df) - min_future_days):
        d0 = df.iloc[idx]
        d1 = df.iloc[idx + 1]
        d2 = df.iloc[idx + 2]
        d0_date = pd.Timestamp(d0["日期"])
        if start_dt is not None and d0_date < start_dt:
            continue
        if end_dt is not None and d0_date > end_dt:
            continue

        prev = df.iloc[idx - 1]
        window_df = df.iloc[idx - vol_window + 1 : idx + 1]
        cold_df = df.iloc[idx - cold_window : idx]
        if len(window_df) < vol_window or len(cold_df) < cold_window:
            continue

        latest_open = float(d0["开盘"])
        latest_close = float(d0["收盘"])
        latest_high = float(d0["最高"])
        latest_low = float(d0["最低"])
        latest_volume = float(d0["成交量"])
        latest_amount = float(d0["成交额"])
        latest_turnover = float(d0["换手率"])
        prev_close = float(prev["收盘"])
        prev_volume = float(prev["成交量"])

        high_n = float(window_df["最高"].max())
        low_n = float(window_df["最低"].min())
        if low_n <= 0:
            continue

        range_vol = high_n / low_n - 1.0
        volume_ratio_prev1 = latest_volume / prev_volume if prev_volume != 0 else float("nan")
        prev_5_avg_vol = float(df["成交量"].iloc[idx - 5 : idx].mean()) if idx >= 5 else float("nan")
        vr5 = latest_volume / prev_5_avg_vol if pd.notna(prev_5_avg_vol) and prev_5_avg_vol != 0 else float("nan")
        prev_20_high = float(df["最高"].iloc[idx - 20 : idx].max())
        br20 = latest_close / prev_20_high if prev_20_high != 0 else float("nan")
        clv = calc_clv(latest_high, latest_low, latest_close)
        pct_change = safe_pct(latest_close, prev_close)
        atr14 = compute_atr14(df, idx)
        cold_max_volume = float(cold_df["成交量"].max())
        cold_volume_pass = cold_max_volume <= latest_volume * cold_ratio

        rule_low_vol = range_vol <= vol_max
        rule_bullish = (latest_close > latest_open) if require_bullish else True
        rule_volume_min = pd.notna(volume_ratio_prev1) and volume_ratio_prev1 >= vol_mult_min
        rule_volume_max = pd.notna(volume_ratio_prev1) and volume_ratio_prev1 <= vol_mult_max
        rule_turnover = latest_turnover > turnover_min
        hard_pass = all([rule_low_vol, rule_bullish, rule_volume_min, rule_volume_max, rule_turnover, cold_volume_pass])

        failed_reasons = []
        if not rule_low_vol:
            failed_reasons.append(f"{vol_window}日波动率>{vol_max:.0%}")
        if require_bullish and not rule_bullish:
            failed_reasons.append("不是阳线")
        if not rule_volume_min:
            failed_reasons.append(f"未达到{vol_mult_min}倍放量")
        if not rule_volume_max:
            failed_reasons.append(f"超过{vol_mult_max}倍放量")
        if not rule_turnover:
            failed_reasons.append(f"换手率<={turnover_min}%")
        if not cold_volume_pass:
            failed_reasons.append(f"近{cold_window}日出现超过当日{cold_ratio:.0%}的历史量峰")

        hit_count = sum([rule_low_vol, rule_bullish, rule_volume_min, rule_volume_max, rule_turnover, cold_volume_pass])
        label, _, _, _ = get_label(vr5 if pd.notna(vr5) else 0.0, clv, br20 if pd.notna(br20) else 0.0, lr)
        research_bucket = classify_research_bucket(hard_pass, label)

        breakout_price = max(latest_high, prev_20_high)
        support_price_1 = latest_close
        support_price_2 = latest_low
        mid_price = (latest_high + latest_low) / 2.0
        target_price_1 = breakout_price + (0.5 * atr14 if pd.notna(atr14) else 0.0)
        target_price_2 = breakout_price + (1.0 * atr14 if pd.notna(atr14) else 0.0)

        d1_breakout_hit = float(d1["最高"]) >= breakout_price
        d1_close_strong = float(d1["收盘"]) >= support_price_1 and float(d1["收盘"]) >= mid_price
        d1_buyable_flag = bool(d1_breakout_hit and d1_close_strong)
        d2_target1_hit = float(d2["最高"]) >= target_price_1 if pd.notna(target_price_1) else False
        d2_target2_hit = float(d2["最高"]) >= target_price_2 if pd.notna(target_price_2) else False
        d2_high_ret_pct = round(safe_pct(float(d2["最高"]), latest_close), 4)

        success_flags = compute_success_labels(
            d0_close=latest_close,
            d1_open=float(d1["开盘"]),
            d1_close=float(d1["收盘"]),
            d2_high_ret_pct=d2_high_ret_pct,
            target1_hit=bool(d2_target1_hit),
            mid_price=mid_price,
            cfg=success_label_cfg,
        )

        date_key = d0_date.strftime("%Y-%m-%d")
        key = (symbol, date_key)
        db_row = raw_maps["daily_basic"].get(key, {})
        af_row = raw_maps["adj_factor"].get(key, {})
        sl_row = raw_maps["stk_limit"].get(key, {})
        mf_row = raw_maps["moneyflow"].get(key, {})

        d0_turnover_f = db_row.get("turnover_rate_f")
        d0_volume_ratio_basic = db_row.get("volume_ratio")
        d0_total_mv = db_row.get("total_mv")
        d0_circ_mv = db_row.get("circ_mv")
        d0_free_share = db_row.get("free_share")
        d0_adj_factor = af_row.get("adj_factor")
        d0_up_limit = sl_row.get("up_limit")
        d0_down_limit = sl_row.get("down_limit")
        d0_limit_up_space_pct = (
            safe_pct(float(d0_up_limit), latest_close)
            if d0_up_limit not in [None, ""]
            and pd.notna(pd.to_numeric(d0_up_limit, errors="coerce"))
            else float("nan")
        )
        d0_limit_down_space_pct = (
            safe_pct(latest_close, float(d0_down_limit))
            if d0_down_limit not in [None, ""]
            and pd.notna(pd.to_numeric(d0_down_limit, errors="coerce"))
            and float(d0_down_limit) != 0
            else float("nan")
        )

        buy_lg_amount = pd.to_numeric(mf_row.get("buy_lg_amount"), errors="coerce")
        sell_lg_amount = pd.to_numeric(mf_row.get("sell_lg_amount"), errors="coerce")
        buy_elg_amount = pd.to_numeric(mf_row.get("buy_elg_amount"), errors="coerce")
        sell_elg_amount = pd.to_numeric(mf_row.get("sell_elg_amount"), errors="coerce")
        buy_sm_amount = pd.to_numeric(mf_row.get("buy_sm_amount"), errors="coerce")
        sell_sm_amount = pd.to_numeric(mf_row.get("sell_sm_amount"), errors="coerce")
        buy_md_amount = pd.to_numeric(mf_row.get("buy_md_amount"), errors="coerce")
        sell_md_amount = pd.to_numeric(mf_row.get("sell_md_amount"), errors="coerce")

        d0_big_order_net_amount = (
            (buy_lg_amount + buy_elg_amount) - (sell_lg_amount + sell_elg_amount)
            if pd.notna(buy_lg_amount)
            and pd.notna(sell_lg_amount)
            and pd.notna(buy_elg_amount)
            and pd.notna(sell_elg_amount)
            else float("nan")
        )
        d0_small_mid_net_amount = (
            (buy_sm_amount + buy_md_amount) - (sell_sm_amount + sell_md_amount)
            if pd.notna(buy_sm_amount)
            and pd.notna(sell_sm_amount)
            and pd.notna(buy_md_amount)
            and pd.notna(sell_md_amount)
            else float("nan")
        )
        d0_big_order_net_ratio = (
            d0_big_order_net_amount / latest_amount
            if pd.notna(d0_big_order_net_amount) and latest_amount != 0
            else float("nan")
        )

        list_date_text = None if sb_row is None else sb_row.get("list_date")
        list_age_days = None
        if list_date_text:
            list_date_dt = pd.to_datetime(str(list_date_text), errors="coerce")
            if pd.notna(list_date_dt):
                list_age_days = int((d0_date - list_date_dt).days)

        row = {
            "股票代码": symbol,
            "股票名称": str(d0["股票名称"]).strip(),
            "setup_date": date_key,
            "research_bucket": research_bucket,
            "hard_pass": "是" if hard_pass else "否",
            "d0_label": label,
            "d0_hit_count": hit_count,
            "d0_failed_reason": "；".join(failed_reasons) if failed_reasons else "",
            "d0_open": round(latest_open, 4),
            "d0_close": round(latest_close, 4),
            "d0_high": round(latest_high, 4),
            "d0_low": round(latest_low, 4),
            "d0_pct_change": round(pct_change, 4),
            "d0_turnover": round(latest_turnover, 4),
            "d0_amount": round(latest_amount, 4),
            "d0_volume": round(latest_volume, 4),
            "d0_range_vol": round(range_vol, 6),
            "volume_ratio_prev1": round(volume_ratio_prev1, 6) if pd.notna(volume_ratio_prev1) else None,
            "vr5": round(vr5, 6) if pd.notna(vr5) else None,
            "clv": round(clv, 6),
            "br20": round(br20, 6) if pd.notna(br20) else None,
            "cold_max_volume": round(cold_max_volume, 4),
            "cold_volume_pass": "是" if cold_volume_pass else "否",
            "breakout_price": round(breakout_price, 4),
            "support_price_1": round(support_price_1, 4),
            "support_price_2": round(support_price_2, 4),
            "mid_price": round(mid_price, 4),
            "atr14": round(atr14, 6) if pd.notna(atr14) else None,
            "target_price_1": round(target_price_1, 4) if pd.notna(target_price_1) else None,
            "target_price_2": round(target_price_2, 4) if pd.notna(target_price_2) else None,
            "d1_date": pd.Timestamp(d1["日期"]).strftime("%Y-%m-%d"),
            "d1_open": round(float(d1["开盘"]), 4),
            "d1_high": round(float(d1["最高"]), 4),
            "d1_low": round(float(d1["最低"]), 4),
            "d1_close": round(float(d1["收盘"]), 4),
            "d1_gap_pct": round(safe_pct(float(d1["开盘"]), latest_close), 4),
            "d1_close_ret_pct": round(safe_pct(float(d1["收盘"]), latest_close), 4),
            "d1_high_ret_pct": round(safe_pct(float(d1["最高"]), latest_close), 4),
            "d1_low_ret_pct": round(safe_pct(float(d1["最低"]), latest_close), 4),
            "d1_breakout_hit": "是" if d1_breakout_hit else "否",
            "d1_close_strong": "是" if d1_close_strong else "否",
            "d1_buyable_flag": "是" if d1_buyable_flag else "否",
            "d2_date": pd.Timestamp(d2["日期"]).strftime("%Y-%m-%d"),
            "d2_open": round(float(d2["开盘"]), 4),
            "d2_high": round(float(d2["最高"]), 4),
            "d2_low": round(float(d2["最低"]), 4),
            "d2_close": round(float(d2["收盘"]), 4),
            "d2_open_ret_pct": round(safe_pct(float(d2["开盘"]), latest_close), 4),
            "d2_close_ret_pct": round(safe_pct(float(d2["收盘"]), latest_close), 4),
            "d2_high_ret_pct": d2_high_ret_pct,
            "d2_low_ret_pct": round(safe_pct(float(d2["最低"]), latest_close), 4),
            "d2_target1_hit": "是" if d2_target1_hit else "否",
            "d2_target2_hit": "是" if d2_target2_hit else "否",
            "industry": None if sb_row is None else sb_row.get("industry"),
            "market": None if sb_row is None else sb_row.get("market"),
            "exchange": None if sb_row is None else sb_row.get("exchange"),
            "is_hs": None if sb_row is None else sb_row.get("is_hs"),
            "list_age_days": list_age_days,
            "d0_turnover_f": float(d0_turnover_f) if pd.notna(pd.to_numeric(d0_turnover_f, errors="coerce")) else None,
            "d0_volume_ratio_basic": float(d0_volume_ratio_basic) if pd.notna(pd.to_numeric(d0_volume_ratio_basic, errors="coerce")) else None,
            "d0_total_mv": float(d0_total_mv) if pd.notna(pd.to_numeric(d0_total_mv, errors="coerce")) else None,
            "d0_circ_mv": float(d0_circ_mv) if pd.notna(pd.to_numeric(d0_circ_mv, errors="coerce")) else None,
            "d0_free_share": float(d0_free_share) if pd.notna(pd.to_numeric(d0_free_share, errors="coerce")) else None,
            "d0_adj_factor": float(d0_adj_factor) if pd.notna(pd.to_numeric(d0_adj_factor, errors="coerce")) else None,
            "d0_up_limit": float(d0_up_limit) if pd.notna(pd.to_numeric(d0_up_limit, errors="coerce")) else None,
            "d0_down_limit": float(d0_down_limit) if pd.notna(pd.to_numeric(d0_down_limit, errors="coerce")) else None,
            "d0_limit_up_space_pct": round(d0_limit_up_space_pct, 6) if pd.notna(d0_limit_up_space_pct) else None,
            "d0_limit_down_space_pct": round(d0_limit_down_space_pct, 6) if pd.notna(d0_limit_down_space_pct) else None,
            "d0_big_order_net_amount": round(float(d0_big_order_net_amount), 6) if pd.notna(d0_big_order_net_amount) else None,
            "d0_small_mid_net_amount": round(float(d0_small_mid_net_amount), 6) if pd.notna(d0_small_mid_net_amount) else None,
            "d0_big_order_net_ratio": round(float(d0_big_order_net_ratio), 8) if pd.notna(d0_big_order_net_ratio) else None,
        }
        row.update(success_flags)
        event_rows.append(row)
    return event_rows, None


def summarize_batch(batch_df: pd.DataFrame) -> dict[str, int]:
    if batch_df.empty:
        return {
            "row_count": 0,
            "hard_pass_count": 0,
            "bucket_selected_count": 0,
            "bucket_candidate_count": 0,
            "bucket_watch_count": 0,
            "bucket_abandon_count": 0,
            "d1_stable_count": 0,
            "d2_sellable_count": 0,
            "success_composite_count": 0,
        }
    return {
        "row_count": len(batch_df),
        "hard_pass_count": int(batch_df["hard_pass"].eq("是").sum()),
        "bucket_selected_count": int(batch_df["research_bucket"].eq("入围").sum()),
        "bucket_candidate_count": int(batch_df["research_bucket"].eq("候选").sum()),
        "bucket_watch_count": int(batch_df["research_bucket"].eq("观察").sum()),
        "bucket_abandon_count": int(batch_df["research_bucket"].eq("放弃").sum()),
        "d1_stable_count": int(batch_df["d1_stable_flag"].eq("是").sum()),
        "d2_sellable_count": int(batch_df["d2_sellable_flag"].eq("是").sum()),
        "success_composite_count": int(batch_df["success_composite_flag"].eq("是").sum()),
    }


def coerce_output_schema(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    for col in NULLABLE_INT_COLUMNS:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce").astype("Int64")
    return work


def main():
    base_dir = resolve_base_dir()
    pack_file = base_dir / "data" / "packed" / "daily_hist_all.parquet"
    output_dir = base_dir / "output" / "research"
    logs_dir = base_dir / "logs"
    output_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    args = parse_args()
    research_cfg = load_research_config(Path(args.research_config) if args.research_config else None)
    scan_cfg = load_scan_config()
    stock_basic_map = load_stock_basic_map(base_dir)

    if not pack_file.exists():
        raise FileNotFoundError(f"未找到 parquet 文件: {pack_file}")

    start_dt = pd.to_datetime(research_cfg.start_date) if research_cfg.start_date else None
    end_dt = pd.to_datetime(research_cfg.end_date) if research_cfg.end_date else None
    batch_symbols = load_pack_symbols(pack_file)
    if not batch_symbols:
        raise ValueError(f"未在 parquet 中读取到任何股票代码: {pack_file}")

    batches = build_symbol_batches(batch_symbols, args.batch_size_symbols)
    now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = research_cfg.output_prefix
    data_parquet = output_dir / f"{prefix}_{now_tag}.parquet"
    data_csv = output_dir / f"{prefix}_{now_tag}.csv"
    summary_csv = output_dir / f"{prefix}_summary_{now_tag}.csv"
    skip_csv = output_dir / f"{prefix}_skipped_{now_tag}.csv"
    log_file = logs_dir / f"{prefix}_{now_tag}.log"
    temp_dir = output_dir / f"_{prefix}_tmp_{now_tag}"

    sink = DatasetSink(data_parquet, temp_dir)
    preview_writer = PreviewCsvWriter(data_csv, PREVIEW_ROW_LIMIT)
    skip_rows: list[dict] = []
    totals = {
        "row_count": 0,
        "hard_pass_count": 0,
        "bucket_selected_count": 0,
        "bucket_candidate_count": 0,
        "bucket_watch_count": 0,
        "bucket_abandon_count": 0,
        "d1_stable_count": 0,
        "d2_sellable_count": 0,
        "success_composite_count": 0,
    }

    print(f"[信息] 股票总数: {len(batch_symbols)}")
    print(f"[信息] 批次数量: {len(batches)}")
    print(f"[信息] 每批股票数: {max(1, int(args.batch_size_symbols))}")

    try:
        for batch_idx, symbols in enumerate(batches, start=1):
            print(f"[批次] {batch_idx}/{len(batches)} 股票数={len(symbols)}")
            pack_df = read_parquet_subset(pack_file, columns=PACK_REQUIRED_COLS, filters=[("股票代码", "in", symbols)])
            pack_df = normalize_pack_df(pack_df)
            if pack_df.empty:
                continue

            batch_start_dt = start_dt
            batch_end_dt = end_dt
            if batch_start_dt is None and not pack_df.empty:
                batch_start_dt = pack_df["日期"].min()
            if batch_end_dt is None and not pack_df.empty:
                batch_end_dt = pack_df["日期"].max()

            raw_maps = load_batch_research_raw_maps(base_dir, stock_basic_map, symbols, batch_start_dt, batch_end_dt)
            batch_event_rows: list[dict] = []
            stock_groups = pack_df.groupby("股票代码", sort=False)

            for stock_idx, (symbol, stock_df) in enumerate(stock_groups, start=1):
                if stock_idx == 1 or stock_idx % 100 == 0 or stock_idx == len(symbols):
                    print(f"[进度] 批次{batch_idx} {stock_idx}/{len(symbols)} {symbol}")
                stock_df = stock_df.reset_index(drop=True)
                rows, skip_row = process_stock_df(
                    stock_df,
                    raw_maps,
                    scan_cfg["hard_filters"],
                    scan_cfg["label_rules"],
                    int(research_cfg.min_future_days),
                    start_dt,
                    end_dt,
                    research_cfg.success_labels,
                )
                if skip_row is not None:
                    skip_rows.append(skip_row)
                if rows:
                    batch_event_rows.extend(rows)

            if batch_event_rows:
                batch_df = coerce_output_schema(pd.DataFrame(batch_event_rows))
                batch_stats = summarize_batch(batch_df)
                for key, value in batch_stats.items():
                    totals[key] += int(value)
                sink.write(batch_df)
                preview_writer.write(batch_df)
            del pack_df
            del raw_maps
            del batch_event_rows
            gc.collect()

        sink.close()
        sink.finalize_fallback()
        preview_writer.finalize_empty()
    finally:
        sink.close()

    skips = pd.DataFrame(skip_rows)
    skips.to_csv(skip_csv, index=False, encoding="utf-8-sig")
    summary = pd.DataFrame([{
        "pack_file": str(pack_file),
        "row_count": totals["row_count"],
        "skip_stock_count": len(skips),
        "hard_pass_count": totals["hard_pass_count"],
        "bucket_selected_count": totals["bucket_selected_count"],
        "bucket_candidate_count": totals["bucket_candidate_count"],
        "bucket_watch_count": totals["bucket_watch_count"],
        "bucket_abandon_count": totals["bucket_abandon_count"],
        "d1_stable_count": totals["d1_stable_count"],
        "d2_sellable_count": totals["d2_sellable_count"],
        "success_composite_count": totals["success_composite_count"],
        "start_date": research_cfg.start_date or "",
        "end_date": research_cfg.end_date or "",
        "output_parquet": str(data_parquet),
        "output_csv_head": str(data_csv),
        "skip_csv": str(skip_csv),
    }])
    summary.to_csv(summary_csv, index=False, encoding="utf-8-sig")
    log_lines = [
        "P9 research dataset build finished",
        f"pack_file={pack_file}",
        f"output_parquet={data_parquet}",
        f"summary_csv={summary_csv}",
        f"row_count={totals['row_count']}",
        f"research_raw_root={base_dir / 'data' / 'research_raw'}",
        f"batch_size_symbols={max(1, int(args.batch_size_symbols))}",
        f"batch_count={len(batches)}",
    ]
    log_file.write_text("\n".join(log_lines), encoding="utf-8")

    print(f"研究样本已生成: {data_parquet}")
    print(f"样本行数: {totals['row_count']}")
    print(f"汇总文件: {summary_csv}")
    print(f"日志文件: {log_file}")
    if temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
