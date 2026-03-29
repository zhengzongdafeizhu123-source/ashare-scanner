import json
from pathlib import Path


DEFAULT_SCAN_CONFIG = {
    "hard_filters": {
        "volatility_window": 90,
        "volatility_max": 0.20,
        "require_bullish": True,
        "volume_multiplier": 3.0,
        "turnover_min": 10.0,
        "min_history_bars": 90
    },
    "label_rules": {
        "candidate": {
            "vr5_min": 1.8,
            "clv_min": 0.3,
            "br20_min": 0.98
        },
        "watch": {
            "vr5_min": 1.2,
            "clv_min": 0.0,
            "br20_min": 0.95
        }
    }
}


def merge_dict(defaults, custom):
    result = {}

    for key, default_value in defaults.items():
        custom_value = custom.get(key)

        if isinstance(default_value, dict):
            if not isinstance(custom_value, dict):
                custom_value = {}
            result[key] = merge_dict(default_value, custom_value)
        else:
            result[key] = default_value if custom_value is None else custom_value

    for key, custom_value in custom.items():
        if key not in result:
            result[key] = custom_value

    return result


def load_scan_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"未找到配置文件: {config_path}")

    try:
        config_raw = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"配置文件读取失败: {repr(e)}")

    if not isinstance(config_raw, dict):
        raise ValueError("scan_config.json 顶层必须是 JSON 对象")

    return merge_dict(DEFAULT_SCAN_CONFIG, config_raw)


def safe_float(value, field_name):
    try:
        return float(value)
    except Exception:
        raise ValueError(f"{field_name} 无法转成 float: {value}")


def calc_range_volatility(df_window):
    high_n = df_window["最高"].astype(float).max()
    low_n = df_window["最低"].astype(float).min()

    if low_n <= 0:
        raise ValueError("窗口最低价异常")

    return high_n / low_n - 1


def calc_clv(high_price, low_price, close_price):
    denominator = high_price - low_price
    if denominator == 0:
        return 0
    return ((close_price - low_price) - (high_price - close_price)) / denominator


def calc_scan_metrics(df, hard_filters):
    latest = df.iloc[-1]
    prev = df.iloc[-2]

    volatility_window = int(hard_filters["volatility_window"])
    df_window = df.iloc[-volatility_window:].copy()

    latest_open = safe_float(latest["开盘"], "开盘")
    latest_close = safe_float(latest["收盘"], "收盘")
    latest_high = safe_float(latest["最高"], "最高")
    latest_low = safe_float(latest["最低"], "最低")
    latest_volume = safe_float(latest["成交量"], "成交量")
    latest_amount = safe_float(latest["成交额"], "成交额")
    latest_turnover = safe_float(latest["换手率"], "换手率")

    prev_volume = safe_float(prev["成交量"], "前一日成交量")
    prev_close = safe_float(prev["收盘"], "前一日收盘")

    range_vol = calc_range_volatility(df_window)

    volume_ratio_prev1 = latest_volume / prev_volume if prev_volume != 0 else 0

    prev_5_avg_vol = df.iloc[-6:-1]["成交量"].astype(float).mean()
    vr5 = latest_volume / prev_5_avg_vol if prev_5_avg_vol != 0 else 0

    prev_20_high = df.iloc[-21:-1]["最高"].astype(float).max()
    br20 = latest_close / prev_20_high if prev_20_high != 0 else 0

    clv = calc_clv(latest_high, latest_low, latest_close)
    pct_change = (latest_close / prev_close - 1) * 100 if prev_close != 0 else 0

    return {
        "latest_open": latest_open,
        "latest_close": latest_close,
        "latest_high": latest_high,
        "latest_low": latest_low,
        "latest_volume": latest_volume,
        "latest_amount": latest_amount,
        "latest_turnover": latest_turnover,
        "prev_volume": prev_volume,
        "prev_close": prev_close,
        "range_vol": range_vol,
        "volume_ratio_prev1": volume_ratio_prev1,
        "vr5": vr5,
        "clv": clv,
        "br20": br20,
        "pct_change": pct_change,
    }


def evaluate_hard_filters(metrics, hard_filters):
    volatility_window = int(hard_filters["volatility_window"])
    volatility_max = float(hard_filters["volatility_max"])
    require_bullish = bool(hard_filters["require_bullish"])
    volume_multiplier = float(hard_filters["volume_multiplier"])
    turnover_min = float(hard_filters["turnover_min"])

    rule_low_vol = metrics["range_vol"] <= volatility_max
    rule_bullish = metrics["latest_close"] > metrics["latest_open"] if require_bullish else True
    rule_big_volume = metrics["volume_ratio_prev1"] >= volume_multiplier
    rule_turnover = metrics["latest_turnover"] > turnover_min

    failed_reasons = []
    if not rule_low_vol:
        failed_reasons.append(f"{volatility_window}日波动率>{volatility_max:.0%}")
    if require_bullish and not rule_bullish:
        failed_reasons.append("不是阳线")
    if not rule_big_volume:
        failed_reasons.append(f"未达到{volume_multiplier}倍放量")
    if not rule_turnover:
        failed_reasons.append(f"换手率<={turnover_min}%")

    hit_count = (
        int(rule_low_vol)
        + int(rule_bullish)
        + int(rule_big_volume)
        + int(rule_turnover)
    )

    return {
        "passed": rule_low_vol and rule_bullish and rule_big_volume and rule_turnover,
        "hit_count": hit_count,
        "rule_low_vol": rule_low_vol,
        "rule_bullish": rule_bullish,
        "rule_big_volume": rule_big_volume,
        "rule_turnover": rule_turnover,
        "failed_reasons": failed_reasons,
    }


def evaluate_label_rules(metrics, label_rules):
    candidate_cfg = label_rules.get("candidate", {})
    watch_cfg = label_rules.get("watch", {})

    candidate_vr5_min = float(candidate_cfg.get("vr5_min", 1.8))
    candidate_clv_min = float(candidate_cfg.get("clv_min", 0.3))
    candidate_br20_min = float(candidate_cfg.get("br20_min", 0.98))

    watch_vr5_min = float(watch_cfg.get("vr5_min", 1.2))
    watch_clv_min = float(watch_cfg.get("clv_min", 0.0))
    watch_br20_min = float(watch_cfg.get("br20_min", 0.95))

    is_candidate = (
        metrics["vr5"] >= candidate_vr5_min
        and metrics["clv"] >= candidate_clv_min
        and metrics["br20"] >= candidate_br20_min
    )

    is_watch = (
        metrics["vr5"] >= watch_vr5_min
        and metrics["clv"] >= watch_clv_min
        and metrics["br20"] >= watch_br20_min
    )

    if is_candidate:
        label = "候选"
        label_rank = 3
    elif is_watch:
        label = "观察"
        label_rank = 2
    else:
        label = "放弃"
        label_rank = 1

    return {
        "label": label,
        "label_rank": label_rank,
        "is_candidate": is_candidate,
        "is_watch": is_watch,
    }
