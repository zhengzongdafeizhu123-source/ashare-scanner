# Tushare 迁移说明（最小落地版）

这套文件的目标不是一次性推翻现有项目，而是**先替换最不稳定的更新层**：

- 保留现有 `data/daily_hist/*.csv`
- 保留现有 `p6b_pack_hist_to_parquet.py`
- 保留现有 `p7_scan_from_parquet_all.py`
- 只把“按股票逐只更新”改成“按交易日批量拉全市场再回写本地 CSV”

## 文件说明

- `p6_update_daily_hist_tushare.py`
  - 日常增量更新脚本
  - 基于本地已有 CSV 库
  - 通过 Tushare 的 `daily(trade_date)` + `daily_basic(trade_date)` 按交易日批量获取全市场数据
  - 再写回现有单股票 CSV

- `p4_bootstrap_hist_all_tushare.py`
  - 初始补库 / 回补脚本
  - 适合回补最近 260 个交易日，或为新环境做第一版历史库

- `tushare_config.example.json`
  - Token 模板
  - 复制为 `tushare_config.json` 后填入自己的 token

## 使用前准备

1. 安装 tushare
```bash
pip install tushare
```

2. 配置 token
两种方式二选一：

### 方式 A：环境变量
```bash
set TUSHARE_TOKEN=你的token
```

### 方式 B：项目根目录配置文件
复制 `tushare_config.example.json` 为 `tushare_config.json`，填入 token

## 推荐使用顺序

### 情况 1：你已经有完整本地历史库，只是每天更新不稳定
直接用：
```bash
python p6_update_daily_hist_tushare.py
```

### 情况 2：你要补建一段最近历史
例如回补最近 400 个自然日（大约覆盖 260 个交易日）：
```bash
python p4_bootstrap_hist_all_tushare.py --start-date 20240101 --end-date 20260330
```

## 设计说明

- `daily` 是未复权日线
- `daily_basic` 提供 `turnover_rate`
- 脚本把 Tushare `amount`（千元）转换成“元”，尽量贴近现有 CSV 口径
- 成交量 `vol` 使用 Tushare 原始“手”口径，与现有日线字段更容易对齐

## 当前版本边界

- 还没有接入 GUI
- 还没有替换现有 `gui_runner.py` 的更新入口
- 还没有把数据层改造成单个主表 / Parquet 主库
- 当前是“最小迁移版”：**先把最脆弱的远端逐股请求替掉**

## 下一步建议

1. 先验证 `p6_update_daily_hist_tushare.py` 跑通
2. 再决定是否把 GUI 的“仅更新历史库”按钮切到这个新脚本
3. 最后再考虑是否把 `daily_hist/*.csv` 进一步迁到单个主表
