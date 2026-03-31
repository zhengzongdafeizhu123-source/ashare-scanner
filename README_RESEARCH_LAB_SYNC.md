# 研究实验室升级版：先补齐研究原始数据，再做 P9/P10

这版不改现有 GUI，也不动当前日常生产流水线。

目标：
1. 用 Tushare 把研究所需原始数据一次性回补并在以后增量补齐
2. 让 P9 在构建研究样本时自动读这些新增原始数据
3. 让 P10 分析时直接看到更多解释变量

## 新增脚本

### `p8_sync_research_raw_tushare.py`
同步研究原始数据到：

- `data/research_raw/trade_cal/trade_cal.parquet`
- `data/research_raw/stock_basic/stock_basic_latest.parquet`
- `data/research_raw/daily_basic/daily_basic.parquet`
- `data/research_raw/adj_factor/adj_factor.parquet`
- `data/research_raw/stk_limit/stk_limit.parquet`
- `data/research_raw/moneyflow/moneyflow.parquet`

特性：
- 第一次运行：自动回补缺失历史
- 后续运行：只补齐新增交易日
- `stock_basic` 每次刷新最新版快照

## 使用顺序

### 第一次（需要回补）
1. 复制 `research_config.example.json` 为 `research_config.json`
2. 确认 `raw_sync.start_date` 设成你要研究的最早日期
3. 确认环境变量 `TUSHARE_TOKEN` 或 `tushare_config.json` 正确
4. 运行：

```bash
python p8_sync_research_raw_tushare.py
```

5. 然后运行：

```bash
python p9_build_research_dataset.py
python p10_analyze_research_dataset.py --dataset W:\AshareScanner\output\research\p9_research_dataset_xxx.parquet
```

### 以后日常/每周研究更新
每次你完成正式库日更后，只需要多跑一次：

```bash
python p8_sync_research_raw_tushare.py
```

它会自动识别：
- 哪些交易日已存在
- 哪些交易日还没同步
- 只抓缺的新增部分

然后再按需要重跑 P9 / P10。

## P9 新增可用特征

### 股票属性
- `industry`
- `market`
- `exchange`
- `is_hs`
- `list_age_days`

### 每日指标
- `d0_turnover_f`
- `d0_volume_ratio_basic`
- `d0_total_mv`
- `d0_circ_mv`
- `d0_free_share`

### 复权 / 涨跌停
- `d0_adj_factor`
- `d0_up_limit`
- `d0_down_limit`
- `d0_limit_up_space_pct`
- `d0_limit_down_space_pct`

### 资金流
- `d0_big_order_net_amount`
- `d0_small_mid_net_amount`
- `d0_big_order_net_ratio`

## 你现在需要做什么

### 必做
- 把这 4 个文件复制到项目根目录
- 把 `research_config.example.json` 复制成 `research_config.json`
- 下一次先运行一次 `p8_sync_research_raw_tushare.py`

### 然后你可以继续做什么
- 重建一版 6 个月 / 12 个月研究样本
- 对比新旧 P10 报告差异
- 再做参数网格实验

## 这版没有做什么
- 没接 GUI
- 没做参数搜索器
- 没做 AI 自动总结器

因为当前最值钱的是：先把数据层补齐。
