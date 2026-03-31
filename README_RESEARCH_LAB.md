# 历史研究实验室（独立于 GUI）

这套包不改动现有 GUI，也不接入当前按钮流程。目的只有一个：

1. 用你现有本地 `daily_hist_all.parquet` 历史库
2. 把每个 `股票-日期` 的 D0 事件样本结构化
3. 自动补上 D1 / D2 的未来结果标签
4. 做第一轮规则研究，而不是直接接 AI

## 文件说明

### `p9_build_research_dataset.py`
构建 D0-D1-D2 历史研究样本库。

输出到：
- `output/research/p9_research_dataset_时间戳.parquet`
- `output/research/p9_research_dataset_时间戳.csv`（仅前 20 万行预览）
- `output/research/p9_research_dataset_summary_时间戳.csv`
- `output/research/p9_research_dataset_skipped_时间戳.csv`

每一行是一条事件样本：
- D0 当天特征
- 关键价位
- D1 / D2 的未来价格结果
- D1 是否 buyable
- D2 是否达到 target1/target2

### `p10_analyze_research_dataset.py`
读取 P9 生成的 parquet，做第一轮统计分析。

输出到：
- `output/research/p10_research_analysis_summary_时间戳.csv`
- `output/research/p10_research_analysis_feature_bins_时间戳.csv`
- `output/research/p10_research_analysis_report_时间戳.md`

### `research_config.example.json`
研究配置模板。复制成 `research_config.json` 后再运行。

## 第一次怎么跑

1. 把 `research_config.example.json` 复制成项目根目录下的 `research_config.json`
2. 运行：
   `python p9_build_research_dataset.py`
3. 找到刚生成的 parquet 文件，再运行：
   `python p10_analyze_research_dataset.py --dataset W:\AshareScanner\output\research\p9_research_dataset_xxx.parquet`

## 当前样本定义（第一版）

### D0 特征
- 硬过滤命中情况
- 分层标签
- 波动率
- 放量倍数
- VR5 / CLV / BR20
- 冷量条件
- 换手率
- 关键价位（breakout/support/mid/ATR/targets）

### D1 / D2 标签
- D1 gap / high / close / low 相对 D0 close 的收益
- D2 open / high / close / low 相对 D0 close 的收益
- D1 是否 breakout
- D1 是否 strong close
- D1 是否 buyable
- D2 是否 hit target1 / target2

## 这版还没有做的事

1. 没接 GUI，故意不接，避免影响现有排版和功能。
2. 没有网格搜索器，这版先做样本库和基础分析。
3. 没有接 AI API，这版先把结构化数据准备好。

## 除了当前数据，后面最值得补充的额外数据

你现在有 Tushare 2000 积分，后续最值得补这几类：

### A. 交易日历 `trade_cal`
作用：
- 更干净地定义 D0 / D1 / D2
- 避免自然日带来的噪音

### B. 更多 `daily_basic` 字段
现在你主要用 `turnover_rate`。后面可考虑额外保留：
- total_mv / circ_mv
- float_share / total_share
- pe_ttm / pb（如果你愿意保留估值维度）

作用：
- 研究“同样形态下，不同市值/流通盘”的表现差异

### C. 前复权/复权因子
如果你后面要更严谨地做跨阶段比较，建议补复权口径统一。

### D. 行业 / 概念 / 板块标签
这是最值得补的解释变量之一。
因为短线里：
- 个股独立走强
- 板块共振走强
是两种完全不同的胜率结构。

## 你接下来还需要做什么

### 必做
1. 复制 `research_config.example.json` 为 `research_config.json`
2. 设置一个合理的研究起始日期（建议先最近 6-12 个月）
3. 运行 P9
4. 运行 P10
5. 把输出的 summary / feature_bins / markdown 报告拿回来继续讨论

### 可选
后续再决定是否做：
- 参数网格实验
- D1 分时判定器
- AI 自动复盘层
