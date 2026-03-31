# 参数区间发现脚本 v1

## 作用

基于 `p9_build_research_dataset.py` 输出的研究样本 parquet，围绕某个主成功标签（默认 `success_composite_flag`），自动寻找：

- 哪些特征存在“高成功率区间”
- 这些区间更像：
  - 下限型（>= 某值）
  - 上限型（<= 某值）
  - 带状型（between A and B）
- 给出一个更窄的“建议参数带宽”

它不是直接给最终交易参数，而是用来回答：

> 过去 6 个月的成功样本，更集中落在什么参数区间里？

## 输入

- `p9_research_dataset_*.parquet`
- 可选：`research_config.json`

## 输出

都写到研究数据集同目录（通常是 `output/research/`）：

- `p11_parameter_interval_summary_*.csv`：每个参数的推荐区间
- `p11_parameter_interval_bins_*.csv`：全部分箱明细
- `p11_parameter_interval_segments_*.csv`：被识别出的优质区间片段
- `p11_parameter_interval_report_*.md`：文字版报告

## 运行方式

```bash
python p11_discover_parameter_ranges.py --dataset W:\AshareScanner\output\research\p9_research_dataset_xxx.parquet
```

如果你想显式指定配置文件：

```bash
python p11_discover_parameter_ranges.py --dataset W:\AshareScanner\output\research\p9_research_dataset_xxx.parquet --research-config W:\AshareScanner\project\research_config.json
```

## 推荐起步

先用：

- `sample_filter = hard_pass_or_watch`
- `primary_success_label = success_composite_flag`

先观察这些特征：

- `br20`
- `clv`
- `d0_turnover_f`
- `d0_big_order_net_ratio`
- `d0_limit_up_space_pct`
- `vr5`
- `volume_ratio_prev1`
- `d0_range_vol`
- `list_age_days`

## 如何理解输出

### `selection_method`
- `best_segment`：找到了连续高胜率区间
- `best_single_bin`：没找到连续区间，只能先给最优单个分箱

### `range_type`
- `lower_bound`：建议设置“>= 某值”
- `upper_bound`：建议设置“<= 某值”
- `band`：建议设置“在某区间内”
- `all_range`：当前特征没有明显边界意义

### `recommended_rule`
这是最适合人直接看的字段。

例如：
- `>= 1.0230`
- `between 0.8200 and 0.9800`

## 注意事项

1. 这只是“区间发现”，不是最终参数优化。
2. 下一步还要拿 12/18 个月数据做稳定性验证。
3. 某些参数即使在近 6 个月很好，也可能只是阶段性有效。
4. `±10%` 带宽只是默认收窄规则，不适合机械套所有特征。
