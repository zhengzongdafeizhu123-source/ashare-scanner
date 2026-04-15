# Research Run Commands

## Dataset 对照

| 用途 | 文件名 | 覆盖时间段 | 说明 |
|:--|:--|:--|:--|
| 主力最新版本 | `p9_research_dataset_20260403_160358.parquet` | `2025-03-03` 到 `2026-04-01` | 当前最推荐，schema 完整，适合直接跑 `P10/P11/P12/P13` |
| 近窗口对照 | `p9_research_dataset_20260401_181734.parquet` | `2025-03-03` 到 `2026-03-30` | 和主力版本很接近，适合做稳定性对照 |
| 长窗口压力测试 | `p9_research_dataset_20260331_203044.parquet` | `2024-05-22` 到 `2026-03-27` | 时间覆盖最长，适合做稳健性验证 |
| 长窗口备选 | `p9_research_dataset_20260331_204242.parquet` | `2024-05-22` 到 `2026-03-27` | 也是完整新 schema，但样本量比上一份少 |
| 早期新 schema 版本 | `p9_research_dataset_20260331_161253.parquet` | `2025-10-09` 到 `2026-03-26` | 能跑当前 research，但窗口较短 |
| 旧 schema，不建议 | `p9_research_dataset_20260331_130142.parquet` | `2025-09-01` 到 `2026-03-26` | 缺 `d0_turnover_f / d0_limit_up_space_pct / success_composite_flag` 等关键列 |

主力 dataset 完整路径：

```powershell
C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet
```

近窗口对照 dataset：

```powershell
C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260401_181734.parquet
```

长窗口压力测试 dataset：

```powershell
C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260331_203044.parquet
```

## Config

默认：

```powershell
research_config.json
```

`hard_pass_or_watch`：

```powershell
research_config.p12.hpow.json
```

`candidate_or_watch`：

```powershell
research_config.p12.cow.json
```

`hard_pass_only`：

```powershell
research_config.p12.hardonly.json
```

## P8 同步 Research Raw

默认全套：

```powershell
python p8_sync_research_raw_tushare.py --start-date 20240701
```

只拉 research 常用数据集：

```powershell
python p8_sync_research_raw_tushare.py --start-date 20240701 --datasets daily_basic,adj_factor,stk_limit,moneyflow,trade_cal,stock_basic
```

## P9 构建 Research Dataset

默认：

```powershell
python p9_build_research_dataset.py --research-config research_config.json
```

指定批大小：

```powershell
python p9_build_research_dataset.py --research-config research_config.json --batch-size-symbols 200
```

## P10 单变量统计

主力 dataset + `hard_pass_or_watch`：

```powershell
python p10_analyze_research_dataset.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json
```

主力 dataset + `candidate_or_watch`：

```powershell
python p10_analyze_research_dataset.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.cow.json
```

主力 dataset + `hard_pass_only`：

```powershell
python p10_analyze_research_dataset.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hardonly.json
```

长窗口压力测试：

```powershell
python p10_analyze_research_dataset.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260331_203044.parquet" --research-config research_config.p12.hpow.json
```

## P11 参数区间发现

主力 dataset + `hard_pass_or_watch`：

```powershell
python p11_discover_parameter_ranges.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json
```

主力 dataset + `candidate_or_watch`：

```powershell
python p11_discover_parameter_ranges.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.cow.json
```

主力 dataset + `hard_pass_only`：

```powershell
python p11_discover_parameter_ranges.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hardonly.json
```

方向一致性自检：

```powershell
python p11_discover_parameter_ranges.py --self-check
```

## P12 Official D0 Combo Validation

主力 dataset + `hard_pass_or_watch`：

```powershell
python p12_validate_official_d0_combos.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json
```

主力 dataset + `candidate_or_watch`：

```powershell
python p12_validate_official_d0_combos.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.cow.json
```

主力 dataset + `hard_pass_only`：

```powershell
python p12_validate_official_d0_combos.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hardonly.json
```

## P13 D0-D1-D2 Trade Templates

主力 dataset + `hard_pass_or_watch`：

```powershell
python p13_validate_d0_d1_d2_trade_templates.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json
```

主力 dataset + `candidate_or_watch`：

```powershell
python p13_validate_d0_d1_d2_trade_templates.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.cow.json
```

主力 dataset + `hard_pass_only`：

```powershell
python p13_validate_d0_d1_d2_trade_templates.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hardonly.json
```

## P14 Pattern Families And Time-Split

主力 dataset + `hard_pass_or_watch`：

```powershell
python p14_discover_pattern_families.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json
```

主力 dataset + `candidate_or_watch`：

```powershell
python p14_discover_pattern_families.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.cow.json
```

主力 dataset + `hard_pass_only`：

```powershell
python p14_discover_pattern_families.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hardonly.json
```

长窗口压力测试：

```powershell
python p14_discover_pattern_families.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260331_203044.parquet" --research-config research_config.p12.hpow.json
```

说明：

- `P14` 现在是慢变量主研究。
- 核心用途是：
  - family discovery
  - time-split validation
  - mainline freezing

## P15 Success-Failure Postmortem

全样本过滤后研究：

```powershell
python p15_analyze_success_failure_pools.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json --output-dir tmp_p15_outputs
```

最近 shortlist 复盘研究：

```powershell
python p15_analyze_success_failure_pools.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json --scope recent_top_ranked_subset --recent-setup-days 40 --top-n-per-day 20 --output-dir tmp_p15_recent_outputs
```

如果想看别的出口模板：

```powershell
python p15_analyze_success_failure_pools.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json --scope recent_top_ranked_subset --recent-setup-days 40 --top-n-per-day 20 --entry-template d1_breakout_buy --exit-template d2_open_exit --output-dir tmp_p15_recent_outputs
```

说明：

- `P15` 现在更推荐用 `recent_top_ranked_subset`。
- 这个模式不是重做整库 feasibility。
- 它是专门看：
  - 最近窗口
  - 每天前 `N` 个 shortlist
  - 后续为什么成功 / 失败 / 根本没触发 breakout
- 当前最值得盯的输出：
  - `p15_outcome_summary_*.csv`
  - `p15_group_summary_*.csv`
  - `p15_breakout_reversal_summary_*.csv`
  - `p15_breakout_reversal_subtype_summary_*.csv`
  - `p15_recent_failure_slice_summary_*.csv`
  - `p15_success_failure_report_*.md`

## 最常用整套命令

主力 dataset + `hard_pass_or_watch`：

```powershell
python p8_sync_research_raw_tushare.py --start-date 20240701
python p9_build_research_dataset.py --research-config research_config.json
python p10_analyze_research_dataset.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json
python p11_discover_parameter_ranges.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json
python p12_validate_official_d0_combos.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json
python p13_validate_d0_d1_d2_trade_templates.py --dataset "C:\Users\wseba\OneDrive\AshareScanner\output\main\research\p9_research_dataset_20260403_160358.parquet" --research-config research_config.p12.hpow.json
```

稳定性对照建议顺序：

1. 主力最新版本：`p9_research_dataset_20260403_160358.parquet`
2. 近窗口对照：`p9_research_dataset_20260401_181734.parquet`
3. 长窗口压力测试：`p9_research_dataset_20260331_203044.parquet`
