# AShareScanner 最新总览与完整上手文档

## 1. 这是什么项目

`AShareScanner` 是一套本地运行的 A 股短线研究与扫描系统。它不是单纯的“看盘软件”，而是一条完整的数据与研究流水线，目标是把下面两件事同时做好：

1. 生产用途  
   每天稳定同步股票池、更新本地历史库、重打包、扫描、输出结果和 Watchlist。

2. 研究用途  
   把 D0-D1-D2 事件样本结构化，补上未来标签和研究特征，分析哪些参数和特征更接近“成功样本”。

这个项目当前已经形成两条相对独立但共享底层数据的主链路：

```text
生产链路
股票池同步 -> 缺失补库 -> 日更历史库 -> 打包 Parquet -> D0扫描 -> Watchlist

研究链路
研究原始数据同步 -> P9事件样本构建 -> P10统计分析 -> P11参数区间发现
```

两条链路共用的核心底层资产是：

- `data/daily_hist/*.csv`：逐股票历史库
- `data/packed/daily_hist_all.parquet`：全市场主扫描表
- `data/research_raw/*.parquet`：研究增强特征原始表

---

## 2. 当前最新状态

以下内容来自当前 `W:\AshareScanner` 下的最新真实产物，代表项目现在已经跑到的状态。

### 2.1 生产库状态

- 最新股票池汇总：`2026-03-31`
- 原始股票池数量：`5496`
- 过滤后股票池数量：`5013`
- 本地 `data/daily_hist` CSV 文件数量：`5016`
- 全市场打包 parquet 行数：`2660784`
- parquet 覆盖股票数：`5016`

### 2.2 最新扫描状态

最新主扫描汇总文件：`output/p7_scan_from_parquet_all_summary_20260331.csv`

- 扫描股票数：`5016`
- 有效结果数：`4984`
- 硬过滤通过数：`1`
- 候选数：`44`
- 观察数：`86`
- 放弃数：`4854`
- 跳过数：`32`
- 错误数：`0`

### 2.3 Watchlist 状态

最新 Watchlist 汇总文件：`output/watchlist/watchlist_summary_20260330.csv`

- 当日入池数：`179`
- 主 Watchlist 总数：`179`

### 2.4 研究链路状态

研究原始数据同步目录已存在：

- `trade_cal`
- `stock_basic`
- `daily_basic`
- `adj_factor`
- `stk_limit`
- `moneyflow`

当前研究样本产物显示：

- 一个较早的完整样本版本：`560003` 行，起始日 `2025-10-01`
- 一个更新的大样本版本已生成 parquet 和 CSV 预览
- `P10` 分析结果显示：
  - `D1稳健` 成功率约 `51.61%`
  - `D2有卖点` 成功率约 `42.42%`
  - `综合成功` 成功率约 `34.26%`

`P11` 当前已经能输出参数区间建议，例如：

- `距涨停空间%` 推荐区间：`between 0.0410 and 4.0410`
- `BR20` 推荐区间：`between 1.0237 and 1.0837`
- `D0换手率` 推荐规则：`>= 25.56`

---

## 3. 根目录结构

当前你所有项目相关文件都在 `W:\AshareScanner` 下。

```text
W:\AshareScanner
├─ data/                    # 正式数据层
│  ├─ daily_hist/           # 逐股票 CSV 历史库
│  ├─ packed/               # 全市场打包 parquet
│  └─ research_raw/         # 研究增强原始表
├─ env/                     # Miniforge / Conda 环境
├─ logs/                    # 正式日志
├─ output/                  # 正式输出
└─ project/                 # 代码、配置、GUI、文档
```

### 3.1 `data/`

#### `data/daily_hist/`

正式生产历史库。每只股票一个 CSV，例如：

- `000001.csv`
- `600519.csv`

每个文件通常包含：

- 股票代码
- 股票名称
- 日期
- 开盘
- 收盘
- 最高
- 最低
- 成交量
- 成交额
- 换手率
- 某些脚本版本还会带振幅、涨跌幅、涨跌额

它是整个生产链路的基础层。

#### `data/packed/`

当前核心文件：

- `daily_hist_all.parquet`

这是把 `daily_hist/*.csv` 全部拼成一张大表后的主扫描输入。`p7` 和 `p9` 都依赖它。

#### `data/research_raw/`

研究增强原始数据，由 `p8_sync_research_raw_tushare.py` 维护。

子目录和文件：

- `trade_cal/trade_cal.parquet`
- `stock_basic/stock_basic_latest.parquet`
- `stock_basic/stock_basic_YYYYMMDD.parquet`
- `daily_basic/daily_basic.parquet`
- `adj_factor/adj_factor.parquet`
- `stk_limit/stk_limit.parquet`
- `moneyflow/moneyflow.parquet`

它们不是 GUI 日常生产链路的必需品，但对研究链路非常关键。

### 3.2 `output/`

正式结果输出目录，当前主要包含：

- 生产扫描输出
- Watchlist 输出
- 研究输出
- 研究原始同步汇总

重要子目录：

- `output/watchlist/`
- `output/research/`
- `output/research_raw_sync/`

### 3.3 `logs/`

每个主脚本都会生成自己的 `.log` 文件，命名模式通常是：

- `脚本名_YYYYMMDD.log`
- 或研究脚本使用 `脚本名_YYYYMMDD_HHMMSS.log`

### 3.4 `env/`

当前存在：

- `env/miniforge3`

Conda 环境列表显示：

- `base -> W:\AshareScanner\env\miniforge3`
- `a_share -> C:\Users\wseba\.conda\envs\a_share`

项目实际开发和运行通常使用 `a_share` 环境。

### 3.5 `project/`

这里是代码与配置中心，包含：

- GUI
- runner
- 所有流水线脚本
- 所有配置文件
- 文档
- 测试与历史辅助脚本
- `.runtime` 测试隔离目录

---

## 4. 正式目录与 `.runtime` 测试目录

项目同时支持两种运行根目录。

### 4.1 正式目录

当前 `app_config.json` 指向：

```json
{
  "base_dir": "W:\\AshareScanner"
}
```

这意味着 GUI 和主脚本默认读写正式目录：

- `W:\AshareScanner\data`
- `W:\AshareScanner\output`
- `W:\AshareScanner\logs`

### 4.2 `.runtime` 测试目录

`project/.runtime/` 是一个隔离测试沙箱，内部有自己的：

- `data/daily_hist`
- `data/packed`
- `output`
- `logs`

用途：

- 测 GUI 单按钮
- 测脚本联通性
- 不污染正式库

注意：

- 如果 `base_dir` 指向 `.runtime`
- 而 `.runtime` 里只放了少量样本 CSV
- 那么“一键日更扫描”会把其余股票识别为“缺失股票”，从而触发大规模补库

所以：

- 测功能时适合 `.runtime`
- 跑正式全流程时应使用正式目录

---

## 5. 整体数据链路

### 5.1 生产链路

```text
p3_build_universe.py
    -> output/p3_universe_filtered_*.csv
    -> 作为“应该存在的股票清单”

gui_runner.find_missing_stocks()
    -> 对比 output 股票池 与 data/daily_hist/*.csv
    -> 得到缺失股票列表

p4_bootstrap_hist_all_resume.py 或 p4_bootstrap_hist_all_tushare.py
    -> 把缺失股票历史补齐到 data/daily_hist/*.csv

p6_update_daily_hist_tushare.py
    -> 对已有历史库做按日期批量日更

p6b_pack_hist_to_parquet.py
    -> data/packed/daily_hist_all.parquet

p7_scan_from_parquet_all.py
    -> output/results/selected/candidate/watch/errors/skipped/summary

p8_build_watchlist.py
    -> output/watchlist/watchlist_master.csv
    -> output/watchlist/snapshots/*.csv
```

### 5.2 研究链路

```text
research_config.json
    -> 定义研究窗口、样本筛选和成功标签

p8_sync_research_raw_tushare.py
    -> data/research_raw/*.parquet

p9_build_research_dataset.py
    -> 读取 packed 主表 + research_raw
    -> 构建 D0-D1-D2 事件样本
    -> output/research/p9_research_dataset_*.parquet

p10_analyze_research_dataset.py
    -> 做样本概览、bucket 统计、特征分箱

p11_discover_parameter_ranges.py
    -> 找高胜率参数区间
```

---

## 6. 核心业务逻辑

## 6.1 生产 D0 扫描逻辑

当前主扫描由 `p7_scan_from_parquet_all.py` 定义，参数来自 `scan_config.json`。

### 硬过滤

默认最新配置为：

- `volatility_window = 90`
- `volatility_max = 0.35`
- `require_bullish = true`
- `volume_multiplier_min = 2.5`
- `volume_multiplier_max = 5.0`
- `turnover_min = 8.0`
- `cold_volume_window = 60`
- `cold_volume_ratio = 0.8`
- `min_history_bars = 90`

对应含义：

1. 低波动约束  
   用过去 `90` 日最高价和最低价估算区间波动率，要求不超过 `35%`。

2. 阳线约束  
   D0 收盘价高于开盘价。

3. 放量区间约束  
   D0 成交量 / D-1 成交量 必须落在 `[2.5, 5.0]` 区间。

4. 换手率约束  
   D0 换手率必须大于 `8%`。

5. 冷量约束  
   回看过去 `60` 日，任意一天成交量不得超过 D0 成交量的 `80%`。

只有全部命中，才算“硬过滤通过”。

### 分层标签

候选与观察来自 `VR5 / CLV / BR20`：

- 候选：
  - `vr5 >= 1.8`
  - `clv >= 0.3`
  - `br20 >= 0.98`

- 观察：
  - `vr5 >= 1.2`
  - `clv >= 0.0`
  - `br20 >= 0.95`

注意：

- “入围”表示硬过滤通过
- “候选/观察”表示分层标签
- 二者不是互斥关系

也就是说：

- 一只股票可以同时是“入围 + 候选”
- 也可以是“未入围，但候选”

### 扫描输出中的四类口径

- `selected`：硬过滤通过
- `candidate`：分层标签为候选
- `watch`：分层标签为观察
- `results`：全量结果表

---

## 6.2 Watchlist 是什么

Watchlist 不是简单的“今日扫描结果复制品”，而是一个给人工复盘和跟踪用的观察池。

它的作用是：

1. 把当日扫描值得跟踪的股票汇总成一个池
2. 补充价格上下文
3. 允许人工写备注、加自选、跟踪 D1/D2
4. 形成一份可持续维护的复盘主表

### Watchlist 的来源

`p8_build_watchlist.py` 会读取最新扫描结果中的三类文件：

- `selected`
- `candidate`
- `watch`

然后做如下处理：

1. 合并三类池子
2. 为每个股票设置 `source_bucket`
3. 按优先级去重  
   优先级为：`selected > candidate > watch`
4. 从 `daily_hist_all.parquet` 提取价格上下文
5. 写入：
   - 当日快照
   - 持续累积的主表

### Watchlist 的价格上下文

为每只入池股票计算：

- `ATR14`
- `prev20_high`
- `breakout_price`
- `support_price_1`
- `support_price_2`
- `mid_price`
- `target_price_1`
- `target_price_2`

### Watchlist 主表中的重要字段

- `watch_id`：`setup_date + 股票代码`
- `setup_date`：入池日期
- `source_bucket`：来自入围/候选/观察哪一层
- `status`：当前阶段，默认 `D0入池`
- `next_stage`：下一阶段，默认 `D1待复核`
- `review_note`：人工复盘备注
- `d1_action`：人工 D1 操作记录
- `d2_action`：人工 D2 操作记录
- `final_result_tag`：最终结果标签

### Watchlist 当前文件

- `output/watchlist/watchlist_master.csv`
- `output/watchlist/watchlist_favorites.csv`
- `output/watchlist/watchlist_summary_*.csv`
- `output/watchlist/snapshots/*_watchlist_snapshot.csv`

---

## 6.3 研究样本逻辑

研究口径由 `p9_build_research_dataset.py` 定义。

### 一行样本代表什么

一行 = 某只股票在某个 D0 日期上的一次事件样本。

### P9 会做什么

1. 从 `daily_hist_all.parquet` 中遍历股票和日期
2. 对每个 D0 复用生产扫描口径做 D0 特征
3. 取 D1、D2 的未来两天表现
4. 拼接研究原始表中的增强特征
5. 生成并行成功标签

### D1/D2 成功标签

当前并行标签有 3 套：

#### `d1_stable_flag`

回答：D1 是否稳健。

默认要求三条里满足至少两条：

- `D1收盘 >= D0收盘`
- `D1收盘 >= D0中枢价`
- `D1收盘 >= D1开盘`

#### `d2_sellable_flag`

回答：D2 是否给出卖点。

默认满足任一即可：

- `D2最高相对 D0收盘 >= 2%`
- `D2 命中 target_price_1`

#### `success_composite_flag`

综合成功：

- `D1稳健` 且 `D2有卖点`

这也是当前研究的主标签。

### P11 参数区间发现逻辑

`p11_discover_parameter_ranges.py` 会：

1. 取 P9 结果中某个成功标签
2. 对候选特征做分位数分箱
3. 计算每箱成功率、提升倍数、样本占比
4. 找连续高质量片段
5. 产出更像成功样本的参数区间建议

它不是最终自动调参器，而是参数发现器。

---

## 7. GUI 详解

GUI 入口文件是 `gui_app.py`，流程编排层是 `gui_runner.py`。

## 7.1 GUI 页面组成

GUI 主要由几块组成：

1. 操作区
2. 当前任务
3. 工具 / 快捷入口
4. 最近一次结果 / 最新扫描统计
5. 扫描结果面板
6. 独立日志窗口
7. 扫描参数窗口
8. Watchlist 窗口

## 7.2 操作区按钮

### `一键日更扫描`

调用 `gui_runner.run_daily_pipeline()`。

真实逻辑：

```text
同步股票池
-> 检查缺失股票
-> 如有缺失则补建
-> 更新历史库
-> 打包 Parquet
-> 扫描
```

如果预检查发现缺失股票太多，GUI 会弹出确认框：

- `继续完整流程`
- `取消`
- `跳过补建，仅继续后续步骤`

### `同步股票池`

运行 `p3_build_universe.py`。

### `补建缺失股票`

先检查缺失股票，再调用 `p4_bootstrap_hist_all_resume.py` 补建。

### `仅更新历史库`

当前优先使用 `p6_update_daily_hist_tushare.py`。

### `仅打包 Parquet`

运行 `p6b_pack_hist_to_parquet.py`。

### `仅扫描`

运行 `p7_scan_from_parquet_all.py`。

## 7.3 工具区按钮

### `扫描参数`

打开参数窗口，编辑 `scan_config.json` 的硬过滤参数。

### `运行日志`

打开独立日志窗口，分成：

- 全部
- 原始日志
- 步骤摘要
- 错误

### `Watchlist`

打开 Watchlist 管理窗口。

### `刷新结果`

重新加载最新扫描产物。

### `打开 output`

打开 `base_dir/output`。

### `打开 logs`

打开 `base_dir/logs`。

## 7.4 扫描参数窗口字段

这些字段映射到 `scan_config.json -> hard_filters`。

### `波动窗口（天）`

用于计算区间波动率的历史窗口长度。

### `波动率上限`

过去窗口最高价 / 最低价 - 1 的上限。

### `是否要求阳线`

是否要求 D0 收盘价高于开盘价。

### `放量倍数下限`

D0 成交量相对 D-1 的最小倍数。

### `放量倍数上限`

D0 成交量相对 D-1 的最大倍数。

### `换手率下限（%）`

D0 最小换手率。

### `冷量回看窗口（天）`

回看多少天，判断 D0 是否显著高于过去冷量区间。

### `前高量占比上限`

过去窗口里任意一天成交量不能超过 D0 成交量的该比例。

### `最少历史条数`

不足该条数的股票直接跳过扫描。

## 7.5 扫描结果面板

包含四个 Tab：

- `全部`
- `入围`
- `候选`
- `观察`

支持：

- 关键词搜索
- 标签筛选
- 硬过滤筛选
- 排序列选择
- 点击列头排序
- 双击弹出行详情

## 7.6 Watchlist 窗口

功能包括：

- 搜索关键词
- 来源筛选：全部 / 入围 / 候选 / 观察
- 仅看自选
- 刷新
- 生成今日 Watchlist
- 加自选
- 取消自选
- 打开 Watchlist 目录
- 编辑并保存复盘备注

`watchlist_favorites.csv` 用来记录自选的 `watch_id` 列表。

---

## 8. 配置文件详解

## 8.1 `app_config.json`

当前内容：

```json
{
  "base_dir": "W:\\AshareScanner"
}
```

含义：

- 定义所有运行数据目录的根
- GUI 和主脚本都会据此解析：
  - `data/`
  - `output/`
  - `logs/`

## 8.2 `scan_config.json`

当前内容分两部分：

### `hard_filters`

- `volatility_window`：波动窗口天数
- `volatility_max`：窗口波动率上限
- `require_bullish`：是否要求阳线
- `volume_multiplier_min`：放量倍数下限
- `volume_multiplier_max`：放量倍数上限
- `turnover_min`：换手率下限
- `cold_volume_window`：冷量回看窗口
- `cold_volume_ratio`：前高量占比上限
- `min_history_bars`：最少历史条数

### `label_rules`

#### `candidate`

- `vr5_min`
- `clv_min`
- `br20_min`

#### `watch`

- `vr5_min`
- `clv_min`
- `br20_min`

## 8.3 `research_config.json`

当前字段及含义：

- `start_date`：研究样本起始日
- `end_date`：研究样本结束日，空表示到最新
- `output_prefix`：P9 输出文件前缀
- `include_all_rows`：是否保留全部样本，当前代码保留兼容字段
- `min_future_days`：至少需要多少未来交易日，当前研究默认为 2
- `sample_filter`：P10/P11 分析时使用的样本筛选方式
- `primary_success_label`：主成功标签
- `success_labels.d1_stable.*`：D1 稳健标签规则
- `success_labels.d2_sellable.*`：D2 可卖标签规则
- `success_labels.composite.mode`：综合成功规则
- `feature_columns`：P10 做分箱统计的特征列
- `raw_sync.start_date`：研究原始数据同步起始日
- `raw_sync.datasets`：要同步哪些研究原始表
- `notes`：说明性备注，不参与计算

### `sample_filter` 可选语义

- `all`：不过滤
- `hard_pass_only`：仅保留硬过滤通过
- `candidate_or_watch`：仅候选或观察
- `hard_pass_or_watch`：保留入围、候选、观察

## 8.4 `parameter_interval_config.json`

用于控制 P11 参数区间发现。

关键字段：

- `sample_filter`
- `primary_success_label`
- `parameter_interval.feature_columns`
- `quantile_bins`
- `smooth_window`
- `min_segment_sample_ratio`
- `min_success_lift`
- `min_success_margin`
- `relative_band_cap`
- `absolute_band_overrides`

## 8.5 `tushare_config.json`

只用于保存 Tushare token。

注意：

- 它是敏感文件
- README 只说明用途，不应展示真实 token
- 也可以使用环境变量 `TUSHARE_TOKEN`

---

## 9. 脚本总表

下面按“当前主力 / 当前研究主力 / 历史脚本 / 环境辅助”分类说明。

## 9.1 当前主力生产脚本

### `p3_build_universe.py`

作用：

- 同步最新股票池

输入：

- 远端 AkShare：
  - 优先 `stock_info_a_code_name()`
  - 失败回退 `stock_zh_a_spot_em()`

输出：

- `output/p3_universe_raw_YYYYMMDD.csv`
- `output/p3_universe_filtered_YYYYMMDD.csv`
- `output/p3_universe_summary_YYYYMMDD.csv`
- `logs/p3_build_universe_YYYYMMDD.log`

过滤逻辑：

- 保留常见 A 股代码段
- 排除 ST / *ST / 退市

### `p4_bootstrap_hist_all_resume.py`

作用：

- 缺失股票补库
- 初始分批建库

输入：

- 股票池文件
- 可选缺失股票清单
- AkShare 历史行情

参数：

- `--stock-list-file`
- `--universe-file`
- `--start-date`
- `--skip-existing`

输出：

- `data/daily_hist/*.csv`
- `output/p4_bootstrap_all_success_*.csv`
- `output/p4_bootstrap_all_errors_*.csv`
- `output/p4_bootstrap_all_skipped_*.csv`
- `logs/p4_bootstrap_all_*.log`

### `p4_bootstrap_hist_all_tushare.py`

作用：

- 用 Tushare 按交易日批量回补一段历史

参数：

- `--start-date`
- `--end-date`
- `--universe-file`
- `--overwrite-existing`

输出：

- `data/daily_hist/*.csv`
- `output/p4_bootstrap_tushare_success_*.csv`
- `output/p4_bootstrap_tushare_errors_*.csv`
- `output/p4_bootstrap_tushare_skipped_*.csv`
- `output/p4_bootstrap_tushare_summary_*.csv`

### `p6_update_daily_hist_tushare.py`

作用：

- 当前主力日更脚本

思路：

- 不逐股请求
- 而是按自然日循环调用：
  - `daily(trade_date)`
  - `daily_basic(trade_date)`
- 再按股票回写本地 CSV

输入：

- `data/daily_hist/*.csv`
- 最新股票池文件
- Tushare token

输出：

- `output/p6_update_daily_hist_tushare_success_*.csv`
- `output/p6_update_daily_hist_tushare_errors_*.csv`
- `output/p6_update_daily_hist_tushare_skipped_*.csv`
- `output/p6_update_daily_hist_tushare_summary_*.csv`
- `logs/p6_update_daily_hist_tushare_*.log`

口径补充：

- 使用 Tushare `daily(trade_date)` 拉全市场日线
- 使用 Tushare `daily_basic(trade_date)` 补 `turnover_rate`
- `amount` 会从“千元”换算到“元”
- `vol` 保持 Tushare 原始“手”口径
- 当前目标不是彻底重构底层库，而是先把最脆弱的“逐股远端更新”替换掉

### `p6b_pack_hist_to_parquet.py`

作用：

- 把全部 `daily_hist/*.csv` 打包成一张 parquet 主表

输入：

- `data/daily_hist/*.csv`

输出：

- `data/packed/daily_hist_all.parquet`
- `output/p6b_pack_hist_summary_*.csv`
- `output/p6b_pack_hist_errors_*.csv`
- `logs/p6b_pack_hist_*.log`

### `p7_scan_from_parquet_all.py`

作用：

- 当前主力扫描器

输入：

- `data/packed/daily_hist_all.parquet`
- `scan_config.json`

输出：

- `output/p7_scan_from_parquet_all_results_*.csv`
- `output/p7_scan_from_parquet_all_selected_*.csv`
- `output/p7_scan_from_parquet_all_candidate_*.csv`
- `output/p7_scan_from_parquet_all_watch_*.csv`
- `output/p7_scan_from_parquet_all_errors_*.csv`
- `output/p7_scan_from_parquet_all_skipped_*.csv`
- `output/p7_scan_from_parquet_all_summary_*.csv`
- `logs/p7_scan_from_parquet_all_*.log`

### `p8_build_watchlist.py`

作用：

- 把最新扫描结果整理为可复盘的 Watchlist

输入：

- 最新 `selected/candidate/watch/results/summary`
- `data/packed/daily_hist_all.parquet`

输出：

- `output/watchlist/watchlist_master.csv`
- `output/watchlist/snapshots/*_watchlist_snapshot.csv`
- `output/watchlist/watchlist_summary_*.csv`
- `logs/p8_build_watchlist_*.log`

## 9.2 当前研究主力脚本

### `p8_sync_research_raw_tushare.py`

作用：

- 同步研究增强原始表

参数：

- `--start-date`
- `--end-date`
- `--datasets`
- `--force`

输入来源：

- Tushare：
  - `trade_cal`
  - `stock_basic`
  - `daily_basic`
  - `adj_factor`
  - `stk_limit`
  - `moneyflow`

输出：

- `data/research_raw/.../*.parquet`
- `output/research_raw_sync/p8_research_raw_sync_summary_*.csv`
- `logs/p8_research_raw_sync_*.log`

研究原始字段层次：

- `stock_basic`
  - 股票静态属性，例如 `industry / market / exchange / list_date / is_hs`
- `daily_basic`
  - 每日基本面和交易特征，例如 `turnover_rate_f / volume_ratio / total_mv / circ_mv`
- `adj_factor`
  - 复权因子
- `stk_limit`
  - 涨跌停价格
- `moneyflow`
  - 大单/超大单/中单/小单资金流

### `p9_build_research_dataset.py`

作用：

- 构建 D0-D1-D2 历史事件样本

当前特点：

- 已改成分批省内存版本
- 支持低内存机器按股票批处理

参数：

- `--research-config`
- `--batch-size-symbols`

输入：

- `data/packed/daily_hist_all.parquet`
- `scan_config.json`
- `research_config.json`
- `data/research_raw/*.parquet`

输出：

- `output/research/p9_research_dataset_*.parquet`
- `output/research/p9_research_dataset_*.csv`  
  仅前 `200000` 行预览
- `output/research/p9_research_dataset_summary_*.csv`
- `output/research/p9_research_dataset_skipped_*.csv`
- `logs/p9_research_dataset_*.log`

### `p10_analyze_research_dataset.py`

作用：

- 对 P9 样本做第一轮统计分析

参数：

- `--dataset`
- `--research-config`

输出：

- `output/research/p10_research_analysis_overview_*.csv`
- `output/research/p10_research_analysis_bucket_stats_*.csv`
- `output/research/p10_research_analysis_feature_bins_*.csv`
- `output/research/p10_research_analysis_report_*.md`

### `p11_discover_parameter_ranges.py`

作用：

- 自动发现高质量参数区间

参数：

- `--dataset`
- `--research-config`

输出：

- `output/research/p11_parameter_interval_summary_*.csv`
- `output/research/p11_parameter_interval_bins_*.csv`
- `output/research/p11_parameter_interval_segments_*.csv`
- `output/research/p11_parameter_interval_report_*.md`

## 9.3 GUI 与编排层

### `gui_runner.py`

作用：

- GUI 的编排层
- 不定义业务规则
- 只组织脚本执行、收集输出、暴露给 GUI

核心函数：

- `sync_universe()`
- `find_missing_stocks()`
- `bootstrap_missing_stocks()`
- `update_daily_hist()`
- `pack_to_parquet()`
- `scan_from_parquet()`
- `build_watchlist()`
- `run_daily_pipeline()`

### `gui_app.py`

作用：

- Tkinter GUI 主界面
- 处理用户交互、后台线程、日志回放、结果展示、Watchlist 编辑

## 9.4 历史脚本 / 过渡脚本

### `p1_single_stock_test.py`

最早期单股连通性测试脚本。

### `p2_sample_scan.py`

10 股样本扫描试验。

### `p2_sample_scan_50.py`

50 股样本扫描试验。

### `p4_bootstrap_hist_100.py`

最早期 100 股样本补库脚本。

### `p5_scan_from_local_100.py`

最早期本地 CSV 扫描脚本。

### `p5_scan_from_local_100_diagnose.py`

最早期扫描诊断脚本。

### `p6_update_daily_hist.py`

旧版 AkShare 逐股日更脚本。仍保留，但现在 GUI 优先走 Tushare 版。

### `p7_scan_from_local_all.py`

旧版全市场 CSV 直扫脚本，当前主力已切到 parquet 扫描。

### `p8_priority_from_results.py`

旧版“优先级 A/B/C”结果加工脚本，目前 Watchlist 已承担更完整的观察池职责。

## 9.5 诊断与辅助脚本

### `p7_profile_io.py`

测全市场 CSV 读盘耗时。

### `p7_probe_file_format.py`

分析慢文件与快文件的 BOM、换行、原始读取和 pandas 读取耗时差异。

### `scan_rules.py`

早期独立规则模块，保留了：

- 配置合并
- 指标计算
- 硬过滤评估
- 分层评估

### `test_env.py`

最早期环境自检脚本，用于确认：

- Python
- `akshare`
- `pandas`
- 写文件权限

### `activate_a_share.bat`

一键打开项目终端并激活 Conda 环境。

### `.gitignore`

忽略：

- `data/`
- `output/`
- `logs/`
- `.runtime/`
- `.vscode/`
- `__pycache__/`

### `.vscode/settings.json`

VS Code 本地设置，指定解释器和终端自动激活环境。

### `CHANGELOG.md`

版本演进记录。当前最新版本为 `2026-03-31 Version 3`。

### 各类 README 文档

- `README.md`
- `README_RESEARCH_LAB.md`
- `README_RESEARCH_LAB_SYNC.md`
- `README_RESEARCH_LABELS.md`
- `README_Adaptive_Analysis.md`
- `README_PARAMETER_INTERVAL_V1.md`
- `README_TUSHARE_MIGRATION.md`
- `README_PACKAGE.txt`

这些文件记录了项目不同阶段的局部说明。  
本文件 `README_newest.md` 的目标是把它们统一成一份最新的总览。

---

## 10. 项目根目录文件说明

下面只列 `project` 根目录中对新人最重要的文件。

| 文件 | 作用 | 当前定位 |
|---|---|---|
| `gui_app.py` | GUI 主入口 | 当前生产操作主入口 |
| `gui_runner.py` | GUI 编排层 | 当前生产调度核心 |
| `app_config.json` | 根目录配置 | 必要 |
| `scan_config.json` | 扫描参数 | 必要 |
| `research_config.json` | 研究参数 | 研究必需 |
| `parameter_interval_config.json` | P11 参数区间配置 | 研究可选 |
| `tushare_config.json` | Tushare token 配置 | 需保密 |
| `p3_build_universe.py` | 股票池同步 | 当前主力 |
| `p4_bootstrap_hist_all_resume.py` | 缺失补库 | 当前仍在用 |
| `p4_bootstrap_hist_all_tushare.py` | Tushare 回补 | 当前主力补库方案之一 |
| `p6_update_daily_hist_tushare.py` | Tushare 日更 | 当前主力 |
| `p6b_pack_hist_to_parquet.py` | 打包 parquet | 当前主力 |
| `p7_scan_from_parquet_all.py` | 全市场扫描 | 当前主力 |
| `p8_build_watchlist.py` | Watchlist 构建 | 当前主力 |
| `p8_sync_research_raw_tushare.py` | 研究原始同步 | 当前研究主力 |
| `p9_build_research_dataset.py` | 研究样本构建 | 当前研究主力 |
| `p10_analyze_research_dataset.py` | 研究统计分析 | 当前研究主力 |
| `p11_discover_parameter_ranges.py` | 参数区间发现 | 当前研究主力 |
| `activate_a_share.bat` | 激活环境 | 实用辅助 |
| `test_env.py` | 环境自检 | 历史辅助 |
| `git` | 空文件 | 当前无业务作用，像占位残留 |

---

## 11. 输入输出速查

## 11.1 日常生产

```text
输入：
  最新股票池远端接口
  本地 data/daily_hist/*.csv
  scan_config.json

过程：
  p3 -> p4(可选) -> p6 -> p6b -> p7 -> p8_watchlist

输出：
  最新股票池 CSV
  日更 success/error/skip/summary
  全市场 parquet
  扫描 results/selected/candidate/watch
  watchlist 主表和快照
```

## 11.2 研究

```text
输入：
  daily_hist_all.parquet
  research_raw/*.parquet
  scan_config.json
  research_config.json

过程：
  p8_sync_research_raw_tushare
  -> p9_build_research_dataset
  -> p10_analyze_research_dataset
  -> p11_discover_parameter_ranges

输出：
  事件样本 parquet
  样本摘要
  标签概览
  bucket 统计
  特征分箱
  参数区间建议
```

---

## 12. 新同事上手顺序

建议新人按这个顺序理解项目。

### 第一步：理解目录

先分清：

- `data/` 是数据层
- `output/` 是结果层
- `logs/` 是日志层
- `project/` 是代码层

### 第二步：理解正式链路

先读：

1. `app_config.json`
2. `scan_config.json`
3. `gui_runner.py`
4. `gui_app.py`
5. `p3_build_universe.py`
6. `p6_update_daily_hist_tushare.py`
7. `p6b_pack_hist_to_parquet.py`
8. `p7_scan_from_parquet_all.py`
9. `p8_build_watchlist.py`

### 第三步：理解研究链路

再读：

1. `research_config.json`
2. `p8_sync_research_raw_tushare.py`
3. `p9_build_research_dataset.py`
4. `p10_analyze_research_dataset.py`
5. `p11_discover_parameter_ranges.py`

### 第四步：理解 GUI 与 Watchlist

重点理解：

- 为什么“入围”和“候选/观察”不是互斥
- 为什么 Watchlist 是复盘池而不是单纯结果表
- GUI 按钮和 runner 函数如何一一对应

---

## 13. 当前最重要的注意事项

1. 当前正式 `base_dir` 已经是 `W:\AshareScanner`  
   所有 GUI 操作都会影响正式数据和正式输出。

2. `tushare_config.json` 是敏感文件  
   不要把真实 token 写进公开文档或公开仓库。

3. `p9_build_research_dataset.py` 已改为分批省内存版本  
   在 8G 机器上建议继续使用较小批次。

4. 生产链路和研究链路故意解耦  
   研究脚本不直接挂到 GUI 按钮上，是为了不影响日常生产流程稳定性。

5. 旧脚本仍保留  
   主要用于：
   - 历史追溯
   - 对比旧方案
   - 诊断
   - 快速小样本验证

6. 旧 README 文档已经基本被本文件吸收  
   后续如果做文档收敛，应该以本文件作为唯一主文档，再删除主题重复、阶段性过时的说明文件

---

## 14. 后续演进方向

从当前代码和已有文档来看，下一阶段最有价值的方向是：

1. 用更长样本窗口验证 P11 参数区间稳定性
2. 做 12/18 个月 walk-forward 检验
3. 继续补充 D0 特征
4. 把研究结果做成更清晰的专用 UI
5. 在“参数区间发现”之后，再考虑自动化调参与模型层

---

## 15. 一句话总结

这个项目当前已经不是“几个散脚本”，而是一套有明确分层的本地 A 股数据与研究系统：

- 生产链路负责每天稳定扫描
- Watchlist 负责复盘与跟踪
- 研究链路负责把 D0-D1-D2 经验转成结构化样本和参数证据

如果把它当成一台机器来看：

- `data/` 是原料仓
- `p3/p4/p6/p6b/p7` 是生产线
- `p8_build_watchlist.py` 是复盘工位
- `p8_sync_research_raw_tushare/p9/p10/p11` 是研发实验室
- `gui_app.py + gui_runner.py` 是总控台

读懂这份文档后，新同事应该能独立回答：

- 数据从哪里来
- 每个脚本在整条链路里的位置
- 每个配置项控制什么
- GUI 每个按钮到底触发什么
- Watchlist 是什么
- 研究样本是怎么定义出来的
- 现在系统已经跑到什么程度
