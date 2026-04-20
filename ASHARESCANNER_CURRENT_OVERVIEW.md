# AShareScanner 当前总览

## 1. 这是什么项目

`AShareScanner` 是一套本地运行的 A 股短线扫描与研究系统。

它不是单纯的“看盘界面”，而是两条共享底层数据的链路：

- 生产链路：每天同步股票池、补历史、更新日线库、打包 parquet、扫描、在 GUI 中查看结果、生成 watchlist。
- 研究链路：把 `D0 -> D1 -> D2` 事件样本化，验证什么样的 `D0`、什么样的 `D1` 买法、什么样的 `D2` 卖法更合理，再把稳定结论回灌到扫描和 GUI。

当前项目的核心目标有两个：

- 给出每天可用的扫描 shortlist。
- 用研究持续收敛“哪些票值得优先看、哪些票虽满足表面条件但后续容易失效”。

## 2. 项目原理

这套系统的底层逻辑可以概括成一句话：

- 先用日线结构在 `D0` 找出“像机会”的票。
- 再用 `D1` 的突破行为确认是否值得买。
- 最后用 `D2` 的结果反过来验证这套路径是否真的有持续性。

这里的关键不是预测所有股票涨跌，而是研究：

- 在已经进入 `观察 / 候选 / 正式D0 / 硬过滤` 的样本里，
- 哪些路径长期可行，
- 哪些路径最近开始变差，
- 哪些“看起来像机会”的票其实更容易失败。

## 3. 运行与目录逻辑

当前项目通过 [project_paths.py](/w:/AshareScanner/project/project_paths.py) 统一解析运行目录。

路径优先级是：

1. 环境变量：`ASHARE_APP_CONFIG` / `ASHARE_BASE_DIR` / `ASHARE_OUTPUT_PROFILE`
2. `app_config.local.json`
3. `app_config.json`
4. `app_config.example.json`
5. 旧兼容回退：`W:\AshareScanner`
6. 最后回退到 `project/.runtime`

几个关键概念：

- `base_dir` 决定整套 `data / output / logs` 根目录。
- `output_profile` 只决定输出落到 `output/main/...` 还是 `output/test/...`。
- 正式机器上通常通过 `app_config.local.json` 指向真实目录。

## 4. 脚本链路

### 4.1 生产链路

GUI 入口在 [gui_app.py](/w:/AshareScanner/project/gui_app.py)，调度在 [gui_runner.py](/w:/AshareScanner/project/gui_runner.py)。

日常主链路大致是：

1. `p3_build_universe.py`
   同步股票池。
2. `p4_bootstrap_hist_all_resume.py`
   给缺失股票补历史。
3. `p6_update_daily_hist_tushare.py`
   日更历史库。
4. `p6b_pack_hist_to_parquet.py`
   把逐股 CSV 打成全市场 parquet。
5. `p7_scan_from_parquet_all.py`
   跑扫描，产出 GUI 结果主表。
6. `p8_build_watchlist.py`
   从扫描结果生成 watchlist。

### 4.2 研究链路

研究主线目前主要集中在：

1. `p9_build_research_dataset.py`
   构建 `D0/D1/D2` 事件样本数据集。
2. `p12_validate_official_d0_combos.py`
   验证正式 `D0` 组合口径。
3. `p13_validate_d0_d1_d2_trade_templates.py`
   验证 `D0 -> D1 -> D2` 模板路径。
4. `p14_discover_pattern_families.py`
   发现并冻结长期可用的 family / 主线。
5. `p15_analyze_success_failure_pools.py`
   专门复盘最近 shortlist 为什么失败。

## 5. 当前扫描与 GUI 的逻辑

### 5.1 D0 层

目前扫描不是只看老式“硬过滤”，而是同时存在两套视角：

- 老扫描视角：`命中硬过滤数`、`分层标签`、`硬过滤是否通过`
- 新研究视角：`official_d0_flag`、`official_d0_score`、`official_d0_tier`

当前 GUI 列表默认更信新研究视角。

### 5.2 列表默认展示什么

GUI 默认列表现在会隐藏这类票：

- `分层标签 = 放弃`
- 且没有 `official_d0_tier`
- 且 `official_d0_flag != 是`

也就是：

- 不符合当前研究逻辑的“没评级 / 放弃”票，默认不出现在列表里
- 但如果用户主动搜索股票代码或名称，仍然可以看到详情

### 5.3 GUI 排序怎么理解

当前 GUI 的默认研究排序分三层：

第一层：`P14` 主骨架

- `研究池`：`主线 > 副池 > 其他`
- `主线优先级`

第二层：`P15` 近期轻修正

- `近期修正`
- 只做小幅微调，不推翻 `P14` 主层级

第三层：D0 结构本身

- `正式D0档位`
- `正式D0分`
- `BR20`
- 换手等结构信息

所以现在读 GUI 时，先看：

1. 它属于 `主线 / 副池 / 其他` 哪一层。
2. 同层里谁的 `主线优先级` 更高。
3. `近期修正` 是不是偏弱。
4. 最后再看 `D1注意`、`D2注意`。

### 5.4 D1 / D2 注意事项是什么

`D1注意`、`D2注意` 不是新的事前预测模型，而是把 `P15` 复盘里比较稳定的经验，转成盘中阅读提示。

例如：

- `更易不触发突破`
  说明这类票更常见的问题是 `D1` 压根不触发 breakout。
- `先要更强D1`
  说明如果 `D1` 本身只是勉强突破，`D2` 更不该直接乐观。
- `先看真突破`
  说明别把盘中冲一下就当成有效确认。
- `弱D1更要谨慎`
  说明 `D1` 质量不高时，`D2` 更容易体感不佳。

## 6. 当前研究体系

### 6.1 P14：慢变量

`P14` 现在的职责已经明确：

- 发现哪些 family 在更长窗口、尤其是 `time-split` 下仍然存活。
- 冻结主线，而不是无限制加新 family。
- 给 GUI 提供长期结构性的主排序骨架。

当前应把 `P14` 理解成：

- “长期还能信什么”

目前收敛出来的主线思路是：

- `candidate` 主线：`candidate_continuation + d1_breakout_buy`
- `hardpass` 主线：`hardpass_high_score / hardpass_space_turnover / hardpass_core + d1_breakout_buy`

当前默认卖点更偏：

- `d2_close_exit`

`d2_open_exit` 还保留，但更像对照。

### 6.2 P15：快变量

`P15` 现在不再重做整库 `D0-D1-D2` 可行性研究，而是明确转成：

- 最近窗口
- 每天按当前 research 排序取前 `N`
- 复盘这些前排 shortlist 为什么成功、为什么失败、为什么没触发

当前应把 `P15` 理解成：

- “最近前排票为什么也会失手”

所以 `P15` 的职责是：

- 观察近期漂移
- 研究失效条件
- 给 GUI 提供轻量修正和盘中注意事项

而不是直接推翻 `P14` 主线。

## 7. 当前研究结论

### 7.1 关于主线与副池

现在已经明确：

- GUI 不再只展示“最纯主线”，而是采用：
  - `主线优先`
  - `副池补充`
  - `其他保留`

当前副池的定义是一个很窄的放宽版本：

- 以 `candidate_continuation` 为底
- 保留：
  - `候选`
  - `official_d0_flag = 是`
  - `score >= 3`
  - `BR20 >= 1.02`
- 只把主线里的 `limit_up_space` 约束放宽
- 再用 `turnover >= 9.67` 承接

这条副池线目前对应：

- `candidate_secondary_core`

它的定位不是替代主线，而是：

- 主线太窄时，给 GUI 保留可用性

### 7.2 关于 D1 默认买法

当前研究默认的 `D1` 买法仍然是：

- `d1_breakout_buy`

也就是：

- 只要 `D1 high >= breakout_price`
- 就认为买点被触发
- 研究默认按 `breakout_price` 记入场

这点非常重要，因为它和“等 `D1` 收盘更确认再买”的主观体感不是同一个买法。

### 7.3 关于 breakout reversal

`breakout reversal` 当前的严格定义是：

- `D1` 盘中已经触发 breakout
- 但 `D1` 收盘又掉回 `breakout_price` 下方

也就是：

- 盘中过线
- 收盘没站稳

它不是“盘中稍微回落一点”这么宽泛，而是明确的：

- “盘中假突破，日线收盘失守 breakout”

在最近窗口的 `P15` 里，这已经是很强的失败信号。

### 7.4 当前 P15 已知的重要结论

最近窗口的前排 shortlist 复盘，已经出现几个高价值现象：

- 触发 breakout 但 `D1` 收不住线，是强失败信号。
- `reversal_weak_close`、`reversal_deep_flush` 比 `reversal_near_line` 更差。
- `candidate_secondary_core` 作为副池并没有明显拖后腿，但 `untriggered` 比例更高。
- 有些票按研究口径不一定是“失败”，但从 `D1 close -> D2 close` 的体感视角仍然会很差。

这说明后续研究要继续区分两件事：

- 研究脚本默认口径：`entry = breakout_price`
- 用户交易体感口径：更接近 `D1` 确认后再买

补充说明：

- 最近刚新增的 `P16` 是为了研究“确认后再买”的可执行性。
- 但其中一版临时 GUI 提示文案把问题过度解释成“高开追价”。
- 这已经被确认不是最贴切的表述。
- 当前正确的后续方向应改为：
  - 研究“确认偏晚 / 衰竭确认风险”
  - 并把刚加进去但看不懂、且方向偏掉的 GUI 提示文案删除

## 8. 当前现状

如果把项目现在的状态说得最直白一点：

- 生产链路已经稳定可用。
- GUI 已经不再只是老硬过滤列表，而是部分承接了研究结论。
- `P14` 已经不再追求“无限发现新规则”，而是转向主线冻结。
- `P15` 已经从“全库再研究一遍”纠正为“最近前排 shortlist 失效复盘”。
- GUI 已经能表达：
  - 研究池
  - 主线优先级
  - 近期修正
  - D1注意
  - D2注意

目前最重要的不是继续加复杂规则，而是：

- 保持 `P14` 稳定
- 用 `P15` 观察近期失效模式
- 只把足够稳定的结论轻量回灌到 GUI

## 9. 当前已知限制

有几个限制必须明确：

- `P15` 很多强信号是复盘后才知道的，不能直接拿未来信息倒灌成盘前排序。
- `D1 breakout buy` 的研究默认买点，和“D1 收盘确认后再买”的真实交易习惯可能不同。
- “统计上没失败”不代表“交易体验好”，因为 `D2` 盘中大回撤仍然可能很难受。
- 现在 GUI 里的 `近期修正` 只是轻量 bias，不是新的预测分数。

## 10. 当前最值得继续做的事

下一步最合理的方向是：

1. 继续把 `P14` 当慢变量，冻结并验证主线。
2. 继续把 `P15` 当快变量，深挖最近 shortlist 的失败簇。
3. 先把稳定的 `P15` 结论做成 GUI 提示层，而不是直接大改筛选。
4. 继续分清：
   - “研究上成立”
   - “交易体感舒服”
   这不是同一个判断标准。

## 11. 建议新智能体先读什么

如果是新智能体接手，建议阅读顺序：

1. [ASHARESCANNER_CURRENT_OVERVIEW.md](/w:/AshareScanner/project/ASHARESCANNER_CURRENT_OVERVIEW.md)
2. [CURRENT_RESEARCH_CHEATSHEET.md](/w:/AshareScanner/project/CURRENT_RESEARCH_CHEATSHEET.md)
3. [RESEARCH_PROGRESS_AND_TODO.md](/w:/AshareScanner/project/RESEARCH_PROGRESS_AND_TODO.md)
4. [RESEARCH_RUN_COMMANDS.md](/w:/AshareScanner/project/RESEARCH_RUN_COMMANDS.md)
5. [project_paths.py](/w:/AshareScanner/project/project_paths.py)
6. [gui_app.py](/w:/AshareScanner/project/gui_app.py)
7. [p7_scan_from_parquet_all.py](/w:/AshareScanner/project/p7_scan_from_parquet_all.py)
8. [p14_discover_pattern_families.py](/w:/AshareScanner/project/p14_discover_pattern_families.py)
9. [p15_analyze_success_failure_pools.py](/w:/AshareScanner/project/p15_analyze_success_failure_pools.py)

这份文档的定位不是替代所有细节文档，而是作为当前项目的总入口。
