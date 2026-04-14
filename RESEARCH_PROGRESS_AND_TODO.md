# Research Progress And TODO

## Latest Status

### As Of

- `2026-04-14`

### Latest Progress

- `P13` 主结果已经修正为严格符合 A 股现货 `T+1`：
  - 主结果只保留 `D1` 买入、`D2` 卖出
  - `D1` 当日卖出不再进入主 summary / 主 report
- 已完成长窗口 `P13` 重跑，使用数据集：
  - `p9_research_dataset_20260331_203044.parquet`
  - 覆盖 `2024-05-22 ~ 2026-03-27`
- 已完成三套口径结果：
  - `hard_pass_or_watch`
  - `candidate_or_watch`
  - `hard_pass_only`
- 长窗口下已经更明确地看到：
  - `hard_pass_only` 仍然是最干净的口径
  - `d1_breakout_buy` 仍然是当前最稳的 D1 主买点
  - `D2 open` 与 `D2 close` 的优劣出现分化，不能再只靠单一窗口拍板
- `P14` 已完成第一版：
  - 已能在最长库上发现可解释的 `pattern family`
  - 已能把长库 discovery 结果拿到较短库做 validation
  - 当前第一批跨窗口存活的 family 已经出现，不再只是停留在方法论阶段

### Current Working Conclusions

- `score >= 3` 仍然可以视为当前正式 D0 主池阈值。
- `score >= 4` 更强，但明显更窄。
- `BR20 + 距涨停空间` 仍然是当前最稳的 D0 骨架。
- `hard_pass` 仍然是当前 research 里非常关键的质量底座。
- `delayed_payoff` 真实存在，但当前更像支线，不是主线。
- 当前 `P14` 第一批跨窗口存活的 route 来自：
  - `family_candidate_continuation`
  - `d1_breakout_buy`
  - `d2_open_exit / d2_close_exit`
- 当前 `hardonly` 不是失效，而是 `P14` 这版 family 定义对它切得太窄，导致样本太少、没有进入 validation。

### Current Biggest Risks

- 最强模板样本量偏小，目前更像“强候选”，还不是“最终定型”。
- `hpow / cow` 在长窗口下整体仍然偏脏，不能把 pool 本身直接当成可交易池。
- `D2 open` 与 `D2 close` 现在都值得继续验证，当前还不能只保留其中一种。
- event / noise / 极端样本 仍未正式纳入。
- `P14` 当前 family 定义仍然偏向宽样本 candidate 路线，对 `hardonly` 的承载能力不足。

## Current TODO

### Priority 1

固定“长库先发现、短库再验证”的 research 顺序。

当前建议：

1. 先用最长库发现候选主线。
2. 再用较短库或时间不重叠库验证。
3. 只保留那些跨窗口仍然成立的路线。

### Priority 2

基于 `P14` 当前结果，先固定第一批跨窗口存活 family，再进入 family 迭代阶段。

当前已经可以视作第一批存活 family / route 的方向包括：

- `family_candidate_continuation + d1_breakout_buy + d2_open_exit`
- `family_candidate_continuation + d1_breakout_buy + d2_close_exit`

它们当前同时满足：

- 长库 discovery 有足够样本量
- 较短库 validation 没有方向反转
- 实际匹配数没有离谱塌缩
- 收益保留比例仍然可接受

### Priority 3

把下一轮 `P14` 迭代重点集中到：

- 调整 family 定义，让 `hardonly` 不再只剩极少量样本
- 让 `hard_pass` 质量底座能在 family discovery 层真正体现出来
- 避免 family 只偏向 candidate continuation 一条线

### Priority 4

把下一轮 exit 验证重点集中到：

- `d2_open_exit`
- `d2_close_exit`

当前最需要回答的问题不是“再加什么新因子”，而是：

- 宽口径下是否更适合 `D2 open`
- 强净化子集下是否更适合 `D2 close`
- 这两类 exit 在不同时间段里是否发生明显漂移

### Priority 5

把 delayed payoff 正式拆成支线研究，不再和 breakout 主线混在一起给主结论。

### Priority 6

在完成时间稳定性验证后，再少量引入：

- `event/noise suspect flag`
- 极端 gap 标签
- 极端放量标签
- 市场环境标签

## Long-Library-First Workflow

### Discovery Library

当前 discovery library 先固定为：

- `p9_research_dataset_20260331_203044.parquet`

原因：

- 时间覆盖最长
- 样本量最大
- 最适合先找候选主线，而不是先被最近行情带偏

### How To Start

建议按下面顺序着手：

1. 先在最长库里只做“发现题”：
   - 哪些 D0 pool 最像真有用
   - 哪些 D1 entry 真正可执行
   - 哪些 D2 exit 真正能兑现
2. 跑完后只输出候选模板名单，不直接下最终结论。
3. 再拿较短库做验证：
   - `p9_research_dataset_20260403_160358.parquet`
   - `p9_research_dataset_20260401_181734.parquet`
4. 只保留那些长库成立、短库也没有明显崩坏的路线。
5. 最后才考虑是否影响正式扫描逻辑或 GUI 提示。

### Why This Order Is Better

这样做的好处是：

- 长库负责发现候选主线
- 短库负责验证是否漂移
- 结果比“先看短库、再回头看长库”更稳

## Pattern-Family Direction

### Why We Are Moving Away From One-Size-Fits-All

当前 research 不应再默认存在一条对所有上涨路径都同样适用的万能路线。

更合理的工作假设是：

- A 股里不同上涨路径背后的微观结构可能不同
- 即使最终都上涨，D0 setup、D1 买点可执行性、D2 兑现方式也可能不同
- 所以 research 更适合先找少数几类可解释的 `pattern family`，而不是继续硬找 one-size-fits-all 模板

### How To Use External Research

海外文献和其他市场研究可以借用的方法，主要是：

- 如何处理 regime shift
- 如何避免把单窗口结果误当成普适结论
- 如何做 discovery / validation 分离

但不能直接照搬到 A 股的，是：

- 具体阈值
- 具体交易模板
- 对某一类上涨路径的最终结论

也就是说，外部研究能借的是框架，不是现成答案；A 股自己的 pattern family 仍然必须从本项目长库里长出来。

### What A Pattern Family Means Here

这里的 `pattern family` 不是事后讲故事的“上涨原因”，而是用 D0 当天可观测字段定义出来的样本类型。

也就是说：

- 先用 D0 可观测字段把样本拆成几类
- 再在每一类里分别研究 D1 entry / D2 exit
- 最后看哪些 family 在长库和短库里都能成立

### Why Manual Families Are Still Useful

人工先定义少数几个 family，并不是“提前知道答案”，而是先构造一批有交易语义的假说容器。

这一步的意义是：

- 先把混在一起的样本拆开
- 让每一类内部更像同一种交易对象
- 再比较不同 family 内部最适合的 entry / exit

所以这一步不是为了直接“发财公式”，而是为了避免把不同类型样本混成一个平均数之后误判。

### Current Pattern-Family Design Principle

当前最合适的做法是：

1. 只用 D0 当天已经可观测的字段定义 family
2. family 数量先控制在少数几个，不要一次拆得太碎
3. 每个 family 都必须有解释性，而不是只给一个无意义的 cluster 编号
4. 每个 family 都必须经过样本量和跨窗口验证

### Current Working Axes

当前最适合先使用的 D0 轴包括：

- 趋势强度轴：`br20`
- 拥挤 / 冲顶轴：`d0_limit_up_space_pct`、`d0_turnover_f`
- 流动性确认轴：`d0_turnover`
- 波动结构轴：`d0_range_vol`

### Current Pattern-Family Discovery Rule

下一步 research 顺序应固定为：

1. 先在最长库里定义并发现少数几个可解释的 pattern family
2. 在长库里找出每个 family 最像真有用的 entry / exit 路线
3. 再拿较短库或时间不重叠库去验证这些 family 是否仍然成立
4. 只保留跨窗口仍然成立的 family 和 route

### Validation Rule Of Thumb

短库验证时，不必要求样本数和长库精确线性一致，但至少要检查：

- 是否发生离谱塌缩
- 收益方向是否反转
- family 内部最强 route 排名是否保住
- `MAE`、`trigger_ratio`、`executable_ratio` 是否明显恶化

## Iteration Log

### 2026-04-14 | First P14 Discovery And Validation

- 已完成第一版 [p14_discover_pattern_families.py](/w:/AshareScanner/project/p14_discover_pattern_families.py)
- 已用 discovery library：
  - `p9_research_dataset_20260331_203044.parquet`
- 已在三套 sample filter 下分别跑 `P14`：
  - `hard_pass_or_watch`
  - `candidate_or_watch`
  - `hard_pass_only`
- 当前第一版 `P14` 结果表明：
  - `hpow` 与 `cow` 当前几乎跑出了同一批主结论
  - 当前真正通过 discovery + validation 的主 family 是：
    - `family_candidate_continuation`
    - `d1_breakout_buy`
    - `d2_open_exit / d2_close_exit`
  - `hardonly` 本轮没有进入 validation，不是因为弱，而是因为当前 family 定义太窄，只剩很少样本
- 当前最重要的 `P14` 结论不是“已经找到最终 family”，而是：
  - 长库 discovery -> 短库 validation 这条流程已经真正跑通
  - `pattern family` 方向不是空想，已经能跑出跨窗口存活路线
  - 下一步最该做的是调整 family 定义，让 `hard_pass` 底座在 `P14` 里也能被更好承载

### 2026-04-13 | Long-Window P13 Validation

- 已用最长库 `p9_research_dataset_20260331_203044.parquet` 重跑三套 `P13`
- 结果表明：
  - `hardonly` 仍然最干净
  - `breakout` 仍是主 entry
  - `D2 open` 与 `D2 close` 的比较已成为下一步核心问题
  - 样本量偏小必须正视，尤其是最强模板

### 2026-04-13 | Research Process Reset

- 明确采用新的 research 顺序：
  - 长库先发现
  - 短库或时间不重叠库再验证
  - 只保留跨窗口成立的路线

## Historical Notes

### Legacy Overview

## 1. 文档目的

这份文档用于记录当前 `main` 分支下 research 主线已经做到哪里、目前得到了哪些相对稳定的结论、这些结论对主程序 `GUI` 有什么意义，以及下一步最值得继续推进的事项。

这不是运行手册。运行命令请看 [RESEARCH_RUN_COMMANDS.md](/w:/AshareScanner/project/RESEARCH_RUN_COMMANDS.md)。

## 2. 当前 research 在研究什么

当前项目的 research 不是泛泛地做“次日涨跌统计”，而是在研究一条完整的 A 股超短线交易路径：

`D0 setup -> D1 entry -> D2 exit`

具体来说，当前研究目标分三层：

1. 找出哪些 D0 特征和组合，能够形成值得进入研究池或正式扫描池的 setup。
2. 研究这些 setup 在 D1 是否存在可执行的买点，而不是只看“事后是否上涨”。
3. 研究在 A 股现货 `T+1` 约束下，D2 哪种退出方式更合理，最终形成真实可执行的交易模板。

## 3. 当前 research 主链

### 3.1 数据准备层

- [p8_sync_research_raw_tushare.py](/w:/AshareScanner/project/p8_sync_research_raw_tushare.py)
  同步 research 所需的 Tushare 原始数据，包括 `daily_basic`、`adj_factor`、`stk_limit`、`moneyflow`、`trade_cal`、`stock_basic` 等。

- [p9_build_research_dataset.py](/w:/AshareScanner/project/p9_build_research_dataset.py)
  把原始数据整理成 `D0-D1-D2` 样本库，是当前所有 research 的核心数据源。

P9 的核心意义：

- 固定样本语义，不再每个脚本各自理解“成功”或“候选池”。
- 为每个样本保留完整 D0、D1、D2 路径信息。
- 产出后续 research 直接复用的关键字段：
  - `research_bucket`
  - `hard_pass`
  - `d1_stable_flag`
  - `d2_sellable_flag`
  - `success_composite_flag`
  - `br20`
  - `d0_turnover`
  - `d0_turnover_f`
  - `d0_range_vol`
  - `d0_limit_up_space_pct`
  - `breakout_price`
  - `mid_price`
  - `support_price_1`
  - `support_price_2`
  - `target_price_1`
  - `target_price_2`

### 3.2 单变量认知层

- [p10_analyze_research_dataset.py](/w:/AshareScanner/project/p10_analyze_research_dataset.py)
  做单变量统计，核心是看每个特征的分箱表现，判断某个因子大致朝什么方向更优。

- [p11_discover_parameter_ranges.py](/w:/AshareScanner/project/p11_discover_parameter_ranges.py)
  在 `P10` 基础上做参数区间发现。当前版本已经修掉了旧版容易出现“方向反转”的问题，改成：
  - 先识别方向
  - 再生成方向一致的区间候选
  - 再标记 `trusted / exploratory / no_recommendation`

P10/P11 当前最重要的意义不是“直接上线”，而是：

- 确定哪些特征方向稳定
- 哪些特征虽然有信号，但当前不够稳
- 哪些区间推荐明显可能过拟合，不应该直接进入正式筛选

### 3.3 多条件正式 D0 验证层

- [p12_validate_official_d0_combos.py](/w:/AshareScanner/project/p12_validate_official_d0_combos.py)
  负责验证当前正式 D0 五条件组合是否站得住。

当前 P12 采用的五个核心条件是：

- `cond_br20_strong`
- `cond_limit_up_space_good`
- `cond_turnover_good`
- `cond_turnover_f_good`
- `cond_range_vol_good`

当前 `official_d0_logic_v2` 的默认阈值来自 [scan_config.json](/w:/AshareScanner/project/scan_config.json)。

P12 当前已经回答的问题：

- `score >= 3` 仍然可以作为主池阈值。
- `score >= 4` 更强，但覆盖更窄。
- `BR20 + 距涨停空间` 是当前最稳的 D0 主骨架。
- `BR20 + 距涨停空间 + turnover` 在某些口径下更强，但更容易变窄。
- 资金流相关变量当前不适合直接作为正式 D0 硬过滤主条件。

### 3.4 真实交易模板验证层

- [p13_validate_d0_d1_d2_trade_templates.py](/w:/AshareScanner/project/p13_validate_d0_d1_d2_trade_templates.py)
  负责研究真实交易路径，而不再只看 `success_composite_flag`。

P13 当前已经从“哪个 D0 条件更强”推进到：

- 哪个 D0 pool 更值得研究
- 哪个 D1 entry 更容易触发且更可执行
- 哪个 D2 exit 更能把收益兑现出来
- `delayed_payoff` 路径是否存在真实价值
- 周五开仓是否有明显弱化

当前 P13 最新口径已经严格改成符合 A 股现货 `T+1`：

- 主结果只保留 `D1` 买入
- 主结果只保留 `D2` 卖出
- `D1` 当天卖出不再进入主 summary / 主 report

## 4. 当前已经相对稳定的结论

以下结论来自当前代码逻辑和近期多轮 P10/P11/P12/P13 的一致结果，属于“可作为当前工作假设”的结论，但仍然不是永久真理。

### 4.1 D0 层

- `score >= 3` 仍然是主阈值。
- `score >= 4` 是更强但更窄的池子。
- `BR20 + 距涨停空间` 是当前最稳的 D0 主骨架。
- `d0_turnover` 和 `d0_turnover_f` 的增强作用仍然成立，但越往更强池收缩，样本覆盖会明显下降。
- `d0_big_order_net_amount`、`d0_big_order_net_ratio` 当前不适合直接作为正式硬过滤主条件，因为它们曾出现过单变量方向与旧版区间推荐冲突的问题。

### 4.2 D1 entry 层

在最新 P13 `T+1` 主结果里，当前最像主模板的 D1 买点仍然是：

- `d1_breakout_buy`

当前含义不是“它永远最好”，而是：

- 它在多个 sample filter 下重复出现
- 它的触发率和可执行率不算极低
- 它配合强 D0 pool 和 `D2 close` 时，当前最稳定地给出正的平均已实现收益

### 4.3 D2 exit 层

在当前 P13 主结果里，最稳的 D2 exit 仍然是：

- `d2_close_exit`

当前观察：

- `d2_close_exit` 在三套口径里都更像主模板
- `d2_open_exit` 可以作为更保守的参考，但整体表现通常弱于 `D2 close`
- `d2_fail_fast_open_else_close` 具备一定防守意义，但当前还没有全面超越纯 `D2 close`
- `d2_target1_then_open / close` 当前胜率不一定差，但整体兑现质量还不够好，不适合作为当前主结论

### 4.4 delayed_payoff

当前结论不是“delayed_payoff 不存在”，而是：

- 它真实存在
- 但暂时不是主路径
- 更像某些子模板里的辅助路径

这意味着后续值得单独做一条支线研究，而不是把它和主模板完全混在一起。

## 5. 当前 research 对 GUI 的意义

当前 GUI 并不直接读取 P10/P11/P12/P13 输出文件来做策略切换，但 research 已经在实质上影响 GUI 的日常使用。

### 5.1 research 在给正式扫描逻辑提供依据

- [p7_scan_from_parquet_all.py](/w:/AshareScanner/project/p7_scan_from_parquet_all.py) 当前已经落了 `official_d0_logic_v2`
- [scan_config.json](/w:/AshareScanner/project/scan_config.json) 中有正式 D0 配置

这意味着：

- research 负责证明“什么条件值得用”
- `p7` 负责把这些条件变成每天真正运行的筛选与打分

### 5.2 GUI 当前承接的是 research 压缩后的正式结果

[gui_app.py](/w:/AshareScanner/project/gui_app.py) 当前已经能展示并使用：

- `official_d0_score`
- `official_d0_flag`
- `official_d0_tier`

所以 GUI 的意义已经不只是“把今天的扫描结果列出来”，而是在逐步转向：

- 用 research 验证过的正式 D0 逻辑排序
- 在候选结果中优先展示更像主池的对象
- 让 Watchlist 的生成和复盘解释，更接近 research 的真实结论

### 5.3 research 当前还没有直接反灌到 GUI 的部分

当前还没有正式接入 GUI 的内容包括：

- P13 的 D1 entry / D2 exit 模板选择
- delayed_payoff 的独立提示
- 周五开仓风险提示
- event/noise suspect 标记

这些属于后续可以逐步考虑的增强项，但现在还不应该贸然直接上线。

## 6. 当前 research 的局限与风险

当前这条 research 线已经成型，但并不代表可以直接视为“已完成策略”。

### 6.1 命中率和稳定性仍不够舒服

当前最强模板并不是“高胜率闭眼做”的状态，更像：

- 胜率中等
- 平均收益依赖少数更好的交易拉高
- 一旦 pool 放宽，平均表现会明显变弱

这意味着当前研究更接近“找到了方向”，还没到“形成稳健正式交易系统”。

### 6.2 时间稳定性还没有完成充分验证

当前已经有多个 P9 数据快照，但还没有把当前主模板做成足够系统的：

- 长窗口 vs 近窗口
- 滚动窗口
- 分阶段市场环境

验证。

所以目前最担心的问题不是“没有结果”，而是：

- 有结果，但还不知道是否跨阶段稳定

### 6.3 event / noise 还未纳入

当前明确没有处理：

- 突发消息驱动
- 极端 gap
- 极端放量
- 一字开板类异常路径

这意味着当前结果仍然可能被少数高噪声样本扭曲。

### 6.4 delayed_payoff 还没完全看清

当前 delayed_payoff 已经被识别为一个真实存在的路径类别，但仍然缺：

- 它适合哪些 pool
- 它适合哪些 entry/exit
- 它是否应该独立于主线模板存在

## 7. 当前推荐的 research 工作假设

在没有进一步反证前，当前建议把下面这几条当作“暂时工作假设”：

1. 正式 D0 主线仍围绕 `score >= 3` 展开。
2. `score >= 4` 更适合做更强但更窄的对照池。
3. `BR20 + 距涨停空间` 是当前最稳骨架。
4. D1 主 entry 暂时优先围绕 `d1_breakout_buy` 研究。
5. D2 主 exit 暂时优先围绕 `d2_close_exit` 研究。
6. delayed_payoff 值得单独拆出验证，但目前不应取代主线。

## 8. 下一步路线图

下面的 TODO 按优先级排序，而不是按脚本编号排序。

### P14 优先级：时间稳定性验证

这是当前最值得先做的，不建议跳过。

目标：

- 检查当前主模板是否只在某一段行情里有效
- 判断 `score >= 3 / score >= 4 / BR20 + 距涨停空间` 是否跨时间稳定

建议做法：

- 近窗口 vs 长窗口对照
- 月度或季度滚动验证
- 强势阶段 / 震荡阶段 / 弱势阶段分开看

想回答的问题：

- 主模板是否稳定
- 哪些结果只是局部时间段好看
- 是否存在时间段依赖

### P15 优先级：P13 主模板失效条件研究

目标：

- 不再只问“什么有效”，开始问“什么情况下失效”

建议重点：

- 强 D0 pool 里失败交易的共性
- breakout entry 下失败单是否集中出现在某些 gap、波动、环境结构
- `D2 close` 明显失效的样本长什么样

这一步很关键，因为当前策略更像中胜率、靠结构拿正收益的模板，失效条件研究比盲目加新因子更重要。

### P16 优先级：delayed_payoff 支线验证

目标：

- 判断 delayed_payoff 是否值得独立成第二条研究线

建议重点：

- 哪些 pool 的 delayed_payoff 占比更高
- 哪些 entry 模板在 delayed_payoff 子样本下仍然可行
- 哪些 D2 exit 对 delayed_payoff 更友好

如果这条线能成立，后面可以把主线和 delayed 支线拆开，而不是混在一个模板里。

### P17 优先级：最小增量引入环境/噪声数据

注意，这一步不建议早于时间稳定性验证。

建议只加少量、目的明确的新数据，不要一下子扩散。

优先考虑：

1. `event/noise suspect flag`
   用于标记消息、异动、极端样本，不一定直接过滤，但要先能识别。

2. 极端 gap / 极端放量标签
   用于看主模板是否对极端样本过于敏感。

3. 市场环境标签
   例如指数环境、情绪强弱、板块热度，用于判断模板是否只在某种市场状态下成立。

### P18 优先级：决定哪些结论值得正式进入 GUI / 扫描层

只有当前面几步验证通过后，才考虑进一步正式落地：

- 是否把某些 P13 主模板结果转化为 GUI 提示字段
- 是否给 Watchlist 增加更明确的 research 提示
- 是否把 delayed_payoff 作为提示标签，而不是直接交易主线

## 9. 当前不建议做的事

为了防止 research 失焦，当前不建议立刻做下面这些事：

1. 一次性引入很多新因子或很多新数据源。
2. 在还没做时间稳定性验证前，就根据某一版 P13 结果大改正式扫描逻辑。
3. 直接把 P13 某个收益最高模板当成正式唯一交易模板。
4. 过早把 delayed_payoff 当成主线，而不先确认它是否稳定。
5. 因为某个模板均值好看，就忽略其覆盖率、可执行率和 MAE。

## 10. 建议的当前工作节奏

如果后面继续沿这条 research 线推进，建议节奏如下：

1. 先固定当前 `official_d0_logic_v2` 主体不乱改。
2. 用当前主模板做时间分段验证。
3. 单独拆 delayed_payoff。
4. 再少量引入 event/noise / 环境标签。
5. 最后才决定哪些结论要正式进入 GUI 或扫描主流程。

## 11. 当前一句话总结

当前 research 已经从“因子统计”发展到“D0 筛选逻辑 + D1 买点 + D2 卖点”的真实交易路径研究。

它最大的价值不是已经找到最终圣杯，而是已经把项目从“经验选股”推进到了“有样本定义、有验证层次、有正式逻辑映射”的阶段。

当前最值得继续推进的方向，不是盲目再加更多因子，而是：

- 先验证现有主模板是否跨时间稳定
- 再研究失效条件与 delayed_payoff
- 最后再引入少量环境/噪声标签做精修

## 12. 2026-04-13 长窗口 P13 补充观察

这一节记录的是在 `P13` 修正为严格符合 A 股现货 `T+1` 之后，使用长窗口数据集重新跑出的最新观察结果。

### 12.1 本次使用的数据集与输出

本次选取的数据集：

- `p9_research_dataset_20260331_203044.parquet`

覆盖时间段：

- `2024-05-22` 到 `2026-03-27`

本次运行的三套配置：

- `research_config.p12.hpow.json`
- `research_config.p12.cow.json`
- `research_config.p12.hardonly.json`

对应主结果文件：

- `p13_trade_template_summary_hpow_20260413_162749.csv`
- `p13_trade_template_summary_cow_20260413_163013.csv`
- `p13_trade_template_summary_hardonly_20260413_162945.csv`

本次结果已确认全部符合 `T+1` 主结果口径：

- `tradable_flag = True`
- `entry_day = D1`
- `exit_day = D2`

### 12.2 长窗口下，主结论没有消失，但明显更严格了

长窗口验证后的总体印象是：

- 这条线没有彻底失效
- 但也绝对不是“主池整体就能稳定赚钱”
- 真正还站得住的，依然只是少数 `pool × entry × exit` 组合

换句话说，长窗口结果更像是在提醒：

- 当前 research 确实已经找到方向
- 但这个方向依赖模板选择
- 不能把 `D0 pool` 本身误当成可直接交易池

### 12.3 `hpow` 与 `cow` 整体仍然偏脏

长窗口下，`hpow` 的各 pool 平均收益全部为负：

- `pool_watch_score_ge_3 = -0.3015%`
- `pool_score_ge_3 = -0.4151%`
- `pool_candidate_score_ge_3 = -0.5667%`
- `pool_score_ge_4 = -0.6711%`
- `pool_br20_limit_up_space = -0.6974%`
- `pool_br20_limit_up_space_turnover = -0.7817%`

`cow` 也基本一样：

- `pool_watch_score_ge_3 = -0.3015%`
- `pool_score_ge_3 = -0.4235%`
- `pool_candidate_score_ge_3 = -0.5667%`
- `pool_score_ge_4 = -0.6881%`
- `pool_br20_limit_up_space = -0.7106%`
- `pool_br20_limit_up_space_turnover = -0.8062%`

这说明：

- 宽口径样本下，pool 整体质量仍然不够干净
- 即使某些强模板为正，也不能反推“整个 pool 都已经可交易”

### 12.4 `hard_pass_only` 仍然是最干净、最值得继续深挖的口径

和 `hpow / cow` 不同，`hardonly` 的四个 pool 平均收益全部为正：

- `pool_score_ge_4 = +0.9032%`
- `pool_br20_limit_up_space_turnover = +0.6488%`
- `pool_br20_limit_up_space = +0.4697%`
- `pool_score_ge_3 = +0.3163%`

这进一步强化了一个当前工作假设：

- `hard_pass` 仍然是正式研究中非常重要的质量底座
- 如果不借助这层净化，长窗口下模板质量会明显恶化

### 12.5 D1 主买点仍然是 `d1_breakout_buy`

三套结果中，`d1_breakout_buy` 仍然是表现最稳定的 D1 entry：

- `hpow = +0.8564%`
- `cow = +0.8534%`
- `hardonly = +1.3317%`

而其他 entry 表现为：

- `d1_open_buy` 在长窗口下依然最弱之一
  - `hpow = -1.6191%`
  - `cow = -1.6317%`
  - `hardonly = -0.2077%`
- `d1_mid_pullback_buy`、`d1_support1_buy` 只有在 `hardonly` 环境下才开始呈现正向结果

所以当前还不能说“各种 entry 都差不多”，而应该继续把：

- `d1_breakout_buy`

视为主线 entry。

### 12.6 D2 卖法出现新的分化：宽口径更偏 `d2_open_exit`

这是本次长窗口验证里最值得注意的新信息。

在 `hpow / cow` 里，最强模板不再是清一色 `d2_close_exit`，而是更偏向：

- `d2_open_exit`

例如：

`hpow`

- `pool_br20_limit_up_space + d1_breakout_buy + d2_open_exit`
  - `sample_count = 19323`
  - `executable_ratio = 0.7413`
  - `avg_realized_ret_pct = 2.8013%`
  - `win_rate = 0.5531`

- `pool_br20_limit_up_space_turnover + d1_breakout_buy + d2_open_exit`
  - `avg_realized_ret_pct = 2.3961%`

`cow`

- `pool_br20_limit_up_space + d1_breakout_buy + d2_open_exit`
  - `avg_realized_ret_pct = 2.8083%`

但在 `hardonly` 里，最强模板仍然是：

- `pool_score_ge_4 + d1_breakout_buy + d2_close_exit`
  - `sample_count = 116`
  - `executable_ratio = 0.6379`
  - `avg_realized_ret_pct = 3.0431%`
  - `median_realized_ret_pct = 1.2183%`
  - `win_rate = 0.5541`

当前更合理的理解是：

- 宽样本、较脏的池子，可能更适合保守一些，倾向 `D2 open`
- 强净化、较干净的池子，可能仍然值得继续持有到 `D2 close`

这说明下一步不应再把 `D2 close` 当成唯一默认答案，而要把：

- `d2_open_exit`
- `d2_close_exit`

放到同等重要的位置做下一轮对照。

### 12.7 样本量问题必须正视

本次强结果里最需要保持克制的地方，不是“没有强模板”，而是：

- 最强模板对应的样本量已经比较小

例如在 `hardonly` 中：

- `pool_score_ge_4 + d1_breakout_buy + d2_close_exit`
  - `sample_count = 116`

- `pool_br20_limit_up_space_turnover + d1_breakout_buy + d2_close_exit`
  - `sample_count = 144`

- `pool_br20_limit_up_space + d1_breakout_buy + d2_close_exit`
  - `sample_count = 200`

这些数字的含义是：

- 它们不是“这么多只不同股票”
- 而是“这么长时间里，满足这套条件的历史样本事件次数”

这个量级的结论可以当作：

- 强候选模板

但还不能当作：

- 已经完全稳定、可以不经复核直接定型的最终模板

### 12.8 delayed_payoff 仍然存在，但当前仍然更像支线

主 breakout 强模板中的 `delayed_payoff_ratio` 大多仍在：

- `0.077 - 0.097`

说明：

- 当前主利润仍主要来自顺滑路径
- delayed_payoff 并不是主引擎

但在 `hardonly` 中，一些次级 entry 模板出现了更高的 delayed 比例：

- `pool_br20_limit_up_space_turnover + d1_mid_pullback_buy + d2_close_exit`
  - `delayed_payoff_ratio = 0.1818`
  - `executable_ratio = 0.4583`
  - `avg_realized_ret_pct = 1.2605%`

- `pool_score_ge_4 + d1_mid_pullback_buy + d2_close_exit`
  - `delayed_payoff_ratio = 0.1724`
  - `executable_ratio = 0.5000`
  - `avg_realized_ret_pct = 1.6097%`

这说明：

- delayed_payoff 是真实存在的
- 但它当前更像一条次级研究线
- 还不适合拿来替代 breakout 主线

### 12.9 周五开仓当前仍需单独拆分验证

本次 `summary` 中的 `friday_entry_ratio` 大致在：

- `0.16 - 0.24`

说明周五样本并不少。

但当前这一批 summary 还不足以直接证明：

- 周五是否显著比非周五更差

所以更合适的结论仍然是：

- 周五不能忽略
- 但现在还不宜下“周五一定不能做”的强结论

### 12.10 对未来 TODO 的影响

这次长窗口结果会让后续 TODO 更明确：

1. 时间稳定性验证仍然是第一优先级，而且已经被进一步证明有必要。
2. 下一轮最值得重点比较的不是更多新因子，而是：
   - `d2_open_exit`
   - `d2_close_exit`
3. `hardonly` 下的强模板值得继续深挖，但必须正视样本量偏小的问题。
4. delayed_payoff 现在更适合独立成支线，不应混在主线结论里。
