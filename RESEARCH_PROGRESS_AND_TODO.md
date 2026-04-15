# Research Progress And TODO

## Handoff Snapshot

### Document Role

- This file is the long-form handoff log.
- A new agent should read this file first when it needs:
  - background
  - current research questions
  - what has already been done
  - what is currently believed
  - what should be done next
- Companion docs:
  - `CURRENT_RESEARCH_CHEATSHEET.md`: fast-read version
  - `RESEARCH_RUN_COMMANDS.md`: runnable command reference

### Background

- The project is no longer trying to find one universal stock-picking route.
- Research has converged into two layers:
  - `P14`: slow-variable research
  - `P15`: fast-variable postmortem research
- `P14` is responsible for:
  - discovering pattern families
  - validating routes across longer windows
  - time-split validation
  - freezing the current mainlines
- `P15` is responsible for:
  - looking only at the recent window
  - focusing on the actually surfaced shortlist
  - explaining why recently good-looking names later failed
  - finding stable failure-condition candidates

### Core Problem Right Now

- `P14` has already moved past simple D0-D1-D2 feasibility and is now about which routes survive time-split validation.
- GUI has already started to consume research conclusions, so the practical problem is no longer only "what works historically".
- The practical problem is now:
  - how to map stable mainlines into GUI ranking
  - how to keep GUI usable when the true mainline is too narrow
  - how to explain recent shortlist failures without redoing old full-universe research

### Current Working State

- Current stable interpretation:
  - there are at least two parallel mainlines, not one
  - both mainlines currently center on `d1_breakout_buy`
- Current `P14` mainlines:
  - `family_candidate_continuation`
  - `family_hardpass_high_score`
  - `family_hardpass_space_turnover`
  - `family_hardpass_core`
- Current exit preference:
  - default bias is `d2_close_exit`
  - `candidate` line still keeps `d2_open_exit` as comparison, not yet deleted
- Current GUI mapping:
  - mainline first
  - secondary pool as usability supplement
  - other names still retained as lower-priority reference
- Current GUI ranking should now be understood as:
  - `P14` mainline skeleton first
  - then a very light `P15` recent-sort adjustment
  - then the usual D0 structural ordering
- The current `P15` integration is intentionally bounded:
  - it must not overturn the `P14` mainline layer
  - it is mainly used to improve readability and actionability
  - recent findings that depend on future information are exposed as `D1/D2` attention notes, not as ex-ante weights
- The scan / GUI layer now explicitly carries:
  - `research_recent_sort_bias`
  - `d1_attention_tag`
  - `d2_attention_tag`
  - longer note fields such as:
    - `research_recent_note`
    - `d1_attention_note`
    - `d2_attention_note`
    - `research_trade_attention`
- Current confirmed GUI secondary pool:
  - `candidate_secondary_core`
  - it is not a new trading mainline
  - it only relaxes one condition on top of `candidate_continuation`
  - keep: `候选`, `official_d0_flag=是`, `score>=3`, `BR20>=1.02`
  - relax: no longer require `limit_up_space`
  - bridge with: `turnover>=9.67`

### Work Done

- `P13`
  - D0-D1-D2 template validation was corrected to be consistent with A-share `T+1`
  - main research answer now keeps `D1` buy and `D2` exit only
- `P14`
  - pattern family discovery is working
  - time-split validation inside the same long dataset is working
  - first batch of time-split survivors has already been identified
  - `candidate` and `hardpass` are now treated as separate mainlines
- GUI / scan mapping
  - scan output now carries research-family / pool / priority style fields
  - GUI now supports research-aware sorting and pool filtering
  - GUI keeps both mainline and secondary-pool views
- `P15`
  - a first postmortem script now exists: `p15_analyze_success_failure_pools.py`
  - it supports:
    - `full_filtered_universe`
    - `recent_top_ranked_subset`
  - current default interpretation should be the recent-top-ranked mode, not the full-universe mode
  - current recent-top-ranked study is:
    - recent `setup_date` window
    - top `N` names per day under current research ranking
    - route fixed to `d1_breakout_buy + d2_close_exit`
  - latest practical finding:
    - intraday breakout followed by `D1` close back below breakout is a strong failure signal

### Most Recent P15 Finding

- The current `P15` definition of breakout reversal is:
  - `d1_high >= breakout_price`
  - `d1_close < breakout_price`
- It means:
  - breakout was touched intraday
  - but `D1` failed to close above the breakout line
- It does not mean:
  - any generic intraday pullback
  - any second-day fade
  - any minute-level "spike then dump" pattern
- In the recent-top-ranked shortlist research, this is currently one of the clearest failure clusters.
- `P15` has now been extended with two more explanatory layers:
  - `breakout reversal subtype` buckets
  - `recent failure slice` summaries by family / weekday / rank band
- GUI-facing application rule:
  - future-information findings such as `breakout reversal` are not fed back as direct D0 ranking weights
  - instead they are summarized into `D1/D2` attention notes
  - only family-level recent actionability drift gets a small bounded sort adjustment
- Current working interpretation from the latest recent-top-ranked run:
  - `reversal_near_line` is still bad, but materially less bad than deeper reversal types
  - `reversal_weak_close` and `reversal_deep_flush` are both strong failure clusters
  - the recent shortlist does not fail in a perfectly linear "higher rank = safer" way
  - `06_10` rank band recently looks weaker than `11_15`
  - `candidate_secondary_core` is not obviously worse than `candidate_continuation` after execution, but it shows a clearly higher untriggered ratio

### Latest P15 Output Set

- `p15_breakout_reversal_summary_*.csv`
  - reversal vs non-reversal comparison
- `p15_breakout_reversal_subtype_summary_*.csv`
  - deeper split of reversal samples
- `p15_recent_failure_slice_summary_*.csv`
  - recent failure slices by:
    - `p15_pool_bucket`
    - `p15_primary_family`
    - `weekday_d1`
    - `postmortem_rank_band`

### Immediate Next TODO

1. Keep `P14` focused on frozen mainlines and time-split stability instead of expanding more families.
2. Keep `P15` focused on recent top-ranked shortlist postmortem instead of redoing full-universe feasibility work.
3. Deepen `P15` around:
   - whether `reversal_near_line` should stay as warning-only while deeper reversal types become stronger caution tags
   - family-level recent drift
   - rank-band drift
   - weekday / gap / volatility context if needed
4. Only after a failure condition survives repeated validation should it be fed back into GUI warning labels or filters.

### Working Rule For New Agents

- Do not treat `P15` as a replacement for `P14`.
- Do not let short-window `P15` findings rewrite the mainline too quickly.
- Treat the current system as:
  - `P14` = what is stable enough to trust
  - `P15` = why recent front-ranked names still failed
- If unsure which direction to continue:
  - first read `CURRENT_RESEARCH_CHEATSHEET.md`
  - then use this file
  - then run commands from `RESEARCH_RUN_COMMANDS.md`

## Latest Status

### As Of

- `2026-04-15`

### Latest Progress

#### Newest Time-Split Progress

- `P14` 已新增“同一长库内部 time-split validation”能力：
  - 同一长库可按 `setup_date` 切成 discovery / validation A / validation B
  - 当前已用最长库完成一轮真正不重叠的时间切分验证
- 本轮 time-split 使用的窗口为：
  - discovery：`2024-05-22 -> 2025-09-01`
  - validation_1：`2025-09-02 -> 2025-12-12`
  - validation_2：`2025-12-15 -> 2026-03-27`
- 已完成三套 `sample_filter` 的 time-split 正式运行：
  - `hard_pass_or_watch`
  - `candidate_or_watch`
  - `hard_pass_only`
- 当前最清楚的新信息是：
  - `hpow` 和 `cow` 在这轮 time-split 里几乎给出同一套主结论
  - 真正的结构分化来自 `hardonly`
  - `candidate` 主线在真正不重叠时间窗口里仍然能活
  - `hardpass` 主线在 `hardonly` 下被更明确地确认下来

#### Newest GUI-Mapping Progress

- 已开始把当前 research 主线正式映射进扫描结果与 GUI：
  - 扫描结果已新增 `research_mainline_family`
  - 扫描结果已新增 `research_mainline_priority`
  - GUI 已开始按“研究主线默认排序”优先展示
- 当前 GUI 承接方式已明确为：
  - 主线优先
  - 副池补充
  - 其他结果继续保留
- 当前确认下来的副池定义为：
  - `candidate_secondary_core`
  - 它不是新的正式交易主线
  - 它是在 `candidate_continuation` 的基础上，只放宽一个条件得到的 GUI 补充池
- 当前确认保留的条件为：
  - `候选`
  - `official_d0_flag = 是`
  - `score >= 3`
  - `BR20 >= 1.02`
- 当前确认放宽的条件为：
  - 不再强制 `limit_up_space` 合格
  - 改用 `turnover >= 9.67` 承接

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
- `P14` 已完成第二轮 family 迭代：
  - 新增并放宽了 `hard_pass` 专属 family
  - shortlist 门槛已改为“默认保守，但对窄口径自适应”
  - `hardonly` 已不再被 family 定义和门槛双重压扁
- 已完成一轮 `D2 open vs D2 close` 对照观察：
  - `candidate` 主线里，`D2 open` 与 `D2 close` 都存活，但短库验证里 `D2 close` 当前更强
  - `hardpass` 主线里，`D2 close` 明显优于 `D2 open`

### Current Working Conclusions

#### Newest Time-Split Conclusions

- 当前 `P14` 的 time-split 结果表明：
  - `family_candidate_continuation + d1_breakout_buy + d2_open_exit` 在两个后续时间窗口都存活
  - `family_candidate_continuation + d1_breakout_buy + d2_close_exit` 在两个后续时间窗口也都存活，而且后段表现更强
  - `family_watch_repair + d1_breakout_buy + d2_close_exit` 只过了一段，暂时只能算 exploratory
- `hardonly` 的 time-split 结果进一步确认了 3 条 `hardpass` 主线：
  - `family_hardpass_high_score`
  - `family_hardpass_space_turnover`
  - `family_hardpass_core`
- 当前最清楚的阶段性判断已经变成：
  - `candidate` 主线和 `hardpass` 主线应该分开理解，不应继续混成一个统一 shortlist
  - 这两条主线都以 `d1_breakout_buy` 为核心买点
  - `candidate` 主线里 `d2_open / d2_close` 都还活着，但当前 time-split 更偏 `d2_close`
  - `hardpass` 主线里 `d2_close` 比 `d2_open` 更稳定、更像默认方向

- `score >= 3` 仍然可以视为当前正式 D0 主池阈值。
- `score >= 4` 更强，但明显更窄。
- `BR20 + 距涨停空间` 仍然是当前最稳的 D0 骨架。
- `hard_pass` 仍然是当前 research 里非常关键的质量底座。
- `delayed_payoff` 真实存在，但当前更像支线，不是主线。
- 当前 `P14` 第一批跨窗口存活的 route 来自：
  - `family_candidate_continuation`
  - `d1_breakout_buy`
  - `d2_open_exit / d2_close_exit`
- 当前 `P14` 第二轮结果进一步表明：
  - `hardonly` 不是失效，而是第一版 family 定义切得太窄
  - 第二版里 `hardonly` 已经跑出多条跨窗口存活 route
  - 当前最有代表性的 `hardonly` family 包括：
    - `family_hardpass_high_score`
    - `family_hardpass_core`
    - `family_hardpass_space_turnover`
- 当前最清楚的 `D2` 结论是：
  - `candidate` 主线当前不必急着删掉 `D2 open`
  - 但 `candidate` 两个短库验证里，`D2 close` 已明显优于 `D2 open`
  - `hardpass` 主线里，`D2 close` 从 discovery 到 validation 都更强
- 当前 GUI 承接层次应临时理解为：
  - `hardpass` 主线 + `candidate_continuation` 主线，属于优先展示层
  - `candidate_secondary_core` 属于副池补充层
  - 副池的作用是保证 GUI 可用性，而不是单独上升为正式交易主线

### Current Biggest Risks

- 最强模板样本量偏小，目前更像“强候选”，还不是“最终定型”。
- `hpow / cow` 在长窗口下整体仍然偏脏，不能把 pool 本身直接当成可交易池。
- `D2 open` 与 `D2 close` 现在都值得继续验证，当前还不能只保留其中一种。
- event / noise / 极端样本 仍未正式纳入。
- `P14` 现在虽然已经把 `hardonly` 纳入，但 `hpow` 口径里 hard-pass family 仍然会被宽口径 shortlist 门槛压制，后续仍需考虑是否对“高质量窄 family”单独分层。
- `candidate` 主线与 `hardpass` 主线当前更像两条并行主线，后续若继续混成一个统一 shortlist，容易把结论重新搅混。
- GUI 若把副池和主线混读，也容易把“研究主线”与“可用性补充池”重新搅混。

## Current TODO

### Priority 1

#### Priority 1A: Make Time-Split The Default Validation Lens

- 现在已经不只是“长库 discovery + 短库快照验证”
- 接下来应把“长库内部真正 time-split 验证”作为主验证口径
- 外部较短 parquet 的验证结果继续保留，但降级为辅助对照口径

固定“长库先发现、短库再验证”的 research 顺序。

当前建议：

1. 先用最长库发现候选主线。
2. 再用较短库或时间不重叠库验证。
3. 只保留那些跨窗口仍然成立的路线。

### Priority 2

#### Priority 2A: Freeze The First Time-Split Survivors

- 当前 time-split 版本下，优先保留的 family / route 为：
  - `family_candidate_continuation + d1_breakout_buy + d2_close_exit`
  - `family_candidate_continuation + d1_breakout_buy + d2_open_exit`
  - `family_hardpass_high_score + d1_breakout_buy + d2_close_exit`
  - `family_hardpass_space_turnover + d1_breakout_buy + d2_close_exit`
  - `family_hardpass_core + d1_breakout_buy + d2_close_exit`
- 先不要继续扩 family，先围绕这几条主线做下一轮判断
- GUI 层当前允许临时承接 1 条副池：
  - `candidate_secondary_core`
  - 只允许在 `candidate_continuation` 基础上放宽一个条件
  - 当前确认放宽的是 `limit_up_space`

基于 `P14` 当前结果，先固定第一批跨窗口存活 family，再进入 family 迭代阶段。

当前已经可以视作第一批存活 family / route 的方向包括：

- `family_candidate_continuation + d1_breakout_buy + d2_open_exit`
- `family_candidate_continuation + d1_breakout_buy + d2_close_exit`
- `family_hardpass_high_score + d1_breakout_buy + d2_close_exit`
- `family_hardpass_space_turnover + d1_breakout_buy + d2_close_exit`
- `family_hardpass_core + d1_breakout_buy + d2_close_exit`

它们当前同时满足：

- 长库 discovery 有足够样本量
- 较短库 validation 没有方向反转
- 实际匹配数没有离谱塌缩
- 收益保留比例仍然可接受

### Priority 3

把下一轮 `P14` 迭代重点集中到：

- 继续比较 `candidate` 系 family 和 `hardpass` 系 family 是否应该分成两条主线，而不是混成一个统一 shortlist
- 评估 `hpow` 口径下是否应允许“小而强”的 hard-pass family 进入单独榜单
- 当前先保留 3 个 hard-pass 主 family：
  - `family_hardpass_high_score`
  - `family_hardpass_core`
  - `family_hardpass_space_turnover`

### Priority 4

把下一轮 exit 验证重点集中到：

- `d2_open_exit`
- `d2_close_exit`

当前最需要回答的问题不是“再加什么新因子”，而是：

- 宽口径下是否更适合 `D2 open`
- 强净化子集下是否更适合 `D2 close`
- 这两类 exit 在不同时间段里是否发生明显漂移
- 当前临时工作假设：
  - `candidate` 主线：`D2 open` 保留，`D2 close` 暂时优先
  - `hardpass` 主线：优先继续围绕 `D2 close`

### Priority 5

把 `P15` 失效条件研究正式提前到主线附近来做，先回答：

- 同样通过 `观察 / 候选 / score>=3 / hard_pass` 的票
- 为什么有些 `D1 breakout -> D2 exit` 能走通
- 为什么有些会在同样框架下失败

当前建议把这条线定义成：

- 成功池：
  - `D1 breakout` 已触发且可成交
  - `D2` 按当前 exit 模板退出后为正收益
- 失败池：
  - `D1 breakout` 已触发且可成交
  - `D2` 按当前 exit 模板退出后为负收益
- 未成交流：
  - 不触发 breakout
  - 或无法执行
  - 暂时不和失败池混在一起

当前建议先比较的不是新因子，而是：

- D0 结构差异：
  - `official_d0_score`
  - `BR20`
  - `d0_limit_up_space_pct`
  - `d0_turnover`
  - `d0_turnover_f`
  - `d0_range_vol`
- family / pool 差异：
  - `candidate_continuation`
  - `candidate_secondary_core`
  - `hardpass_high_score`
  - `hardpass_space_turnover`
  - `hardpass_core`
- D1 行为差异：
  - 盘中触线后是否快速回落
  - 收盘是否站上 breakout
  - 触发后回撤是否更深

这条线的目标不是“自动改规则”，而是：

- 先做可解释的成功池 / 失败池对照
- 先找到稳定的失效条件候选
- 通过 time-split 后，再决定是否把极少数结论反灌回 GUI 提示或筛选逻辑

### Priority 6

把 delayed payoff 正式拆成支线研究，不再和 breakout 主线混在一起给主结论。

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

### 2026-04-15 | Success-Pool Vs Failure-Pool Research Direction

- 已明确下一条值得推进的 research 线，不是盲目再加新因子，而是：
  - 在已通过当前筛选的样本里
  - 比较成功池与失败池的结构差异
- 当前更合理的样本切法是：
  - 成功池：`D1 breakout` 触发且可成交，`D2` 退出为正
  - 失败池：`D1 breakout` 触发且可成交，`D2` 退出为负
  - 未成交流：未触发或不可执行，暂不并入失败池
- 当前已经明确这条线的定位：
  - 它不是“让程序自己自动发明新规则”
  - 而是先做失效条件研究
  - 先找解释性差异，再决定是否少量反灌到 GUI 或筛选逻辑
- 已新建首版脚本：
  - [p15_analyze_success_failure_pools.py](/w:/AshareScanner/project/p15_analyze_success_failure_pools.py)
- 当前脚本默认已切到：
  - `recent_top_ranked_subset`
  - 最近 `setup_date` 窗口
  - 每日按当前 research 排序取前 `N`
  - 再做 `d1_breakout_buy + d2_close_exit` 复盘
- 首版输出包括：
  - `scope_subset_details`
  - `success_pool_details`
  - `failure_pool_details`
  - `nontrade_pool_details`
  - `outcome_summary`
  - `numeric_feature_comparison`
  - `categorical_feature_comparison`
  - `group_summary`
  - `daily_postmortem_summary`

### 2026-04-15 | GUI Mainline And Secondary-Pool Mapping

- 已开始把 research 主线映射进扫描结果与 GUI 排序：
  - 主线结果优先展示
  - 副池结果作为可用性补充
- 当前确认的 GUI 副池只有 1 条：
  - `candidate_secondary_core`
- 当前已确认这条副池的工作原则：
  - 它不是新的交易主线
  - 它只是在 `candidate_continuation` 的基础上放宽 1 个条件
  - 当前放宽的是 `limit_up_space`
  - 保留的主干条件仍然是：
    - `候选`
    - `official_d0_flag = 是`
    - `score >= 3`
    - `BR20 >= 1.02`
  - 当前承接条件是：
    - `turnover >= 9.67`
- 当前阶段性理解已经变成：
  - 主线负责“优先展示真正最像研究主答案的对象”
  - 副池负责“在不明显背离研究结论的前提下，保证 GUI 日常可用性”
  - 后续若副池继续扩宽，必须继续坚持“只放宽一个条件”的原则，避免重新把研究口径放散

### 2026-04-14 | P14 Time-Split Validation

- 已为 `P14` 新增长库内部 `time-split validation` 能力：
  - 同一长库按 `setup_date` 切成 discovery / validation A / validation B
  - 当前测试比例：`0.7 / 0.15 / 0.15`
- 已完成三套 `sample_filter` 的正式运行：
  - `hard_pass_or_watch`
  - `candidate_or_watch`
  - `hard_pass_only`
- 本轮最关键的横向结论：
  - `hpow` 与 `cow` 结果几乎一致，说明这一步真正提供信息的是 `candidate` 线
  - `hardonly` 则清晰跑出了独立的 `hardpass` 主线
- `candidate` 主线当前 time-split 结果：
  - `family_candidate_continuation + d1_breakout_buy + d2_open_exit`
    - discovery：`3.04%`
    - validation：`2.38% / 2.59%`
  - `family_candidate_continuation + d1_breakout_buy + d2_close_exit`
    - discovery：`1.97%`
    - validation：`3.05% / 3.37%`
  - 说明：
    - `D2 open` 仍然活着
    - 但在真正 time-split 的后续窗口里，`D2 close` 现在更强
- `watch_repair` 当前结论：
  - `family_watch_repair + d1_breakout_buy + d2_close_exit`
    - discovery：`1.34%`
    - validation：`0.14% / 0.90%`
  - 只过了一段，暂时降级为 exploratory
- `hardonly` 当前结论：
  - `family_hardpass_high_score + breakout + d2_close`
    - discovery：`2.71%`
    - validation：`5.74% / 1.40%`
  - `family_hardpass_space_turnover + breakout + d2_close`
    - discovery：`2.40%`
    - validation：`5.05% / 1.54%`
  - `family_hardpass_core + breakout + d2_close`
    - discovery：`1.71%`
    - validation：`4.75% / 0.54%`
  - 说明：
    - `hardpass` 线确实是一条单独主线
    - 而且目前仍更偏 `D2 close`
- 当前阶段性工作假设进一步收敛为：
  - 主线 1：`candidate_continuation + breakout + D2 close`
  - 主线 2：`hardpass_high_score / space_turnover / core + breakout + D2 close`
  - `candidate + D2 open` 继续保留为并行比较线
  - `watch_repair` 暂不进入默认主结论

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

### 2026-04-14 | P14 Hard-Pass Family Expansion

- 已对 `P14` 做第二轮迭代：
  - 新增 `hard_pass` 专属 family
  - 新增自适应 shortlist 门槛
- 第二轮结果表明：
  - `candidate_continuation` 主线仍然成立
  - `hardonly` 也已正式跑出多条跨窗口存活路线
- 当前最清楚的 hard-pass 结果是：
  - `family_hardpass_high_score + breakout + d2_close`
  - `family_hardpass_space_turnover + breakout + d2_close`
  - `family_hardpass_breakout_core + breakout + d2_close`
  - `family_hardpass_orderly_lowvol + breakout + d2_close`
- 这说明：
  - `P14` 现在不再只会发现 candidate 线
  - `hard_pass` 质量底座已经能被 pattern family 体系承接
  - 下一步重点应转向 family 去重、主线分层，以及 `D2 open / D2 close` 的场景化比较

### 2026-04-14 | P14 D2 Open Vs D2 Close Check

- 已完成当前主线上的 `D2 open` 与 `D2 close` 对照：
  - `candidate` 主线：
    - discovery：`D2 open = 2.90%`，`D2 close = 2.29%`
    - 但短库验证：`D2 close = 3.02% / 3.21%`，已高于 `D2 open = 2.23% / 2.39%`
  - `hardpass_high_score`：
    - discovery：`D2 close = 3.04%`，`D2 open = 2.45%`
    - validation：`D2 close = 3.30% / 2.91%`，仍高于 `D2 open = 2.67% / 2.32%`
  - `hardpass_space_turnover`：
    - discovery：`D2 close = 2.81%`，`D2 open = 2.38%`
    - validation：`D2 close = 2.67% / 2.77%`，仍高于 `D2 open = 2.17% / 2.47%`
  - `hardpass_core`：
    - discovery：`D2 close = 2.23%`，`D2 open = 1.88%`
    - validation：`D2 close = 2.41% / 2.03%`，仍高于 `D2 open = 1.79% / 1.83%`
- 当前结论：
  - `candidate` 主线里 `D2 open` 还没被彻底淘汰，但 `D2 close` 暂时已更值得优先观察
  - `hardpass` 主线里，`D2 close` 目前是更清楚的默认方向

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
- `research_mainline_family`
- `research_mainline_priority`
- `research_pool_bucket`

所以 GUI 的意义已经不只是“把今天的扫描结果列出来”，而是在逐步转向：

- 用 research 验证过的正式 D0 逻辑排序
- 在候选结果中优先展示更像主池的对象
- 把结果临时分成“主线优先 + 副池补充 + 其他保留”
- 让 Watchlist 的生成和复盘解释，更接近 research 的真实结论

这里当前新增的一层含义是：

- GUI 不再只是承接 `official_d0_logic_v2`
- 也开始承接当前 research 的主线分层
- 其中副池当前只允许做“主线基础上放宽一个条件”的补充层，不应被误读为新的正式交易主线

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

当前更建议把这一步细化成：

- 先把研究范围切到：
  - 最新窗口
  - 当时真正排在前面的 shortlist
  - 而不是整库所有过滤后样本
- 当前默认做法是：
  - 最近若干个 `setup_date`
  - 每日按当前 research 排序取前 `N`
  - 再固定看 `d1_breakout_buy + d2_close_exit`
  - 之后再平行看 `d1_breakout_buy + d2_open_exit`
- 再把样本分成 3 类：
  - 成功池：`D1 breakout` 触发且可成交，`D2` 退出后收益为正
  - 失败池：`D1 breakout` 触发且可成交，`D2` 退出后收益为负
  - 未成交流：未触发 breakout 或无法执行，暂不并入失败池
- 当前最值得先比较的字段包括：
  - `official_d0_score`
  - `BR20`
  - `d0_limit_up_space_pct`
  - `d0_turnover`
  - `d0_turnover_f`
  - `d0_range_vol`
  - `research_mainline_family`
  - `research_pool_bucket`
- 当前最值得先输出的不是自动新规则，而是：
  - 成功池 / 失败池的 summary 对照表
  - family / pool 内部的成功率与亏损率对照
  - 哪些“看起来也能过当前筛选”的结构，后续更容易失败

这一步的正确落点应该是：

- 先做解释性研究
- 再做 time-split 验证
- 最后才决定是否把极少数失效条件反灌到 GUI 提示或正式筛选逻辑

这一步当前最重要的变化是：

- 不再重复做“整库 D0-D1-D2 是否成立”
- 改成做“最新时期里，当时本来排在前面的票，为什么后来失败”

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
