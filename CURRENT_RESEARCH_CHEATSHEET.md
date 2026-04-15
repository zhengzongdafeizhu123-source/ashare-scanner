# 当前 Research 备忘卡

## 给新智能体的最短说明

- 先看这张卡，再看 `RESEARCH_PROGRESS_AND_TODO.md`。
- 这张卡只讲 5 件事：
  - 背景
  - 当前主问题
  - 当前主线
  - 已经做完什么
  - 下一步做什么

## 当前背景

- 研究已经不再追求一条通吃路线。
- 现在要分成两条线看：
  - `P14`：慢变量，负责发现和冻结主线
  - `P15`：快变量，负责复盘最近 shortlist 为什么失手
- GUI 已经开始承接 research 结论，所以问题不只是研究正确性，还包括：
  - 如何让 GUI 先显示真正的主线
  - 如何在主线太窄时保留可用性
  - 如何解释最近前排票为什么失败

## 当前主问题

- `P14` 要回答：
  - 哪些 family / route 经得住 time-split
  - 哪些是当前应该冻结的主线
- `P15` 要回答：
  - 最近窗口里、当时排在前面的票
  - 后续为什么没有走出预期路径

## 当前已经完成的事

- `P14` 已经完成第一批 time-split 生存路线识别。
- GUI 已经接入主线 / 副池 / 其他三层理解。
- 副池定义已确认，只允许放宽一个条件：
  - `candidate_secondary_core`
  - 保留 `候选`、`official_d0_flag=是`、`score>=3`、`BR20>=1.02`
  - 放宽 `limit_up_space`
  - 用 `turnover>=9.67` 承接
- `P15` 首版脚本已经落地：
  - `p15_analyze_success_failure_pools.py`
  - 默认更应该用 `recent_top_ranked_subset`
  - 不是重做整库模板验证
  - 而是研究最近 shortlist 的失效原因
- GUI 排序现在开始承接一层“轻量 P15 修正”：
  - 不推翻 `P14` 主骨架
  - 只做很轻的近期修正
  - 同时把 `D1/D2` 注意事项直接挂进结果列表
- 扫描结果里现在已经有这些 GUI 可见字段：
  - `近期修正`
  - `D1注意`
  - `D2注意`
  - 以及更长的说明字段，供详情区 / 搜索使用

## 当前状态一句话

- `P14` 负责“长期还能信什么”。
- `P15` 负责“最近前排票为什么也会失手”。

## 现在怎么读 GUI 排序

- 现在 GUI 默认排序不应该理解成“单一分数越高越好”。
- 更准确的理解是：
  - 第一层：`P14` 主骨架
    - 主线 / 副池 / 其他
    - 主线优先级
    - 正式 D0 档位和得分
  - 第二层：`P15` 轻量修正
    - 只做很轻的近期 bias
    - 不允许短期结果推翻主线层级
  - 第三层：盘中注意事项
    - `D1注意`
    - `D2注意`
- 所以一个票排得靠前，意思更接近：
  - 它先通过了当前 `P14` 主线理解
  - 再叠加了一点最近窗口的轻量实战修正
  - 同时 GUI 会告诉你盘中应该重点盯什么

## 这张卡是干什么的

这是一张“现在到底研究到哪了”的超短备忘卡。
只回答 4 件事：

- 现在有哪些主线
- 现在默认怎么买
- 现在默认怎么卖
- 哪些地方还没定

## 一句话版

我们已经不再找“一条通吃所有股票的万能路线”。目前研究正在收敛成两条主线，而且这两条主线都以 `D1 breakout 买入` 为核心。按最新的 time-split 结果，卖点暂时都更偏向 `D2 close`。GUI 暂时按“主线优先 + 副池补充”承接，其中副池当前只放宽主线里的一个条件。

## 最新 time-split 结论

- 现在已经不是只看“长库 + 短库快照”，而是已经做了真正不重叠的时间切分验证。
- 在这轮 time-split 里：
  - `candidate` 主线继续存活
  - `hardpass` 主线也被更明确地确认下来
  - `watch_repair` 暂时只能算观察线
- 当前最实用的理解是：
  - `candidate` 主线：`D2 open` 还活着，但 `D2 close` 现在更值得优先
  - `hardpass` 主线：更明确偏向 `D2 close`

## 最新 GUI 承接

- GUI 现在不是只看“硬过滤通过 / 候选 / 观察”，而是开始承接 research 的主线分层。
- 当前 GUI 的临时结构是：
  - 主线优先展示
  - 副池补充可用性
  - 其他结果继续保留做普通参考
- 当前确认下来的副池只有一条：
  - `candidate_secondary_core`
- 它的作用不是替代主线，而是：
  - 在主线样本偏窄时，给 GUI 留出一层“仍然像研究主线、但稍微放宽”的补充池
  - 避免 GUI 结果太窄、日常使用不顺手

### 当前副池怎么定义

- 基础还是 `candidate` 这条线
- 保留：
  - `候选`
  - `official_d0_flag = 是`
  - `score >= 3`
  - `BR20 >= 1.02`
- 当前只放宽一条：
  - 不再强制 `limit_up_space` 合格
- 当前补上的承接条件是：
  - `turnover >= 9.67`

## 主线 1：Candidate Continuation

- family：`family_candidate_continuation`
- 大意：
  - 这是更宽一些的延续型主线
  - 不属于最强的 hard-pass 子集
  - 但结构已经足够稳定，值得继续做交易研究
- 当前默认买点：
  - `d1_breakout_buy`
- 当前卖点看法：
  - 暂时优先 `d2_close_exit`
  - `d2_open_exit` 还活着，先不要删

### 现在怎么理解它

- 这是更宽、更常见的一条主线。
- 它已经通过了跨窗口验证，也通过了 time-split 验证。
- 它是“能研究、能交易”的线，但不是最干净、最强的那一类。
- 按最新 time-split 结果，它现在更像：
  - 默认先看 `d2_close_exit`
  - 但保留 `d2_open_exit` 做并行比较

## 主线 2：Hardpass 主线

这是更强、更干净的主线。目前先保留 3 个 family：

- `family_hardpass_high_score`
- `family_hardpass_space_turnover`
- `family_hardpass_core`

### 共同特征

- D0 质量更好
- 比 candidate 主线更窄、更挑剔
- 默认买点也是 `d1_breakout_buy`
- 卖点更明确偏向 `d2_close_exit`

### 这 3 个 family 各自代表什么

#### `family_hardpass_high_score`

- 最强、最窄的那条
- 更像高确信度强子集

#### `family_hardpass_space_turnover`

- 强度很高
- 比 `high_score` 稍微宽一点
- 目前看起来是“强度和宽度相对更平衡”的一条

#### `family_hardpass_core`

- 更宽的 hard-pass 基线
- 适合拿来做参照

## 当前默认买点

### `d1_breakout_buy`

这是当前最重要的买点模板。

它的意思是：

- 不是 D1 开盘就买
- 而是等 D1 盘中价格真正突破预先算好的 `breakout_price`
- 如果 D1 没有突破，就不买

为什么它重要：

- 它是 candidate 主线和 hardpass 主线共同存活下来的买点
- 其他买点目前更弱，或者还不够稳定，暂时还不适合当默认主买法

## 当前默认卖点

### 当前默认答案

- `d2_close_exit`

### 为什么

- 它在当前验证过的 hardpass 主线里更强
- 在 candidate 主线的验证里也表现不错
- 按最新 time-split：
  - `candidate` 主线后段窗口里，`D2 close` 已经强于 `D2 open`
  - `hardpass` 主线里，`D2 close` 的方向更明确

### 还在观察的卖法

- `d2_open_exit`

它不是死了，而是：

- 在 candidate 这条更宽的主线里，仍然有竞争力
- 在更宽、更杂的样本里，它未来可能仍然更合适

所以现在的临时态度是：

- 默认先看 `d2_close_exit`
- 但 `d2_open_exit` 继续保留做对照

## 我们现在还没有下的结论

我们现在**没有**在说：

- 一条路线能适合所有股票
- `D2 open` 已经彻底没用
- delayed payoff 已经是主线
- GUI 里的副池已经等于正式交易主线
- 现在的结果已经稳到可以直接硬编码进正式生产逻辑

## 当前最直白的理解

- 现在至少有两条有用的路线，不是一条。
- 这两条路线当前都更喜欢 `D1 breakout 买`。
- D0 质量越干净，越像应该拿到 `D2 close`。
- 更宽的 candidate 主线，仍然值得继续比较 `D2 open` 和 `D2 close`。

## 现在还没定的地方

1. `candidate continuation` 最后是不是应该固定偏 `D2 close`，还是继续保留 `D2 open`。
2. delayed payoff 到底只是支线，还是值得单独发展成第二条交易路线。
3. 这些结论在更多时间切分验证后，会不会漂移。
4. 同样通过当前筛选的票，为什么有些 `D1 breakout -> D2 exit` 能走通，有些会明显失败。

## 当前临时工作假设

如果我们现在必须先用一个临时版本，可以先这样记：

- Candidate 主线：
  - family：`family_candidate_continuation`
  - 买点：`d1_breakout_buy`
  - 卖点：先偏 `d2_close_exit`，但继续保留 `d2_open_exit` 做比较
  - 当前备注：time-split 下两者都存活，但 `d2_close_exit` 暂时更像主答案

- GUI 副池：
  - family：`candidate_secondary_core`
  - 定义：在 `candidate_continuation` 的基础上，只放宽 `limit_up_space`
  - 保留条件：`候选`、`official_d0_flag=是`、`score>=3`、`BR20>=1.02`
  - 补充条件：`turnover>=9.67`
  - 当前备注：它是 GUI 可用性的补充池，不是新的正式交易主线

- Hardpass 主线：
  - family：
    - `family_hardpass_high_score`
    - `family_hardpass_space_turnover`
    - `family_hardpass_core`
  - 买点：`d1_breakout_buy`
  - 卖点：`d2_close_exit`
  - 当前备注：这是现在更干净、更强、也更适合做 GUI 主池优先级的主线

## 下一步最值得继续研究的问题

下一步最重要的问题不是“要不要再加一个新因子”。

而是：

- 哪些 family 更适合 `D2 open`
- 哪些 family 更适合 `D2 close`
- 这个结论在不同时间窗口里会不会变
- 在已经通过 `观察 / 候选 / score>=3 / hard_pass` 的样本里，成功单和失败单到底差在哪

### 这条新研究线怎么理解

- 它不是“让程序自己发明一套新规则”
- 更像是做一轮：
  - 成功池 vs 失败池
  - 同池内分化
  - 失效条件研究
- 而且当前更重要的是：
  - 不再从整库重做一次模板验证
  - 改成只看最新窗口里、当时排在前面的 shortlist
  - 去复盘这些“本来应该不错”的票，后续为什么不行

### 当前 `P15` 的最重要发现

- 目前最清楚的失败簇之一是 `breakout reversal`。
- 当前定义不是泛泛的“冲高回落”，而是：
  - `D1` 盘中已经碰到或越过 `breakout_price`
  - 但 `D1` 收盘又回到 `breakout_price` 下方
- 也就是：
  - `d1_high >= breakout_price`
  - `d1_close < breakout_price`
- 它更接近“盘中假突破，收盘没站住 breakout 线”。

### 当前 `P15` 的进一步拆分

- 现在 `P15` 已经不只看“有没有 reversal”，还开始看 reversal 的深浅。
- 当前临时解释分层是：
  - `reversal_near_line`
    - 盘中过线后收盘跌回线下，但离 breakout 线不太远
  - `reversal_weak_close`
    - 收盘明显回到 breakout 线下
  - `reversal_deep_flush`
    - 不只是收不住，而且日内向下回撤更深
- 当前最近 shortlist 里的直觉性结论是：
  - `reversal_near_line` 仍然偏弱，但没有另外两类那么差
  - `reversal_weak_close / deep_flush` 更像强失败簇

### 当前 recent shortlist 的补充观察

- `candidate_secondary_core` 目前在“已触发并可执行”的样本里，没有明显比 `candidate_continuation` 差。
- 但它有一个很重要的区别：
  - 它的 `untriggered` 比例更高
  - 说明它更像 GUI 可用性补充，而不是和主线完全同质
- 最近窗口里，rank 也不是越靠前越一定更稳：
  - `06_10` 这一段最近比 `11_15` 更弱
  - 所以 `P15` 现在更适合看 drift，而不是简单相信排序前列一定最好

## 现在最值得继续做的事

1. 不要让 `P15` 去重做 `P14`。
2. 继续深挖最近 shortlist 的 `breakout reversal`。
3. 继续看最近失效是否集中在：
   - 某些 family
   - 某些 rank 段
   - 某些市场环境切片
4. 只有稳定失效特征，才考虑回灌进 GUI 提示或筛选。

### 当前更合理的分法

- 成功池：
  - `D1 breakout` 触发且可成交
  - `D2` 按当前模板退出后为正收益
- 失败池：
  - `D1 breakout` 触发且可成交
  - `D2` 按当前模板退出后为负收益
- 未成交流：
  - 没触发 breakout
  - 或者触发逻辑不完整、无法执行

### 为什么这条线有意义

- 我们不是想解释所有股票为什么涨跌
- 而是想解释：
  - 已经通过当前筛选的票
  - 为什么最后会分化成“能做”和“失败”
- 这比盲目再加新因子更贴近现在真正缺的东西
