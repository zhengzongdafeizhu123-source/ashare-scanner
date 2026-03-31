项目名称

A 股 D0-D1-D2 短线研究系统

1. 项目目标

目标不是做一个“万能选股器”，而是做一套可迭代的短线研究系统：

D0：盘后筛选异动股票
D1：判断是否值得买入
D2：判断是否有卖点
再利用历史样本，反推更适合这套逻辑的参数和特征

这套系统既包含生产链路，也包含研究链路。

2. 当前生产链路（已经可用）

当前日常使用流程仍然是：

同步股票池
更新历史库
打包 parquet
扫描
输出结果 / 候选 / 观察 / watchlist

这个主流程由 GUI 和 gui_runner.py 串起来；gui_runner.py 当前已经优先使用 Tushare 版历史更新脚本，并支持 build_watchlist()。

当前 D0 扫描参数（最新版本）是：

volatility_window = 90
volatility_max = 0.35
require_bullish = true
volume_multiplier_min = 2.5
volume_multiplier_max = 5.0
turnover_min = 8.0
cold_volume_window = 60
cold_volume_ratio = 0.8
min_history_bars = 90

分层阈值：

candidate: vr5 >= 1.8, clv >= 0.3, br20 >= 0.98
watch: vr5 >= 1.2, clv >= 0.0, br20 >= 0.95
3. 当前研究链路（已经有雏形）

研究链路目前是独立于 GUI 的，不影响日常生产使用。

已有脚本
p8_sync_research_raw_tushare.py
p9_build_research_dataset.py
p10_analyze_research_dataset.py
各自作用
P8：同步研究原始数据

把研究需要的 Tushare 原始数据同步到本地：

trade_cal
stock_basic
daily_basic
adj_factor
stk_limit
moneyflow
P9：构建研究样本集

把原始行情和研究原始数据合并成一份“事件样本表”。

一行 = 一只股票在某个 D0 日期 上的一次样本。

包含：

D0 指标与特征
D1 / D2 标签
关键价位
研究辅助字段
P10：分析研究样本

读取 P9 生成的样本 parquet，输出：

标签总览
bucket 统计
特征分箱统计
markdown 报告
4. 当前研究标签体系（已经升级）

目前已经不是只用一个“成功/失败”。

而是 3 套并行标签：

d1_stable_flag

表示：
D1 收盘是否稳健

d2_sellable_flag

表示：
D2 是否给出可卖点

success_composite_flag

表示：
综合成功 = D1 稳健 + D2 有卖点

这正是当前研究的主标签。
你最新一版 6 个月研究报告也是按这个逻辑输出的。

5. 当前 6 个月研究结果（最关键结论）

最新 6 个月结果已经告诉我们几件很重要的事：

1. 这套系统是有 edge 的

综合成功率约 34.26%。

2. 候选层最值得当主池研究

候选 bucket：

样本数够大
综合成功率接近入围
target1_hit_rate 最高
3. 真正强的特征

当前证据最强的是：

BR20
CLV
大单净流入额 / 占比
换手率 / 自由流通换手率
距涨停空间
上市年龄（越新越强）
4. 目前被高估的特征
cold_max_volume
90 天“低波动约束”

至少按当前定义，它们的区分度没有想象中强。

6. 当前模块的 current state
已完成
正式生产库可日更
Tushare 更新层已接通
D0 扫描逻辑已参数化
Watchlist 已有基础功能
研究原始数据同步脚本已完成
研究样本构建脚本已完成
三套成功标签已完成
第一轮 6 个月分析已完成
未完成
参数区间发现脚本
参数网格实验脚本
12/18 个月稳定性验证脚本
Walk-forward 验证
贝叶斯优化 / 多目标优化
研究结果专用 UI
自动“新特征发现”模块
7. 目前最重要的 TODO（按优先级）
TODO 1：参数区间发现脚本

目标：

用过去 6 个月数据
找出每个关键参数在成功样本上的高胜率区间
不是找单点，而是找连续稳定区间

输出：

每个参数的优选区间
中心值
建议带宽
样本数
区间成功率提升幅度
TODO 2：12/18 个月稳定性验证

目标：

把 6 个月得到的参数区间固定住
在更长历史上验证它们是否仍有效

输出：

月度胜率曲线
月度样本数
最差月份表现
参数区间漂移情况
TODO 3：参数网格实验 v1

目标：

批量测试参数组合
以 success_composite_flag 为主目标
综合考虑样本数、成功率、卖点命中率、稳定性
TODO 4：加入更多候选特征

尤其优先加入：

D0 实体占比
上影线比例
近 10 日压缩度
第一次放量特征
板块 / 市场 / 行业标签更系统的处理
TODO 5：Walk-forward 验证

目标：

防止只在一段行情里有效
观察参数随时间是否稳定
TODO 6：自动化调参框架

先规则化，再优化：

网格搜索
贝叶斯优化
多目标评分
最后再考虑模型化评分
8. 外人如何上手
日常生产使用

继续走 GUI，不需要碰研究脚本。

研究使用

按这个顺序跑：

python p8_sync_research_raw_tushare.py
python p9_build_research_dataset.py
python p10_analyze_research_dataset.py --dataset W:\AshareScanner\output\research\p9_research_dataset_xxx.parquet
看什么结果

重点看：

overview
bucket_stats
feature_bins
markdown 报告
9. 自适应调参：当前状态与正确方向
当前状态

现在还没有真正实现“系统自动找最优参数”。
目前只是完成了：

样本定义
标签定义
特征分析
正确方向

不要一步跳到 AI 调参。
应该按这个顺序走：

第一步

参数区间发现

第二步

更长历史稳定性验证

第三步

参数网格实验

第四步

Walk-forward 验证

第五步

贝叶斯优化 / 多目标优化

第六步

树模型 / 打分模型

AI 的位置

AI 更适合做：

研究报告总结
成功/失败模式归纳
新特征建议

而不是第一步直接替你“选参数”。

10. 目前最值得继续做的事情

如果只做一件事，我会选：

先做“参数区间发现 + 稳定性验证”

原因是：

你已经有 6 个月研究结果
已经看到了强弱特征
已经知道综合成功是最适合的目标
下一步最自然的就是把“哪些参数区间最像成功样本”定出来

而不是继续改 GUI，或者直接上复杂模型。