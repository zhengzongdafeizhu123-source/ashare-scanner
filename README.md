# AShare Scanner

一个面向 A 股全市场的本地筛选脚本项目。  
当前阶段目标：先把 **本地历史库、每日增量更新、全市场本地扫描** 跑稳，再逐步迭代筛选规则，后续再考虑 GUI / EXE / 云端部署。

---

## 1. 项目目标

本项目用于在 A 股全市场中，基于本地历史日线数据进行筛选，输出值得人工复核的候选标的。

当前边界：

- 做 **收盘后日线扫描**
- 做 **全市场股票池**
- 做 **本地历史库**
- 做 **每日增量更新**
- 做 **本地规则筛选**
- **不做自动下单**
- **不做盘中高频监控**
- **不做新闻/公告自动解读**

---

## 2. 当前环境

- 系统：Windows 11
- 设备：Surface Pro 9
- Python 环境：Miniforge + conda
- Conda 环境名：`a_share`
- 项目目录：`W:\AshareScanner\project`
- 数据目录：`W:\AshareScanner\data`
- 输出目录：`W:\AshareScanner\output`
- 日志目录：`W:\AshareScanner\logs`

---

## 3. 当前目录结构

```text
W:\AshareScanner
├─ project
│  ├─ p1_single_stock_test.py
│  ├─ p2_sample_scan.py
│  ├─ p2_sample_scan_50.py
│  ├─ p3_build_universe.py
│  ├─ p4_bootstrap_hist_100.py
│  ├─ p4_bootstrap_hist_all_resume.py
│  ├─ p5_scan_from_local_100.py
│  ├─ p5_scan_from_local_100_diagnose.py
│  ├─ p6_update_daily_hist.py
│  ├─ p7_scan_from_local_all.py
│  ├─ scan_config.json
│  ├─ test_env.py
│  └─ README.md
├─ data
│  └─ daily_hist
├─ output
└─ logs
```

---

## 4. 已完成模块

### P1：单票试跑
文件：
- `p1_single_stock_test.py`

作用：
- 拉取单只股票历史数据
- 写入本地文件
- 验证环境、依赖、读写路径是否正常

---

### P2：小样本扫描
文件：
- `p2_sample_scan.py`
- `p2_sample_scan_50.py`

作用：
- 用 10 / 50 只样本股验证扫描流程
- 计算基础指标
- 输出结果文件、异常文件、日志

早期使用过的指标：
- VR5
- CLV
- BR20

早期标签逻辑：
- 候选
- 观察
- 放弃

说明：
- 这套逻辑后续可以作为“分层标签逻辑”重新接回主流程

---

### P3：全市场股票池
文件：
- `p3_build_universe.py`

作用：
- 获取全市场股票池
- 做基础过滤
- 输出原始股票池、过滤后股票池、汇总文件

当前过滤后股票池规模约：
- 5012 只

---

### P4：本地历史库建库
文件：
- `p4_bootstrap_hist_100.py`
- `p4_bootstrap_hist_all_resume.py`

作用：
- 先做 100 只测试建库
- 再做全市场分批建库
- 支持：
  - 分批执行
  - 跳过已存在文件
  - 失败重试
  - 断点续跑

当前状态：
- 全市场本地历史库已建完

数据落盘方式：
- 一股一文件
- 路径：`W:\AshareScanner\data\daily_hist\股票代码.csv`

---

### P5：100 只本地规则验证
文件：
- `p5_scan_from_local_100.py`
- `p5_scan_from_local_100_diagnose.py`

作用：
- 在本地历史库上验证规则
- 诊断每条规则的通过情况
- 找出规则过严的位置

---

### P6：每日增量更新
文件：
- `p6_update_daily_hist.py`

作用：
- 每天收盘后对本地历史库做增量更新
- 不再整段重拉历史
- 输出：
  - `success`
  - `skipped`
  - `errors`
  - `logs`

---

### P7：全市场本地扫描
文件：
- `p7_scan_from_local_all.py`

作用：
- 读取全市场本地历史库
- 按规则做全市场扫描
- 输出：
  - 全量结果
  - 入选结果
  - skipped
  - errors
  - summary
  - logs

当前实现：
- 历史不足 `MIN_HISTORY_BARS` 的股票记为 `skip`
- 真异常记为 `error`

---

## 5. 当前规则体系

当前 `p7` 使用的是“硬过滤”模式，而不是早期的“候选 / 观察 / 放弃”模式。

参数定义在：
- `scan_config.json`

当前规则包括：

1. 近 `volatility_window` 个交易日波动率不超过 `volatility_max`
2. 最近一个交易日要求：
   - 阳线
   - 成交量相对前一日放大到 `volume_multiplier` 倍以上
   - 换手率大于 `turnover_min`
3. 历史条数至少达到 `min_history_bars`

当前默认参数为：

- `volatility_window = 90`
- `volatility_max = 0.2`
- `require_bullish = true`
- `volume_multiplier = 3.0`
- `turnover_min = 10.0`
- `min_history_bars = 90`

说明：
- 这套规则目前偏严
- 全市场跑下来曾出现：
  - 扫描总数：5012
  - 结果数量：4982
  - 跳过数量：30
  - 失败数量：0
  - 入选数量：0

---

## 6. 当前结论

项目的底层引擎已经成型：

- 全市场本地库：已完成
- 每日更新：已完成
- 全市场本地扫描：已完成
- 跳过 / 异常 / 日志机制：已完成

当前最重要的不是再重构底层，而是：

1. 继续优化规则
2. 把“硬过滤”和“候选 / 观察 / 放弃”分层结合起来
3. 后续再考虑 GUI / EXE / 网站 / 云部署

---

## 7. 推荐的后续路线

### 近阶段
- 把规则参数彻底从主脚本中抽离
- 继续迭代筛选逻辑
- 考虑做“两层结构”：
  - 第一层：硬过滤
  - 第二层：VR5 / CLV / BR20 分层标签

### 中阶段
- 做本地 GUI 雏形
- 让用户可以在界面里：
  - 更新历史库
  - 扫描全市场
  - 修改参数
  - 导出结果
  - 查看日志

### 远阶段
- 云服务器定时更新
- 云端数据库
- 网站展示结果
- 自动化日更

---

## 8. 运行顺序建议

### 首次搭建后
1. 跑 `p3_build_universe.py`
2. 跑 `p4_bootstrap_hist_all_resume.py`
3. 跑 `p7_scan_from_local_all.py`

### 日常使用
1. 跑 `p6_update_daily_hist.py`
2. 跑 `p7_scan_from_local_all.py`

---

## 9. 注意事项

- 运行联网脚本时，优先 **关闭 VPN**
- 本地规则扫描不需要联网
- 不要把本地历史数据库直接放进 GitHub
- GitHub 只保存代码和配置，不保存大体量历史数据
- 后续规则尽量通过配置文件改，不要反复手改主脚本

---

## 10. 当前关键文件

- `p3_build_universe.py`：股票池
- `p4_bootstrap_hist_all_resume.py`：全市场建库
- `p6_update_daily_hist.py`：每日更新
- `p7_scan_from_local_all.py`：全市场本地扫描
- `scan_config.json`：规则参数配置

---

## 11. 当前仓库用途

这个仓库主要用于：

- 同步代码版本
- 方便后续在新对话里继续项目
- 方便未来接入 GitHub / ChatGPT / GUI / 云部署流程

---

## 12. Git 提交流程

每次改完代码后，在项目目录执行：

```bash
git add .
git commit -m "写明本次改动"
git push
```

---

## 13. 当前项目状态摘要

目前已经完成：

- 本地部署
- Git 仓库初始化
- GitHub 仓库同步
- 全市场股票池构建
- 全市场历史库建库
- 每日增量更新脚本
- 全市场本地扫描脚本

当前最值得继续做的事：

1. 优化筛选规则
2. 把规则配置和主逻辑进一步分离
3. 设计 GUI 雏形
4. 以后再考虑云端自动化和网站展示