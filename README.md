# AShare Scanner

一个面向 A 股全市场的本地筛选脚本项目。

当前阶段目标不是做花哨界面，而是先把这三层跑稳：

1. **数据层**：全市场股票池、本地历史库、日更更新  
2. **扫描层**：基于本地数据进行全市场日线扫描  
3. **应用层**：后续再接 GUI，把“更新 / 打包 / 扫描 / 导出”做成可点击操作  

目前项目已经从“CSV 逐文件扫描”升级到“**Parquet 打包后扫描**”，扫描速度相比旧方案有数量级提升。  
当前推荐的主扫描流程已经切换为：

- `p6_update_daily_hist.py`：更新原始 CSV 历史库
- `p6b_pack_hist_to_parquet.py`：把全市场 CSV 打包成单个 Parquet
- `p7_scan_from_parquet_all.py`：基于 Parquet 做全市场扫描

---

## 1. 项目目标

本项目用于在 A 股全市场中，基于本地历史日线数据进行筛选，输出值得人工复核的候选标的。

当前边界：

- 做 **收盘后日线扫描**
- 做 **全市场股票池**
- 做 **本地历史库**
- 做 **每日增量更新**
- 做 **本地规则筛选**
- 后续预留 **GUI**
- **不做自动下单**
- **不做盘中高频监控**
- **不做新闻/公告自动解读**

---

## 2. 当前环境

- 系统：Windows 11
- Python 环境：conda
- 当前主要环境名：`a_share`
- 项目目录：`W:\AshareScanner\project`
- 原始数据目录：`W:\AshareScanner\data`
- 打包数据目录：`W:\AshareScanner\data\packed`
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
│  ├─ p6b_pack_hist_to_parquet.py
│  ├─ p7_scan_from_local_all.py
│  ├─ p7_scan_from_parquet_all.py
│  ├─ p7_profile_io.py
│  ├─ p7_probe_file_format.py
│  ├─ scan_config.json
│  ├─ test_env.py
│  ├─ README.md
│  └─ CHANGELOG.md
├─ data
│  ├─ daily_hist
│  └─ packed
├─ output
└─ logs
```

---

## 4. 脚本总表

| 脚本 | 作用 | 当前是否主用 |
|---|---|---|
| `test_env.py` | 验证 Python 环境和依赖是否正常 | 否 |
| `p1_single_stock_test.py` | 单票试拉取、验证环境和路径 | 否 |
| `p2_sample_scan.py` | 10 只样本扫描验证 | 否 |
| `p2_sample_scan_50.py` | 50 只样本扫描验证 | 否 |
| `p3_build_universe.py` | 构建全市场股票池 | 低频使用 |
| `p4_bootstrap_hist_100.py` | 100 只样本建库验证 | 否 |
| `p4_bootstrap_hist_all_resume.py` | 全市场历史库初始化建库 | 低频使用 |
| `p5_scan_from_local_100.py` | 100 只本地规则验证 | 否 |
| `p5_scan_from_local_100_diagnose.py` | 规则诊断、拆规则看通过率 | 排查时用 |
| `p6_update_daily_hist.py` | 每日增量更新 CSV 历史库 | 是 |
| `p6b_pack_hist_to_parquet.py` | 把全市场 CSV 打包成单个 Parquet | 是 |
| `p7_scan_from_local_all.py` | 直接扫描 5000+ 个 CSV | 仅对照/备用 |
| `p7_scan_from_parquet_all.py` | 基于 Parquet 做主扫描 | 是 |
| `p7_profile_io.py` | 分析 CSV 逐文件扫描的 I/O 耗时 | 排查时用 |
| `p7_probe_file_format.py` | 排查慢文件是否格式异常 | 排查时用 |
| `scan_config.json` | 扫描参数配置文件 | 是 |
| `README.md` | 项目说明文档 | 是 |
| `CHANGELOG.md` | 更新日志 | 是 |

---

## 5. 每个脚本的作用说明

### `test_env.py`
作用：
- 验证 Python 环境、依赖和基础执行是否正常
- 排查环境问题时先跑它

当前地位：
- 环境排查工具

---

### `p1_single_stock_test.py`
作用：
- 拉取单只股票历史数据
- 写入本地文件
- 验证网络、依赖、路径读写是否正常

使用场景：
- 第一次本地部署时
- 怀疑环境坏了时
- 想确认 akshare / pandas / 路径是否正常时

当前地位：
- 一次性验证脚本
- 日常不需要频繁跑

---

### `p2_sample_scan.py`
作用：
- 用 10 只样本股验证扫描流程
- 计算基础指标
- 输出结果、异常、日志

当前地位：
- 小样本规则原型验证脚本

---

### `p2_sample_scan_50.py`
作用：
- 用 50 只样本股做更接近实战的测试
- 验证扫描逻辑在小范围样本上的稳定性

当前地位：
- 小样本扩展验证脚本

---

### `p3_build_universe.py`
作用：
- 获取全市场股票池
- 做基础过滤
- 输出原始股票池、过滤后股票池、汇总文件

用途：
- 给后续建库和扫描提供股票范围

当前地位：
- 底层基础脚本
- 只有在股票池规则变化、或需要重建 universe 时才需要重新跑

---

### `p4_bootstrap_hist_100.py`
作用：
- 先拿 100 只股票测试建库流程
- 验证批量拉取和本地落盘是否正常

当前地位：
- 建库前的小样本验证脚本

---

### `p4_bootstrap_hist_all_resume.py`
作用：
- 做全市场历史库初始化建库
- 支持：
  - 分批执行
  - 跳过已存在文件
  - 错误记录
  - 断点续跑
  - 重试机制

落盘方式：
- 一股一文件
- 路径：`W:\AshareScanner\data\daily_hist\股票代码.csv`

当前地位：
- 全市场历史库初始化主脚本
- 首次搭建时关键
- 正常情况下不需要频繁重跑

---

### `p5_scan_from_local_100.py`
作用：
- 在本地历史库上，对 100 只股票做规则验证
- 先看逻辑能不能跑通

当前地位：
- 小范围规则验证脚本

---

### `p5_scan_from_local_100_diagnose.py`
作用：
- 不只判断入选或不入选
- 还统计每条规则的通过情况
- 用来定位“规则太严”还是“数据有问题”

当前地位：
- 规则诊断脚本
- 当主扫描结果异常时，可回到这里拆解排查

---

### `p6_update_daily_hist.py`
作用：
- 每天收盘后对本地历史库做增量更新
- 不再整段重拉历史
- 输出 success / skipped / errors / logs

用途：
- 保持 `data\daily_hist` 下的 CSV 历史库是最新的

当前地位：
- 日常必跑脚本之一

---

### `p6b_pack_hist_to_parquet.py`
作用：
- 读取 `data\daily_hist` 下的全市场 CSV
- 清洗并拼接
- 输出单个 Parquet 文件：
  - `W:\AshareScanner\data\packed\daily_hist_all.parquet`

为什么需要它：
- CSV 逐文件扫描虽然逻辑简单，但面对 5000+ 小文件时，I/O 开销非常大
- 项目真正瓶颈不在规则本身，而在“连续读取几千个小 CSV”
- 因此增加 Parquet 打包层，作为扫描层的主输入数据

当前地位：
- 日常推荐脚本
- 每次更新完 CSV 历史库后，建议重新打包一次

---

### `p7_scan_from_local_all.py`
作用：
- 直接从 `data\daily_hist` 下逐个 CSV 扫描全市场
- 输出结果、入选、候选、观察、跳过、异常、汇总、日志

当前地位：
- 保留为 CSV 直扫版本
- 主要用于：
  - 回溯对照
  - 验证结果一致性
  - 在没有 Parquet 时备用

说明：
- 当前已经不建议把它作为主力日常扫描脚本
- 因为速度瓶颈主要来自 5000+ 小文件读取

---

### `p7_scan_from_parquet_all.py`
作用：
- 直接读取 `daily_hist_all.parquet`
- 按股票分组做全市场扫描
- 输出与 CSV 版同类结果文件

优势：
- 与 CSV 版结果一致
- 扫描速度大幅提升
- 已经成为当前推荐主扫描脚本

当前地位：
- 当前主力扫描脚本
- 日常扫描优先使用这个版本

---

### `p7_profile_io.py`
作用：
- 针对 CSV 逐文件扫描做 I/O profiling
- 统计每个文件的：
  - `read_csv` 耗时
  - 单文件总耗时
  - 文件大小
  - 行数
- 找出慢区段和最慢文件

用途：
- 定位“慢在代码逻辑”还是“慢在文件读取”

当前地位：
- 性能诊断脚本
- 平时不用跑，出现性能异常时使用

---

### `p7_probe_file_format.py`
作用：
- 对比快文件与慢文件的：
  - BOM
  - 换行风格
  - 原始字节读取耗时
  - pandas 读取耗时
- 用来排除是否是某批 CSV 文件格式异常

当前地位：
- 文件格式诊断脚本
- 一次性排查工具

---

### `scan_config.json`
作用：
- 存放扫描参数
- 让扫描阈值尽量通过配置修改，而不是反复手改主脚本

当前参数分两部分：
- `hard_filters`
- `label_rules`

---

## 6. 当前规则体系

当前扫描规则分两层：

### 第一层：硬过滤
主要作用：
- 把当天最值得优先看的一小批票筛出来

当前字段包括：
- `volatility_window`
- `volatility_max`
- `require_bullish`
- `volume_multiplier`
- `turnover_min`
- `min_history_bars`

当前推荐参数：

```json
{
  "hard_filters": {
    "volatility_window": 90,
    "volatility_max": 0.35,
    "require_bullish": true,
    "volume_multiplier": 2.0,
    "turnover_min": 5.0,
    "min_history_bars": 90
  }
}
```

### 第二层：分层标签
主要作用：
- 不只给出“过 / 不过”
- 还给出更细的候选层次

当前标签逻辑：
- 候选
- 观察
- 放弃

当前标签指标包括：
- `VR5`
- `CLV`
- `BR20`

当前推荐参数：

```json
{
  "label_rules": {
    "candidate": {
      "vr5_min": 1.8,
      "clv_min": 0.3,
      "br20_min": 0.98
    },
    "watch": {
      "vr5_min": 1.2,
      "clv_min": 0.0,
      "br20_min": 0.95
    }
  }
}
```

---

## 7. 当前已验证结果

在当前参数下，项目实测结果为：

- 股票数量：`5012`
- 总行数：`2649711`
- 结果数量：`4982`
- 硬过滤通过数量：`13`
- 候选数量：`39`
- 观察数量：`133`
- 跳过数量：`30`
- 失败数量：`0`

说明：
- 规则已经能筛出可人工复核的候选池
- Parquet 扫描与 CSV 扫描结果一致
- 当前瓶颈已经从“逻辑能不能跑”转移到“怎么把应用层做顺”

---

## 8. 当前推荐工作流

### 首次搭建
1. 跑 `p3_build_universe.py`
2. 跑 `p4_bootstrap_hist_all_resume.py`
3. 跑 `p6b_pack_hist_to_parquet.py`
4. 跑 `p7_scan_from_parquet_all.py`

### 日常使用
1. 跑 `p6_update_daily_hist.py`
2. 跑 `p6b_pack_hist_to_parquet.py`
3. 跑 `p7_scan_from_parquet_all.py`

---

## 9. GUI 路径设计

GUI 不应该直接把所有逻辑重写到界面里，而应该做成“界面层调用已有脚本 / 后续函数”的模式。

### GUI 的正确定位
GUI 应该只是一个操作面板，而不是重写数据逻辑。

未来 GUI 主要应该提供这些按钮：

1. **更新历史库**
   - 调用 `p6_update_daily_hist.py`

2. **打包扫描数据**
   - 调用 `p6b_pack_hist_to_parquet.py`

3. **执行全市场扫描**
   - 调用 `p7_scan_from_parquet_all.py`

4. **修改扫描参数**
   - 读写 `scan_config.json`

5. **查看扫描结果**
   - 打开 `output` 里的结果 CSV

6. **查看日志**
   - 打开 `logs` 里的日志文件

### GUI 推荐迭代路线

#### 第一步：本地轻量 GUI
建议技术路线：
- `tkinter` 或 `PySide6`

目标：
- 先做一个本地可点击面板
- 不求美观，先把：
  - 跑脚本
  - 看进度
  - 看结果
  - 改参数
  这些能力接起来

#### 第二步：把脚本逻辑进一步函数化
目前很多脚本还是“可直接运行脚本”的形态。  
后续为了 GUI 更稳，建议逐步改成：

- `main()` 入口
- 核心逻辑抽成函数
- GUI 只调用函数，不靠 subprocess 硬跑

#### 第三步：再考虑 EXE
等 GUI 稳了，再考虑：
- `PyInstaller` 打包本地 EXE

---

## 10. GitHub 仓库应该保存什么

应该保存：
- 脚本代码
- `README.md`
- `CHANGELOG.md`
- `scan_config.json`
- `.gitignore`

不应该保存：
- `data\daily_hist` 下的大量历史 CSV
- `data\packed\daily_hist_all.parquet`
- `output` 结果文件
- `logs` 日志文件
- `__pycache__`

---

## 11. 推荐 `.gitignore`

```gitignore
# Python
__pycache__/
*.pyc

# Data
data/daily_hist/
data/packed/

# Outputs
output/
logs/

# OS / editor
.DS_Store
Thumbs.db
.vscode/
```

---

## 12. 当前关键主线文件

当前最关键的主线文件有：

- `p3_build_universe.py`：构建股票池
- `p4_bootstrap_hist_all_resume.py`：初始化建库
- `p6_update_daily_hist.py`：日更 CSV 历史库
- `p6b_pack_hist_to_parquet.py`：打包扫描数据
- `p7_scan_from_parquet_all.py`：当前主力扫描脚本
- `scan_config.json`：参数配置

---

## 13. 当前项目状态摘要

目前已经完成：

- 本地部署
- Git 仓库初始化
- GitHub 仓库同步
- 全市场股票池构建
- 全市场历史库建库
- 每日增量更新脚本
- CSV 版全市场扫描
- I/O 性能诊断
- Parquet 打包层
- Parquet 版全市场扫描

当前最值得继续做的事：

1. 固化 README 和 CHANGELOG
2. 把主流程进一步整理成一键运行
3. 设计轻量 GUI 原型
4. 后续再考虑优先级输出、图形化结果展示、EXE、云端自动化