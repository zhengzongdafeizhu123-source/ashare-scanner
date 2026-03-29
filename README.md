# AShare Scanner

一个本地运行的 A 股全市场扫描项目。

项目目标不是做一个“看盘软件”，而是搭建一条**可长期维护、可本地落地、可通过 GUI 控制的全市场扫描流水线**：

- 同步最新股票池
- 维护本地日线历史库
- 将全市场历史库打包为单个 Parquet
- 按自定义规则执行扫描
- 输出结果、候选、观察、异常、跳过与汇总文件

当前项目已经同时支持两种使用方式：

1. **脚本模式**：按步骤手动运行各阶段脚本
2. **GUI 模式**：通过 `gui_app.py` 在本地控制台中执行同步、补库、更新、打包和扫描

---

## 一、当前项目能力

### 1. 数据链路
项目当前已经具备以下完整链路：

- 同步全市场股票池
- 检查缺失股票
- 对缺失股票补建本地历史库
- 对已有历史库做日更
- 将全市场 CSV 历史库打包成单个 Parquet
- 从 Parquet 执行全市场扫描
- 输出扫描结果、候选、观察、异常、跳过和汇总文件

### 2. GUI 能力（当前版本）
当前已经有一个本地 Tkinter GUI，可执行以下操作：

- 一键日更扫描
- 同步股票池
- 补建缺失股票
- 仅更新历史库
- 仅打包 Parquet
- 仅扫描
- 打开 output 目录
- 打开 logs 目录
- 读取与保存 `scan_config.json` 中的核心扫描参数

> 说明：当前 GUI 已可用，但仍属于 **V1 可运行版**。  
> 后续还会继续优化布局、中文参数名、tooltip、进度条、ETA、结果表格展示等体验问题。

---

## 二、核心流程说明

### 1. 初始化 / 补库流程
适用于第一次搭建本地库，或正式补齐缺失股票时使用：

1. `p3_build_universe.py`
   - 同步最新股票池
2. `p4_bootstrap_hist_all_resume.py`
   - 初始化或补建本地历史库
3. `p6b_pack_hist_to_parquet.py`
   - 打包全市场历史库
4. `p7_scan_from_parquet_all.py`
   - 执行扫描

### 2. 日常流程
适用于已有完整本地库后的日常使用：

1. `p3_build_universe.py`
   - 同步最新股票池
2. `p6_update_daily_hist.py`
   - 更新已有本地历史库
3. `p6b_pack_hist_to_parquet.py`
   - 重打包 Parquet
4. `p7_scan_from_parquet_all.py`
   - 执行扫描

### 3. GUI 中“一键日更扫描”的真实逻辑
GUI 里的“一键日更扫描”不是简单地执行更新和扫描，而是按下面顺序串行执行：

1. 同步股票池
2. 检查缺失股票
3. 如果有缺失股票，则先补建缺失股票
4. 更新已有历史库
5. 打包 Parquet
6. 执行扫描

这条逻辑适合**正式环境**。  
但如果当前 `base_dir` 指向的是仅含少量样本 CSV 的测试目录，就会把其余股票全部识别为“缺失股票”，从而触发大规模补建，耗时可能数小时。

---

## 三、目录结构（关键文件）

```text
ashare-scanner/
├─ gui_app.py                         # GUI 控制台入口（Tkinter）
├─ gui_runner.py                      # GUI 编排层
├─ app_config.json                    # 运行根目录配置
├─ scan_config.json                   # 扫描参数配置
├─ README.md
├─ CHANGELOG.md
│
├─ p3_build_universe.py               # 同步股票池
├─ p4_bootstrap_hist_all_resume.py    # 初始化 / 补建本地历史库
├─ p5_scan_from_local_100.py          # 早期 CSV 小范围扫描脚本（保留）
├─ p5_scan_from_local_100_diagnose.py # 早期诊断脚本（保留）
├─ p6_update_daily_hist.py            # 更新已有本地历史库
├─ p6b_pack_hist_to_parquet.py        # 将全市场 CSV 打包为 parquet
├─ p7_scan_from_local_all.py          # 旧版 CSV 全市场扫描（保留）
├─ p7_scan_from_parquet_all.py        # 当前主力扫描脚本
├─ p7_profile_io.py                   # IO profiling 脚本
├─ p7_probe_file_format.py            # 文件格式探测脚本
│
└─ 其他测试或环境检查脚本
```

---

## 四、配置文件说明

### 1. `app_config.json`
用于指定项目运行根目录。

示例：

```json
{
  "base_dir": "W:\\AshareScanner\\project\\.runtime"
}
```

### 含义
所有这些目录都会基于 `base_dir` 解析：

- `data/daily_hist`
- `data/packed`
- `output`
- `logs`

### 推荐用法
#### 测试阶段
建议把 `base_dir` 指向一个隔离测试目录，例如：

```json
{
  "base_dir": "W:\\AshareScanner\\project\\.runtime"
}
```

这样不会污染你的正式数据目录。

#### 正式运行阶段
等 GUI 和流程稳定后，再把 `base_dir` 改成你的正式数据根目录，例如：

```json
{
  "base_dir": "W:\\AshareScanner"
}
```

> 注意：`app_config.json` 文件本身应该放在**项目根目录**，  
> 不是放进 `.runtime` 里。  
> `.runtime` 只是 `base_dir` 指向的运行产物目录。

---

### 2. `scan_config.json`
用于控制扫描规则与分层阈值。

当前主要包括两部分：

#### `hard_filters`
核心硬过滤参数，例如：

- `volatility_window`
- `volatility_max`
- `require_bullish`
- `volume_multiplier`
- `turnover_min`
- `min_history_bars`

#### `label_rules`
用于候选 / 观察分层的阈值，例如：

- `vr5_min`
- `clv_min`
- `br20_min`

GUI 当前已经支持读取与保存这几个核心参数。

---

## 五、脚本说明

## 1. `p3_build_universe.py`
用于同步最新股票池。

逻辑：
- 优先尝试 `stock_info_a_code_name()`
- 若失败，再回退到 `stock_zh_a_spot_em()`
- 做基础过滤：
  - A 股常见代码段
  - 排除 ST / *ST / 退市等名称

输出：
- `p3_universe_raw_YYYYMMDD.csv`
- `p3_universe_filtered_YYYYMMDD.csv`
- `p3_universe_summary_YYYYMMDD.csv`
- `p3_build_universe_YYYYMMDD.log`

---

## 2. `p4_bootstrap_hist_all_resume.py`
用于初始化全市场历史库，或补建缺失股票历史库。

当前支持两种模式：

### 批次模式
按 `START_INDEX + BATCH_SIZE` 跑一段股票池

### 清单模式
通过参数只处理指定股票代码列表，例如：

- `--stock-list-file`
- `--universe-file`
- `--start-date`
- `--skip-existing`

这也是 GUI 中“补建缺失股票”的底层实现方式。

---

## 3. `p6_update_daily_hist.py`
用于更新已有本地历史库。

逻辑：
- 遍历本地 `daily_hist/*.csv`
- 读取每只股票的最新本地日期
- 从下一交易日起请求新增数据
- 合并并覆盖写回原 CSV
- 输出成功 / 异常 / 跳过清单与日志

---

## 4. `p6b_pack_hist_to_parquet.py`
用于把全市场 CSV 历史库打包为一个 Parquet 文件。

输出：
- `data/packed/daily_hist_all.parquet`
- `p6b_pack_hist_summary_YYYYMMDD.csv`
- `p6b_pack_hist_errors_YYYYMMDD.csv`
- `p6b_pack_hist_YYYYMMDD.log`

---

## 5. `p7_scan_from_parquet_all.py`
当前主力扫描脚本。

逻辑：
- 读取单个 Parquet 文件
- 按股票分组计算指标
- 应用硬过滤规则
- 再根据标签规则分层为：
  - 候选
  - 观察
  - 放弃

输出：
- 全量结果
- 硬过滤通过
- 候选
- 观察
- 异常
- 跳过
- 汇总

---

## 6. `gui_runner.py`
这是 GUI 的编排层，不负责定义业务规则，只负责串流程。

当前封装的主要函数包括：

- `sync_universe()`
- `find_missing_stocks()`
- `bootstrap_missing_stocks()`
- `update_daily_hist()`
- `pack_to_parquet()`
- `scan_from_parquet()`
- `run_daily_pipeline()`

---

## 7. `gui_app.py`
Tkinter 图形界面入口。

当前支持：
- 按钮触发各阶段脚本
- 参数读取与保存
- 日志显示
- 最近一次运行结果显示
- 打开 output / logs 目录

当前版本定位：
- **V1 可运行版**
- 核心目标是稳定接通整条本地流程，而不是视觉完成度

---

## 六、运行方式

### 方式 A：GUI
启动：

```bash
python gui_app.py
```

适合日常使用与参数调整。

---

### 方式 B：脚本模式

#### 初始化 / 全量补库
```bash
python p3_build_universe.py
python p4_bootstrap_hist_all_resume.py
python p6b_pack_hist_to_parquet.py
python p7_scan_from_parquet_all.py
```

#### 日常运行
```bash
python p3_build_universe.py
python p6_update_daily_hist.py
python p6b_pack_hist_to_parquet.py
python p7_scan_from_parquet_all.py
```

---

## 七、典型输出文件

扫描完成后，常见输出包括：

- `p7_scan_from_parquet_all_results_YYYYMMDD.csv`
- `p7_scan_from_parquet_all_selected_YYYYMMDD.csv`
- `p7_scan_from_parquet_all_candidate_YYYYMMDD.csv`
- `p7_scan_from_parquet_all_watch_YYYYMMDD.csv`
- `p7_scan_from_parquet_all_errors_YYYYMMDD.csv`
- `p7_scan_from_parquet_all_skipped_YYYYMMDD.csv`
- `p7_scan_from_parquet_all_summary_YYYYMMDD.csv`

更新与打包阶段也会产出对应的：
- success
- errors
- skipped
- summary
- log

---

## 八、当前测试状态说明

当前项目已经完成以下联调验证：

- 同步股票池可运行
- 仅更新历史库可运行
- 仅打包 Parquet 可运行
- 仅扫描可运行
- GUI 能正确调用 `gui_runner.py`
- 关键脚本已统一：
  - UTF-8 stdout/stderr
  - `app_config.json -> base_dir`
  - 坏代理清理逻辑

但需要注意：

### 测试目录 `.runtime`
如果当前 `base_dir` 指向 `.runtime`，且里面只有少量样本 CSV：

- “仅更新历史库”只会处理这些样本文件
- “仅打包 Parquet”只会打包这些样本文件
- “仅扫描”也只会扫描这些样本股票
- “一键日更扫描”则会因为发现大量缺失股票，而触发全市场补建，耗时很长

所以：

- **测试 GUI 单按钮功能时**：`.runtime` 很合适
- **测试真实一键流程时**：应切换到正式完整历史库目录

---

## 九、已处理过的环境问题

当前版本已针对以下问题做过兼容：

### 1. Windows 中文输出编码问题
关键脚本统一处理了 UTF-8 stdout/stderr，减少 GUI 子进程中文输出报错。

### 2. 运行目录硬编码问题
关键脚本不再强依赖固定硬编码目录，而是统一读取 `app_config.json` 的 `base_dir`。

### 3. 异常代理环境问题
脚本会清理异常代理值（如 `127.0.0.1:9`），避免 AkShare 请求被坏代理拦截。

### 4. GUI 与脚本路径不一致问题
GUI / runner / 各阶段脚本的路径口径已统一。

---

## 十、注意事项

1. **首次全量补库很慢**
   - 补建历史库是逐股请求远端数据，耗时可能按小时计算。

2. **不要在测试目录里随意点击“一键日更扫描”**
   - 如果测试目录里只有少量样本 CSV，会触发全市场缺失补建。

3. **网络环境会显著影响股票池同步速度**
   - VPN / 代理 / 不稳定网络可能导致 AkShare 接口超时或断流。

4. **当前 GUI 主要解决“流程可视化”和“本地可操作性”**
   - 还不是最终形态
   - 后续会继续做体验优化

---

## 十一、后续计划（GUI V2）

下一阶段 GUI 重点包括：

- 参数区中文化
- 参数 tooltip
- 当前任务与最近结果拆开显示
- 进度条与 ETA
- 大量缺失股票补建前的确认弹窗
- 结果直接在 GUI 中表格展示
- 更紧凑、更合理的布局
- 更清晰的日志摘要与步骤状态

---

## 十二、适合当前阶段的使用建议

### 如果你还在测试 GUI
- 保持 `base_dir` 指向 `.runtime`
- 重点测试单按钮：
  - 同步股票池
  - 仅更新历史库
  - 仅打包 Parquet
  - 仅扫描

### 如果你准备正式使用
- 将 `app_config.json` 中的 `base_dir` 改到真实完整历史库目录
- 再使用“一键日更扫描”

---

## 十三、项目定位总结

这个项目当前已经不再只是“几个零散脚本”，而是一条逐步成形的本地 A 股扫描流水线：

- 数据链路已经打通
- GUI 控制面板已经落地
- 路径配置、编码、代理等运行环境问题已经开始统一治理
- 下一阶段主要是 GUI 体验与日常使用效率优化