# Changelog

## 2026-03-31 Version 3

### Added
- 新增研究实验室数据同步与分析链路：
  - `p8_sync_research_raw_tushare.py`
  - `p9_build_research_dataset.py`
  - `p10_analyze_research_dataset.py`
  - `p11_discover_parameter_ranges.py`
- 新增研究链路说明文档：
  - `README_RESEARCH_LAB.md`
  - `README_RESEARCH_LAB_SYNC.md`
  - `README_RESEARCH_LABELS.md`
  - `README_Adaptive_Analysis.md`
  - `README_PARAMETER_INTERVAL_V1.md`
- 新增研究配置文件：
  - `research_config.json`
  - `parameter_interval_config.json`

### Changed
- `p9_build_research_dataset.py` 改为按股票分批处理、按批次写出数据集，降低 8G 内存机器在大样本区间下的峰值内存占用。
- 研究样本构建流程改为按批次加载 `daily_basic / adj_factor / stk_limit / moneyflow`，不再把全量 research_raw 映射一次性常驻内存。
- 研究样本输出改为增量写 parquet，并只保留前 20 万行 CSV 预览，保持原有输出口径同时减少内存压力。

### Notes
- 当前 GitHub 版本从“扫描生产线 V2”扩展为“生产线 + 研究实验室”双链路版本。
- 研究实验室链路默认围绕 D0-D1-D2 事件样本、并行成功标签和参数区间发现展开。
- `p9_build_research_dataset.py` 的当前版本优先保证在低内存 Windows 机器上可稳定跑完，而不是追求一次性全量驻留内存的实现方式。

## 2026-03-29 Version 2

### Added
- 新增 `gui_app.py`，提供本地 Tkinter GUI 控制面板，支持：
  - 一键日更扫描
  - 同步股票池
  - 补建缺失股票
  - 仅更新历史库
  - 仅打包 Parquet
  - 仅扫描
  - 打开 output / logs 目录
  - 读取与保存 `scan_config.json`
- 新增 `gui_runner.py`，作为 GUI 调用的编排层，统一封装：
  - `sync_universe()`
  - `find_missing_stocks()`
  - `bootstrap_missing_stocks()`
  - `update_daily_hist()`
  - `pack_to_parquet()`
  - `scan_from_parquet()`
  - `run_daily_pipeline()`
- 新增 `app_config.json`，通过 `base_dir` 统一切换测试目录与正式目录

### Changed
- `p4_bootstrap_hist_all_resume.py` 新增参数：
  - `--stock-list-file`
  - `--universe-file`
  - `--start-date`
  - `--skip-existing`
  使其支持只对缺失股票进行补建，而不是只能按批次初始化建库
- `gui_runner.py` 改为通过 `p4_bootstrap_hist_all_resume.py` 补建缺失股票，不再在 runner 内重复实现历史数据抓取逻辑
- `p3_build_universe.py`、`p6_update_daily_hist.py`、`p6b_pack_hist_to_parquet.py`、`p7_scan_from_parquet_all.py` 全部改为基于 `app_config.json` 解析路径，而不是依赖固定硬编码目录

### Fixed
- 统一关键脚本的 UTF-8 stdout/stderr 配置，修复 Windows GUI 子进程中文输出编码问题
- 清理异常代理变量（如 `127.0.0.1:9`），减少 AkShare 请求因坏代理导致的失败
- 修复 `p3_build_universe.py` 在不可写目录下写日志/输出失败的问题
- 修复 `gui_runner.py` 与 `p4_bootstrap_hist_all_resume.py` 在 universe 文件定位上的不一致问题
- 修复 GUI 一键流程在测试目录下只能读取少量样本文件时，缺失股票补建链路无法正确衔接的问题

### Notes
- 当前 GUI 已可完成基础联调：
  - 同步股票池
  - 仅更新历史库
  - 仅打包 Parquet
  - 仅扫描
- 当前 `.runtime` 目录仅用于隔离测试；若在该目录执行“一键日更扫描”，由于本地历史库不完整，会触发大规模缺失股票补建，耗时较长

## 2026-03-29 Version 1

### Added
- 新增 `p6b_pack_hist_to_parquet.py`，用于把全市场 CSV 历史库打包为单个 Parquet 文件
- 新增 `p7_scan_from_parquet_all.py`，用于基于 Parquet 做全市场扫描
- 新增 `p7_profile_io.py`，用于分析 CSV 逐文件扫描的 I/O 耗时
- 新增 `p7_probe_file_format.py`，用于排查慢文件是否存在编码或格式异常
- 新增 `CHANGELOG.md`，用于记录项目更新历史
- 更新 `README.md`，补充项目结构、脚本说明、推荐工作流、GUI 路线与仓库边界

### Changed
- 将扫描主流程从“CSV 逐文件直扫”升级为“Parquet 打包后扫描”
- 明确 `p7_scan_from_parquet_all.py` 为当前推荐主力扫描脚本
- 保留 `p7_scan_from_local_all.py` 作为 CSV 直扫对照版，而不再作为默认日常扫描版本
- 将 `scan_config.json` 中的主扫描参数调整为更可用的区间：
  - `volatility_max: 0.35`
  - `volume_multiplier: 2.0`
  - `turnover_min: 5.0`

### Fixed
- 通过 profiling 发现项目瓶颈不在规则计算，而在连续读取 5000+ 小 CSV 的 I/O 开销
- 排除了“慢文件格式异常”这一假设，确认问题主要来自批量小文件读取方式
- 使用 Parquet 打包后，扫描结果与 CSV 版一致，同时速度大幅提升

### Current verified result
- 股票数量：5012
- 总行数：2649711
- 结果数量：4982
- 硬过滤通过数量：13
- 候选数量：39
- 观察数量：133
- 跳过数量：30
- 失败数量：0
