# Changelog

## 2026-04-01 Version 4

### Added
- 补充最新版项目总文档，统一沉淀为主 README：
  - `README.md`
- 新增路径治理与协作配置相关文件：
  - `project_paths.py`
  - `app_config.example.json`
  - `switch_output_profile.py`
  - `README_ZYB_SOP.md`

### Changed
- 修复 `p9_build_research_dataset.py` 在分批写入 parquet 时的 schema 不一致问题，统一 `list_age_days`、`d0_hit_count`、`d1_stable_score` 等批次字段类型，避免 `ValueError: Table schema does not match schema used to create file`。
- 将多份专题 README 的信息汇总回主文档，主 README 现在同时覆盖：
  - 项目背景与目标
  - 目录结构与数据链路
  - GUI 按钮与 Watchlist 说明
  - 生产线与研究线输入输出
  - 主要配置文件参数解释
- 清理已被主文档覆盖的旧说明文件，减少项目根目录噪音。
- 将路径解析收口到 `project_paths.py`，统一 `base_dir`、`output_profile`、输出目录分层和运行时优先级。
- `app_config.json` 改为可安全提交的公共默认配置，当前默认指向 `.runtime`，不再携带个人正式路径。
- 引入 `app_config.local.json` 作为本地真实配置入口，并通过 `.gitignore` 忽略，避免多人协作时互相覆盖本地路径。
- `switch_output_profile.py` 改为只写本地配置，不再修改公共 `app_config.json`。
- 以下入口脚本改为通过统一模块获取 `base_dir`，不再各自保留个人化硬编码路径：
  - `p3_build_universe.py`
  - `p4_bootstrap_hist_all_resume.py`
  - `p4_bootstrap_hist_all_tushare.py`
  - `p6_update_daily_hist.py`
  - `p6_update_daily_hist_tushare.py`
  - `p6b_pack_hist_to_parquet.py`
  - `p7_scan_from_parquet_all.py`
  - `p8_build_watchlist.py`
  - `p8_sync_research_raw_tushare.py`
  - `p9_build_research_dataset.py`
- `activate_a_share.bat` 与 `test_env.py` 去个人化，减少对固定机器路径的依赖。
- 输出目录进一步规范为按职责分层写入 `output/<profile>/universe|bootstrap|maintenance|scan|watchlist|research|research_raw_sync|samples`。

### Removed
- 删除以下已被主 README 吸收的历史文档：
  - `README_Adaptive_Analysis.md`
  - `README_PARAMETER_INTERVAL_V1.md`
  - `README_RESEARCH_LAB.md`
  - `README_RESEARCH_LAB_SYNC.md`
  - `README_RESEARCH_LABELS.md`
  - `README_TUSHARE_MIGRATION.md`
  - `README_PACKAGE.txt`
  - `README_newest.md`
- 删除空占位文件：
  - `git`

### Notes
- 这一版主要是“稳定性修复 + 文档收敛”版本，功能口径未变。
- 经过这轮整理后，项目根目录的主要入口文档只保留 `README.md` 和 `CHANGELOG.md`。
- 接手判断：这次整理前的仓库状态更接近“部分统一，部分混用”的 B 类状态；主流程已围绕 `app_config.json -> base_dir` 运作，但多处脚本仍残留重复配置解析与 `W:\AshareScanner` 这类个人化兜底。
- 本轮采取“最小侵入、兼容现有流程”的路径收口方式：保留 `.runtime` 测试沙箱接入方式，也暂时保留 `W:\AshareScanner` 作为低优先级旧行为兼容兜底。
- 尚未彻底消除的风险包括：`LEGACY_BASE_DIR` 静默回退风险、`tushare_config.json` 尚未本地化、并非所有配置都支持 local override、`.runtime` 与 `output_profile` 容易被混淆，以及文档中仍保留部分机器路径示例。

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

## 2026-03-30 Version 2.5

### Added
- 新增 Tushare 迁移与日更相关脚本：
  - `p4_bootstrap_hist_all_tushare.py`
  - `p6_update_daily_hist_tushare.py`
- 新增正式观察清单构建脚本：
  - `p8_build_watchlist.py`
- 新增 Tushare 配置文件：
  - `tushare_config.json`
- 新增迁移与打包说明文档：
  - `README_TUSHARE_MIGRATION.md`
  - `README_PACKAGE.txt`

### Changed
- `gui_app.py` 与 `gui_runner.py` 进行大幅重构，GUI 从“能跑”升级为更完整的操作台，工作流更贴近日常生产流程。
- 本地扫描链路与一键流程继续细化，界面中的配置读取、目录打开、结果展示、日志输出和任务串联更加完整。
- `p7_scan_from_parquet_all.py` 与 `scan_config.json` 继续调整，以适配 watchlist 生产逻辑和新的日更链路。
- 明确 Watchlist 在项目中的定位：它不是扫描原始结果，而是从扫描结果中提炼出的交易观察清单。

### Notes
- 这一轮提交把项目从“扫描器”进一步推进成“带 GUI 的日常工作台”。
- 数据源路径开始从 AkShare 单线演进为“AkShare 旧库 + Tushare 日更/迁移”的过渡形态。

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
  使其支持只对缺失股票进行补建，而不是只能按批次初始化建库。
- `gui_runner.py` 改为通过 `p4_bootstrap_hist_all_resume.py` 补建缺失股票，不再在 runner 内重复实现历史数据抓取逻辑。
- `p3_build_universe.py`、`p6_update_daily_hist.py`、`p6b_pack_hist_to_parquet.py`、`p7_scan_from_parquet_all.py` 全部改为基于 `app_config.json` 解析路径，而不是依赖固定硬编码目录。

### Fixed
- 统一关键脚本的 UTF-8 stdout/stderr 配置，修复 Windows GUI 子进程中文输出编码问题。
- 清理异常代理变量（如 `127.0.0.1:9`），减少 AkShare 请求因坏代理导致的失败。
- 修复 `p3_build_universe.py` 在不可写目录下写日志/输出失败的问题。
- 修复 `gui_runner.py` 与 `p4_bootstrap_hist_all_resume.py` 在 universe 文件定位上的不一致问题。
- 修复 GUI 一键流程在测试目录下只能读取少量样本文件时，缺失股票补建链路无法正确衔接的问题。

### Notes
- 当前 GUI 已可完成基础联调：
  - 同步股票池
  - 仅更新历史库
  - 仅打包 Parquet
  - 仅扫描
- 当前 `.runtime` 目录仅用于隔离测试；若在该目录执行“一键日更扫描”，由于本地历史库不完整，会触发大规模缺失股票补建，耗时较长。

## 2026-03-29 Version 1

### Added
- 新增 `p6b_pack_hist_to_parquet.py`，用于把全市场 CSV 历史库打包为单个 Parquet 文件。
- 新增 `p7_scan_from_parquet_all.py`，用于基于 Parquet 做全市场扫描。
- 新增 `p7_profile_io.py`，用于分析 CSV 逐文件扫描的 I/O 耗时。
- 新增 `p7_probe_file_format.py`，用于排查慢文件是否存在编码或格式异常。
- 新增 `p8_priority_from_results.py`，用于从扫描结果生成优先级视角的辅助输出。
- 新增 `scan_rules.py`，统一沉淀核心扫描规则。
- 新增 `CHANGELOG.md`，用于记录项目更新历史。
- 更新 `README.md`，补充项目结构、脚本说明、推荐工作流、GUI 路线与仓库边界。

### Changed
- 将扫描主流程从“CSV 逐文件直扫”升级为“Parquet 打包后扫描”。
- 明确 `p7_scan_from_parquet_all.py` 为当前推荐主力扫描脚本。
- 保留 `p7_scan_from_local_all.py` 作为 CSV 直扫对照版，而不再作为默认日常扫描版本。
- 将 `scan_config.json` 中的主扫描参数调整为更可用的区间：
  - `volatility_max: 0.35`
  - `volume_multiplier: 2.0`
  - `turnover_min: 5.0`

### Fixed
- 通过 profiling 发现项目瓶颈不在规则计算，而在连续读取 5000+ 小 CSV 的 I/O 开销。
- 排除了“慢文件格式异常”这一假设，确认问题主要来自批量小文件读取方式。
- 使用 Parquet 打包后，扫描结果与 CSV 版一致，同时速度大幅提升。

### Current verified result
- 股票数量：5012
- 总行数：2649711
- 结果数量：4982
- 硬过滤通过数量：13
- 候选数量：39
- 观察数量：133
- 跳过数量：30
- 失败数量：0

## 2026-03-29 Version 0

### Added
- 初始化项目代码仓库。
- 建立最早期的 CSV 本地扫描主链路：
  - `p1_single_stock_test.py`
  - `p2_sample_scan.py`
  - `p2_sample_scan_50.py`
  - `p3_build_universe.py`
  - `p4_bootstrap_hist_100.py`
  - `p4_bootstrap_hist_all_resume.py`
  - `p5_scan_from_local_100.py`
  - `p5_scan_from_local_100_diagnose.py`
  - `p6_update_daily_hist.py`
  - `p7_scan_from_local_all.py`
- 新增基础配置与环境探针：
  - `scan_config.json`
  - `test_env.py`
- 新增项目首版 `README.md`，完成最初的目标说明、目录说明与使用方法记录。

### Notes
- 这一版对应项目从 0 到 1 的启动期，核心能力还是“本地 CSV 历史库 + 规则扫描”。
- 后续的 Parquet 化、GUI 化、Watchlist 化和研究实验室链路，都是在这个起点上逐步演进出来的。
