# Changelog

## 2026-03-29

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