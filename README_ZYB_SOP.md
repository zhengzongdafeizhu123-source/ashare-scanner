# AShareScanner 给朋友的上手 SOP

## 1. 这份文档是干什么的

这份 SOP 是给第一次接手 `AShareScanner` 的人准备的。

目标不是讲全项目历史，而是让你能尽快搞清楚三件事：

1. 现在这套系统每天到底在跑什么
2. 哪些按钮和脚本是当前主力入口
3. 怎样不跑错目录、不把正式和测试结果混掉

如果你需要项目全景说明，再配合主文档一起看：

- [README.md](/w:/AshareScanner/project/README.md)

---

## 2. 先建立最重要的认知

这套系统有两条主链路：

```text
生产链路
同步股票池 -> 补缺失历史 -> 日更历史库 -> 打包 parquet -> 扫描 -> 生成 Watchlist

研究链路
同步 research_raw -> 构建 P9 数据集 -> 跑 P10 分析 -> 跑 P11 区间发现
```

你第一次接手时，优先看懂生产链路。

研究链路不是 GUI 日常值班的一部分，它是单独的分析工作流。

---

## 3. 当前这台机器的实际目录

我已经确认过，这台机器当前本地真实配置是：

注意：下面这段是**当前机器示例**，不是新协作者应该原样照抄的配置。

```json
{
  "base_dir": "C:\\Users\\wseba\\OneDrive\\AshareScanner",
  "output_profile": "main"
}
```

也就是说，这台机器当前正式根目录是：

- `C:\Users\wseba\OneDrive\AshareScanner`
  这是**当前机器示例路径**，不是朋友机器上的固定答案

我还能直接读到这套目录，当前已确认存在：

- `data/`
- `output/`
- `output/main/`
- `output/test/`

其中 `output/main/` 下面已经按新版脚本分层：

- `universe/`
- `bootstrap/`
- `maintenance/`
- `scan/`
- `watchlist/`
- `research/`
- `research_raw_sync/`
- `diagnostics/`
- `samples/`

所以你现在要记住一件事：

- 正式和测试不再只是“有没有 `.runtime`”的区别
- 现在还有 `output_profile = main/test` 这层输出分流
- `main/test` 只切 `output/...`
- 真正决定整套 `data/output/logs` 根目录的是 `base_dir`

---

## 4. 先看懂配置，不要一上来就点按钮

当前路径配置的读取优先级是：

1. 环境变量：`ASHARE_APP_CONFIG` / `ASHARE_BASE_DIR` / `ASHARE_OUTPUT_PROFILE`
2. `app_config.local.json`
3. `app_config.json`
4. `app_config.example.json`
5. 旧兼容兜底：`W:\AshareScanner`
6. 最后回退到 `project/.runtime`

你最该看的几个文件是：

- [project_paths.py](/w:/AshareScanner/project/project_paths.py)
  统一路径解析入口
- [app_config.example.json](/w:/AshareScanner/project/app_config.example.json)
  公共模板
- [app_config.json](/w:/AshareScanner/project/app_config.json)
  公共默认配置，当前默认是 `.runtime`
- `app_config.local.json`
  这台机器自己的真实路径配置，优先级最高，不提交到 Git

最重要的原则：

- 个人机器只改 `app_config.local.json`
- 不要改公共的 `app_config.json`

如果要切换输出档位，使用：

```powershell
python switch_output_profile.py main
python switch_output_profile.py test
```

这只会改本地配置，不会改公共配置。

---

## 5. 第一天应该怎么做

第一天不要直接点“一键日更扫描”。

建议按这个顺序：

1. 确认当前 `base_dir`
2. 确认当前 `output_profile`
3. 确认 conda 环境能正常启动
4. 打开 GUI
5. 单独跑一遍每个生产步骤
6. 最后再理解“一键日更扫描”只是把这些步骤串起来

研究链路也一样：

1. 先同步 `research_raw`
2. 再构建 P9
3. 再跑 P10
4. 最后跑 P11

---

## 6. 开始前检查清单

### 6.1 检查当前运行根目录

打开 `app_config.local.json`，确认：

- `base_dir` 是否指向你想操作的目录
- `output_profile` 是否是你想写入的档位

示例：

重点：这里真正需要按自己机器修改的是 `base_dir`。

```json
{
  "base_dir": "C:\\Users\\wseba\\OneDrive\\AshareScanner",
  "output_profile": "main"
}
```

重点提醒：

- `base_dir` 要改成你自己机器上的实际目录
- `output_profile` 通常先保持 `main`
- 不要照抄 `C:\\Users\\wseba\\OneDrive\\AshareScanner`

判断方法：

- `base_dir` 指向正式目录：你会影响正式数据
- `base_dir` 指向 `.runtime`：你是在测试沙箱
- `output_profile = main`：输出写到 `output/main/...`
- `output_profile = test`：输出写到 `output/test/...`

特别注意：

- `.runtime` 是“整套根目录切换”
- `output_profile` 是“输出分流”
- 这两个不是一回事
- `main/test` 只切 `output/main/...` 和 `output/test/...`
- 它不会切走 `data/`，也不会单独切出另一套 `logs/`

### 6.2 检查 conda 环境

在 [project](/w:/AshareScanner/project) 目录下执行：

```powershell
conda activate a_share
```

或者：

```powershell
.\activate_a_share.bat
```

这一步的意义是：

- 确保后面所有脚本用的都是项目依赖齐全的 Python 环境

### 6.3 检查 GUI 能打开

```powershell
python gui_app.py
```

成功标志：

- GUI 正常启动
- 能看到当前 `base_dir`
- 各主要按钮可点击
- 日志区能正常刷信息

### 6.4 检查 Tushare token

优先检查环境变量 `TUSHARE_TOKEN`，或者本地的 `tushare_config.local.json`，确认 token 可用。

因为这些脚本都依赖它：

- `p4_bootstrap_hist_all_tushare.py`
- `p6_update_daily_hist_tushare.py`
- `p8_sync_research_raw_tushare.py`

注意：

- 真实 token 应放在 `tushare_config.local.json`
- `tushare_config.example.json` 和公共 `tushare_config.json` 只应该保留模板
- 不要把真实 token 提交到远端

---

## 7. GUI 按钮和脚本的对应关系

```text
GUI按钮                        背后主函数 / 脚本
一键日更扫描                   gui_runner.run_daily_pipeline()
同步股票池                     gui_runner.sync_universe() -> p3_build_universe.py
补建缺失股票                   gui_runner.bootstrap_missing_stocks() -> p4_bootstrap_hist_all_resume.py
仅更新历史库                   gui_runner.update_daily_hist() -> p6_update_daily_hist_tushare.py
仅打包 Parquet                 gui_runner.pack_to_parquet() -> p6b_pack_hist_to_parquet.py
仅扫描                         gui_runner.scan_from_parquet() -> p7_scan_from_parquet_all.py
生成 Watchlist                 gui_runner.build_watchlist() -> p8_build_watchlist.py
Watchlist                      打开 Watchlist 管理窗口
```

补充说明：

- `一键日更扫描` 会跑生产主链路
- 它默认不替你自动点“生成 Watchlist”
- `Watchlist` 是单独窗口，不是扫描结果表本身

---

## 8. 生产链路，一步一步怎么跑

### 8.1 `同步股票池`

这一步在干什么：

- 拉最新股票池
- 过滤出当前策略真正要扫描的股票范围

主要输出：

- `output/<profile>/universe/p3_universe_raw_YYYYMMDD.csv`
- `output/<profile>/universe/p3_universe_filtered_YYYYMMDD.csv`
- `output/<profile>/universe/p3_universe_summary_YYYYMMDD.csv`

成功标志：

- 新的 `filtered` 文件生成了
- `summary` 里的股票池数量看起来正常

### 8.2 `补建缺失股票`

这一步在干什么：

- 找出股票池里有、但本地历史库缺失的股票
- 补齐 `data/daily_hist/*.csv`

适用场景：

- 第一次建库
- 股票池新增了股票
- 某些 CSV 丢了或损坏了

主要输出：

- `data/daily_hist/*.csv`
- `output/<profile>/bootstrap/p4_bootstrap_all_success_*.csv`
- `output/<profile>/bootstrap/p4_bootstrap_all_errors_*.csv`
- `output/<profile>/bootstrap/p4_bootstrap_all_skipped_*.csv`

### 8.3 `仅更新历史库`

这一步在干什么：

- 对已有股票历史 CSV 做日更

当前 GUI 主力走的是：

- `p6_update_daily_hist_tushare.py`

主要输出：

- `output/<profile>/maintenance/p6_update_daily_hist_tushare_success_*.csv`
- `output/<profile>/maintenance/p6_update_daily_hist_tushare_errors_*.csv`
- `output/<profile>/maintenance/p6_update_daily_hist_tushare_skipped_*.csv`
- `output/<profile>/maintenance/p6_update_daily_hist_tushare_summary_*.csv`

成功标志：

- `summary` 文件生成
- `success` 明显多于 `errors`
- 最新交易日看起来正常

### 8.4 `仅打包 Parquet`

这一步在干什么：

- 把 5000 多个逐股 CSV 合并成一张主表

为什么不能跳过：

- 当前主力扫描器 `p7_scan_from_parquet_all.py` 直接读的是 `data/packed/daily_hist_all.parquet`

主要输出：

- `data/packed/daily_hist_all.parquet`
- `output/<profile>/maintenance/p6b_pack_hist_summary_*.csv`
- `output/<profile>/maintenance/p6b_pack_hist_errors_*.csv`

### 8.5 `仅扫描`

这一步在干什么：

- 对 `daily_hist_all.parquet` 做 D0 扫描

主要输出：

- `output/<profile>/scan/p7_scan_from_parquet_all_results_*.csv`
- `output/<profile>/scan/p7_scan_from_parquet_all_selected_*.csv`
- `output/<profile>/scan/p7_scan_from_parquet_all_candidate_*.csv`
- `output/<profile>/scan/p7_scan_from_parquet_all_watch_*.csv`
- `output/<profile>/scan/p7_scan_from_parquet_all_skipped_*.csv`
- `output/<profile>/scan/p7_scan_from_parquet_all_errors_*.csv`
- `output/<profile>/scan/p7_scan_from_parquet_all_summary_*.csv`

成功标志：

- `summary` 已生成
- `errors` 很低或为 0
- `selected / candidate / watch` 文件都齐

### 8.6 `生成 Watchlist`

这一步在干什么：

- 把扫描结果整理成给人复盘和跟踪的观察池

它不是简单复制扫描结果，而是把：

- 入围
- 候选
- 观察

整合成一个更适合人工使用的工作表。

主要输出：

- `output/<profile>/watchlist/watchlist_master.csv`
- `output/<profile>/watchlist/watchlist_summary_*.csv`
- `output/<profile>/watchlist/snapshots/*_watchlist_snapshot.csv`

### 8.7 `Watchlist`

这一步在干什么：

- 打开 Watchlist 管理窗口
- 看条目详情
- 写备注
- 标记 favorites
- 保存回主表

---

## 9. 标准值班顺序

第一次上手，请按这个顺序：

1. 打开 GUI
2. 点 `同步股票池`
3. 如有需要，点 `补建缺失股票`
4. 点 `仅更新历史库`
5. 点 `仅打包 Parquet`
6. 点 `仅扫描`
7. 点 `生成 Watchlist`
8. 打开 `Watchlist`

最后再去理解：

- `一键日更扫描` 只是把前 2 到 6 步自动串起来

---

## 10. 命令行版最小 SOP

如果 GUI 出问题，可以按命令行逐步跑。

先进入项目目录并激活环境：

重点：下面的 `W:\AshareScanner\project` 是历史示例路径；你朋友应改成自己机器上的项目目录。

```powershell
cd W:\AshareScanner\project
conda activate a_share
```

然后依次执行：

```powershell
python p3_build_universe.py
python p6_update_daily_hist_tushare.py
python p6b_pack_hist_to_parquet.py
python p7_scan_from_parquet_all.py
python p8_build_watchlist.py
```

如果需要研究链路，再执行：

```powershell
python p8_sync_research_raw_tushare.py
python p9_build_research_dataset.py --batch-size-symbols 100
```

---

## 11. 研究链路怎么理解

研究链路和 GUI 日更是解耦的。

它的顺序是：

### 11.1 同步 `research_raw`

```powershell
python p8_sync_research_raw_tushare.py
```

主要输出：

- `data/research_raw/.../*.parquet`
- `output/<profile>/research_raw_sync/p8_research_raw_sync_summary_*.csv`

### 11.2 构建 P9 数据集

```powershell
python p9_build_research_dataset.py --batch-size-symbols 100
```

主要输出：

- `output/<profile>/research/p9_research_dataset_*.parquet`
- `output/<profile>/research/p9_research_dataset_*.csv`
- `output/<profile>/research/p9_research_dataset_summary_*.csv`
- `output/<profile>/research/p9_research_dataset_skipped_*.csv`

说明：

- 8G 机器建议先用较小批次
- 如果吃紧，可以再降到 `50`

### 11.3 跑 P10

```powershell
python p10_analyze_research_dataset.py --dataset <你的p9数据集路径>
```

主要输出：

- `output/<profile>/research/p10_research_analysis_overview_*.csv`
- `output/<profile>/research/p10_research_analysis_bucket_stats_*.csv`
- `output/<profile>/research/p10_research_analysis_feature_bins_*.csv`
- `output/<profile>/research/p10_research_analysis_report_*.md`

### 11.4 跑 P11

```powershell
python p11_discover_parameter_ranges.py --dataset <你的p9数据集路径>
```

主要输出：

- `output/<profile>/research/p11_parameter_interval_summary_*.csv`
- `output/<profile>/research/p11_parameter_interval_bins_*.csv`
- `output/<profile>/research/p11_parameter_interval_segments_*.csv`
- `output/<profile>/research/p11_parameter_interval_report_*.md`

---

## 12. 新人最容易踩的坑

### 12.1 路径跑错

现象：

- 你以为在正式目录，实际写进了 `.runtime`
- 你以为只是切测试输出，实际把整个 `base_dir` 切走了

根因：

- 没看清 `base_dir` 和 `output_profile` 是两层不同机制

### 12.2 忘记重新打包 parquet

现象：

- 历史库明明更新了，扫描结果却像没更新

根因：

- 更新了 CSV，但没重跑 `p6b_pack_hist_to_parquet.py`

### 12.3 一键失败但不知道挂哪一步

处理方法：

- 回退到单步执行
- 按 `同步股票池 -> 仅更新历史库 -> 仅打包 Parquet -> 仅扫描` 一步一步查

### 12.4 P9 内存占用高

处理方法：

```powershell
python p9_build_research_dataset.py --batch-size-symbols 100
```

必要时：

```powershell
python p9_build_research_dataset.py --batch-size-symbols 50
```

### 12.5 Tushare token 被覆盖或失效

现象：

- Tushare 脚本一启动就失败

处理方法：

- 检查 [tushare_config.json](/w:/AshareScanner/project/tushare_config.json)
- 检查 token 是否有效
- 记住这个文件目前还不是本地 override 机制

### 12.6 旧路径示例看混了

现象：

- 文档里看到了 `W:\AshareScanner`
- 配置里却是 `C:\Users\wseba\OneDrive\AshareScanner`

解释：

- `W:\AshareScanner` 仍然是兼容兜底和历史示例
- `C:\Users\wseba\OneDrive\AshareScanner` 是当前机器示例
- 真正应该以自己机器 `app_config.local.json` 里的 `base_dir` 为准

---

## 13. 一句话值班卡片

```text
正式值班
先看 app_config.local.json
-> 确认 base_dir
-> 确认 output_profile
-> 打开 GUI
-> 同步股票池
-> 必要时补建缺失股票
-> 仅更新历史库
-> 仅打包 Parquet
-> 仅扫描
-> 生成 Watchlist
-> 打开 Watchlist 做人工复盘

研究流程
同步 research_raw
-> P9 构建样本
-> P10 分析
-> P11 发现参数区间
```

读完这份 SOP 后，你应该能独立回答：

- 当前正式目录在哪里
- `main/test` 输出写到哪里
- 每个按钮后面跑的是哪个脚本
- 生产链路为什么一定要先更新再打包再扫描
- Watchlist 为什么不是扫描结果本身
