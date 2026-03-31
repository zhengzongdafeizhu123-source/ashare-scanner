# 研究脚本升级说明：并行成功标签

这次升级把研究口径从单一成功定义，改成三套并行标签：

1. `d1_stable_flag`
   - 用来回答：D0筛到的票，D1收盘是否“稳健”。
   - 默认规则：以下三条满足至少两条
     - D1收盘 >= D0收盘
     - D1收盘 >= D0中枢价 `(D0高 + D0低)/2`
     - D1收盘 >= D1开盘（收成阳线或至少不弱）

2. `d2_sellable_flag`
   - 用来回答：D2是否给出了“可卖”的窗口。
   - 默认规则：满足任一条
     - D2最高相对D0收盘 >= 2%
     - D2命中 `target_price_1`

3. `success_composite_flag`
   - 综合成功：`D1稳健` 且 `D2有卖点`

## 文件说明

- `p9_build_research_dataset.py`
  - 构建研究样本，并把三套成功标签写入样本集。
- `p10_analyze_research_dataset.py`
  - 同时输出：
    - 成功标签总览
    - 按 bucket 的统计
    - 主标签的特征分箱统计
- `research_config.example.json`
  - 支持设置 `primary_success_label`，控制分箱统计使用哪套成功标签。

## 推荐用法

1. 先把 `research_config.example.json` 复制成 `research_config.json`
2. 运行：

```bash
python p9_build_research_dataset.py
```

3. 再运行：

```bash
python p10_analyze_research_dataset.py --dataset W:\AshareScanner\output\research\p9_research_dataset_xxx.parquet
```

## 推荐主标签

如果你的核心目标是：

- D1盘后要稳健
- D2可以卖

建议先把：

```json
"primary_success_label": "success_composite_flag"
```

作为默认主标签。
