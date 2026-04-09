from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import re
import threading
import time
import traceback
import tkinter as tk
import tkinter.font as tkfont
from tkinter import messagebox, ttk

PROJECT_DIR = Path(__file__).resolve().parent
SCAN_CONFIG_FILE = PROJECT_DIR / "scan_config.json"
MISSING_CONFIRM_THRESHOLD = 100
pd = None
# loaded lazily
_gui_runner = None


def _ensure_runtime_modules():
    global pd, _gui_runner
    if pd is None:
        import pandas as _pd
        pd = _pd
    if _gui_runner is None:
        import gui_runner as gui_runner_mod
        _gui_runner = gui_runner_mod
    return pd, _gui_runner


STEP_LABELS = {
    "sync_universe": "同步股票池",
    "find_missing_stocks": "检查缺失股票",
    "bootstrap_missing_stocks": "补建缺失股票",
    "update_daily_hist": "更新历史库",
    "pack_to_parquet": "打包 Parquet",
    "scan_from_parquet": "执行扫描",
    "build_watchlist": "生成 Watchlist",
    "run_daily_pipeline": "一键日更扫描",
}

FIELD_META = {
    "volatility_window": {"label": "波动窗口（天）", "tooltip": "用于计算区间波动率的历史窗口长度。", "type": "entry"},
    "volatility_max": {"label": "波动率上限", "tooltip": "窗口内最高价 / 最低价 - 1 的上限。", "type": "entry"},
    "require_bullish": {"label": "是否要求阳线", "tooltip": "开启后，仅保留当日收盘价高于开盘价的股票。", "type": "check"},
    "volume_multiplier_min": {"label": "放量倍数下限", "tooltip": "筛选当天成交量相对前一日成交量的最小倍数。", "type": "entry"},
    "volume_multiplier_max": {"label": "放量倍数上限", "tooltip": "筛选当天成交量相对前一日成交量的最大倍数。", "type": "entry"},
    "turnover_min": {"label": "换手率下限（%）", "tooltip": "低于该换手率阈值的股票直接跳过。", "type": "entry"},
    "cold_volume_window": {"label": "冷量回看窗口（天）", "tooltip": "回看多少个交易日，用于判断筛选日成交量是否显著高于过去低迷区间。", "type": "entry"},
    "cold_volume_ratio": {"label": "前高量占比上限", "tooltip": "回看窗口内任意一天成交量都不能超过筛选日成交量的该比例。0.8333 约等于 5/6。", "type": "entry"},
    "min_history_bars": {"label": "最少历史条数", "tooltip": "不足该条数的股票直接跳过扫描。", "type": "entry"},
}
FIELD_ORDER = list(FIELD_META.keys())

PROGRESS_PATTERNS = [
    re.compile(r"\[进度\]\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)"),
    re.compile(r"(?<!\d)(?P<done>\d+)\s*/\s*(?P<total>\d+)(?!\d)"),
]

RESULT_TABS = {"results": "全部", "selected": "入围", "candidate": "候选", "watch": "观察"}
RESULT_DISPLAY_COLUMNS = [
    "股票代码",
    "股票名称",
    "日期",
    "涨跌幅%",
    "换手率",
    "量比前一日",
    "VR5",
    "BR20",
    "命中硬过滤数",
    "分层标签",
    "硬过滤是否通过",
]
COLUMN_WIDTHS = {
    "股票代码": 86,
    "股票名称": 120,
    "日期": 95,
    "涨跌幅%": 78,
    "换手率": 78,
    "量比前一日": 92,
    "VR5": 72,
    "BR20": 72,
    "命中硬过滤数": 92,
    "分层标签": 78,
    "硬过滤是否通过": 96,
}
SORTABLE_COLUMNS = ["股票代码", "股票名称", "日期", "涨跌幅%", "换手率", "量比前一日", "VR5", "BR20", "命中硬过滤数", "分层标签"]
DEFAULT_ASCENDING_COLUMNS = {"股票代码", "股票名称", "日期", "分层标签"}
WATCHLIST_COLUMNS = [
    "setup_date",
    "股票代码",
    "股票名称",
    "source_bucket",
    "status",
    "next_stage",
    "d0_label",
    "d0_pct_chg",
    "d0_turnover",
    "d0_vr5",
    "d0_br20",
    "breakout_price",
    "support_price_1",
    "support_price_2",
    "target_price_1",
    "target_price_2",
]
WATCHLIST_COLUMN_WIDTHS = {
    "setup_date": 95,
    "股票代码": 86,
    "股票名称": 120,
    "source_bucket": 82,
    "status": 86,
    "next_stage": 92,
    "d0_label": 72,
    "d0_pct_chg": 78,
    "d0_turnover": 78,
    "d0_vr5": 72,
    "d0_br20": 72,
    "breakout_price": 92,
    "support_price_1": 92,
    "support_price_2": 92,
    "target_price_1": 92,
    "target_price_2": 92,
}
WATCHLIST_SOURCE_LABELS = {"selected": "入围", "candidate": "候选", "watch": "观察"}
WATCHLIST_SOURCE_ORDER = {"selected": 0, "candidate": 1, "watch": 2}
WATCHLIST_FIELD_LABELS = {
    "watch_id": "事件ID",
    "setup_date": "入池日期",
    "股票代码": "股票代码",
    "股票名称": "股票名称",
    "source_bucket": "来源分层",
    "source_bucket_cn": "来源分层",
    "entry_reason": "入池说明",
    "d0_failed_reason_display": "D0未通过原因",
    "分层标签说明": "分层说明",
    "status": "状态",
    "next_stage": "下一阶段",
    "d0_date": "D0日期",
    "d0_open": "D0开盘",
    "d0_close": "D0收盘",
    "d0_high": "D0最高",
    "d0_low": "D0最低",
    "d0_pct_chg": "D0涨跌幅%",
    "d0_turnover": "D0换手率",
    "d0_volume_ratio_prev1": "D0量比前一日",
    "d0_vr5": "D0-VR5",
    "d0_clv": "D0-CLV",
    "d0_br20": "D0-BR20",
    "d0_hit_count": "D0命中硬过滤数",
    "d0_hard_pass": "D0硬过滤通过",
    "d0_failed_reason": "D0未通过原因",
    "d0_label": "D0分层标签",
    "atr14": "ATR14",
    "prev20_high": "近20日高点",
    "breakout_price": "突破线",
    "support_price_1": "承接线",
    "support_price_2": "失效线",
    "mid_price": "中枢线",
    "target_price_1": "目标位1",
    "target_price_2": "目标位2",
    "created_at": "创建时间",
    "updated_at": "更新时间",
    "review_note": "复盘备注",
    "d1_action": "D1操作",
    "d2_action": "D2操作",
    "final_result_tag": "最终标签",
}

class ToolTip:

    def __init__(self, widget: tk.Widget, text: str):
        self.widget = widget
        self.text = text
        self.tip_window: tk.Toplevel | None = None
        self.widget.bind("<Enter>", self.show_tip, add="+")
        self.widget.bind("<Leave>", self.hide_tip, add="+")
        self.widget.bind("<ButtonPress>", self.hide_tip, add="+")

    def show_tip(self, _event=None):
        if self.tip_window is not None or not self.text:
            return
        x = self.widget.winfo_rootx() + 12
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 6
        self.tip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, justify="left", background="#fff8dc", relief="solid", borderwidth=1, padx=8, pady=5, wraplength=280)
        label.pack()

    def hide_tip(self, _event=None):
        if self.tip_window is not None:
            self.tip_window.destroy()
            self.tip_window = None


class MissingActionDialog(tk.Toplevel):
    def __init__(self, parent: tk.Tk, missing_count: int, base_dir: str):
        super().__init__(parent)
        self.title("确认补建策略")
        self.resizable(False, False)
        self.result = "cancel"
        self.transient(parent)
        self.grab_set()
        frame = ttk.Frame(self, padding=16)
        frame.grid(row=0, column=0, sticky="nsew")
        message = (
            f"检测到大量缺失股票：{missing_count} 只。\n\n"
            "继续将触发大规模补建，可能耗时数小时。\n"
            f"当前 base_dir：{base_dir}\n\n"
            "请选择后续操作。"
        )
        ttk.Label(frame, text=message, justify="left", wraplength=420).grid(row=0, column=0, columnspan=3, sticky="w")
        ttk.Button(frame, text="继续完整流程", command=lambda: self._close("continue")).grid(row=1, column=0, sticky="ew", pady=(14, 0), padx=(0, 6))
        ttk.Button(frame, text="取消", command=lambda: self._close("cancel")).grid(row=1, column=1, sticky="ew", pady=(14, 0), padx=6)
        ttk.Button(frame, text="跳过补建，仅继续后续步骤", command=lambda: self._close("skip")).grid(row=1, column=2, sticky="ew", pady=(14, 0), padx=(6, 0))
        for col in range(3):
            frame.columnconfigure(col, weight=1)
        self.protocol("WM_DELETE_WINDOW", lambda: self._close("cancel"))
        self.update_idletasks()
        x = parent.winfo_rootx() + max(0, (parent.winfo_width() - self.winfo_width()) // 2)
        y = parent.winfo_rooty() + max(0, (parent.winfo_height() - self.winfo_height()) // 2)
        self.geometry(f"+{x}+{y}")

    def _close(self, result: str):
        self.result = result
        self.destroy()


class ParameterWindow(tk.Toplevel):
    def __init__(self, app: "GuiApp"):
        super().__init__(app.root)
        self.app = app
        self.title("扫描参数")
        self.geometry("520x430")
        self.minsize(470, 360)
        self.transient(app.root)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.status_var = tk.StringVar(value="修改后自动保存到 scan_config.json")
        frame = ttk.Frame(self, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)
        row = 0
        for key in FIELD_ORDER:
            meta = FIELD_META[key]
            label = ttk.Label(frame, text=meta["label"])
            label.grid(row=row, column=0, sticky="w", pady=5, padx=(0, 8))
            ToolTip(label, meta["tooltip"])
            if meta["type"] == "check":
                widget = ttk.Checkbutton(frame, variable=app.config_vars[key], command=self._auto_save)
            else:
                widget = ttk.Entry(frame, textvariable=app.config_vars[key])
                widget.bind("<FocusOut>", self._auto_save)
                widget.bind("<Return>", self._auto_save)
            widget.grid(row=row, column=1, sticky="ew", pady=5)
            ToolTip(widget, meta["tooltip"])
            row += 1
        btns = ttk.Frame(frame)
        btns.grid(row=row, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        for col in range(3):
            btns.columnconfigure(col, weight=1)
        ttk.Button(btns, text="从文件重载", command=self._reload).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(btns, text="立即保存", command=self._save_now).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(btns, text="关闭", command=self._on_close).grid(row=0, column=2, sticky="ew", padx=(4, 0))
        ttk.Label(frame, textvariable=self.status_var, foreground="#0b5394", wraplength=460).grid(row=row + 1, column=0, columnspan=2, sticky="ew", pady=(10, 0))

    def _reload(self):
        self.app.load_config_to_form(log_message=False)
        self.status_var.set("已从本地 scan_config.json 重载。")

    def _save_now(self):
        ok, msg = self.app.save_config_from_form(silent=True)
        self.status_var.set(msg)

    def _auto_save(self, _event=None):
        ok, msg = self.app.save_config_from_form(silent=True)
        self.status_var.set(msg)

    def _on_close(self):
        self.app.param_window = None
        self.destroy()


class LogWindow(tk.Toplevel):
    def __init__(self, app: "GuiApp"):
        super().__init__(app.root)
        self.app = app
        self.title("运行日志")
        self.geometry("980x680")
        self.minsize(760, 420)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        toolbar = ttk.Frame(self, padding=(10, 10, 10, 0))
        toolbar.grid(row=0, column=0, sticky="ew")
        toolbar.columnconfigure(0, weight=1)
        ttk.Label(toolbar, text="日志独立窗口。支持横向与纵向滚动。", justify="left").grid(row=0, column=0, sticky="w")
        ttk.Button(toolbar, text="清空日志", command=app.clear_logs).grid(row=0, column=1, sticky="e")
        self.notebook = ttk.Notebook(self)
        self.notebook.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        app.log_texts = {}
        for key, title in [("all", "全部"), ("raw", "原始日志"), ("summary", "步骤摘要"), ("error", "错误")]:
            self._create_tab(key, title)
        app._configure_log_tags()
        app._replay_logs()

    def _create_tab(self, key: str, title: str):
        tab = ttk.Frame(self.notebook)
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        self.notebook.add(tab, text=title)
        text = tk.Text(tab, wrap="none", state="disabled")
        text.grid(row=0, column=0, sticky="nsew")
        ybar = ttk.Scrollbar(tab, orient="vertical", command=text.yview)
        ybar.grid(row=0, column=1, sticky="ns")
        xbar = ttk.Scrollbar(tab, orient="horizontal", command=text.xview)
        xbar.grid(row=1, column=0, sticky="ew")
        text.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        self.app.log_texts[key] = text

    def _on_close(self):
        self.app.log_window = None
        self.app.log_texts = {}
        self.destroy()



class WatchlistWindow(tk.Toplevel):
    EDITABLE_TEXT_COLUMNS = [
        "review_note",
        "updated_at",
        "status",
        "next_stage",
        "d1_action",
        "d2_action",
        "final_result_tag",
    ]

    TABLE_COLUMNS = [
        ("自选", "is_favorite", 58),
        ("入池日期", "setup_date", 90),
        ("股票代码", "股票代码", 86),
        ("股票名称", "股票名称", 118),
        ("来源", "source_bucket_cn", 72),
        ("状态", "status", 82),
        ("下一阶段", "next_stage", 92),
        ("D0标签", "d0_label", 72),
        ("涨跌幅%", "d0_pct_chg", 76),
        ("换手率", "d0_turnover", 76),
        ("VR5", "d0_vr5", 70),
        ("BR20", "d0_br20", 70),
        ("突破线", "breakout_price", 86),
        ("承接线", "support_price_1", 86),
        ("失效线", "support_price_2", 86),
        ("目标1", "target_price_1", 86),
        ("目标2", "target_price_2", 86),
    ]

    def __init__(self, app: "GuiApp"):
        super().__init__(app.root)
        self.app = app
        self.title("Watchlist / 复盘观察池")
        self.geometry("1280x820")
        self.minsize(980, 620)
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)

        self.keyword_var = tk.StringVar(value="")
        self.source_var = tk.StringVar(value="全部")
        self.only_favorite_var = tk.BooleanVar(value=False)
        self.info_var = tk.StringVar(value="尚未加载 watchlist。")

        self.master_df = pd.DataFrame()
        self.view_df = pd.DataFrame()
        self.favorite_ids = set()

        top = ttk.Frame(self, padding=10)
        top.grid(row=0, column=0, sticky="ew")
        for col in range(10):
            top.columnconfigure(col, weight=0)
        top.columnconfigure(1, weight=1)

        ttk.Label(top, text="关键词", style="Small.TLabel").grid(row=0, column=0, sticky="w")
        keyword = ttk.Entry(top, textvariable=self.keyword_var)
        keyword.grid(row=0, column=1, sticky="ew", padx=(6, 10))
        keyword.bind("<Return>", lambda _e: self.apply_filters())

        ttk.Label(top, text="来源筛选", style="Small.TLabel").grid(row=0, column=2, sticky="w")
        ttk.Combobox(top, textvariable=self.source_var, state="readonly", values=["全部", "入围", "候选", "观察"], width=8).grid(row=0, column=3, sticky="w", padx=(6, 10))

        ttk.Checkbutton(top, text="仅看自选", variable=self.only_favorite_var, command=self.apply_filters).grid(row=0, column=4, sticky="w", padx=(0, 10))
        ttk.Button(top, text="应用筛选", command=self.apply_filters).grid(row=0, column=5, sticky="ew", padx=(0, 4))
        ttk.Button(top, text="刷新", command=self.refresh).grid(row=0, column=6, sticky="ew", padx=4)
        ttk.Button(top, text="生成今日Watchlist", command=self.app.run_build_watchlist).grid(row=0, column=7, sticky="ew", padx=4)
        ttk.Button(top, text="加自选", command=self.add_favorite_selected).grid(row=0, column=8, sticky="ew", padx=4)
        ttk.Button(top, text="取消自选", command=self.remove_favorite_selected).grid(row=0, column=9, sticky="ew", padx=(4, 0))

        top2 = ttk.Frame(self, padding=(10, 0, 10, 0))
        top2.grid(row=1, column=0, sticky="ew")
        top2.columnconfigure(0, weight=1)
        ttk.Label(top2, textvariable=self.info_var, style="Small.TLabel", justify="left").grid(row=0, column=0, sticky="w")
        ttk.Button(top2, text="打开 Watchlist 目录", command=self.app.open_watchlist_dir).grid(row=0, column=1, sticky="e")
        ttk.Button(top2, text="关闭", command=self._on_close).grid(row=0, column=2, sticky="e", padx=(6, 0))
        ttk.Label(top2, text="逻辑说明：入围=硬过滤通过；候选/观察=基于 VR5、CLV、BR20 的分层标签，可能与入围重叠。", style="Small.TLabel", justify="left", wraplength=1180).grid(row=1, column=0, columnspan=3, sticky="w", pady=(4, 0))

        self.paned = ttk.Panedwindow(self, orient=tk.VERTICAL)
        self.paned.grid(row=2, column=0, sticky="nsew", padx=10, pady=10)

        upper = ttk.Frame(self.paned)
        upper.columnconfigure(0, weight=1)
        upper.rowconfigure(0, weight=1)
        lower = ttk.LabelFrame(self.paned, text="当前选中条目详情", padding=8)
        lower.columnconfigure(0, weight=1)
        lower.rowconfigure(0, weight=1)
        self.paned.add(upper, weight=7)
        self.paned.add(lower, weight=3)

        self.tree = ttk.Treeview(upper, columns=[c[1] for c in self.TABLE_COLUMNS], show="headings", style="Big.Treeview")
        self.tree.grid(row=0, column=0, sticky="nsew")
        ybar = ttk.Scrollbar(upper, orient="vertical", command=self.tree.yview)
        ybar.grid(row=0, column=1, sticky="ns")
        xbar = ttk.Scrollbar(upper, orient="horizontal", command=self.tree.xview)
        xbar.grid(row=1, column=0, sticky="ew")
        self.tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)

        for title, field, width in self.TABLE_COLUMNS:
            self.tree.heading(field, text=title)
            anchor = "w" if field in {"股票名称", "status", "next_stage", "source_bucket_cn"} else "center"
            stretch = True if field in {"股票名称"} else False
            self.tree.column(field, width=width, minwidth=60, anchor=anchor, stretch=stretch)

        self.tree.bind("<<TreeviewSelect>>", self._update_detail)
        self.tree.bind("<Double-1>", self._toggle_favorite_double_click)

        lower.rowconfigure(0, weight=2)
        lower.rowconfigure(1, weight=1)
        self.detail_text = tk.Text(lower, wrap="word", height=7, state="disabled", font=("Microsoft YaHei UI", 10))
        self.detail_text.grid(row=0, column=0, sticky="nsew")
        ybar2 = ttk.Scrollbar(lower, orient="vertical", command=self.detail_text.yview)
        ybar2.grid(row=0, column=1, sticky="ns")
        self.detail_text.configure(yscrollcommand=ybar2.set)
        note_frame = ttk.Frame(lower)
        note_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", pady=(8, 0))
        note_frame.columnconfigure(0, weight=1)
        note_frame.rowconfigure(1, weight=1)
        ttk.Label(note_frame, text="复盘备注（可编辑并保存到 watchlist_master.csv）", style="Small.TLabel").grid(row=0, column=0, sticky="w")
        self.note_text = tk.Text(note_frame, wrap="word", height=5, font=("Microsoft YaHei UI", 10))
        self.note_text.grid(row=1, column=0, sticky="nsew")
        note_scroll = ttk.Scrollbar(note_frame, orient="vertical", command=self.note_text.yview)
        note_scroll.grid(row=1, column=1, sticky="ns")
        self.note_text.configure(yscrollcommand=note_scroll.set)
        note_btns = ttk.Frame(note_frame)
        note_btns.grid(row=2, column=0, sticky="e", pady=(6, 0))
        ttk.Button(note_btns, text="保存备注", command=self.save_review_note).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(note_btns, text="清空备注", command=self.clear_review_note).grid(row=0, column=1)

        self.after_idle(self._set_sash)
        self.refresh()

    def _favorites_file(self):
        _, gui_runner = _ensure_runtime_modules()
        return Path(gui_runner.WATCHLIST_DIR) / "watchlist_favorites.csv"

    def _load_favorites(self):
        fp = self._favorites_file()
        if not fp.exists():
            self.favorite_ids = set()
            return
        try:
            fav = pd.read_csv(fp, dtype=str)
            self.favorite_ids = set(fav.get("watch_id", pd.Series(dtype=str)).astype(str).tolist())
        except Exception:
            self.favorite_ids = set()

    def _save_favorites(self):
        fp = self._favorites_file()
        fp.parent.mkdir(parents=True, exist_ok=True)
        data = pd.DataFrame({"watch_id": sorted(self.favorite_ids)})
        data.to_csv(fp, index=False, encoding="utf-8-sig")

    def _source_label(self, raw):
        return WATCHLIST_SOURCE_LABELS.get(str(raw), str(raw))

    def _set_sash(self):
        try:
            height = self.winfo_height()
            if height > 200:
                self.paned.sashpos(0, int(height * 0.68))
        except Exception:
            pass

    def refresh(self):
        _, gui_runner = _ensure_runtime_modules()
        payload = gui_runner.load_watchlist_master()
        self._load_favorites()
        self.master_df = payload.get("master", pd.DataFrame()).copy()
        if self.master_df.empty:
            self.view_df = pd.DataFrame()
            self._render()
            self.info_var.set("未找到 watchlist_master.csv。")
            return

        self.master_df["股票代码"] = self.master_df["股票代码"].astype(str).str.zfill(6)
        for col in self.EDITABLE_TEXT_COLUMNS:
            self._ensure_text_column(col)
        self.master_df["source_bucket_cn"] = self.master_df.get("source_bucket", "").map(WATCHLIST_SOURCE_LABELS).fillna(self.master_df.get("source_bucket", ""))
        self.master_df["is_favorite"] = self.master_df.get("watch_id", pd.Series(dtype=str)).astype(str).isin(self.favorite_ids).map(lambda x: "★" if x else "")
        self.master_df["_fav_sort"] = self.master_df["is_favorite"].eq("★").astype(int)
        self.master_df["_source_order"] = self.master_df.get("source_bucket", "").map(WATCHLIST_SOURCE_ORDER).fillna(99)
        if "setup_date" in self.master_df.columns:
            self.master_df["_setup_sort"] = pd.to_datetime(self.master_df["setup_date"], errors="coerce")
        else:
            self.master_df["_setup_sort"] = pd.NaT

        self.apply_filters(log_message=False)
        files = payload.get("files", {})
        self.info_var.set(f"{Path(files.get('master', '')).name}｜共 {len(self.master_df)} 条，当前显示 {len(self.view_df)} 条，自选 {len(self.favorite_ids)} 条")

    def apply_filters(self, log_message: bool = True):
        df = self.master_df.copy()
        if df.empty:
            self.view_df = df
            self._render()
            return

        keyword = self.keyword_var.get().strip().lower()
        if keyword:
            masks = []
            for col in ["股票代码", "股票名称", "review_note", "d0_failed_reason"]:
                if col in df.columns:
                    masks.append(df[col].astype(str).str.lower().str.contains(keyword, na=False))
            if masks:
                mask = masks[0]
                for m in masks[1:]:
                    mask = mask | m
                df = df[mask].copy()

        source = self.source_var.get()
        if source != "全部":
            df = df[df["source_bucket_cn"] == source].copy()

        if self.only_favorite_var.get():
            df = df[df["is_favorite"] == "★"].copy()

        df = df.sort_values(by=["_source_order", "_fav_sort", "_setup_sort", "股票代码"], ascending=[True, False, False, True], na_position="last").reset_index(drop=True)
        self.view_df = df
        self._render()

    def _render(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._set_detail_text("未选择任何 Watchlist 条目。")
        if self.view_df.empty:
            return
        cols = [c[1] for c in self.TABLE_COLUMNS]
        for _, row in self.view_df.iterrows():
            iid = str(row.get("watch_id", ""))
            vals = [self.app._coerce_display_value(row.get(col)) for col in cols]
            self.tree.insert("", "end", iid=iid or None, values=vals)
        children = self.tree.get_children()
        if children:
            self.tree.selection_set(children[0])
            self.tree.focus(children[0])
            self._update_detail()

    def _current_row(self):
        selected = self.tree.selection()
        if not selected or self.view_df.empty:
            return None
        iid = selected[0]
        hit = self.view_df[self.view_df["watch_id"].astype(str) == iid]
        if hit.empty:
            idx = self.tree.index(selected[0])
            if idx >= len(self.view_df):
                return None
            return self.view_df.iloc[idx]
        return hit.iloc[0]

    def _ensure_text_column(self, column: str):
        if column not in self.master_df.columns:
            self.master_df[column] = ""
            return
        if not (pd.api.types.is_object_dtype(self.master_df[column]) or pd.api.types.is_string_dtype(self.master_df[column])):
            self.master_df[column] = self.master_df[column].astype("string")
        self.master_df[column] = self.master_df[column].fillna("")

    def _update_master_row_value(self, watch_id: str, column: str, value: str):
        if self.master_df.empty or "watch_id" not in self.master_df.columns:
            return False
        mask = self.master_df["watch_id"].astype(str) == str(watch_id)
        if not mask.any():
            return False
        self._ensure_text_column(column)
        self._ensure_text_column("updated_at")
        self.master_df.loc[mask, column] = value
        self.master_df.loc[mask, "updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        _, gui_runner = _ensure_runtime_modules()
        gui_runner.save_watchlist_master(self.master_df.drop(columns=[c for c in ["_fav_sort", "_source_order", "_setup_sort"] if c in self.master_df.columns]))
        return True

    def save_review_note(self):
        row = self._current_row()
        if row is None:
            return
        note = self.note_text.get("1.0", "end").strip()
        if self._update_master_row_value(str(row.get("watch_id", "")), "review_note", note):
            self.refresh()
            # restore selection
            self._reselect_watch_id(str(row.get("watch_id", "")))

    def clear_review_note(self):
        self.note_text.delete("1.0", "end")
        self.save_review_note()

    def _reselect_watch_id(self, watch_id: str):
        for iid in self.tree.get_children():
            if iid == watch_id:
                self.tree.selection_set(iid)
                self.tree.focus(iid)
                self._update_detail()
                break

    def add_favorite_selected(self):
        row = self._current_row()
        if row is None:
            return
        self.favorite_ids.add(str(row.get("watch_id", "")))
        self._save_favorites()
        self.refresh()

    def remove_favorite_selected(self):
        row = self._current_row()
        if row is None:
            return
        self.favorite_ids.discard(str(row.get("watch_id", "")))
        self._save_favorites()
        self.refresh()

    def _toggle_favorite_double_click(self, _event=None):
        row = self._current_row()
        if row is None:
            return
        wid = str(row.get("watch_id", ""))
        if wid in self.favorite_ids:
            self.favorite_ids.discard(wid)
        else:
            self.favorite_ids.add(wid)
        self._save_favorites()
        self.refresh()

    def _set_detail_text(self, text: str):
        self.detail_text.configure(state="normal")
        self.detail_text.delete("1.0", "end")
        self.detail_text.insert("1.0", text)
        self.detail_text.configure(state="disabled")

    def _update_detail(self, _event=None):
        row = self._current_row()
        if row is None:
            self._set_detail_text("未选择任何 Watchlist 条目。")
            self.note_text.delete("1.0", "end")
            return
        fields = [
            "股票代码", "股票名称", "setup_date", "source_bucket_cn", "status", "next_stage",
            "entry_reason", "d0_label", "分层标签说明", "d0_pct_chg", "d0_turnover", "d0_vr5", "d0_br20",
            "breakout_price", "support_price_1", "support_price_2", "target_price_1", "target_price_2",
            "d0_failed_reason_display",
        ]
        pieces = []
        per_line = 3
        line = []
        for field in fields:
            if field not in row.index:
                continue
            value = self.app._coerce_display_value(row.get(field))
            label = WATCHLIST_FIELD_LABELS.get(field, field)
            line.append(f"{label}: {value}")
            if len(line) >= per_line or field in {"entry_reason", "d0_failed_reason_display"}:
                pieces.append("    ".join(line))
                line = []
        if line:
            pieces.append("    ".join(line))
        self._set_detail_text("\n".join(pieces))
        self.note_text.delete("1.0", "end")
        self.note_text.insert("1.0", str(row.get("review_note", "") or ""))

    def _on_close(self):
        self.app.watchlist_window = None
        self.destroy()


class GuiApp:

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("AShare Scanner")
        self._apply_styles()
        self._apply_adaptive_geometry()
        self.root.minsize(1320, 820)
        self.log_queue: queue.Queue[tuple[str, str, object]] = queue.Queue()
        self.current_worker: threading.Thread | None = None
        self.action_buttons: list[ttk.Button] = []
        self.param_window: ParameterWindow | None = None
        self.log_window: LogWindow | None = None
        self.watchlist_window: WatchlistWindow | None = None

        _ensure_runtime_modules()
        self.runtime_info = {
            "base_dir": "初始化中...",
            "mode": "初始化中...",
            "db_sync_date": "未知",
            "db_sync_source": "",
        }
        self.total_steps_expected = 1
        self.total_steps_completed = 0
        self.current_step_name = ""
        self.current_step_started_at: float | None = None
        self.task_started_at: float | None = None
        self.last_progress_match: tuple[int, int] | None = None
        self.current_action_label = "空闲"
        self.current_step_order: list[str] = []
        self._task_finalized = False
        self.log_history: list[tuple[str, str]] = []
        self.log_texts: dict[str, tk.Text] = {}
        self.result_frames_raw = {key: pd.DataFrame() for key in RESULT_TABS}
        self.result_frames_view = {key: pd.DataFrame() for key in RESULT_TABS}
        self.latest_result_files: dict[str, str] = {}
        self.result_treeviews: dict[str, ttk.Treeview] = {}
        self.result_tab_frames: dict[str, ttk.Frame] = {}
        self.result_columns_by_tab: dict[str, list[str]] = {}
        self.result_sort_column = "VR5"
        self.result_sort_ascending = False

        self.status_vars = {
            "success": tk.StringVar(value="-"),
            "missing_count": tk.StringVar(value="0"),
            "created_count": tk.StringVar(value="0"),
            "error_count": tk.StringVar(value="0"),
            "skipped_count": tk.StringVar(value="0"),
            "output_paths_count": tk.StringVar(value="0"),
            "message": tk.StringVar(value="尚未运行。"),
        }
        self.scan_stats_vars = {
            "selected": tk.StringVar(value="-"),
            "candidate": tk.StringVar(value="-"),
            "watch": tk.StringVar(value="-"),
            "skip": tk.StringVar(value="-"),
            "error": tk.StringVar(value="-"),
            "source": tk.StringVar(value="未加载"),
        }
        self.task_vars = {
            "task_name": tk.StringVar(value="空闲"),
            "main_step": tk.StringVar(value="-"),
            "sub_step": tk.StringVar(value="-"),
            "progress_text": tk.StringVar(value="-"),
            "elapsed": tk.StringVar(value="00:00:00"),
            "eta": tk.StringVar(value="--"),
            "base_dir": tk.StringVar(value="初始化中..."),
            "mode": tk.StringVar(value="初始化中..."),
            "db_sync_date": tk.StringVar(value="未知"),
            "total_progress": tk.StringVar(value="0 / 1"),
            "step_progress": tk.StringVar(value="等待开始"),
        }
        self.result_ui_vars = {
            "keyword": tk.StringVar(value=""),
            "label_filter": tk.StringVar(value="全部"),
            "hard_filter": tk.StringVar(value="全部"),
            "sort_by": tk.StringVar(value=self.result_sort_column),
            "sort_order": tk.StringVar(value="降序"),
            "info": tk.StringVar(value="尚未加载扫描结果。"),
        }
        self.config_vars = {k: (tk.BooleanVar(value=True) if FIELD_META[k]["type"] == "check" else tk.StringVar()) for k in FIELD_ORDER}
        self.config_vars["require_bullish"].set(True)

        self._build_ui()
        self.load_config_to_form(log_message=False)
        self.refresh_runtime_info()
        self.refresh_latest_scan_results(silent=True)
        self.root.after(100, self._poll_log_queue)
        self.root.after(500, self._tick_clock)
        self.root.after_idle(self._set_initial_sashes)
        self.open_log_window()

    def _apply_styles(self):
        default_font = tkfont.nametofont("TkDefaultFont")
        default_font.configure(family="Microsoft YaHei UI", size=9)
        text_font = tkfont.nametofont("TkTextFont")
        text_font.configure(family="Microsoft YaHei UI", size=9)
        heading_font = tkfont.nametofont("TkHeadingFont")
        heading_font.configure(family="Microsoft YaHei UI", size=9, weight="bold")
        style = ttk.Style(self.root)
        style.configure(".", font=("Microsoft YaHei UI", 9))
        style.configure("Small.TLabel", font=("Microsoft YaHei UI", 8))
        style.configure("Small.TButton", font=("Microsoft YaHei UI", 8))
        style.configure("Big.Treeview", font=("Microsoft YaHei UI", 10), rowheight=24)
        style.configure("Big.Treeview.Heading", font=("Microsoft YaHei UI", 10, "bold"))
        style.configure("Treeview", font=("Microsoft YaHei UI", 10), rowheight=24)
        style.configure("Treeview.Heading", font=("Microsoft YaHei UI", 10, "bold"))

    def _apply_adaptive_geometry(self):
        sw = max(1280, self.root.winfo_screenwidth())
        sh = max(800, self.root.winfo_screenheight())
        width = min(max(int(sw * 0.88), 1380), sw - 40)
        height = min(max(int(sh * 0.88), 860), sh - 60)
        x = max(0, (sw - width) // 2)
        y = max(0, (sh - height) // 2)
        self.root.geometry(f"{width}x{height}+{x}+{y}")


    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        self.main_paned = ttk.Panedwindow(self.root, orient=tk.VERTICAL)
        self.main_paned.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)

        self.top_paned = ttk.Panedwindow(self.main_paned, orient=tk.HORIZONTAL)
        self.status_host = ttk.Frame(self.main_paned)
        self.status_host.columnconfigure(0, weight=1)
        self.status_host.rowconfigure(0, weight=1)
        self.result_host = ttk.Frame(self.main_paned)
        self.result_host.columnconfigure(0, weight=1)
        self.result_host.rowconfigure(0, weight=1)

        self.main_paned.add(self.top_paned, weight=2)
        self.main_paned.add(self.status_host, weight=1)
        self.main_paned.add(self.result_host, weight=7)

        self.actions_panel = self._build_actions_panel(self.top_paned)
        self.task_panel = self._build_task_panel(self.top_paned)
        self.tools_panel = self._build_tools_panel(self.top_paned)
        self.top_paned.add(self.actions_panel, weight=2)
        self.top_paned.add(self.task_panel, weight=3)
        self.top_paned.add(self.tools_panel, weight=2)

        self.status_panel = self._build_status_panel(self.status_host)
        self.result_panel = self._build_result_panel(self.result_host)

    def _set_initial_sashes(self):
        try:
            width = self.root.winfo_width()
            height = self.root.winfo_height()
            if width > 1200:
                self.top_paned.sashpos(0, int(width * 0.22))
                self.top_paned.sashpos(1, int(width * 0.68))
            if height > 700:
                self.main_paned.sashpos(0, int(height * 0.18))
                self.main_paned.sashpos(1, int(height * 0.30))
                if hasattr(self, "result_paned"):
                    self.result_paned.sashpos(0, int(height * 0.68))
        except Exception:
            pass

    def _build_actions_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="操作区", padding=6)
        for col in range(3):
            frame.columnconfigure(col, weight=1)
        specs = [
            ("一键日更扫描", self.run_daily_pipeline),
            ("同步股票池", self.run_sync_universe),
            ("补建缺失股票", self.run_bootstrap_missing),
            ("仅更新历史库", self.run_update_daily_hist),
            ("仅打包 Parquet", self.run_pack_to_parquet),
            ("仅扫描", self.run_scan_from_parquet),
        ]
        for idx, (label, cmd) in enumerate(specs):
            r, c = divmod(idx, 3)
            btn = ttk.Button(frame, text=label, command=cmd, style="Small.TButton")
            btn.grid(row=r, column=c, sticky="ew", padx=3, pady=3)
            self.action_buttons.append(btn)
        ttk.Separator(frame, orient="horizontal").grid(row=2, column=0, columnspan=3, sticky="ew", pady=(6, 4))
        ttk.Label(frame, textvariable=self.task_vars["total_progress"], style="Small.TLabel").grid(row=3, column=0, columnspan=3, sticky="w", padx=3)
        self.total_progressbar = ttk.Progressbar(frame, mode="determinate", maximum=1, value=0)
        self.total_progressbar.grid(row=4, column=0, columnspan=3, sticky="ew", padx=3, pady=(2, 4))
        ttk.Label(frame, textvariable=self.task_vars["step_progress"], style="Small.TLabel").grid(row=5, column=0, columnspan=3, sticky="w", padx=3)
        self.step_progressbar = ttk.Progressbar(frame, mode="indeterminate")
        self.step_progressbar.grid(row=6, column=0, columnspan=3, sticky="ew", padx=3, pady=(2, 0))
        return frame

    def _build_task_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="当前任务", padding=6)
        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(3, weight=1)
        rows = [
            ("任务", "task_name", 0, 0),
            ("步骤", "main_step", 0, 2),
            ("进度", "progress_text", 1, 0),
            ("子状态", "sub_step", 1, 2),
            ("耗时", "elapsed", 2, 0),
            ("ETA", "eta", 2, 2),
            ("同步至", "db_sync_date", 3, 0),
            ("模式", "mode", 3, 2),
        ]
        for label, key, row, col in rows:
            ttk.Label(frame, text=label, style="Small.TLabel").grid(row=row, column=col, sticky="nw", padx=(0, 4), pady=1)
            wrap = 300 if key in {"progress_text", "sub_step"} else 180
            ttk.Label(frame, textvariable=self.task_vars[key], style="Small.TLabel", justify="left", wraplength=wrap).grid(row=row, column=col + 1, sticky="ew", pady=1)
        ttk.Label(frame, text="目录", style="Small.TLabel").grid(row=4, column=0, sticky="nw", padx=(0, 4), pady=(2, 0))
        ttk.Label(frame, textvariable=self.task_vars["base_dir"], style="Small.TLabel", justify="left", wraplength=620).grid(row=4, column=1, columnspan=3, sticky="ew", pady=(2, 0))
        ttk.Label(frame, text="说明", style="Small.TLabel").grid(row=5, column=0, sticky="nw", padx=(0, 4), pady=(4, 0))
        ttk.Label(frame, text="入围=硬过滤通过；候选/观察=VR5、CLV、BR20 的分层标签，可与入围重叠。", style="Small.TLabel", justify="left", wraplength=620).grid(row=5, column=1, columnspan=3, sticky="ew", pady=(4, 0))
        return frame

    def _build_tools_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="工具 / 快捷入口", padding=6)
        for col in range(2):
            frame.columnconfigure(col, weight=1)
        ttk.Label(frame, text="数据库同步至", style="Small.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(frame, textvariable=self.task_vars["db_sync_date"], style="Small.TLabel").grid(row=0, column=1, sticky="w")
        ttk.Label(frame, text="最新扫描统计", style="Small.TLabel").grid(row=1, column=0, sticky="w", pady=(4, 0))
        self.scan_summary_label = ttk.Label(frame, text="-", style="Small.TLabel")
        self.scan_summary_label.grid(row=1, column=1, sticky="w", pady=(4, 0))
        self._refresh_scan_summary_label()
        btn_specs = [
            ("扫描参数", self.open_param_window),
            ("运行日志", self.open_log_window),
            ("Watchlist", self.open_watchlist_window),
            ("刷新结果", self.refresh_latest_scan_results),
            ("打开 output", self.open_output_dir),
            ("打开 logs", self.open_logs_dir),
        ]
        for idx, (label, cmd) in enumerate(btn_specs):
            r = 2 + idx // 2
            c = idx % 2
            ttk.Button(frame, text=label, command=cmd, style="Small.TButton").grid(row=r, column=c, sticky="ew", padx=3, pady=3)
        return frame

    def _build_status_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="最近一次结果 / 最新扫描统计", padding=6)
        frame.grid(row=0, column=0, sticky="nsew")
        for col in range(7):
            frame.columnconfigure(col, weight=1 if col < 6 else 2)
        pairs = [
            ("成功", self.status_vars["success"]),
            ("缺失", self.status_vars["missing_count"]),
            ("补建", self.status_vars["created_count"]),
            ("错误", self.status_vars["error_count"]),
            ("跳过", self.status_vars["skipped_count"]),
            ("输出文件", self.status_vars["output_paths_count"]),
        ]
        for idx, (label, var) in enumerate(pairs):
            ttk.Label(frame, text=label, style="Small.TLabel").grid(row=0, column=idx, sticky="w")
            ttk.Label(frame, textvariable=var, style="Small.TLabel").grid(row=1, column=idx, sticky="w")
        ttk.Label(frame, text="扫描统计", style="Small.TLabel").grid(row=0, column=6, sticky="w")
        self.scan_stats_inline = ttk.Label(frame, text="-", style="Small.TLabel")
        self.scan_stats_inline.grid(row=1, column=6, sticky="w")
        ttk.Label(frame, text="消息", style="Small.TLabel").grid(row=2, column=0, sticky="nw", pady=(4, 0))
        self.status_message = ttk.Label(frame, textvariable=self.status_vars["message"], style="Small.TLabel", wraplength=1140, justify="left")
        self.status_message.grid(row=2, column=1, columnspan=6, sticky="ew", pady=(4, 0))
        self._refresh_scan_summary_label()
        return frame

    def _build_result_panel(self, parent):
        frame = ttk.LabelFrame(parent, text="扫描结果", padding=8)
        frame.grid(row=0, column=0, sticky="nsew")
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(4, weight=1)

        toolbar1 = ttk.Frame(frame)
        toolbar1.grid(row=0, column=0, sticky="ew")
        toolbar1.columnconfigure(1, weight=1)
        ttk.Label(toolbar1, text="关键词").grid(row=0, column=0, sticky="w")
        keyword_entry = ttk.Entry(toolbar1, textvariable=self.result_ui_vars["keyword"])
        keyword_entry.grid(row=0, column=1, sticky="ew", padx=(6, 10))
        keyword_entry.bind("<Return>", lambda _e: self.apply_result_filters())
        ttk.Label(toolbar1, text="标签").grid(row=0, column=2, sticky="w")
        ttk.Combobox(toolbar1, textvariable=self.result_ui_vars["label_filter"], state="readonly", values=["全部", "候选", "观察", "放弃"], width=8).grid(row=0, column=3, padx=(6, 10), sticky="w")
        ttk.Label(toolbar1, text="硬过滤").grid(row=0, column=4, sticky="w")
        ttk.Combobox(toolbar1, textvariable=self.result_ui_vars["hard_filter"], state="readonly", values=["全部", "仅硬过滤通过", "仅硬过滤未通过"], width=14).grid(row=0, column=5, padx=(6, 10), sticky="w")

        toolbar2 = ttk.Frame(frame)
        toolbar2.grid(row=1, column=0, sticky="ew", pady=(6, 0))
        for col in range(8):
            toolbar2.columnconfigure(col, weight=0)
        toolbar2.columnconfigure(8, weight=1)
        ttk.Label(toolbar2, text="排序列").grid(row=0, column=0, sticky="w")
        ttk.Combobox(toolbar2, textvariable=self.result_ui_vars["sort_by"], state="readonly", values=SORTABLE_COLUMNS, width=12).grid(row=0, column=1, padx=(6, 10), sticky="w")
        ttk.Combobox(toolbar2, textvariable=self.result_ui_vars["sort_order"], state="readonly", values=["升序", "降序"], width=6).grid(row=0, column=2, padx=(0, 10), sticky="w")
        ttk.Button(toolbar2, text="应用筛选", command=self.apply_result_filters).grid(row=0, column=3, padx=(0, 6), sticky="ew")
        ttk.Button(toolbar2, text="重置", command=self.reset_result_filters).grid(row=0, column=4, padx=(0, 6), sticky="ew")
        ttk.Button(toolbar2, text="刷新扫描结果", command=self.refresh_latest_scan_results).grid(row=0, column=5, padx=(0, 6), sticky="ew")
        ttk.Button(toolbar2, text="生成今日 Watchlist", command=self.run_build_watchlist).grid(row=0, column=6, padx=(0, 6), sticky="ew")

        ttk.Label(frame, textvariable=self.result_ui_vars["info"], justify="left", wraplength=1240).grid(row=2, column=0, sticky="ew", pady=(6, 2))
        ttk.Label(frame, text="提示：入围=硬过滤通过；候选/观察=分层标签。入围与候选/观察不是互斥关系。", style="Small.TLabel", justify="left").grid(row=3, column=0, sticky="w", pady=(0, 4))

        self.result_paned = ttk.Panedwindow(frame, orient=tk.VERTICAL)
        self.result_paned.grid(row=4, column=0, sticky="nsew")
        upper = ttk.Frame(self.result_paned)
        upper.columnconfigure(0, weight=1)
        upper.rowconfigure(0, weight=1)
        lower = ttk.LabelFrame(self.result_paned, text="当前选中行详情", padding=6)
        lower.columnconfigure(0, weight=1)
        lower.rowconfigure(0, weight=1)
        self.result_paned.add(upper, weight=8)
        self.result_paned.add(lower, weight=2)

        self.result_notebook = ttk.Notebook(upper)
        self.result_notebook.grid(row=0, column=0, sticky="nsew")
        for key, title in RESULT_TABS.items():
            self._create_result_tab(key, title)

        self.detail_text = tk.Text(lower, wrap="word", height=5, state="disabled", font=("Microsoft YaHei UI", 10))
        self.detail_text.grid(row=0, column=0, sticky="nsew")
        ybar = ttk.Scrollbar(lower, orient="vertical", command=self.detail_text.yview)
        ybar.grid(row=0, column=1, sticky="ns")
        self.detail_text.configure(yscrollcommand=ybar.set)
        return frame

    def _create_result_tab(self, tab_key: str, title: str):
        tab = ttk.Frame(self.result_notebook)
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        self.result_notebook.add(tab, text=title)
        self.result_tab_frames[tab_key] = tab
        table = ttk.Frame(tab)
        table.grid(row=0, column=0, sticky="nsew")
        table.columnconfigure(0, weight=1)
        table.rowconfigure(0, weight=1)
        tree = ttk.Treeview(table, columns=RESULT_DISPLAY_COLUMNS, show="headings", style="Big.Treeview", height=14)
        tree.grid(row=0, column=0, sticky="nsew")
        ybar = ttk.Scrollbar(table, orient="vertical", command=tree.yview)
        ybar.grid(row=0, column=1, sticky="ns")
        xbar = ttk.Scrollbar(table, orient="horizontal", command=tree.xview)
        xbar.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        self.result_treeviews[tab_key] = tree
        tree.bind("<<TreeviewSelect>>", lambda _e, key=tab_key: self._update_result_detail_from_selection(key))
        tree.bind("<Double-1>", lambda _e, key=tab_key: self._open_result_detail_popup(key))
        self._setup_tree_columns(tab_key, tree, RESULT_DISPLAY_COLUMNS)

    def _format_horizontal_detail(self, row):
        if row is None:
            return "未选择任何结果。"
        preferred = [
            "股票代码", "股票名称", "日期", "开盘", "收盘", "最高", "最低",
            "涨跌幅%", "换手率", "量比前一日", "VR5", "BR20", "命中硬过滤数",
            "分层标签", "硬过滤是否通过", "硬过滤未通过原因",
        ]
        cols = [c for c in preferred if c in row.index] + [c for c in row.index if c not in preferred]
        lines, current = [], []
        for col in cols:
            value = self._coerce_display_value(row.get(col))
            current.append(f"{col}: {value}")
            if len(current) >= 4 or (col == "硬过滤未通过原因" and current):
                lines.append("    ".join(current))
                current = []
        if current:
            lines.append("    ".join(current))
        return "\n".join(lines)

    def _configure_log_tags(self):

        for widget in self.log_texts.values():
            widget.tag_configure("raw", foreground="#222222")
            widget.tag_configure("summary", foreground="#0b5394")
            widget.tag_configure("error", foreground="#a61c00")
            widget.tag_configure("ui", foreground="#38761d")

    def _replay_logs(self):
        if not self.log_texts:
            return
        for widget in self.log_texts.values():
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.configure(state="disabled")
        for message, tag in self.log_history:
            self._append_log_to_widgets(message, tag)

    def _append_to_log_widget(self, widget: tk.Text, message: str, tag: str):
        try:
            if widget is None or not widget.winfo_exists():
                return
            widget.configure(state="normal")
            widget.insert("end", message + "\n", tag)
            widget.see("end")
            widget.configure(state="disabled")
        except tk.TclError:
            return

    def _append_log_to_widgets(self, message: str, tag: str):
        if not self.log_texts:
            return
        targets = ["all"]
        if tag == "raw":
            targets.append("raw")
        elif tag == "error":
            targets.append("error")
        else:
            targets.append("summary")
        for key in targets:
            widget = self.log_texts.get(key)
            if widget is not None:
                self._append_to_log_widget(widget, message, tag)

    def append_log(self, message: str, tag: str = "raw"):
        self.log_history.append((message, tag))
        self._append_log_to_widgets(message, tag)

    def clear_logs(self):
        self.log_history.clear()
        self._replay_logs()

    def open_param_window(self):
        if self.param_window is None or not self.param_window.winfo_exists():
            self.param_window = ParameterWindow(self)
        else:
            self.param_window.lift()
            self.param_window.focus_force()

    def open_log_window(self):
        if self.log_window is None or not self.log_window.winfo_exists():
            self.log_window = LogWindow(self)
        else:
            self.log_window.lift()
            self.log_window.focus_force()

    def open_watchlist_window(self):
        if self.watchlist_window is None or not self.watchlist_window.winfo_exists():
            self.watchlist_window = WatchlistWindow(self)
        else:
            self.watchlist_window.lift()
            self.watchlist_window.focus_force()
            self.watchlist_window.refresh()

    def refresh_runtime_info(self):
        _, gui_runner = _ensure_runtime_modules()
        self.runtime_info = gui_runner.get_runtime_info()
        self.task_vars["base_dir"].set(self.runtime_info.get("base_dir", ""))
        self.task_vars["mode"].set(self.runtime_info.get("mode", ""))
        self.task_vars["db_sync_date"].set(self.runtime_info.get("db_sync_date", "未知"))

    def set_busy(self, busy: bool):
        state = "disabled" if busy else "normal"
        for btn in self.action_buttons:
            try:
                btn.configure(state=state)
            except Exception:
                pass


    def _tick_clock(self):
        if self.task_started_at is not None:
            elapsed_seconds = max(0, int(time.time() - self.task_started_at))
            self.task_vars["elapsed"].set(self._format_seconds(elapsed_seconds))
            self.task_vars["eta"].set(self._estimate_eta(elapsed_seconds))
        self.root.after(500, self._tick_clock)

    def _poll_log_queue(self):
        try:
            while True:
                event_type, step_name, payload = self.log_queue.get_nowait()
                if event_type == "log":
                    prefix = f"[{STEP_LABELS.get(step_name, step_name)}] " if step_name else ""
                    self.append_log(prefix + str(payload), "raw")
                    self._handle_live_log(step_name, str(payload))
                elif event_type == "step_start":
                    self._handle_step_start(step_name)
                elif event_type == "step_done":
                    self._handle_step_done(step_name, payload if isinstance(payload, dict) else {})
                elif event_type == "pipeline_start":
                    meta = payload if isinstance(payload, dict) else {}
                    self.total_steps_expected = int(meta.get("total_steps", 1) or 1)
                    self.total_progressbar.configure(maximum=self.total_steps_expected)
                    self.task_vars["total_progress"].set(f"0 / {self.total_steps_expected}")
                elif event_type == "result":
                    self._apply_result(payload)
                elif event_type == "finished":
                    self.set_busy(False)
                    self._finish_task_state()
                elif event_type == "error":
                    self.append_log(f"[{STEP_LABELS.get(step_name, step_name)}] {payload}", "error")
                    self._apply_result({"success": False, "step_name": step_name, "message": str(payload), "output_paths": []})
                    self.set_busy(False)
                    self._finish_task_state()
                elif event_type == "done":
                    self.set_busy(False)
                    self._finish_task_state()
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self._poll_log_queue)

    def _run_in_background(self, label: str, func, *args, total_steps: int = 1, step_order: list[str] | None = None):
        if self.current_worker is not None and self.current_worker.is_alive():
            messagebox.showwarning("Busy", "当前已有任务在运行，请先等待完成。")
            return
        self._reset_task_state(label, total_steps, step_order)
        self.set_busy(True)
        self.append_log(f"[ui] 开始执行：{label}", "ui")

        def worker():
            try:
                def log_callback(step_name, line):
                    self.log_queue.put(("log", step_name, line))

                def event_callback(event_type, step_name, payload=None):
                    self.log_queue.put((event_type, step_name, payload))

                result = func(*args, log_callback=log_callback, event_callback=event_callback)
                self.log_queue.put(("result", result.get("step_name", label), result))
                self.log_queue.put(("finished", label, result))
            except Exception:
                self.log_queue.put(("error", label, traceback.format_exc()))
            finally:
                self.log_queue.put(("done", label, None))

        self.current_worker = threading.Thread(target=worker, daemon=True)
        self.current_worker.start()

    def _reset_task_state(self, label: str, total_steps: int, step_order: list[str] | None = None):
        self.current_action_label = label
        self.task_started_at = time.time()
        self.current_step_started_at = None
        self.current_step_name = ""
        self.last_progress_match = None
        self.current_step_order = list(step_order or [])
        self.total_steps_expected = max(1, total_steps)
        self.total_steps_completed = 0
        self._task_finalized = False
        self.task_vars["task_name"].set(label)
        self.task_vars["main_step"].set("等待开始")
        self.task_vars["sub_step"].set("-")
        self.task_vars["progress_text"].set("-")
        self.task_vars["elapsed"].set("00:00:00")
        self.task_vars["eta"].set("--")
        self.task_vars["total_progress"].set(f"0 / {self.total_steps_expected}")
        self.task_vars["step_progress"].set("等待开始")
        self.total_progressbar.configure(mode="determinate", maximum=self.total_steps_expected, value=0)
        self.step_progressbar.stop()
        self.step_progressbar.configure(mode="indeterminate", maximum=100, value=0)

    def _handle_step_start(self, step_name: str):
        self.current_step_name = step_name
        self.current_step_started_at = time.time()
        self.last_progress_match = None
        self.task_vars["main_step"].set(STEP_LABELS.get(step_name, step_name))
        self.task_vars["sub_step"].set("启动中")
        self.task_vars["progress_text"].set("等待进度输出")
        self.task_vars["step_progress"].set(f"当前步骤：{STEP_LABELS.get(step_name, step_name)}")
        self.step_progressbar.stop()
        self.step_progressbar.configure(mode="indeterminate", value=0)
        self.step_progressbar.start(12)
        self.total_progressbar.configure(value=self.total_steps_completed)

    def _handle_step_done(self, step_name: str, result: dict):
        idx = self._step_index(step_name)
        if idx is not None:
            self.total_steps_completed = max(self.total_steps_completed, idx)
            self.total_progressbar.configure(value=self.total_steps_completed)
            self.task_vars["total_progress"].set(f"{self.total_steps_completed} / {self.total_steps_expected}")
        self.step_progressbar.stop()
        self.step_progressbar.configure(mode="determinate", maximum=100, value=100)
        msg = result.get("message", "步骤完成")
        self.task_vars["sub_step"].set(msg)
        self.task_vars["progress_text"].set(msg)
        self.task_vars["step_progress"].set(f"当前步骤：{STEP_LABELS.get(step_name, step_name)} 已完成")
        self.append_log(f"[{STEP_LABELS.get(step_name, step_name)}] 完成：success={result.get('success', '-')}, errors={result.get('error_count', 0)}, skipped={result.get('skipped_count', 0)}", "summary")

    def _finish_task_state(self):
        if self._task_finalized:
            return
        self._task_finalized = True
        self.step_progressbar.stop()
        self.current_step_started_at = None
        self.current_step_name = ""
        self.last_progress_match = None
        self.task_vars["eta"].set("--")
        self.task_vars["task_name"].set(f"{self.current_action_label}（已结束）")
        if self.total_steps_completed >= self.total_steps_expected:
            self.task_vars["main_step"].set("全部完成")
        # 保留最终耗时显示，不再继续累计
        self.task_started_at = None

    def _handle_live_log(self, step_name: str, line: str):

        if step_name:
            self.task_vars["main_step"].set(STEP_LABELS.get(step_name, step_name))
        self.task_vars["sub_step"].set(line)
        match = self._extract_progress(line)
        if match is None:
            self.task_vars["progress_text"].set(line)
            return
        done, total = match
        self.last_progress_match = (done, total)
        pct = (done / total * 100.0) if total else 0.0
        self.step_progressbar.stop()
        self.step_progressbar.configure(mode="determinate", maximum=100, value=pct)
        self.task_vars["step_progress"].set(f"当前步骤进度：{done} / {total}（{pct:.1f}%）")
        self.task_vars["progress_text"].set(f"{STEP_LABELS.get(step_name, step_name)}：{done} / {total}")
        idx = self._step_index(step_name)
        if idx is not None:
            total_value = (idx - 1) + (done / total if total else 0.0)
            self.total_progressbar.configure(value=total_value)
            self.task_vars["total_progress"].set(f"{idx - 1} + 当前步骤 / {self.total_steps_expected}")

    def _apply_result(self, result: dict):
        success_value = result.get("success", "-")
        if success_value is True:
            success_display = "成功"
        elif success_value is False:
            success_display = "失败"
        else:
            success_display = str(success_value)
        self.status_vars["success"].set(success_display)
        self.status_vars["missing_count"].set(str(result.get("missing_count", 0)))
        self.status_vars["created_count"].set(str(result.get("created_count", 0)))
        self.status_vars["error_count"].set(str(result.get("error_count", 0)))
        self.status_vars["skipped_count"].set(str(result.get("skipped_count", 0)))
        self.status_vars["output_paths_count"].set(str(len(result.get("output_paths", []) or [])))
        message = str(result.get("message", ""))
        if success_value is False and message:
            message = f"失败：{message}"
        self.status_vars["message"].set(message)
        step_name = result.get("step_name", "result")
        summary = f"success={result.get('success', '-')}, missing={result.get('missing_count', 0)}, created={result.get('created_count', 0)}, errors={result.get('error_count', 0)}, skipped={result.get('skipped_count', 0)}, outputs={len(result.get('output_paths', []) or [])}"
        self.append_log(f"[{STEP_LABELS.get(step_name, step_name)}] 结果：{summary}", "summary")
        self.refresh_runtime_info()
        if self._result_contains_scan_outputs(result):
            self.refresh_latest_scan_results(silent=True)
        if self._result_contains_watchlist_outputs(result) and self.watchlist_window is not None and self.watchlist_window.winfo_exists():
            self.watchlist_window.refresh()

    def _result_contains_scan_outputs(self, result: dict) -> bool:
        return any("p7_scan_from_parquet_all_" in str(path) for path in (result.get("output_paths", []) or []))

    def _result_contains_watchlist_outputs(self, result: dict) -> bool:
        return any("watchlist" in str(path).lower() or "p8_build_watchlist" in str(path).lower() for path in (result.get("output_paths", []) or []))

    def _extract_progress(self, line: str):
        for pattern in PROGRESS_PATTERNS:
            m = pattern.search(line)
            if m:
                done, total = int(m.group("done")), int(m.group("total"))
                if total > 0 and done <= total:
                    return done, total
        return None

    def _estimate_eta(self, elapsed_seconds: int) -> str:
        frac = self._overall_progress_fraction()
        if frac <= 0.02:
            return "--"
        total_est = elapsed_seconds / frac
        remaining = max(0, int(total_est - elapsed_seconds))
        return self._format_seconds(remaining)

    def _overall_progress_fraction(self) -> float:
        if self.total_steps_expected <= 0:
            return 0.0
        idx = self._step_index(self.current_step_name)
        if idx is None or self.last_progress_match is None:
            return min(1.0, self.total_steps_completed / self.total_steps_expected)
        done, total = self.last_progress_match
        return min(1.0, max(0.0, ((idx - 1) + (done / total if total else 0.0)) / self.total_steps_expected))

    def _step_index(self, step_name: str):
        _, gui_runner = _ensure_runtime_modules()
        search_order = self.current_step_order or list(gui_runner.PIPELINE_STEPS)
        try:
            return search_order.index(step_name) + 1
        except ValueError:
            return None

    @staticmethod
    def _format_seconds(seconds: int):
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    def load_config_to_form(self, log_message: bool = True):
        try:
            config = self._read_scan_config()
            hard = config.get("hard_filters", {})
            for key in FIELD_ORDER:
                val = hard.get(key, True if FIELD_META[key]["type"] == "check" else "")
                if FIELD_META[key]["type"] == "check":
                    self.config_vars[key].set(bool(val))
                else:
                    self.config_vars[key].set(str(val))
            if log_message:
                self.append_log("[config] 已读取 scan_config.json", "ui")
        except Exception as exc:
            messagebox.showerror("配置读取失败", str(exc))

    def save_config_from_form(self, silent: bool = False):
        try:
            config = self._read_scan_config()
            hard = config.setdefault("hard_filters", {})
            for key in FIELD_ORDER:
                var = self.config_vars[key]
                if FIELD_META[key]["type"] == "check":
                    hard[key] = bool(var.get())
                else:
                    text = str(var.get()).strip()
                    if key in {"volatility_window", "cold_volume_window", "min_history_bars"}:
                        hard[key] = int(text)
                    else:
                        hard[key] = float(text)
            SCAN_CONFIG_FILE.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
            if not silent:
                self.append_log("[config] 已保存 scan_config.json", "ui")
            return True, "已自动保存到 scan_config.json"
        except Exception as exc:
            if not silent:
                messagebox.showerror("配置保存失败", str(exc))
            return False, f"保存失败：{exc}"

    def _read_scan_config(self):
        if not SCAN_CONFIG_FILE.exists():
            raise FileNotFoundError(f"未找到配置文件: {SCAN_CONFIG_FILE}")
        return json.loads(SCAN_CONFIG_FILE.read_text(encoding="utf-8"))

    def refresh_latest_scan_results(self, silent: bool = False):
        _, gui_runner = _ensure_runtime_modules()
        payload = gui_runner.load_latest_scan_frames()
        self.latest_result_files = payload.get("files", {})
        raw_frames = payload.get("frames", {})
        found_any = bool(payload.get("found_any", False))
        for key in RESULT_TABS:
            df = raw_frames.get(key, pd.DataFrame()).copy()
            if "股票代码" in df.columns:
                df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
            self.result_frames_raw[key] = df
        summary_df = raw_frames.get("summary", pd.DataFrame()).copy()
        if not summary_df.empty:
            row = summary_df.iloc[-1]
            self.scan_stats_vars["selected"].set(str(row.get("硬过滤通过数量", row.get("hard_pass_count", "-"))))
            self.scan_stats_vars["candidate"].set(str(row.get("候选数量", row.get("candidate_count", "-"))))
            self.scan_stats_vars["watch"].set(str(row.get("观察数量", row.get("watch_count", "-"))))
            self.scan_stats_vars["skip"].set(str(row.get("跳过数量", row.get("skip_count", "-"))))
            self.scan_stats_vars["error"].set(str(row.get("失败数量", row.get("error_count", "-"))))
            self.scan_stats_vars["source"].set(Path(self.latest_result_files.get("summary", "")).name or "summary 未找到")
        else:
            for key in ["selected", "candidate", "watch", "skip", "error"]:
                self.scan_stats_vars[key].set("-")
            self.scan_stats_vars["source"].set("summary 未找到")
        self._refresh_scan_summary_label()
        if not found_any:
            self.result_ui_vars["info"].set("未找到扫描输出文件。请先执行“仅扫描”或“一键日更扫描”。")
            self._clear_result_tables()
            return
        self.apply_result_filters(silent=True)
        if not silent:
            self.append_log("[results] 已加载最新扫描结果文件。", "ui")

    def _refresh_scan_summary_label(self):
        text = f"入围 {self.scan_stats_vars['selected'].get()}｜候选 {self.scan_stats_vars['candidate'].get()}｜观察 {self.scan_stats_vars['watch'].get()}"
        if hasattr(self, "scan_summary_label"):
            self.scan_summary_label.configure(text=text)
        if hasattr(self, "scan_stats_inline"):
            self.scan_stats_inline.configure(text=f"入围 {self.scan_stats_vars['selected'].get()}｜候选 {self.scan_stats_vars['candidate'].get()}｜观察 {self.scan_stats_vars['watch'].get()}｜跳过 {self.scan_stats_vars['skip'].get()}｜错误 {self.scan_stats_vars['error'].get()}")

    def _clear_result_tables(self):
        for key, tree in self.result_treeviews.items():
            for item in tree.get_children():
                tree.delete(item)
            self.result_frames_view[key] = pd.DataFrame(columns=RESULT_DISPLAY_COLUMNS)
            self.result_notebook.tab(self.result_tab_frames[key], text=RESULT_TABS[key])
        self._clear_detail_tree()

    def apply_result_filters(self, silent: bool = False):
        self.result_sort_column = self.result_ui_vars["sort_by"].get().strip() or self.result_sort_column
        self.result_sort_ascending = self.result_ui_vars["sort_order"].get() == "升序"
        for key in RESULT_TABS:
            filtered = self._get_filtered_result_frame(key)
            self.result_frames_view[key] = filtered
            self._render_result_table(key, filtered)
        source_name = Path(self.latest_result_files.get("results", "")).name or Path(self.latest_result_files.get("summary", "")).name or "未找到扫描文件"
        self.result_ui_vars["info"].set(
            f"已加载最新扫描结果｜全部 {len(self.result_frames_view.get('results', pd.DataFrame()))} 行，入围 {len(self.result_frames_view.get('selected', pd.DataFrame()))} 行，"
            f"候选 {len(self.result_frames_view.get('candidate', pd.DataFrame()))} 行，观察 {len(self.result_frames_view.get('watch', pd.DataFrame()))} 行。"
            f" 当前排序：{self.result_sort_column}（{'升序' if self.result_sort_ascending else '降序'}）。 来源：{source_name}"
        )
        if not silent:
            self.append_log("[results] 已应用结果筛选与排序。", "summary")

    def reset_result_filters(self):
        self.result_ui_vars["keyword"].set("")
        self.result_ui_vars["label_filter"].set("全部")
        self.result_ui_vars["hard_filter"].set("全部")
        self.result_sort_column = "VR5"
        self.result_sort_ascending = False
        self.result_ui_vars["sort_by"].set(self.result_sort_column)
        self.result_ui_vars["sort_order"].set("降序")
        self.apply_result_filters()

    def _get_filtered_result_frame(self, tab_key: str):
        df = self.result_frames_raw.get(tab_key, pd.DataFrame()).copy()
        if df.empty:
            return df
        keyword = self.result_ui_vars["keyword"].get().strip().lower()
        if keyword:
            masks = []
            for col in ["股票代码", "股票名称", "硬过滤未通过原因"]:
                if col in df.columns:
                    masks.append(df[col].astype(str).str.lower().str.contains(keyword, na=False))
            if masks:
                mask = masks[0]
                for extra in masks[1:]:
                    mask = mask | extra
                df = df[mask].copy()
        label_filter = self.result_ui_vars["label_filter"].get()
        if label_filter != "全部" and "分层标签" in df.columns:
            df = df[df["分层标签"] == label_filter].copy()
        hard_filter = self.result_ui_vars["hard_filter"].get()
        if hard_filter == "仅硬过滤通过" and "硬过滤是否通过" in df.columns:
            df = df[df["硬过滤是否通过"] == "是"].copy()
        elif hard_filter == "仅硬过滤未通过" and "硬过滤是否通过" in df.columns:
            df = df[df["硬过滤是否通过"] != "是"].copy()
        if self.result_sort_column in df.columns:
            df = self._sort_dataframe(df, self.result_sort_column, self.result_sort_ascending)
        return df

    def _sort_dataframe(self, df, column: str, ascending: bool):
        if df.empty or column not in df.columns:
            return df
        sort_df = df.copy()
        if column == "日期":
            sort_df["__sort_key"] = pd.to_datetime(sort_df[column], errors="coerce")
        else:
            numeric = pd.to_numeric(sort_df[column], errors="coerce")
            sort_df["__sort_key"] = numeric if numeric.notna().sum() > 0 else sort_df[column].astype(str)
        secondary = "股票代码" if "股票代码" in sort_df.columns else sort_df.columns[0]
        sort_df = sort_df.sort_values(by=["__sort_key", secondary], ascending=[ascending, True], na_position="last")
        return sort_df.drop(columns=["__sort_key"])

    def _setup_tree_columns(self, tab_key: str, tree: ttk.Treeview, columns):
        self.result_columns_by_tab[tab_key] = list(columns)
        tree.configure(columns=columns, show="headings")
        for col in columns:
            tree.heading(col, text=col, command=lambda c=col: self._sort_by_header(c))
            anchor = "w" if col == "股票名称" else "center"
            tree.column(col, width=COLUMN_WIDTHS.get(col, 96), minwidth=70, stretch=False, anchor=anchor)

    def _render_result_table(self, tab_key: str, df):
        tree = self.result_treeviews[tab_key]
        for item in tree.get_children():
            tree.delete(item)
        columns = [c for c in RESULT_DISPLAY_COLUMNS if c in df.columns]
        if not columns:
            columns = RESULT_DISPLAY_COLUMNS
        self._setup_tree_columns(tab_key, tree, columns)
        if not df.empty:
            view = df[columns].copy()
            for row in view.itertuples(index=False, name=None):
                tree.insert("", "end", values=[self._coerce_display_value(v) for v in row])
            children = tree.get_children()
            if children:
                tree.selection_set(children[0])
                tree.focus(children[0])
                self._update_result_detail_from_selection(tab_key)
        else:
            if self.result_notebook.index(self.result_notebook.select()) == list(RESULT_TABS).index(tab_key):
                self._clear_detail_tree()
        self.result_notebook.tab(self.result_tab_frames[tab_key], text=f"{RESULT_TABS[tab_key]}（{len(df)}）")

    def _sort_by_header(self, column: str):
        if column == self.result_sort_column:
            self.result_sort_ascending = not self.result_sort_ascending
        else:
            self.result_sort_column = column
            self.result_sort_ascending = column in DEFAULT_ASCENDING_COLUMNS
        self.result_ui_vars["sort_by"].set(self.result_sort_column)
        self.result_ui_vars["sort_order"].set("升序" if self.result_sort_ascending else "降序")
        self.apply_result_filters(silent=True)
        self.append_log(f"[results] 已按 {column} {'升序' if self.result_sort_ascending else '降序'} 排序。", "summary")


    def _clear_detail_tree(self):
        if hasattr(self, "detail_text"):
            self.detail_text.configure(state="normal")
            self.detail_text.delete("1.0", "end")
            self.detail_text.insert("1.0", "未选择任何结果。")
            self.detail_text.configure(state="disabled")

    def _row_for_detail(self, tab_key: str, idx: int):
        df = self.result_frames_view.get(tab_key, pd.DataFrame())
        if df.empty or idx >= len(df):
            return None
        return df.iloc[idx]

    def _update_result_detail_from_selection(self, tab_key: str):
        tree = self.result_treeviews.get(tab_key)
        if tree is None:
            return
        selected = tree.selection()
        row = None
        if selected:
            idx = tree.index(selected[0])
            row = self._row_for_detail(tab_key, idx)
        text = self._format_horizontal_detail(row)
        if hasattr(self, "detail_text"):
            self.detail_text.configure(state="normal")
            self.detail_text.delete("1.0", "end")
            self.detail_text.insert("1.0", text)
            self.detail_text.configure(state="disabled")

    def _open_result_detail_popup(self, tab_key: str):

        tree = self.result_treeviews.get(tab_key)
        if tree is None:
            return
        selected = tree.selection()
        if not selected:
            return
        idx = tree.index(selected[0])
        row = self._row_for_detail(tab_key, idx)
        if row is None:
            return
        win = tk.Toplevel(self.root)
        win.title(f"结果详情 - {row.get('股票代码', '')} {row.get('股票名称', '')}")
        win.geometry("760x540")
        win.columnconfigure(0, weight=1)
        win.rowconfigure(0, weight=1)
        tree2 = ttk.Treeview(win, columns=("field", "value"), show="headings")
        tree2.grid(row=0, column=0, sticky="nsew")
        ybar = ttk.Scrollbar(win, orient="vertical", command=tree2.yview)
        ybar.grid(row=0, column=1, sticky="ns")
        xbar = ttk.Scrollbar(win, orient="horizontal", command=tree2.xview)
        xbar.grid(row=1, column=0, sticky="ew")
        tree2.configure(yscrollcommand=ybar.set, xscrollcommand=xbar.set)
        tree2.heading("field", text="字段")
        tree2.heading("value", text="值")
        tree2.column("field", width=220, stretch=False)
        tree2.column("value", width=500, stretch=True)
        for col in row.index:
            tree2.insert("", "end", values=(col, self._coerce_display_value(row.get(col))))

    def _coerce_display_value(self, value):
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        if isinstance(value, float):
            return f"{value:.3f}" if abs(value) < 1000 else f"{value:.0f}"
        return str(value)

    def run_daily_pipeline(self):
        _, gui_runner = _ensure_runtime_modules()
        precheck = gui_runner.find_missing_stocks()
        if not precheck.get("success", False):
            proceed = messagebox.askyesno("缺失预检查失败", "未能完成缺失股票预检查。\n\n通常是因为还没有最新的股票池文件。\n是否仍继续执行完整一键流程？")
            if not proceed:
                return
            skip_bootstrap = False
        else:
            missing_count = int(precheck.get("missing_count", 0) or 0)
            self.append_log(f"[precheck] 缺失股票预检查：{missing_count} 只", "summary")
            if missing_count > MISSING_CONFIRM_THRESHOLD:
                dialog = MissingActionDialog(self.root, missing_count, self.runtime_info["base_dir"])
                self.root.wait_window(dialog)
                if dialog.result == "cancel":
                    self.append_log("[ui] 已取消一键日更扫描。", "ui")
                    return
                skip_bootstrap = dialog.result == "skip"
            else:
                skip_bootstrap = False
        self._run_in_background("一键日更扫描", gui_runner.run_daily_pipeline, skip_bootstrap, total_steps=len(gui_runner.PIPELINE_STEPS), step_order=list(gui_runner.PIPELINE_STEPS))

    def run_sync_universe(self):
        _, gui_runner = _ensure_runtime_modules()
        self._run_in_background("同步股票池", gui_runner.sync_universe, total_steps=1, step_order=["sync_universe"])

    def run_update_daily_hist(self):
        _, gui_runner = _ensure_runtime_modules()
        self._run_in_background("仅更新历史库", gui_runner.update_daily_hist, total_steps=1, step_order=["update_daily_hist"])

    def run_pack_to_parquet(self):
        _, gui_runner = _ensure_runtime_modules()
        self._run_in_background("仅打包 Parquet", gui_runner.pack_to_parquet, total_steps=1, step_order=["pack_to_parquet"])

    def run_scan_from_parquet(self):
        _, gui_runner = _ensure_runtime_modules()
        self._run_in_background("仅扫描", gui_runner.scan_from_parquet, total_steps=1, step_order=["scan_from_parquet"])

    def run_build_watchlist(self):
        _, gui_runner = _ensure_runtime_modules()
        self._run_in_background("生成 Watchlist", gui_runner.build_watchlist, total_steps=1, step_order=["build_watchlist"])

    def run_bootstrap_missing(self):
        _, gui_runner = _ensure_runtime_modules()

        def task(log_callback=None, event_callback=None):
            missing_result = gui_runner.find_missing_stocks(log_callback=log_callback, event_callback=event_callback)
            if not missing_result.get("success", False):
                return missing_result
            missing_codes = missing_result.get("missing_codes", []) or []
            if not missing_codes:
                result = gui_runner.bootstrap_missing_stocks([], log_callback=log_callback, event_callback=event_callback)
                result["output_paths"] = missing_result.get("output_paths", [])
                result["missing_count"] = 0
                return result
            result = gui_runner.bootstrap_missing_stocks(missing_codes, log_callback=log_callback, event_callback=event_callback)
            result["missing_count"] = missing_result.get("missing_count", len(missing_codes))
            return result

        self._run_in_background("补建缺失股票", task, total_steps=2, step_order=["find_missing_stocks", "bootstrap_missing_stocks"])

    def open_output_dir(self):
        _, gui_runner = _ensure_runtime_modules()
        self._open_dir(Path(gui_runner.OUTPUT_DIR))

    def open_logs_dir(self):
        _, gui_runner = _ensure_runtime_modules()
        self._open_dir(Path(gui_runner.LOGS_DIR))

    def open_watchlist_dir(self):
        _, gui_runner = _ensure_runtime_modules()
        self._open_dir(Path(gui_runner.WATCHLIST_DIR))

    def _open_dir(self, path: Path):
        try:
            path.mkdir(parents=True, exist_ok=True)
            os.startfile(str(path))
        except Exception as exc:
            messagebox.showerror("打开目录失败", str(exc))

# --- official D0 result view overrides ---

OFFICIAL_RESULT_SORT_KEY = "正式D0默认排序"
RESULT_DISPLAY_COLUMNS = [
    "股票代码",
    "股票名称",
    "日期",
    "涨跌幅%",
    "换手率",
    "量比前一日",
    "VR5",
    "BR20",
    "official_d0_tier",
    "official_d0_score",
    "official_d0_flag",
    "命中硬过滤数",
    "分层标签",
    "硬过滤是否通过",
]
COLUMN_WIDTHS = dict(
    COLUMN_WIDTHS,
    official_d0_tier=90,
    official_d0_score=94,
    official_d0_flag=96,
)
SORTABLE_COLUMNS = [
    OFFICIAL_RESULT_SORT_KEY,
    "股票代码",
    "股票名称",
    "日期",
    "涨跌幅%",
    "换手率",
    "量比前一日",
    "VR5",
    "BR20",
    "official_d0_tier",
    "official_d0_score",
    "official_d0_flag",
    "命中硬过滤数",
    "分层标签",
]
DEFAULT_ASCENDING_COLUMNS = set(DEFAULT_ASCENDING_COLUMNS) | {"official_d0_tier"}

_ORIGINAL_GUIAPP_INIT = GuiApp.__init__


def _sort_official_result_dataframe(self, df):
    pd_mod, _ = _ensure_runtime_modules()
    if df.empty:
        return df
    required_cols = {"official_d0_flag", "official_d0_tier", "official_d0_score"}
    if not required_cols.issubset(df.columns):
        return self._sort_dataframe(df, "VR5", False) if "VR5" in df.columns else df
    sort_df = df.copy()
    sort_df["__official_flag"] = sort_df["official_d0_flag"].astype(str).eq("是").astype(int)
    sort_df["__official_tier"] = sort_df["official_d0_tier"].map({"A": 0, "B": 1, "C": 2}).fillna(9)
    sort_df["__official_score"] = pd_mod.to_numeric(sort_df["official_d0_score"], errors="coerce").fillna(-1)
    sort_df["__br20"] = pd_mod.to_numeric(sort_df.get("BR20"), errors="coerce").fillna(-1)
    sort_df["__turnover"] = pd_mod.to_numeric(sort_df.get("换手率"), errors="coerce").fillna(-1)
    sort_df["__turnover_f"] = pd_mod.to_numeric(sort_df.get("d0_turnover_f"), errors="coerce").fillna(-1)
    secondary = "股票代码" if "股票代码" in sort_df.columns else sort_df.columns[0]
    sort_df = sort_df.sort_values(
        by=["__official_flag", "__official_tier", "__official_score", "__br20", "__turnover", "__turnover_f", secondary],
        ascending=[False, True, False, False, False, False, True],
        na_position="last",
    )
    return sort_df.drop(columns=["__official_flag", "__official_tier", "__official_score", "__br20", "__turnover", "__turnover_f"])


def _patched_get_filtered_result_frame(self, tab_key: str):
    pd_mod, _ = _ensure_runtime_modules()
    df = self.result_frames_raw.get(tab_key, pd_mod.DataFrame()).copy()
    if df.empty:
        return df
    keyword = self.result_ui_vars["keyword"].get().strip().lower()
    if keyword:
        masks = []
        for col in ["股票代码", "股票名称", "硬过滤未通过原因", "official_d0_hit_rules", "official_d0_miss_rules"]:
            if col in df.columns:
                masks.append(df[col].astype(str).str.lower().str.contains(keyword, na=False))
        if masks:
            mask = masks[0]
            for extra in masks[1:]:
                mask = mask | extra
            df = df[mask].copy()
    label_filter = self.result_ui_vars["label_filter"].get()
    if label_filter != "全部" and "分层标签" in df.columns:
        df = df[df["分层标签"] == label_filter].copy()
    hard_filter = self.result_ui_vars["hard_filter"].get()
    if hard_filter == "仅硬过滤通过" and "硬过滤是否通过" in df.columns:
        df = df[df["硬过滤是否通过"] == "是"].copy()
    elif hard_filter == "仅硬过滤未通过" and "硬过滤是否通过" in df.columns:
        df = df[df["硬过滤是否通过"] != "是"].copy()
    if self.result_sort_column == OFFICIAL_RESULT_SORT_KEY:
        return _sort_official_result_dataframe(self, df)
    if self.result_sort_column in df.columns:
        return self._sort_dataframe(df, self.result_sort_column, self.result_sort_ascending)
    return df


def _patched_reset_result_filters(self):
    self.result_ui_vars["keyword"].set("")
    self.result_ui_vars["label_filter"].set("全部")
    self.result_ui_vars["hard_filter"].set("全部")
    self.result_sort_column = OFFICIAL_RESULT_SORT_KEY
    self.result_sort_ascending = False
    self.result_ui_vars["sort_by"].set(self.result_sort_column)
    self.result_ui_vars["sort_order"].set("降序")
    self.apply_result_filters()


def _patched_guiapp_init(self, root):
    _ORIGINAL_GUIAPP_INIT(self, root)
    self.result_sort_column = OFFICIAL_RESULT_SORT_KEY
    self.result_sort_ascending = False
    if hasattr(self, "result_ui_vars"):
        self.result_ui_vars["sort_by"].set(self.result_sort_column)
        self.result_ui_vars["sort_order"].set("降序")


GuiApp.__init__ = _patched_guiapp_init
GuiApp._get_filtered_result_frame = _patched_get_filtered_result_frame
GuiApp.reset_result_filters = _patched_reset_result_filters


def main():
    root = tk.Tk()
    app = GuiApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
