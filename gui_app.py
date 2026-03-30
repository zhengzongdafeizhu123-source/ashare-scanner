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
from tkinter import messagebox, ttk


PROJECT_DIR = Path(__file__).resolve().parent
SCAN_CONFIG_FILE = PROJECT_DIR / "scan_config.json"
MISSING_CONFIRM_THRESHOLD = 100
pd = None
gui_runner = None


def _ensure_runtime_modules():
    global pd, gui_runner

    if pd is None:
        import pandas as _pd

        pd = _pd

    if gui_runner is None:
        import gui_runner as _gui_runner

        gui_runner = _gui_runner

    return pd, gui_runner

STEP_LABELS = {
    "sync_universe": "同步股票池",
    "find_missing_stocks": "检查缺失股票",
    "bootstrap_missing_stocks": "补建缺失股票",
    "update_daily_hist": "更新历史库",
    "pack_to_parquet": "打包 Parquet",
    "scan_from_parquet": "执行扫描",
    "run_daily_pipeline": "一键日更扫描",
}

FIELD_META = {
    "volatility_window": {
        "label": "波动窗口（天）",
        "tooltip": "用于计算区间波动率的历史窗口长度。",
        "type": "entry",
    },
    "volatility_max": {
        "label": "波动率上限",
        "tooltip": "窗口内最高价 / 最低价 - 1 的上限。",
        "type": "entry",
    },
    "require_bullish": {
        "label": "是否要求阳线",
        "tooltip": "开启后，仅保留当日收盘价高于开盘价的股票。",
        "type": "check",
    },
    "volume_multiplier": {
        "label": "放量倍数",
        "tooltip": "今日成交量相对前一日的倍数阈值。",
        "type": "entry",
    },
    "turnover_min": {
        "label": "换手率下限（%）",
        "tooltip": "低于该换手率阈值的股票直接跳过。",
        "type": "entry",
    },
    "min_history_bars": {
        "label": "最少历史条数",
        "tooltip": "不足该条数的股票直接跳过扫描。",
        "type": "entry",
    },
}

PROGRESS_PATTERNS = [
    re.compile(r"\[进度\]\s*(?P<done>\d+)\s*/\s*(?P<total>\d+)"),
    re.compile(r"(?<!\d)(?P<done>\d+)\s*/\s*(?P<total>\d+)(?!\d)"),
]

RESULT_TABS = {
    "results": "全部",
    "selected": "入围",
    "candidate": "候选",
    "watch": "观察",
}
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
    "股票代码": 88,
    "股票名称": 128,
    "日期": 96,
    "涨跌幅%": 88,
    "换手率": 84,
    "量比前一日": 94,
    "VR5": 76,
    "BR20": 76,
    "命中硬过滤数": 100,
    "分层标签": 86,
    "硬过滤是否通过": 108,
}
DEFAULT_ASCENDING_COLUMNS = {"股票代码", "股票名称", "日期", "分层标签", "硬过滤是否通过", "硬过滤未通过原因"}
SORTABLE_COLUMNS = [
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
]


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
        x = self.widget.winfo_rootx() + 16
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 8
        self.tip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tw,
            text=self.text,
            justify="left",
            background="#fff8dc",
            relief="solid",
            borderwidth=1,
            padx=8,
            pady=5,
            wraplength=260,
        )
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


class GuiApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("AShare Scanner GUI V2")
        self._apply_adaptive_geometry()

        self.log_queue: queue.Queue[tuple[str, str, object]] = queue.Queue()
        self.current_worker: threading.Thread | None = None
        self.action_buttons: list[ttk.Button] = []

        self.runtime_info = {
            "base_dir": "初始化中...",
            "mode": "初始化中...",
            "data_dir": "",
            "output_dir": "",
            "logs_dir": "",
        }
        self.total_steps_expected = 1
        self.total_steps_completed = 0
        self.current_step_name = ""
        self.current_step_started_at: float | None = None
        self.task_started_at: float | None = None
        self.last_progress_match: tuple[int, int] | None = None
        self.current_action_label = "空闲"
        self.current_step_order: list[str] = []

        self.result_frames_raw = {key: None for key in RESULT_TABS}
        self.result_frames_view = {key: None for key in RESULT_TABS}
        self.latest_result_files: dict[str, str] = {}
        self.result_treeviews: dict[str, ttk.Treeview] = {}
        self.result_tab_frames: dict[str, ttk.Frame] = {}
        self.result_columns_by_tab: dict[str, list[str]] = {}
        self.result_detail_records: dict[tuple[str, str], dict] = {}
        self.log_texts: dict[str, tk.Text] = {}
        self.log_history: list[tuple[str, str]] = []
        self.log_window: tk.Toplevel | None = None
        self.log_notebook: ttk.Notebook | None = None
        self.param_window: tk.Toplevel | None = None
        self.param_summary_var = tk.StringVar(value="参数窗口未打开")
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

        self.task_vars = {
            "task_name": tk.StringVar(value="空闲"),
            "main_step": tk.StringVar(value="-"),
            "sub_step": tk.StringVar(value="-"),
            "progress_text": tk.StringVar(value="-"),
            "elapsed": tk.StringVar(value="00:00:00"),
            "eta": tk.StringVar(value="--"),
            "base_dir": tk.StringVar(value=self.runtime_info["base_dir"]),
            "mode": tk.StringVar(value=self.runtime_info["mode"]),
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

        self.config_vars = {
            "volatility_window": tk.StringVar(),
            "volatility_max": tk.StringVar(),
            "require_bullish": tk.BooleanVar(value=True),
            "volume_multiplier": tk.StringVar(),
            "turnover_min": tk.StringVar(),
            "min_history_bars": tk.StringVar(),
        }

        self._build_ui()
        self._configure_styles()
        self._set_status_message("GUI 正在初始化，请稍候...")
        self._set_result_detail_text("未选择任何扫描结果。")
        self.set_busy(True)
        self.append_log("[ui] GUI 已创建，正在后台初始化运行环境...", "ui")
        self.root.after(100, self._poll_log_queue)
        self.root.after(500, self._tick_clock)
        self.root.after(50, self._finish_startup)
        self.root.after(200, self.show_log_window)

    def _configure_styles(self):
        style = ttk.Style(self.root)
        try:
            style.configure("Result.Treeview", rowheight=24)
            style.configure("Result.Treeview.Heading", padding=(4, 4))
            style.configure("Detail.Treeview", rowheight=24)
        except Exception:
            pass

    def _apply_adaptive_geometry(self):
        screen_width = max(1024, self.root.winfo_screenwidth())
        screen_height = max(768, self.root.winfo_screenheight())

        target_width = int(screen_width * 0.84)
        target_height = int(screen_height * 0.88)

        target_width = min(target_width, 1880 if screen_width >= 2560 else 1720)
        target_height = min(target_height, 1120 if screen_height >= 1440 else 1000)

        target_width = max(1100, min(target_width, screen_width - 40))
        target_height = max(760, min(target_height, screen_height - 60))

        min_width = min(target_width, max(980, int(screen_width * 0.68)))
        min_height = min(target_height, max(720, int(screen_height * 0.72)))

        pos_x = max(0, (screen_width - target_width) // 2)
        pos_y = max(0, (screen_height - target_height) // 2)

        self.root.geometry(f"{target_width}x{target_height}+{pos_x}+{pos_y}")
        self.root.minsize(min_width, min_height)

    def _finish_startup(self):
        try:
            _pd, _gui_runner = _ensure_runtime_modules()
            self.runtime_info = _gui_runner.get_runtime_info()
            self.task_vars["base_dir"].set(self.runtime_info["base_dir"])
            self.task_vars["mode"].set(self.runtime_info["mode"])
            self.result_frames_raw = {key: _pd.DataFrame() for key in RESULT_TABS}
            self.result_frames_view = {key: _pd.DataFrame() for key in RESULT_TABS}
            self.load_config_to_form()
            self.refresh_latest_scan_results(silent=True)
            self._set_status_message(self.status_vars["message"].get())
            self.append_log("[ui] 初始化完成。", "ui")
        except Exception:
            error_text = traceback.format_exc()
            self.append_log("[ui] 初始化失败：\n" + error_text, "error")
            self.status_vars["message"].set("GUI 初始化失败，请查看日志。")
            self._set_status_message(self.status_vars["message"].get())
        finally:
            self.set_busy(False)


    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        outer = ttk.Frame(self.root, padding=10)
        outer.grid(row=0, column=0, sticky="nsew")
        outer.columnconfigure(0, weight=1)
        outer.rowconfigure(0, weight=1)

        self.main_paned = ttk.Panedwindow(outer, orient="vertical")
        self.main_paned.grid(row=0, column=0, sticky="nsew")

        self.top_frame = ttk.Frame(self.main_paned)
        self.top_frame.columnconfigure(0, weight=0)
        self.top_frame.columnconfigure(1, weight=1)
        self.top_frame.columnconfigure(2, weight=0)
        self.top_frame.rowconfigure(0, weight=1)

        self.bottom_frame = ttk.Frame(self.main_paned)
        self.bottom_frame.columnconfigure(0, weight=1)
        self.bottom_frame.rowconfigure(0, weight=0)
        self.bottom_frame.rowconfigure(1, weight=1)

        self.main_paned.add(self.top_frame, weight=1)
        self.main_paned.add(self.bottom_frame, weight=4)

        self._build_actions_panel(self.top_frame)
        self._build_task_panel(self.top_frame)
        self._build_config_panel(self.top_frame)

        self._build_status_panel(self.bottom_frame)
        self._build_result_panel(self.bottom_frame)

        self.root.after_idle(self._apply_initial_layout)

    def _apply_initial_layout(self):
        try:
            total_h = max(760, self.root.winfo_height())
            top_h = max(190, min(240, int(total_h * 0.27)))
            self.main_paned.sashpos(0, top_h)
        except Exception:
            pass
        try:
            total_h = max(400, self.result_vertical_paned.winfo_height())
            detail_h = max(180, min(260, int(total_h * 0.28)))
            self.result_vertical_paned.sashpos(0, total_h - detail_h)
        except Exception:
            pass

    def _build_actions_panel(self, parent: ttk.Frame):
        frame = ttk.LabelFrame(parent, text="操作区", padding=8)
        frame.grid(row=0, column=0, sticky="nsw", padx=(0, 8))
        frame.columnconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)

        button_specs = [
            ("一键日更扫描", self.run_daily_pipeline),
            ("同步股票池", self.run_sync_universe),
            ("补建缺失股票", self.run_bootstrap_missing),
            ("仅更新历史库", self.run_update_daily_hist),
            ("仅打包 Parquet", self.run_pack_to_parquet),
            ("仅扫描", self.run_scan_from_parquet),
        ]
        for idx, (label, command) in enumerate(button_specs):
            button = ttk.Button(frame, text=label, command=command)
            button.grid(row=idx, column=0, columnspan=2, sticky="ew", pady=3)
            self.action_buttons.append(button)

        row = len(button_specs)
        for col, (label, cmd) in enumerate([
            ("打开 output 目录", self.open_output_dir),
            ("打开 logs 目录", self.open_logs_dir),
        ]):
            button = ttk.Button(frame, text=label, command=cmd)
            button.grid(row=row, column=col, sticky="ew", pady=(6, 0), padx=(0 if col == 0 else 4, 4 if col == 0 else 0))
            self.action_buttons.append(button)

        tools_row = ttk.Frame(frame)
        tools_row.grid(row=row + 1, column=0, columnspan=2, sticky="ew", pady=(6, 0))
        tools_row.columnconfigure(0, weight=1)
        tools_row.columnconfigure(1, weight=1)

        log_btn = ttk.Button(tools_row, text="显示运行日志", command=self.show_log_window)
        log_btn.grid(row=0, column=0, sticky="ew", padx=(0, 4))
        self.action_buttons.append(log_btn)

        param_btn = ttk.Button(tools_row, text="扫描参数…", command=self.open_param_window)
        param_btn.grid(row=0, column=1, sticky="ew", padx=(4, 0))
        self.action_buttons.append(param_btn)

    def _build_task_panel(self, parent: ttk.Frame):
        frame = ttk.LabelFrame(parent, text="当前任务", padding=10)
        frame.grid(row=0, column=1, sticky="nsew", padx=8)
        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(3, weight=1)

        rows = [
            ("当前任务", "task_name", 0, 0),
            ("当前主步骤", "main_step", 0, 2),
            ("当前子步骤", "sub_step", 1, 0),
            ("当前进度文本", "progress_text", 1, 2),
            ("当前耗时", "elapsed", 2, 0),
            ("ETA", "eta", 2, 2),
            ("当前 base_dir", "base_dir", 3, 0),
            ("当前模式", "mode", 3, 2),
        ]
        wrap = max(260, int(self.root.winfo_screenwidth() * 0.18))
        for label, key, row, col in rows:
            ttk.Label(frame, text=label).grid(row=row, column=col, sticky="nw", pady=2, padx=(0, 6))
            ttk.Label(frame, textvariable=self.task_vars[key], wraplength=wrap, justify="left").grid(
                row=row, column=col + 1, sticky="ew", pady=2
            )

        progress_frame = ttk.Frame(frame)
        progress_frame.grid(row=4, column=0, columnspan=4, sticky="ew", pady=(8, 0))
        progress_frame.columnconfigure(0, weight=1)

        ttk.Label(progress_frame, textvariable=self.task_vars["total_progress"]).grid(row=0, column=0, sticky="w")
        self.total_progressbar = ttk.Progressbar(progress_frame, mode="determinate", maximum=1, value=0)
        self.total_progressbar.grid(row=1, column=0, sticky="ew", pady=(4, 8))

        ttk.Label(progress_frame, textvariable=self.task_vars["step_progress"]).grid(row=2, column=0, sticky="w")
        self.step_progressbar = ttk.Progressbar(progress_frame, mode="indeterminate")
        self.step_progressbar.grid(row=3, column=0, sticky="ew", pady=(4, 0))

    def _build_config_panel(self, parent: ttk.Frame):
        frame = ttk.LabelFrame(parent, text="参数 / 工具", padding=10)
        frame.grid(row=0, column=2, sticky="nsew", padx=(8, 0))
        frame.columnconfigure(0, weight=1)

        summary = ttk.Label(frame, textvariable=self.param_summary_var, justify="left", wraplength=260)
        summary.grid(row=0, column=0, sticky="ew")
        ToolTip(summary, "扫描参数已移动到二级窗口，避免占用主界面高度。")

        ttk.Button(frame, text="打开扫描参数窗口", command=self.open_param_window).grid(row=1, column=0, sticky="ew", pady=(8, 6))
        ttk.Button(frame, text="显示运行日志窗口", command=self.show_log_window).grid(row=2, column=0, sticky="ew", pady=6)
        ttk.Button(frame, text="刷新最新结果", command=self.refresh_latest_scan_results).grid(row=3, column=0, sticky="ew", pady=6)

    def open_param_window(self):
        if self.param_window is not None and self.param_window.winfo_exists():
            self.param_window.deiconify()
            self.param_window.lift()
            self.param_window.focus_force()
            return

        win = tk.Toplevel(self.root)
        win.title("扫描参数")
        win.geometry("520x360")
        win.minsize(460, 320)
        win.transient(self.root)
        self.param_window = win

        frame = ttk.Frame(win, padding=12)
        frame.grid(row=0, column=0, sticky="nsew")
        win.columnconfigure(0, weight=1)
        win.rowconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)

        field_order = [
            "volatility_window",
            "volatility_max",
            "require_bullish",
            "volume_multiplier",
            "turnover_min",
            "min_history_bars",
        ]
        for idx, key in enumerate(field_order):
            meta = FIELD_META[key]
            label = ttk.Label(frame, text=meta["label"])
            label.grid(row=idx, column=0, sticky="w", pady=6, padx=(0, 10))
            ToolTip(label, meta["tooltip"])
            if meta["type"] == "check":
                widget = ttk.Checkbutton(frame, variable=self.config_vars[key])
            else:
                widget = ttk.Entry(frame, textvariable=self.config_vars[key])
            widget.grid(row=idx, column=1, sticky="ew", pady=6)
            ToolTip(widget, meta["tooltip"])

        button_row = ttk.Frame(frame)
        button_row.grid(row=len(field_order), column=0, columnspan=2, sticky="ew", pady=(14, 0))
        for col in range(4):
            button_row.columnconfigure(col, weight=1)

        ttk.Button(button_row, text="读取配置", command=self.load_config_to_form).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(button_row, text="保存配置", command=self.save_config_from_form).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(button_row, text="刷新配置", command=self.load_config_to_form).grid(row=0, column=2, sticky="ew", padx=4)
        ttk.Button(button_row, text="关闭", command=win.withdraw).grid(row=0, column=3, sticky="ew", padx=(4, 0))

        win.protocol("WM_DELETE_WINDOW", win.withdraw)

    def _refresh_param_summary(self):
        parts = [
            f"{FIELD_META['volatility_window']['label']}: {self.config_vars['volatility_window'].get() or '-'}",
            f"{FIELD_META['volatility_max']['label']}: {self.config_vars['volatility_max'].get() or '-'}",
            f"{FIELD_META['volume_multiplier']['label']}: {self.config_vars['volume_multiplier'].get() or '-'}",
            f"{FIELD_META['turnover_min']['label']}: {self.config_vars['turnover_min'].get() or '-'}",
        ]
        bullish = "是" if self.config_vars["require_bullish"].get() else "否"
        bars = self.config_vars["min_history_bars"].get() or "-"
        parts.append(f"{FIELD_META['require_bullish']['label']}: {bullish}")
        parts.append(f"{FIELD_META['min_history_bars']['label']}: {bars}")
        self.param_summary_var.set(" | ".join(parts))

    def _build_status_panel(self, parent: ttk.Frame):
        frame = ttk.LabelFrame(parent, text="最近一次结果", padding=10)
        frame.grid(row=0, column=0, sticky="ew")
        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(3, weight=1)

        rows = [
            ("success", "是否成功"),
            ("missing_count", "缺失数量"),
            ("created_count", "补建数量"),
            ("error_count", "错误数量"),
            ("skipped_count", "跳过数量"),
            ("output_paths_count", "输出文件数"),
        ]
        for idx, (key, label) in enumerate(rows):
            row = idx // 3
            col = (idx % 3) * 2
            ttk.Label(frame, text=label).grid(row=row, column=col, sticky="w", pady=2, padx=(0, 6))
            ttk.Label(frame, textvariable=self.status_vars[key]).grid(row=row, column=col + 1, sticky="w", pady=2)

        ttk.Label(frame, text="消息 / 日志文件").grid(row=2, column=0, sticky="nw", pady=(8, 0))
        message_frame = ttk.Frame(frame)
        message_frame.grid(row=2, column=1, columnspan=5, sticky="ew", pady=(8, 0))
        message_frame.columnconfigure(0, weight=1)
        self.status_message_text = tk.Text(message_frame, wrap="word", height=2, state="disabled")
        self.status_message_text.grid(row=0, column=0, sticky="ew")
        message_scrollbar = ttk.Scrollbar(message_frame, orient="vertical", command=self.status_message_text.yview)
        message_scrollbar.grid(row=0, column=1, sticky="ns")
        self.status_message_text.configure(yscrollcommand=message_scrollbar.set)

    def _build_result_panel(self, parent: ttk.Frame):
        frame = ttk.LabelFrame(parent, text="扫描结果", padding=10)
        frame.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=0)
        frame.rowconfigure(1, weight=1)

        controls = ttk.Frame(frame)
        controls.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        for col in range(11):
            controls.columnconfigure(col, weight=0)
        controls.columnconfigure(1, weight=1)

        ttk.Label(controls, text="关键词").grid(row=0, column=0, sticky="w")
        keyword_entry = ttk.Entry(controls, textvariable=self.result_ui_vars["keyword"])
        keyword_entry.grid(row=0, column=1, sticky="ew", padx=(6, 10))
        keyword_entry.bind("<Return>", lambda _e: self.apply_result_filters())

        ttk.Label(controls, text="标签筛选").grid(row=0, column=2, sticky="w")
        ttk.Combobox(
            controls, textvariable=self.result_ui_vars["label_filter"], state="readonly",
            values=["全部", "候选", "观察", "放弃"], width=8
        ).grid(row=0, column=3, sticky="w", padx=(6, 10))

        ttk.Label(controls, text="硬过滤").grid(row=0, column=4, sticky="w")
        ttk.Combobox(
            controls, textvariable=self.result_ui_vars["hard_filter"], state="readonly",
            values=["全部", "仅硬过滤通过", "仅硬过滤未通过"], width=14
        ).grid(row=0, column=5, sticky="w", padx=(6, 10))

        ttk.Label(controls, text="排序列").grid(row=0, column=6, sticky="w")
        ttk.Combobox(
            controls, textvariable=self.result_ui_vars["sort_by"], state="readonly",
            values=SORTABLE_COLUMNS, width=12
        ).grid(row=0, column=7, sticky="w", padx=(6, 10))

        ttk.Combobox(
            controls, textvariable=self.result_ui_vars["sort_order"], state="readonly",
            values=["升序", "降序"], width=6
        ).grid(row=0, column=8, sticky="w", padx=(0, 10))

        ttk.Button(controls, text="应用筛选", command=self.apply_result_filters).grid(row=0, column=9, sticky="ew", padx=(0, 6))
        ttk.Button(controls, text="重置", command=self.reset_result_filters).grid(row=0, column=10, sticky="ew")

        info_label = ttk.Label(frame, textvariable=self.result_ui_vars["info"], justify="left")
        info_label.grid(row=1, column=0, sticky="ew", pady=(0, 8))

        self.result_vertical_paned = ttk.Panedwindow(frame, orient="vertical")
        self.result_vertical_paned.grid(row=1, column=0, sticky="nsew")

        upper = ttk.Frame(self.result_vertical_paned)
        upper.columnconfigure(0, weight=1)
        upper.rowconfigure(0, weight=1)

        self.result_notebook = ttk.Notebook(upper)
        self.result_notebook.grid(row=0, column=0, sticky="nsew")
        for tab_key, title in RESULT_TABS.items():
            self._create_result_tab(tab_key, title)

        lower = ttk.LabelFrame(self.result_vertical_paned, text="当前选中行详情", padding=8)
        lower.columnconfigure(0, weight=1)
        lower.rowconfigure(0, weight=1)

        detail_tree = ttk.Treeview(lower, columns=("field", "value"), show="headings", style="Detail.Treeview")
        detail_tree.heading("field", text="字段")
        detail_tree.heading("value", text="值")
        detail_tree.column("field", width=160, minwidth=120, anchor="w", stretch=False)
        detail_tree.column("value", width=800, minwidth=260, anchor="w", stretch=True)
        detail_tree.grid(row=0, column=0, sticky="nsew")
        detail_vsb = ttk.Scrollbar(lower, orient="vertical", command=detail_tree.yview)
        detail_vsb.grid(row=0, column=1, sticky="ns")
        detail_hsb = ttk.Scrollbar(lower, orient="horizontal", command=detail_tree.xview)
        detail_hsb.grid(row=1, column=0, sticky="ew")
        detail_tree.configure(yscrollcommand=detail_vsb.set, xscrollcommand=detail_hsb.set)
        self.result_detail_tree = detail_tree

        self.result_vertical_paned.add(upper, weight=4)
        self.result_vertical_paned.add(lower, weight=2)

    def _create_result_tab(self, tab_key: str, title: str):
        tab = ttk.Frame(self.result_notebook)
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        self.result_notebook.add(tab, text=title)
        self.result_tab_frames[tab_key] = tab

        table_frame = ttk.Frame(tab)
        table_frame.grid(row=0, column=0, sticky="nsew")
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        tree = ttk.Treeview(table_frame, columns=RESULT_DISPLAY_COLUMNS, show="headings", style="Result.Treeview")
        tree.grid(row=0, column=0, sticky="nsew")

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=tree.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=tree.xview)
        hsb.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.result_treeviews[tab_key] = tree
        tree.bind("<<TreeviewSelect>>", lambda _event, key=tab_key: self._update_result_detail_from_selection(key))
        tree.bind("<Double-1>", lambda _event, key=tab_key: self._show_selected_record_popup(key))
        self._setup_tree_columns(tab_key, tree, RESULT_DISPLAY_COLUMNS)

    def show_log_window(self):
        if self.log_window is not None and self.log_window.winfo_exists():
            self.log_window.deiconify()
            self.log_window.lift()
            self.log_window.focus_force()
            return

        win = tk.Toplevel(self.root)
        win.title("运行日志")
        main_x = self.root.winfo_rootx()
        main_y = self.root.winfo_rooty()
        main_w = max(1200, self.root.winfo_width())
        main_h = max(800, self.root.winfo_height())
        width = max(520, int(main_w * 0.34))
        height = max(500, int(main_h * 0.9))
        x = main_x + main_w + 12
        y = main_y
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        if x + width > screen_w - 20:
            x = max(0, screen_w - width - 20)
        if y + height > screen_h - 40:
            y = max(0, screen_h - height - 40)
        win.geometry(f"{width}x{height}+{x}+{y}")
        win.minsize(460, 380)
        win.columnconfigure(0, weight=1)
        win.rowconfigure(1, weight=1)

        toolbar = ttk.Frame(win, padding=(10, 10, 10, 6))
        toolbar.grid(row=0, column=0, sticky="ew")
        toolbar.columnconfigure(0, weight=1)
        ttk.Label(toolbar, text="日志独立窗口常驻显示，避免主界面被挤压。").grid(row=0, column=0, sticky="w")
        ttk.Button(toolbar, text="清空日志", command=self.clear_logs).grid(row=0, column=1, sticky="e")

        notebook = ttk.Notebook(win)
        notebook.grid(row=1, column=0, sticky="nsew", padx=10, pady=(0, 10))
        self.log_window = win
        self.log_notebook = notebook

        self.log_texts = {}
        for key, title in [("all", "全部"), ("raw", "原始日志"), ("summary", "步骤摘要"), ("error", "错误")]:
            self._create_log_tab(key, title)

        self._configure_log_tags()
        for message, tag in self.log_history:
            self._append_log_to_targets(message, tag)

        def _hide():
            win.withdraw()
        win.protocol("WM_DELETE_WINDOW", _hide)

    def _create_log_tab(self, key: str, title: str):
        if self.log_notebook is None:
            return
        tab = ttk.Frame(self.log_notebook)
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        self.log_notebook.add(tab, text=title)

        text = tk.Text(tab, wrap="none", state="disabled")
        text.grid(row=0, column=0, sticky="nsew")
        y_scroll = ttk.Scrollbar(tab, orient="vertical", command=text.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = ttk.Scrollbar(tab, orient="horizontal", command=text.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        text.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        self.log_texts[key] = text

    def _configure_log_tags(self):
        for widget in self.log_texts.values():
            try:
                widget.tag_configure("raw", foreground="#222222")
                widget.tag_configure("summary", foreground="#0b5394")
                widget.tag_configure("error", foreground="#a61c00")
                widget.tag_configure("ui", foreground="#38761d")
            except Exception:
                pass

    def _append_log_to_targets(self, message: str, tag: str):
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

    def _append_to_log_widget(self, widget: tk.Text, message: str, tag: str):
        widget.configure(state="normal")
        widget.insert("end", message + "\n", tag)
        widget.see("end")
        widget.configure(state="disabled")

    def append_log(self, message: str, tag: str = "raw"):
        self.log_history.append((message, tag))
        self._append_log_to_targets(message, tag)

    def clear_logs(self):
        self.log_history.clear()
        for widget in self.log_texts.values():
            widget.configure(state="normal")
            widget.delete("1.0", "end")
            widget.configure(state="disabled")

    def set_busy(self, is_busy: bool):
        state = "disabled" if is_busy else "normal"
        for button in self.action_buttons:
            try:
                button.configure(state=state)
            except Exception:
                pass

        if is_busy:
            try:
                self.step_progressbar.start(12)
            except Exception:
                pass
        else:
            try:
                self.step_progressbar.stop()
            except Exception:
                pass

    def _set_status_message(self, text: str):
        widget = self.status_message_text
        widget.configure(state="normal")
        widget.delete("1.0", "end")
        widget.insert("1.0", text or "")
        widget.configure(state="disabled")

    def _set_result_detail_text(self, text: str):
        tree = self.result_detail_tree
        for item in tree.get_children():
            tree.delete(item)
        tree.insert("", "end", values=("message", text or ""))

    def _setup_tree_columns(self, tab_key: str, tree: ttk.Treeview, columns: list[str]):
        self.result_columns_by_tab[tab_key] = list(columns)
        tree.configure(columns=columns)
        for column in columns:
            width = COLUMN_WIDTHS.get(column, 96)
            default_ascending = column in DEFAULT_ASCENDING_COLUMNS
            tree.heading(
                column,
                text=column,
                command=lambda c=column, key=tab_key, asc=default_ascending: self._sort_by_header(key, c, asc),
            )
            tree.column(column, width=width, minwidth=max(72, min(width, 120)), anchor="w", stretch=True)

    def _clear_tree(self, tree: ttk.Treeview):
        for item in tree.get_children():
            tree.delete(item)

    def _coerce_display_value(self, value):
        if value is None:
            return ""
        if pd is not None and getattr(pd, "isna", None) is not None:
            try:
                if pd.isna(value):
                    return ""
            except Exception:
                pass
        if isinstance(value, float):
            return f"{value:.4f}".rstrip("0").rstrip(".")
        return str(value)

    def _clear_result_tables(self):
        for tree in self.result_treeviews.values():
            self._clear_tree(tree)
        self.result_detail_records.clear()
        self._set_result_detail_text("未加载扫描结果。")

    def refresh_latest_scan_results(self, silent: bool = False):
        _pd, _gui_runner = _ensure_runtime_modules()
        payload = _gui_runner.load_latest_scan_frames()
        frames = payload.get("frames", {})
        files = payload.get("files", {})
        self.latest_result_files = files

        for key in RESULT_TABS:
            frame = frames.get(key)
            if frame is None:
                frame = _pd.DataFrame()
            self.result_frames_raw[key] = frame.copy()

        if not payload.get("found_any", False):
            self.result_frames_view = {key: _pd.DataFrame() for key in RESULT_TABS}
            self._clear_result_tables()
            self.result_ui_vars["info"].set("尚未找到最新扫描结果文件。")
            if not silent:
                self.append_log("[result] 未找到最新扫描结果文件。", "summary")
            return

        self.apply_result_filters()
        if not silent:
            result_file = files.get("results") or "未找到全量结果文件"
            self.append_log(f"[result] 已刷新最新结果：{result_file}", "summary")

    def _update_result_info(self):
        parts = []
        for key, title in RESULT_TABS.items():
            frame = self.result_frames_view.get(key)
            count = 0 if frame is None else len(frame)
            parts.append(f"{title}: {count}")
        log_file = self.latest_result_files.get("log", "")
        if log_file:
            parts.append(f"日志: {Path(log_file).name}")
        self.result_ui_vars["info"].set(" | ".join(parts) if parts else "尚未加载扫描结果。")

    def _sort_dataframe(self, frame, sort_by: str, ascending: bool):
        if frame is None or frame.empty or sort_by not in frame.columns:
            return frame.copy() if frame is not None else pd.DataFrame()

        result = frame.copy()
        numeric = _ensure_runtime_modules()[0].to_numeric(result[sort_by], errors="coerce")
        if numeric.notna().any():
            result = result.assign(__sort_key=numeric)
        else:
            result = result.assign(__sort_key=result[sort_by].astype(str))
        result = result.sort_values(by="__sort_key", ascending=ascending, kind="mergesort")
        return result.drop(columns=["__sort_key"])

    def _get_filtered_result_frame(self, tab_key: str):
        frame = self.result_frames_raw.get(tab_key)
        _pd, _ = _ensure_runtime_modules()
        if frame is None or frame.empty:
            return _pd.DataFrame()

        result = frame.copy()
        keyword = self.result_ui_vars["keyword"].get().strip().lower()
        if keyword:
            mask = _pd.Series(False, index=result.index)
            for column in ["股票代码", "股票名称", "分层标签", "硬过滤未通过原因"]:
                if column in result.columns:
                    mask = mask | result[column].astype(str).str.lower().str.contains(keyword, na=False)
            result = result[mask]

        label_filter = self.result_ui_vars["label_filter"].get()
        if label_filter and label_filter != "全部" and "分层标签" in result.columns:
            result = result[result["分层标签"].astype(str) == label_filter]

        hard_filter = self.result_ui_vars["hard_filter"].get()
        if hard_filter != "全部" and "硬过滤是否通过" in result.columns:
            passed = result["硬过滤是否通过"].astype(str)
            if hard_filter == "仅硬过滤通过":
                result = result[passed.isin(["True", "true", "1", "是", "通过"])]
            elif hard_filter == "仅硬过滤未通过":
                result = result[~passed.isin(["True", "true", "1", "是", "通过"])]

        sort_by = self.result_ui_vars["sort_by"].get().strip()
        ascending = self.result_ui_vars["sort_order"].get() == "升序"
        result = self._sort_dataframe(result, sort_by, ascending)
        return result.reset_index(drop=True)

    def apply_result_filters(self):
        _pd, _ = _ensure_runtime_modules()
        for tab_key in RESULT_TABS:
            raw = self.result_frames_raw.get(tab_key)
            if raw is None:
                self.result_frames_view[tab_key] = _pd.DataFrame()
            else:
                self.result_frames_view[tab_key] = self._get_filtered_result_frame(tab_key)
            self._render_result_table(tab_key)
        self._update_result_info()

    def reset_result_filters(self):
        self.result_ui_vars["keyword"].set("")
        self.result_ui_vars["label_filter"].set("全部")
        self.result_ui_vars["hard_filter"].set("全部")
        self.result_ui_vars["sort_by"].set(self.result_sort_column)
        self.result_ui_vars["sort_order"].set("降序")
        self.apply_result_filters()

    def _render_result_table(self, tab_key: str):
        tree = self.result_treeviews[tab_key]
        self._clear_tree(tree)
        self.result_detail_records = {
            key: value for key, value in self.result_detail_records.items() if key[0] != tab_key
        }

        frame = self.result_frames_view.get(tab_key)
        if frame is None or frame.empty:
            if self.result_notebook.index(self.result_notebook.select()) == list(RESULT_TABS).index(tab_key):
                self._set_result_detail_text("当前标签页没有可显示的数据。")
            return

        columns = [column for column in RESULT_DISPLAY_COLUMNS if column in frame.columns]
        if not columns:
            columns = list(frame.columns)
        self._setup_tree_columns(tab_key, tree, columns)

        for row_index, record in frame.iterrows():
            values = [self._coerce_display_value(record.get(column, "")) for column in columns]
            item_id = tree.insert("", "end", iid=f"{tab_key}:{row_index}", values=values)
            self.result_detail_records[(tab_key, item_id)] = record.to_dict()

    def _sort_by_header(self, tab_key: str, column: str, default_ascending: bool):
        current_column = self.result_ui_vars["sort_by"].get()
        current_order = self.result_ui_vars["sort_order"].get()
        if current_column == column:
            self.result_ui_vars["sort_order"].set("降序" if current_order == "升序" else "升序")
        else:
            self.result_ui_vars["sort_by"].set(column)
            self.result_ui_vars["sort_order"].set("升序" if default_ascending else "降序")
        self.apply_result_filters()

    def _update_result_detail_from_selection(self, tab_key: str):
        tree = self.result_treeviews.get(tab_key)
        if tree is None:
            return
        selection = tree.selection()
        if not selection:
            self._set_result_detail_text("未选择任何扫描结果。")
            return
        record = self.result_detail_records.get((tab_key, selection[0]), {})
        detail_tree = self.result_detail_tree
        for item in detail_tree.get_children():
            detail_tree.delete(item)
        for field, value in record.items():
            detail_tree.insert("", "end", values=(field, self._coerce_display_value(value)))

    def _show_selected_record_popup(self, tab_key: str):
        tree = self.result_treeviews.get(tab_key)
        if tree is None or not tree.selection():
            return
        record = self.result_detail_records.get((tab_key, tree.selection()[0]), {})
        if not record:
            return
        lines = [f"{field}: {self._coerce_display_value(value)}" for field, value in record.items()]
        messagebox.showinfo("扫描结果详情", "\n".join(lines))

    def _format_seconds(self, seconds: float | None):
        if seconds is None or seconds < 0:
            return "--"
        total = int(seconds)
        hours = total // 3600
        minutes = (total % 3600) // 60
        secs = total % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def _reset_task_state(self, action_label: str, total_steps: int, step_order: list[str] | None):
        self.current_action_label = action_label
        self.total_steps_expected = max(1, int(total_steps or 1))
        self.total_steps_completed = 0
        self.current_step_name = ""
        self.current_step_started_at = None
        self.task_started_at = time.time()
        self.last_progress_match = None
        self.current_step_order = step_order or []
        self.task_vars["task_name"].set(action_label)
        self.task_vars["main_step"].set("-")
        self.task_vars["sub_step"].set("-")
        self.task_vars["progress_text"].set("等待脚本输出...")
        self.task_vars["elapsed"].set("00:00:00")
        self.task_vars["eta"].set("--")
        self.task_vars["total_progress"].set(f"0 / {self.total_steps_expected}")
        self.task_vars["step_progress"].set("等待开始")
        self.total_progressbar.configure(maximum=self.total_steps_expected, value=0)
        self.step_progressbar.configure(mode="indeterminate", maximum=100, value=0)
        try:
            self.step_progressbar.start(12)
        except Exception:
            pass

    def _step_index(self, step_name: str):
        if step_name in self.current_step_order:
            return self.current_step_order.index(step_name)
        return self.total_steps_completed

    def _overall_progress_fraction(self):
        if self.total_steps_expected <= 0:
            return 0.0
        fraction = float(self.total_steps_completed)
        if self.last_progress_match and self.last_progress_match[1] > 0:
            fraction += self.last_progress_match[0] / self.last_progress_match[1]
        return min(1.0, max(0.0, fraction / self.total_steps_expected))

    def _extract_progress(self, message: str):
        for pattern in PROGRESS_PATTERNS:
            match = pattern.search(message)
            if match:
                done = int(match.group("done"))
                total = int(match.group("total"))
                if total > 0:
                    return done, total
        return None

    def _estimate_eta(self):
        if not self.task_started_at:
            return "--"
        fraction = self._overall_progress_fraction()
        if fraction <= 0:
            return "--"
        elapsed = time.time() - self.task_started_at
        remaining = elapsed * (1 - fraction) / fraction
        return self._format_seconds(remaining)

    def _handle_step_start(self, step_name: str, payload=None):
        label = STEP_LABELS.get(step_name, step_name)
        self.current_step_name = step_name
        self.current_step_started_at = time.time()
        self.last_progress_match = None
        self.task_vars["main_step"].set(self.current_action_label)
        self.task_vars["sub_step"].set(label)
        self.task_vars["progress_text"].set("步骤已启动，等待日志...")
        self.task_vars["step_progress"].set(f"{label}：运行中")
        self.task_vars["total_progress"].set(f"{self.total_steps_completed} / {self.total_steps_expected}")
        self.total_progressbar.configure(value=self.total_steps_completed)
        self.step_progressbar.stop()
        self.step_progressbar.configure(mode="indeterminate", maximum=100, value=0)
        self.step_progressbar.start(12)

    def _handle_step_done(self, step_name: str, payload=None):
        self.total_steps_completed = min(self.total_steps_expected, self.total_steps_completed + 1)
        label = STEP_LABELS.get(step_name, step_name)
        message = ""
        if isinstance(payload, dict):
            message = str(payload.get("message", ""))
        self.task_vars["sub_step"].set(label)
        self.task_vars["progress_text"].set(message or f"{label} 已完成")
        self.task_vars["step_progress"].set(f"{label}：已完成")
        self.task_vars["total_progress"].set(f"{self.total_steps_completed} / {self.total_steps_expected}")
        self.total_progressbar.configure(value=self.total_steps_completed)
        self.last_progress_match = None
        self.step_progressbar.stop()
        self.step_progressbar.configure(mode="determinate", maximum=100, value=100)

    def _finish_task_state(self):
        self.current_step_name = ""
        self.current_step_started_at = None
        self.last_progress_match = None
        self.task_vars["task_name"].set("空闲")
        self.task_vars["main_step"].set("-")
        self.task_vars["sub_step"].set("-")
        self.task_vars["step_progress"].set("等待开始")
        self.total_progressbar.configure(value=self.total_steps_expected)
        self.step_progressbar.stop()
        self.step_progressbar.configure(mode="determinate", maximum=100, value=0)
        self.task_vars["eta"].set("--")

    def _handle_live_log(self, step_name: str, message: str):
        label = STEP_LABELS.get(step_name, step_name)
        self.task_vars["sub_step"].set(label)
        self.task_vars["progress_text"].set(message)
        progress = self._extract_progress(message)
        if progress is None:
            self.task_vars["step_progress"].set(f"{label}：运行中")
            return

        done, total = progress
        self.last_progress_match = (done, total)
        fraction = max(0.0, min(1.0, done / total))
        self.step_progressbar.stop()
        self.step_progressbar.configure(mode="determinate", maximum=total, value=done)
        self.task_vars["step_progress"].set(f"{label}：{done} / {total} ({fraction * 100:.1f}%)")
        self.task_vars["eta"].set(self._estimate_eta())
        overall = self._overall_progress_fraction()
        self.total_progressbar.configure(value=overall * self.total_steps_expected)

    def _poll_log_queue(self):
        while True:
            try:
                item_type, step_name, payload = self.log_queue.get_nowait()
            except queue.Empty:
                break

            if item_type == "log":
                prefix = STEP_LABELS.get(step_name, step_name)
                self.append_log(f"[{prefix}] {payload}", "raw")
                self._handle_live_log(step_name, str(payload))
            elif item_type == "event":
                event_type, event_payload = payload
                if event_type == "step_start":
                    self._handle_step_start(step_name, event_payload)
                elif event_type == "step_done":
                    self._handle_step_done(step_name, event_payload)
            elif item_type == "result":
                self._apply_result(payload)
                self._finish_task_state()
                self.set_busy(False)
                self.current_worker = None
            elif item_type == "error":
                self.append_log(str(payload), "error")
                self.status_vars["message"].set("任务执行失败，请查看运行日志。")
                self._set_status_message(self.status_vars["message"].get())
                self._finish_task_state()
                self.set_busy(False)
                self.current_worker = None

        self.root.after(100, self._poll_log_queue)

    def _tick_clock(self):
        now = time.time()
        if self.task_started_at:
            self.task_vars["elapsed"].set(self._format_seconds(now - self.task_started_at))
            if self.current_worker is not None:
                self.task_vars["eta"].set(self._estimate_eta())
        self.root.after(500, self._tick_clock)

    def _result_contains_scan_outputs(self, result: dict):
        output_paths = result.get("output_paths", []) or []
        return any("p7_scan_from_parquet_all_" in Path(path).name for path in output_paths)

    def _apply_result(self, result: dict):
        success = bool(result.get("success", False))
        self.status_vars["success"].set("成功" if success else "失败")
        self.status_vars["missing_count"].set(str(result.get("missing_count", 0) or 0))
        self.status_vars["created_count"].set(str(result.get("created_count", 0) or 0))
        self.status_vars["error_count"].set(str(result.get("error_count", 0) or 0))
        self.status_vars["skipped_count"].set(str(result.get("skipped_count", 0) or 0))
        self.status_vars["output_paths_count"].set(str(len(result.get("output_paths", []) or [])))
        self.status_vars["message"].set(str(result.get("message", "")))
        self._set_status_message(self.status_vars["message"].get())

        summary_tag = "summary" if success else "error"
        self.append_log(f"[result] {result.get('message', '')}", summary_tag)
        for path in result.get("output_paths", []) or []:
            self.append_log(f"[output] {path}", "summary")

        if self._result_contains_scan_outputs(result):
            self.refresh_latest_scan_results(silent=True)

    def _run_in_background(self, action_label: str, target, *args, total_steps: int = 1, step_order=None):
        if self.current_worker is not None and self.current_worker.is_alive():
            messagebox.showwarning("任务仍在运行", "请等待当前后台任务结束后再启动新的任务。")
            return

        self._reset_task_state(action_label, total_steps, step_order)
        self.set_busy(True)

        def log_callback(step_name, line):
            self.log_queue.put(("log", step_name, line))

        def event_callback(event_type, step_name, payload):
            self.log_queue.put(("event", step_name, (event_type, payload)))

        def worker():
            try:
                result = target(*args, log_callback=log_callback, event_callback=event_callback)
                self.log_queue.put(("result", result.get("step_name", action_label), result))
            except Exception:
                self.log_queue.put(("error", action_label, traceback.format_exc()))

        self.current_worker = threading.Thread(target=worker, daemon=True)
        self.current_worker.start()

    def load_config_to_form(self):

        try:
            config = self._read_scan_config()
            hard_filters = config.get("hard_filters", {})
            self.config_vars["volatility_window"].set(str(hard_filters.get("volatility_window", "")))
            self.config_vars["volatility_max"].set(str(hard_filters.get("volatility_max", "")))
            self.config_vars["require_bullish"].set(bool(hard_filters.get("require_bullish", True)))
            self.config_vars["volume_multiplier"].set(str(hard_filters.get("volume_multiplier", "")))
            self.config_vars["turnover_min"].set(str(hard_filters.get("turnover_min", "")))
            self.config_vars["min_history_bars"].set(str(hard_filters.get("min_history_bars", "")))
            self._refresh_param_summary()
            self.append_log("[config] 已读取 scan_config.json", "ui")
        except Exception as exc:
            messagebox.showerror("配置读取失败", str(exc))

    def save_config_from_form(self):
        try:
            config = self._read_scan_config()
            hard_filters = config.setdefault("hard_filters", {})
            hard_filters["volatility_window"] = int(self.config_vars["volatility_window"].get().strip())
            hard_filters["volatility_max"] = float(self.config_vars["volatility_max"].get().strip())
            hard_filters["require_bullish"] = bool(self.config_vars["require_bullish"].get())
            hard_filters["volume_multiplier"] = float(self.config_vars["volume_multiplier"].get().strip())
            hard_filters["turnover_min"] = float(self.config_vars["turnover_min"].get().strip())
            hard_filters["min_history_bars"] = int(self.config_vars["min_history_bars"].get().strip())

            SCAN_CONFIG_FILE.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
            self._refresh_param_summary()
            self.append_log("[config] 已保存 scan_config.json", "ui")
            messagebox.showinfo("保存成功", "scan_config.json 已保存。")
        except Exception as exc:
            messagebox.showerror("配置保存失败", str(exc))

    def _read_scan_config(self) -> dict:
        if not SCAN_CONFIG_FILE.exists():
            raise FileNotFoundError(f"未找到配置文件: {SCAN_CONFIG_FILE}")
        return json.loads(SCAN_CONFIG_FILE.read_text(encoding="utf-8"))

    def run_daily_pipeline(self):
        _ensure_runtime_modules()
        precheck = gui_runner.find_missing_stocks()
        if not precheck.get("success", False):
            proceed = messagebox.askyesno(
                "缺失预检查失败",
                "未能完成缺失股票预检查。\n\n通常是因为还没有最新的股票池文件。\n是否仍继续执行完整一键流程？",
            )
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

        self._run_in_background(
            "一键日更扫描",
            gui_runner.run_daily_pipeline,
            skip_bootstrap,
            total_steps=len(gui_runner.PIPELINE_STEPS),
            step_order=list(gui_runner.PIPELINE_STEPS),
        )

    def run_sync_universe(self):
        _ensure_runtime_modules()
        self._run_in_background("同步股票池", gui_runner.sync_universe, total_steps=1, step_order=["sync_universe"])

    def run_update_daily_hist(self):
        _ensure_runtime_modules()
        self._run_in_background("仅更新历史库", gui_runner.update_daily_hist, total_steps=1, step_order=["update_daily_hist"])

    def run_pack_to_parquet(self):
        _ensure_runtime_modules()
        self._run_in_background("仅打包 Parquet", gui_runner.pack_to_parquet, total_steps=1, step_order=["pack_to_parquet"])

    def run_scan_from_parquet(self):
        _ensure_runtime_modules()
        self._run_in_background("仅扫描", gui_runner.scan_from_parquet, total_steps=1, step_order=["scan_from_parquet"])

    def run_bootstrap_missing(self):
        _ensure_runtime_modules()
        def task(log_callback=None, event_callback=None):
            missing_result = gui_runner.find_missing_stocks(log_callback=log_callback, event_callback=event_callback)
            if not missing_result.get("success", False):
                return missing_result

            missing_codes = missing_result.get("missing_codes", []) or []
            if not missing_codes:
                result = gui_runner.bootstrap_missing_stocks(
                    [],
                    log_callback=log_callback,
                    event_callback=event_callback,
                )
                result["output_paths"] = missing_result.get("output_paths", [])
                result["missing_count"] = 0
                return result

            result = gui_runner.bootstrap_missing_stocks(
                missing_codes,
                log_callback=log_callback,
                event_callback=event_callback,
            )
            result["missing_count"] = missing_result.get("missing_count", len(missing_codes))
            return result

        self._run_in_background("补建缺失股票", task, total_steps=2, step_order=["find_missing_stocks", "bootstrap_missing_stocks"])

    def open_output_dir(self):
        _ensure_runtime_modules()
        self._open_dir(gui_runner.OUTPUT_DIR)

    def open_logs_dir(self):
        _ensure_runtime_modules()
        self._open_dir(gui_runner.LOGS_DIR)

    def _open_dir(self, path: Path):
        try:
            path.mkdir(parents=True, exist_ok=True)
            os.startfile(str(path))
        except Exception as exc:
            messagebox.showerror("打开目录失败", str(exc))



def main():
    root = tk.Tk()
    app = GuiApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
