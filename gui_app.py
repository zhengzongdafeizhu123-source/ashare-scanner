from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import threading
import traceback
import tkinter as tk
from tkinter import messagebox, ttk

import gui_runner


PROJECT_DIR = Path(__file__).resolve().parent
SCAN_CONFIG_FILE = PROJECT_DIR / "scan_config.json"


class GuiApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("AShare Scanner Control Panel")
        self.root.geometry("1200x760")
        self.root.minsize(980, 680)

        self.log_queue: queue.Queue[tuple[str, str, object]] = queue.Queue()
        self.current_worker: threading.Thread | None = None
        self.action_buttons: list[ttk.Button] = []

        self.status_vars = {
            "success": tk.StringVar(value="-"),
            "missing_count": tk.StringVar(value="0"),
            "created_count": tk.StringVar(value="0"),
            "error_count": tk.StringVar(value="0"),
            "skipped_count": tk.StringVar(value="0"),
            "output_paths_count": tk.StringVar(value="0"),
            "message": tk.StringVar(value="Ready."),
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
        self.load_config_to_form()
        self.root.after(100, self._poll_log_queue)

    def _build_ui(self):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

        top_frame = ttk.Frame(self.root, padding=12)
        top_frame.grid(row=0, column=0, sticky="nsew")
        top_frame.columnconfigure(0, weight=3)
        top_frame.columnconfigure(1, weight=2)

        self._build_actions_panel(top_frame)
        self._build_config_panel(top_frame)

        bottom_frame = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        bottom_frame.grid(row=1, column=0, sticky="nsew")
        bottom_frame.columnconfigure(0, weight=1)
        bottom_frame.rowconfigure(1, weight=1)

        self._build_status_panel(bottom_frame)
        self._build_log_panel(bottom_frame)

    def _build_actions_panel(self, parent: ttk.Frame):
        frame = ttk.LabelFrame(parent, text="Actions", padding=12)
        frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
        frame.columnconfigure(0, weight=1)

        button_specs = [
            ("一键日更扫描", self.run_daily_pipeline),
            ("同步股票池", self.run_sync_universe),
            ("补建缺失股票", self.run_bootstrap_missing),
            ("仅更新历史库", self.run_update_daily_hist),
            ("仅打包 Parquet", self.run_pack_to_parquet),
            ("仅扫描", self.run_scan_from_parquet),
            ("打开 output 目录", self.open_output_dir),
            ("打开 logs 目录", self.open_logs_dir),
        ]

        for idx, (label, command) in enumerate(button_specs):
            button = ttk.Button(frame, text=label, command=command)
            button.grid(row=idx, column=0, sticky="ew", pady=4)
            self.action_buttons.append(button)

    def _build_config_panel(self, parent: ttk.Frame):
        frame = ttk.LabelFrame(parent, text="扫描参数", padding=12)
        frame.grid(row=0, column=1, sticky="nsew")
        frame.columnconfigure(1, weight=1)

        fields = [
            ("volatility_window", "volatility_window", "entry"),
            ("volatility_max", "volatility_max", "entry"),
            ("require_bullish", "require_bullish", "check"),
            ("volume_multiplier", "volume_multiplier", "entry"),
            ("turnover_min", "turnover_min", "entry"),
            ("min_history_bars", "min_history_bars", "entry"),
        ]

        for idx, (key, label, field_type) in enumerate(fields):
            ttk.Label(frame, text=label).grid(row=idx, column=0, sticky="w", pady=4, padx=(0, 8))
            if field_type == "check":
                widget = ttk.Checkbutton(frame, variable=self.config_vars[key])
            else:
                widget = ttk.Entry(frame, textvariable=self.config_vars[key])
            widget.grid(row=idx, column=1, sticky="ew", pady=4)

        button_row = ttk.Frame(frame)
        button_row.grid(row=len(fields), column=0, columnspan=2, sticky="ew", pady=(10, 0))
        button_row.columnconfigure((0, 1, 2), weight=1)

        ttk.Button(button_row, text="读取配置", command=self.load_config_to_form).grid(row=0, column=0, sticky="ew", padx=(0, 4))
        ttk.Button(button_row, text="保存配置", command=self.save_config_from_form).grid(row=0, column=1, sticky="ew", padx=4)
        ttk.Button(button_row, text="刷新配置", command=self.load_config_to_form).grid(row=0, column=2, sticky="ew", padx=(4, 0))

    def _build_status_panel(self, parent: ttk.Frame):
        frame = ttk.LabelFrame(parent, text="最近一次运行结果", padding=12)
        frame.grid(row=0, column=0, sticky="ew")
        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(3, weight=1)

        rows = [
            ("success", "Success"),
            ("missing_count", "Missing"),
            ("created_count", "Created"),
            ("error_count", "Errors"),
            ("skipped_count", "Skipped"),
            ("output_paths_count", "Output Paths"),
            ("message", "Message"),
        ]

        for idx, (key, label) in enumerate(rows):
            row = idx // 2
            col = (idx % 2) * 2
            if key == "message":
                ttk.Label(frame, text=label).grid(row=3, column=0, sticky="nw", pady=(8, 0))
                ttk.Label(frame, textvariable=self.status_vars[key], wraplength=820, justify="left").grid(
                    row=3, column=1, columnspan=3, sticky="ew", pady=(8, 0)
                )
            else:
                ttk.Label(frame, text=label).grid(row=row, column=col, sticky="w", pady=2, padx=(0, 8))
                ttk.Label(frame, textvariable=self.status_vars[key]).grid(row=row, column=col + 1, sticky="w", pady=2)

    def _build_log_panel(self, parent: ttk.Frame):
        frame = ttk.LabelFrame(parent, text="运行日志", padding=12)
        frame.grid(row=1, column=0, sticky="nsew", pady=(12, 0))
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(frame, wrap="word", height=20, state="disabled")
        self.log_text.grid(row=0, column=0, sticky="nsew")

        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scrollbar.set)

    def append_log(self, message: str):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", message + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def set_busy(self, busy: bool):
        state = "disabled" if busy else "normal"
        for button in self.action_buttons:
            button.configure(state=state)

    def _poll_log_queue(self):
        try:
            while True:
                event_type, step_name, payload = self.log_queue.get_nowait()
                if event_type == "log":
                    prefix = f"[{step_name}] " if step_name else ""
                    self.append_log(prefix + str(payload))
                elif event_type == "result":
                    self._apply_result(payload)
                elif event_type == "error":
                    self.append_log(f"[{step_name}] {payload}")
                    self._apply_result(
                        {
                            "success": False,
                            "step_name": step_name,
                            "message": str(payload),
                            "output_paths": [],
                        }
                    )
                elif event_type == "done":
                    self.set_busy(False)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self._poll_log_queue)

    def _apply_result(self, result: dict):
        self.status_vars["success"].set(str(result.get("success", "-")))
        self.status_vars["missing_count"].set(str(result.get("missing_count", 0)))
        self.status_vars["created_count"].set(str(result.get("created_count", 0)))
        self.status_vars["error_count"].set(str(result.get("error_count", 0)))
        self.status_vars["skipped_count"].set(str(result.get("skipped_count", 0)))
        self.status_vars["output_paths_count"].set(str(len(result.get("output_paths", []) or [])))
        self.status_vars["message"].set(str(result.get("message", "")))

        step_name = result.get("step_name", "result")
        summary = (
            f"success={result.get('success', '-')}, "
            f"missing={result.get('missing_count', 0)}, "
            f"created={result.get('created_count', 0)}, "
            f"errors={result.get('error_count', 0)}, "
            f"skipped={result.get('skipped_count', 0)}, "
            f"outputs={len(result.get('output_paths', []) or [])}, "
            f"message={result.get('message', '')}"
        )
        self.append_log(f"[{step_name}] Result: {summary}")

    def _run_in_background(self, label: str, func, *args):
        if self.current_worker is not None and self.current_worker.is_alive():
            messagebox.showwarning("Busy", "当前已有任务在运行，请先等待完成。")
            return

        self.set_busy(True)
        self.append_log(f"[ui] Start: {label}")

        def worker():
            try:
                def log_callback(step_name, line):
                    self.log_queue.put(("log", step_name, line))

                result = func(*args, log_callback=log_callback)
                self.log_queue.put(("result", result.get("step_name", label), result))
            except Exception:
                self.log_queue.put(("error", label, traceback.format_exc()))
            finally:
                self.log_queue.put(("done", label, None))

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
            self.append_log("[config] 已读取 scan_config.json")
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
            self.append_log("[config] 已保存 scan_config.json")
            messagebox.showinfo("保存成功", "scan_config.json 已保存。")
        except Exception as exc:
            messagebox.showerror("配置保存失败", str(exc))

    def _read_scan_config(self) -> dict:
        if not SCAN_CONFIG_FILE.exists():
            raise FileNotFoundError(f"未找到配置文件: {SCAN_CONFIG_FILE}")
        return json.loads(SCAN_CONFIG_FILE.read_text(encoding="utf-8"))

    def run_daily_pipeline(self):
        self._run_in_background("run_daily_pipeline", gui_runner.run_daily_pipeline)

    def run_sync_universe(self):
        self._run_in_background("sync_universe", gui_runner.sync_universe)

    def run_update_daily_hist(self):
        self._run_in_background("update_daily_hist", gui_runner.update_daily_hist)

    def run_pack_to_parquet(self):
        self._run_in_background("pack_to_parquet", gui_runner.pack_to_parquet)

    def run_scan_from_parquet(self):
        self._run_in_background("scan_from_parquet", gui_runner.scan_from_parquet)

    def run_bootstrap_missing(self):
        def task(log_callback=None):
            missing_result = gui_runner.find_missing_stocks()
            if log_callback is not None:
                log_callback("find_missing_stocks", missing_result.get("message", ""))

            if not missing_result.get("success", False):
                return missing_result

            missing_codes = missing_result.get("missing_codes", []) or []
            if not missing_codes:
                return {
                    "success": True,
                    "step_name": "bootstrap_missing_stocks",
                    "message": "No missing stocks to bootstrap.",
                    "output_paths": missing_result.get("output_paths", []),
                    "missing_count": 0,
                    "created_count": 0,
                    "error_count": 0,
                    "skipped_count": 0,
                }

            result = gui_runner.bootstrap_missing_stocks(missing_codes, log_callback=log_callback)
            result["missing_count"] = missing_result.get("missing_count", len(missing_codes))
            return result

        self._run_in_background("bootstrap_missing_stocks", task)

    def open_output_dir(self):
        self._open_dir(gui_runner.OUTPUT_DIR)

    def open_logs_dir(self):
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
