from __future__ import annotations

import concurrent.futures
import itertools
import json
import os
import platform
import queue
import re
import subprocess
import threading
import time
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from tkinter import font as tkfont
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw, ImageTk

from .core import (
    JSONScene,
    LSFOption,
    LSFScene,
    PNGResolver,
    ProjectError,
    analyze_json_scene,
    analyze_lsf_scene,
    collect_input_files,
    compose_json_scene,
    compose_lsf_scene,
    parse_json_project,
    parse_lsf_file,
)

TITLE = "Haison Shoujo Series CG & Gareki Viewer & Extractor Ver1.4（ 由ユイ可愛ね制作 GPT编写 本软件免费使用，开源于Github，严禁倒卖牟利！）"
SUPPORTED_SERIES_TEXT = """目前支持：
1.廃村少女 ～妖し惑ひの籠の郷～ (原版）
2.廃村少女 外伝 ～嬌絡夢現～
3.廃村少女 番外 ～籠愛拾遺～
4.廃村少女［弐］ ～陰り誘う秘姫の匣～

基本支持：
1.悠刻のファムファタル
2.戦巫〈センナギ〉―穢れた契りと神ころも―
3.姫と艶欲のインペリウム
4.姫と婬欲のテスタメント
5.姫と穢欲のサクリファイス"""

APP_ICON_PNG = Path(__file__).resolve().parents[1] / "assets" / "app_icon.png"
APP_ICON_ICO = Path(__file__).resolve().parents[1] / "assets" / "app_icon.ico"
_APP_ICON_PHOTO: ImageTk.PhotoImage | None = None


def apply_window_icon(win: tk.Misc) -> None:
    """给主窗口和所有 Toplevel 二级窗口设置同一个程序图标。"""
    global _APP_ICON_PHOTO
    try:
        if APP_ICON_ICO.exists() and hasattr(win, "iconbitmap"):
            win.iconbitmap(str(APP_ICON_ICO))
    except Exception:
        # 某些非 Windows 环境或窗口管理器不支持 ico，继续尝试 PNG。
        pass
    try:
        if _APP_ICON_PHOTO is None and APP_ICON_PNG.exists():
            _APP_ICON_PHOTO = ImageTk.PhotoImage(file=str(APP_ICON_PNG))
        if _APP_ICON_PHOTO is not None and hasattr(win, "iconphoto"):
            win.iconphoto(True, _APP_ICON_PHOTO)
    except Exception:
        pass


def make_checkerboard(width: int, height: int, cell: int = 16) -> Image.Image:
    img = Image.new("RGBA", (width, height), (230, 230, 230, 255))
    draw = ImageDraw.Draw(img)
    c1 = (232, 232, 232, 255)
    c2 = (205, 205, 205, 255)
    for y in range(0, height, cell):
        for x in range(0, width, cell):
            draw.rectangle([x, y, x + cell - 1, y + cell - 1], fill=c1 if ((x // cell) + (y // cell)) % 2 == 0 else c2)
    return img


def count_dir_files(dir_text: str, patterns: tuple[str, ...]) -> int:
    text = (dir_text or "").strip()
    if not text:
        return 0
    p = Path(text).expanduser()
    if not p.is_dir():
        return 0
    total = 0
    for pattern in patterns:
        total += len(list(p.glob(pattern)))
    return total


def safe_filename_part(text: str, max_len: int = 48) -> str:
    text = (text or "").strip()
    text = re.sub(r"[\\/:*?\"<>|\r\n\t]+", "_", text)
    text = re.sub(r"\s+", " ", text).strip(" ._")
    if not text:
        text = "未命名"
    if len(text) > max_len:
        text = text[:max_len].rstrip(" ._")
    return text or "未命名"


def make_unique_png_path(folder: Path, stem: str) -> Path:
    base = safe_filename_part(stem, 160)
    path = folder / f"{base}.png"
    if not path.exists():
        return path
    for i in range(2, 10000):
        alt = folder / f"{base}_{i:03d}.png"
        if not alt.exists():
            return alt
    return folder / f"{base}_extra.png"


def detect_cpu_counts() -> tuple[int, int]:
    """尽量识别用户电脑的物理核心数和逻辑线程数。"""
    logical_threads = max(1, int(os.cpu_count() or 4))
    physical_cores: int | None = None

    # 如果用户环境装了 psutil，就优先用 psutil；没有也不强制依赖。
    try:
        import psutil  # type: ignore
        psutil_logical = psutil.cpu_count(logical=True)
        psutil_physical = psutil.cpu_count(logical=False)
        if psutil_logical:
            logical_threads = max(1, int(psutil_logical))
        if psutil_physical:
            physical_cores = max(1, int(psutil_physical))
    except Exception:
        pass

    # Windows 打包版通常没有 psutil，尝试用系统自带 CIM 读取核心/线程数。
    if physical_cores is None and platform.system().lower() == "windows":
        try:
            startupinfo = None
            creationflags = 0
            if hasattr(subprocess, "STARTUPINFO"):
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            cmd = [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy", "Bypass",
                "-Command",
                "(Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfCores -Sum).Sum; "
                "(Get-CimInstance Win32_Processor | Measure-Object -Property NumberOfLogicalProcessors -Sum).Sum",
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=2,
                startupinfo=startupinfo,
                creationflags=creationflags,
            )
            nums = []
            for line in (result.stdout or "").splitlines():
                line = line.strip()
                if line:
                    try:
                        nums.append(int(float(line)))
                    except Exception:
                        pass
            if len(nums) >= 1 and nums[0] > 0:
                physical_cores = nums[0]
            if len(nums) >= 2 and nums[1] > 0:
                logical_threads = nums[1]
        except Exception:
            pass

    if physical_cores is None:
        # 无法可靠识别物理核心时，用逻辑线程数兜底，避免显示 0。
        physical_cores = logical_threads
    return max(1, physical_cores), max(1, logical_threads)


CPU_PHYSICAL_CORES, CPU_LOGICAL_THREADS = detect_cpu_counts()
CPU_INFO_TEXT = f"检测到 CPU：物理核心 {CPU_PHYSICAL_CORES} 个 / 逻辑线程 {CPU_LOGICAL_THREADS} 个"
DEFAULT_THREAD_COUNT = str(CPU_LOGICAL_THREADS)


DEFAULT_AUTOMATION_SETTINGS: dict[str, object] = {
    "has_run": False,
    "interval_seconds": 0.8,
    "start_from_current": False,
    "auto_pick_next_dir": False,
    "apply_linkage": True,
}


def _settings_path() -> Path:
    """返回用户本机的配置文件路径；打包成 exe 后也可用。"""
    base = os.environ.get("APPDATA")
    if base:
        root = Path(base) / "HaisonShoujoViewerExtractor"
    else:
        root = Path.home() / ".HaisonShoujoViewerExtractor"
    return root / "settings.json"


def load_app_settings() -> dict:
    path = _settings_path()
    try:
        if path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return {}


def save_app_settings(settings: dict) -> None:
    path = _settings_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(settings, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        # 设置保存失败不影响主功能。
        pass


def load_automation_settings() -> dict[str, object]:
    settings = load_app_settings()
    saved = settings.get("automation_test", {}) if isinstance(settings, dict) else {}
    result = dict(DEFAULT_AUTOMATION_SETTINGS)
    if isinstance(saved, dict):
        result.update(saved)
    return result


def save_automation_settings(auto_settings: dict[str, object]) -> None:
    settings = load_app_settings()
    if not isinstance(settings, dict):
        settings = {}
    current = dict(DEFAULT_AUTOMATION_SETTINGS)
    current.update(auto_settings)
    settings["automation_test"] = current
    save_app_settings(settings)


DEFAULT_ONE_CLICK_CLEAR_SETTINGS: dict[str, object] = {
    # configured=False means: for the current LSF, all available combination items
    # except “人物或者场景” are cleared. Once the user saves the dialog, the chosen
    # keys are remembered here.
    "configured": False,
    "selected_keys": [],
}


def load_one_click_clear_settings() -> dict[str, object]:
    settings = load_app_settings()
    saved = settings.get("one_click_clear", {}) if isinstance(settings, dict) else {}
    result = dict(DEFAULT_ONE_CLICK_CLEAR_SETTINGS)
    if isinstance(saved, dict):
        result.update(saved)
    if not isinstance(result.get("selected_keys"), list):
        result["selected_keys"] = []
    return result


def save_one_click_clear_settings(clear_settings: dict[str, object]) -> None:
    settings = load_app_settings()
    if not isinstance(settings, dict):
        settings = {}
    current = dict(DEFAULT_ONE_CLICK_CLEAR_SETTINGS)
    current.update(clear_settings)
    if not isinstance(current.get("selected_keys"), list):
        current["selected_keys"] = []
    settings["one_click_clear"] = current
    save_app_settings(settings)


def build_thread_count_choices() -> tuple[str, ...]:
    candidates = {
        1,
        2,
        4,
        6,
        8,
        12,
        16,
        24,
        32,
        48,
        64,
        96,
        128,
        CPU_PHYSICAL_CORES,
        CPU_LOGICAL_THREADS,
    }
    values = sorted(x for x in candidates if 1 <= int(x) <= CPU_LOGICAL_THREADS)
    if CPU_LOGICAL_THREADS not in values:
        values.append(CPU_LOGICAL_THREADS)
    return tuple(str(x) for x in values)


THREAD_COUNT_CHOICES = build_thread_count_choices()


def normalize_thread_count(value) -> int:
    try:
        requested = int(value)
    except Exception:
        requested = CPU_LOGICAL_THREADS
    return max(1, min(requested, CPU_LOGICAL_THREADS))


def make_unique_png_path_reserved(folder: Path, stem: str, reserved_paths: set[str]) -> Path:
    """Like make_unique_png_path, but also avoids names already reserved by other worker threads."""
    base = safe_filename_part(stem, 160)
    candidates = [folder / f"{base}.png"]
    candidates.extend(folder / f"{base}_{i:03d}.png" for i in range(2, 10000))
    for path in candidates:
        key = str(path.resolve()).lower()
        if key not in reserved_paths and not path.exists():
            reserved_paths.add(key)
            return path
    path = folder / f"{base}_extra_{len(reserved_paths) + 1:06d}.png"
    reserved_paths.add(str(path.resolve()).lower())
    return path


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def batch_progress_text(completed: int, total: int, start_time: float) -> str:
    total = max(0, total)
    completed = max(0, min(completed, total)) if total else completed
    percent = 100.0 if total <= 0 else completed * 100.0 / total
    if total <= 0:
        eta = "00:00"
    elif completed <= 0:
        eta = "计算中"
    elif completed >= total:
        eta = "00:00"
    else:
        elapsed = max(0.0, time.time() - start_time)
        eta = format_duration((elapsed / completed) * (total - completed))
    return f"进度：{completed}/{total} ({percent:.1f}%)｜预计导出数量：{total}｜预计剩余时间：{eta}"


class PreviewCanvas(ttk.Frame):
    def __init__(self, master: tk.Misc, resolution_var: tk.StringVar):
        super().__init__(master)
        self.resolution_var = resolution_var
        self.canvas = tk.Canvas(self, bg="#d0d0d0", highlightthickness=0)
        self.canvas.pack(fill="both", expand=True)
        self._photo = None
        self._last_image = None
        self.canvas.bind("<Configure>", lambda e: self._refresh())

    def show_image(self, image: Image.Image | None) -> None:
        self._last_image = image
        self._refresh()

    def _refresh(self) -> None:
        self.canvas.delete("all")
        if self._last_image is None:
            self._photo = None
            self.resolution_var.set("当前预览分辨率：- x -")
            return
        cw = max(100, self.canvas.winfo_width())
        ch = max(100, self.canvas.winfo_height())
        img = self._last_image
        self.resolution_var.set(f"当前预览分辨率：{img.width} x {img.height}")
        scale = min(cw / img.width, ch / img.height, 1.0)
        disp = img
        if scale != 1.0:
            disp = img.resize((max(1, int(img.width * scale)), max(1, int(img.height * scale))), Image.LANCZOS)
        bg = make_checkerboard(disp.width, disp.height)
        bg.alpha_composite(disp, (0, 0))
        self._photo = ImageTk.PhotoImage(bg)
        self.canvas.create_image(cw // 2, ch // 2, image=self._photo, anchor="center")


class WrapButtonFrame(ttk.Frame):
    """紧凑按钮条：按钮按文字自然宽度显示，横向优先，空间不足时自动换行。"""

    def __init__(self, master: tk.Misc, buttons: list[tuple[str, object]]):
        super().__init__(master)
        self._buttons: list[ttk.Button] = []
        for text, command in buttons:
            btn = ttk.Button(self, text=text, command=command)
            self._buttons.append(btn)
        self.bind("<Configure>", lambda _e: self._reflow())
        self.after_idle(self._reflow)

    def _reflow(self) -> None:
        if not self._buttons:
            return
        available = max(1, self.winfo_width())
        if available <= 1:
            available = max(1, self.winfo_reqwidth())
        row = 0
        col = 0
        used = 0
        gap = 6
        for btn in self._buttons:
            btn.grid_forget()
            btn_width = btn.winfo_reqwidth()
            need = btn_width if col == 0 else btn_width + gap
            if col > 0 and used + need > available:
                row += 1
                col = 0
                used = 0
                need = btn_width
            btn.grid(row=row, column=col, padx=(0 if col == 0 else gap, 0), pady=(2, 4), sticky="w")
            used += need
            col += 1
        for i in range(col + 1):
            self.grid_columnconfigure(i, weight=0)


class BaseTab(ttk.Frame):
    def __init__(self, master: tk.Misc, mode_name: str):
        super().__init__(master)
        self.mode_name = mode_name
        self.resolution_var = tk.StringVar(value="当前预览分辨率：- x -")
        self._title_font = tkfont.nametofont("TkDefaultFont").copy()
        self._title_font.configure(size=11, weight="bold")
        self._res_font = tkfont.nametofont("TkDefaultFont").copy()
        self._res_font.configure(size=12, weight="bold")

        # 左侧操作区改成可滚动容器：组合项很多时，当前信息不会被挤出看不到。
        self.left_shell = ttk.Frame(self)
        self.left_shell.pack(side="left", fill="y", padx=8, pady=8)

        self.left_canvas = tk.Canvas(self.left_shell, highlightthickness=0, borderwidth=0)
        self.left_scrollbar = ttk.Scrollbar(self.left_shell, orient="vertical", command=self.left_canvas.yview)
        self.left_canvas.configure(yscrollcommand=self.left_scrollbar.set)
        self.left_canvas.pack(side="left", fill="y", expand=False)
        self.left_scrollbar.pack(side="right", fill="y")

        self.left = ttk.Frame(self.left_canvas)
        self._left_window = self.left_canvas.create_window((0, 0), window=self.left, anchor="nw")
        self._left_default_width = 410
        self.left_canvas.configure(width=self._left_default_width)
        self.left.bind("<Configure>", self._on_left_frame_configure)
        self.left_canvas.bind("<Configure>", self._on_left_canvas_configure)
        # 禁用左侧滚动区的鼠标滚轮：只能用鼠标拖动/点击右侧滚动条来滚动。
        # 原来这里会在鼠标进入左侧区域时 bind_all <MouseWheel>/<Button-4>/<Button-5>，
        # 导致滚轮也能移动滚动条；现在不再绑定滚轮事件。

        self.right = ttk.Frame(self)
        self.right.pack(side="left", fill="both", expand=True, padx=(0, 8), pady=8)

    def _on_left_frame_configure(self, _event=None) -> None:
        self.left_canvas.configure(scrollregion=self.left_canvas.bbox("all"))
        req_width = max(self._left_default_width, self.left.winfo_reqwidth())
        cur_width = int(float(self.left_canvas.cget("width")))
        if req_width != cur_width:
            self.left_canvas.configure(width=req_width)
        self.left_canvas.itemconfigure(self._left_window, width=req_width)

    def _on_left_canvas_configure(self, event=None) -> None:
        width = max(self._left_default_width, event.width if event else self.left_canvas.winfo_width())
        self.left_canvas.itemconfigure(self._left_window, width=width)
        self.left_canvas.configure(scrollregion=self.left_canvas.bbox("all"))

    def _scroll_left_canvas(self, event) -> None:
        if getattr(event, "num", None) == 4:
            self.left_canvas.yview_scroll(-1, "units")
        elif getattr(event, "num", None) == 5:
            self.left_canvas.yview_scroll(1, "units")
        else:
            delta = int(-1 * (getattr(event, "delta", 0) / 120))
            if delta:
                self.left_canvas.yview_scroll(delta, "units")

    def _bind_left_mousewheel(self) -> None:
        root = self.winfo_toplevel()
        root.bind_all("<MouseWheel>", self._scroll_left_canvas)
        root.bind_all("<Button-4>", self._scroll_left_canvas)
        root.bind_all("<Button-5>", self._scroll_left_canvas)

    def _unbind_left_mousewheel(self) -> None:
        root = self.winfo_toplevel()
        root.unbind_all("<MouseWheel>")
        root.unbind_all("<Button-4>")
        root.unbind_all("<Button-5>")

    def create_preview_area(self):
        header = ttk.Frame(self.right)
        header.pack(fill="x", anchor="nw")
        ttk.Label(header, text="预览", font=self._title_font).pack(anchor="w")
        ttk.Label(header, textvariable=self.resolution_var, font=self._res_font).pack(anchor="w", pady=(2, 8))
        self.preview = PreviewCanvas(self.right, self.resolution_var)
        self.preview.pack(fill="both", expand=True)

    def create_info_box(self):
        info_box = ttk.LabelFrame(self.left, text="当前信息")
        info_box.pack(fill="both", expand=True, pady=(8, 0))
        self.info_text = tk.Text(info_box, width=54, height=16)
        self.info_text.pack(fill="both", expand=True, padx=8, pady=8)
        self.info_text.configure(state="disabled")

    def _place_popup_like_left_panel(self, win: tk.Toplevel) -> None:
        # 所有二级弹窗与主程序窗口左上角对齐，并统一使用程序图标。
        root = self.winfo_toplevel()
        apply_window_icon(win)

        def apply_position() -> None:
            root.update_idletasks()
            win.update_idletasks()
            x = max(0, root.winfo_rootx())
            y = max(0, root.winfo_rooty())
            win.geometry(f"+{x}+{y}")
            win.lift(root)
            win.focus_force()

        apply_position()
        # Windows 下部分窗口管理器会在 transient/grab 后重新摆放一次，延迟再校准，避免飘到屏幕左上角。
        win.after(50, apply_position)

    def _set_info(self, lines: list[str]) -> None:
        self.info_text.configure(state="normal")
        self.info_text.delete("1.0", "end")
        self.info_text.insert("1.0", "\n".join(lines))
        self.info_text.configure(state="disabled")

    def _path_row(self, parent: tk.Misc, label: str, variable: tk.StringVar, command, on_change=None) -> None:
        row = ttk.Frame(parent)
        row.pack(fill="x", padx=8, pady=4)
        ttk.Label(row, text=label, width=14).pack(side="left")
        ttk.Entry(row, textvariable=variable).pack(side="left", fill="x", expand=True, padx=(4, 4))
        if on_change:
            variable.trace_add("write", lambda *_: on_change())
        ttk.Button(row, text="选择", command=command, width=8).pack(side="left")

    def _compact_button_bar(self, parent: tk.Misc, buttons: list[tuple[str, object]]) -> WrapButtonFrame:
        bar = WrapButtonFrame(parent, buttons)
        bar.pack(fill="x", padx=8, pady=(2, 8), anchor="w")
        return bar

    def _detach_loaded_resources(self) -> dict[str, object | None]:
        """把当前目录资源从前台状态中摘下来，但先不在主线程释放。"""
        old_resources: dict[str, object | None] = {
            "resolver": getattr(self, "resolver", None),
            "scene": getattr(self, "scene", None),
            "current_image": getattr(self, "current_image", None),
        }
        if hasattr(self, "resolver"):
            self.resolver = None
        if hasattr(self, "scene"):
            self.scene = None
        if hasattr(self, "current_image"):
            self.current_image = None
        return old_resources

    def _schedule_background_resource_cleanup(self, resources: dict[str, object | None], delay_ms: int = 300) -> None:
        """等新目录完成加载后，再把旧目录缓存交给后台线程分批释放。"""
        if not resources or not any(value is not None for value in resources.values()):
            return

        def start_worker() -> None:
            def worker() -> None:
                old_resolver = resources.pop("resolver", None)
                old_scene = resources.pop("scene", None)
                old_image = resources.pop("current_image", None)
                try:
                    if old_resolver is not None:
                        clear_gradually = getattr(old_resolver, "clear_cache_gradually", None)
                        if callable(clear_gradually):
                            clear_gradually(batch_size=8, delay_seconds=0.02)
                        else:
                            clear_cache = getattr(old_resolver, "clear_cache", None)
                            if callable(clear_cache):
                                clear_cache()
                except Exception:
                    pass
                finally:
                    # 这些对象只在后台线程中最后释放，避免切换目录时主线程一次性析构大量图片。
                    old_resolver = None
                    old_scene = None
                    old_image = None
                    resources.clear()

            threading.Thread(target=worker, daemon=True, name="old-directory-cache-cleanup").start()

        try:
            self.after(max(0, int(delay_ms)), start_worker)
        except Exception:
            start_worker()



class LSFTab(BaseTab):
    def __init__(self, master: tk.Misc):
        super().__init__(master, "LSF")
        self.lsf_files: list[Path] = []
        self.scene: Optional[LSFScene] = None
        self.resolver: Optional[PNGResolver] = None
        self.current_image: Optional[Image.Image] = None

        self.lsf_input_var = tk.StringVar()
        self.png_var = tk.StringVar()
        self.scene_var = tk.StringVar()
        self.body_var = tk.StringVar()
        self.linkage_vars: dict[str, tk.BooleanVar] = {}
        self.linkage_summary_var = tk.StringVar(value="联动：未开启")
        self.automation_settings = load_automation_settings()
        self.clear_settings = load_one_click_clear_settings()
        self.expression_vars: list[tk.StringVar] = []
        self.blush_vars: list[tk.StringVar] = []
        self.special_vars: list[tk.StringVar] = []
        self.expression_combos: list[ttk.Combobox] = []
        self.blush_combos: list[ttk.Combobox] = []
        self.special_combos: list[ttk.Combobox] = []
        self.holy_var = tk.StringVar()
        self.stats_var = tk.StringVar(value="当前目录统计：LSF 0 个，PNG 0 个")
        self._build_ui()

    def _build_ui(self) -> None:
        input_box = ttk.LabelFrame(self.left, text="输入")
        input_box.pack(fill="x", pady=(0, 8))
        self._path_row(input_box, "LSF 目录", self.lsf_input_var, self._pick_lsf_dir, self._on_dir_changed)
        self._path_row(input_box, "PNG 目录", self.png_var, self._pick_png, self._on_dir_changed)
        self._compact_button_bar(input_box, [
            ("加载 LSF 项目", self.load_project),
            ("导出当前 PNG", self.export_current),
            ("批量导出当前组合", self.open_batch_export_dialog),
            ("支持系列", self.open_supported_series_dialog),
        ])

        stats_box = ttk.LabelFrame(self.left, text="目录统计")
        stats_box.pack(fill="x", pady=(0, 8))
        ttk.Label(stats_box, textvariable=self.stats_var, justify="left", anchor="w").pack(fill="x", padx=8, pady=8)

        linkage_box = ttk.LabelFrame(self.left, text="联动")
        linkage_box.pack(fill="x", pady=(0, 8))
        ttk.Button(linkage_box, text="联动设置", command=self.open_linkage_dialog).pack(fill="x", padx=8, pady=(8, 2))
        ttk.Label(linkage_box, textvariable=self.linkage_summary_var, justify="left", anchor="w").pack(fill="x", padx=8, pady=(0, 8))

        options = ttk.LabelFrame(self.left, text="组合选项")
        options.pack(fill="x")
        scene_header = ttk.Frame(options)
        scene_header.pack(fill="x", padx=8, pady=(8, 2))
        ttk.Label(scene_header, text="人物或者场景").pack(side="left")
        ttk.Button(scene_header, text="软件自动化测试", command=self.open_automation_test_dialog).pack(side="right")
        ttk.Button(scene_header, text="开始测试", command=self.quick_start_automation_test).pack(side="right", padx=(0, 6))
        self.clear_button = ttk.Button(scene_header, text="一键清空", command=self.one_click_clear)
        self.clear_button.pack(side="right", padx=(0, 6))
        self.clear_button.bind("<Button-3>", self._show_one_click_clear_menu)
        self.scene_combo = ttk.Combobox(options, textvariable=self.scene_var, state="readonly", width=48)
        self.scene_combo.pack(fill="x", padx=8)
        self.scene_combo.bind("<<ComboboxSelected>>", lambda e: self._load_selected_scene())

        self.body_label = ttk.Label(options, text="衣服或者其他时间端")
        self.body_label.pack(anchor="w", padx=8, pady=(8, 2))
        self.body_combo = ttk.Combobox(options, textvariable=self.body_var, state="readonly", width=48)
        self.body_combo.pack(fill="x", padx=8)
        self.body_combo.bind("<<ComboboxSelected>>", lambda e: self._on_body_selected())

        self.group_controls_frame = ttk.Frame(options)
        self.group_controls_frame.pack(fill="x")

        ttk.Label(options, text="圣光").pack(anchor="w", padx=8, pady=(8, 2))
        self.holy_combo = ttk.Combobox(options, textvariable=self.holy_var, state="readonly", width=48)
        self.holy_combo.pack(fill="x", padx=8, pady=(0, 8))
        self.holy_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_preview())

        self.create_info_box()
        self.create_preview_area()



    def _rebuild_group_controls(self, expression_groups, blush_groups, special_groups) -> None:
        for child in self.group_controls_frame.winfo_children():
            child.destroy()
        self.expression_vars = []
        self.blush_vars = []
        self.special_vars = []
        self.expression_combos = []
        self.blush_combos = []
        self.special_combos = []

        for i, (_group_name, options) in enumerate(expression_groups, start=1):
            var = tk.StringVar()
            self.expression_vars.append(var)
            display_name = str(_group_name or "").strip() or f"表情{i}"
            ttk.Label(self.group_controls_frame, text=display_name).pack(anchor="w", padx=8, pady=(8, 2))
            combo = ttk.Combobox(self.group_controls_frame, textvariable=var, state="readonly", width=48)
            combo.pack(fill="x", padx=8)
            combo["values"] = [x.label for x in options]
            if len(options) > 1:
                var.set(options[1].label)
            else:
                var.set(options[0].label if options else "")
            combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_preview())
            self.expression_combos.append(combo)

        for i, (_group_name, options) in enumerate(blush_groups, start=1):
            var = tk.StringVar()
            self.blush_vars.append(var)
            display_name = str(_group_name or "").strip() or f"红晕{i}"
            ttk.Label(self.group_controls_frame, text=display_name).pack(anchor="w", padx=8, pady=(8, 2))
            combo = ttk.Combobox(self.group_controls_frame, textvariable=var, state="readonly", width=48)
            combo.pack(fill="x", padx=8)
            combo["values"] = [x.label for x in options]
            var.set(options[0].label if options else "")
            combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_preview())
            self.blush_combos.append(combo)

        for i, (_group_name, options) in enumerate(special_groups, start=1):
            var = tk.StringVar()
            self.special_vars.append(var)
            display_name = str(_group_name or "").strip() or f"特殊{i}"
            ttk.Label(self.group_controls_frame, text=display_name).pack(anchor="w", padx=8, pady=(8, 2))
            combo = ttk.Combobox(self.group_controls_frame, textvariable=var, state="readonly", width=48)
            combo.pack(fill="x", padx=8)
            combo["values"] = [x.label for x in options]
            var.set(options[0].label if options else "")
            combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_preview())
            self.special_combos.append(combo)

    def _label_is_none_choice(self, label: str) -> bool:
        return label.startswith("(") or label.startswith("原图")

    def _advance_combobox(self, combo: ttk.Combobox, var: tk.StringVar, *, skip_none_choices: bool = False) -> bool:
        values = list(combo["values"] or [])
        if not values or str(combo.cget("state")) == "disabled":
            return False

        current = var.get()
        try:
            current_idx = values.index(current)
        except ValueError:
            current_idx = -1

        usable_indices = list(range(len(values)))
        if skip_none_choices and len(values) > 1:
            real_indices = [i for i, label in enumerate(values) if not self._label_is_none_choice(str(label))]
            if real_indices:
                usable_indices = real_indices

        if current_idx in usable_indices:
            pos = usable_indices.index(current_idx)
            next_idx = usable_indices[(pos + 1) % len(usable_indices)]
        else:
            next_idx = next((i for i in usable_indices if i > current_idx), usable_indices[0])

        var.set(values[next_idx])
        return True

    def _get_linkage_var(self, key: str) -> tk.BooleanVar:
        if key not in self.linkage_vars:
            self.linkage_vars[key] = tk.BooleanVar(value=False)
        return self.linkage_vars[key]

    def _group_display_label(self, groups, index: int, fallback: str) -> str:
        try:
            if 0 <= index - 1 < len(groups):
                name = str(groups[index - 1][0] or "").strip()
                if name:
                    return name
        except Exception:
            pass
        return fallback

    def _iter_link_targets(self):
        expression_groups = self.scene.expression_groups if self.scene else []
        blush_groups = self.scene.blush_groups if self.scene else []
        special_groups = self.scene.special_groups if self.scene else []
        for i, (combo, var) in enumerate(zip(self.expression_combos, self.expression_vars), start=1):
            yield f"expression_{i}", self._group_display_label(expression_groups, i, f"表情{i}"), combo, var
        for i, (combo, var) in enumerate(zip(self.blush_combos, self.blush_vars), start=1):
            yield f"blush_{i}", self._group_display_label(blush_groups, i, f"红晕{i}"), combo, var
        for i, (combo, var) in enumerate(zip(self.special_combos, self.special_vars), start=1):
            yield f"special_{i}", self._group_display_label(special_groups, i, f"特殊{i}"), combo, var
        yield "holy", "圣光", self.holy_combo, self.holy_var

    def _combo_has_real_options(self, combo: ttk.Combobox) -> bool:
        values = list(combo["values"] or [])
        if str(combo.cget("state")) == "disabled":
            return False
        return any(not self._label_is_none_choice(str(v)) for v in values)

    def _update_linkage_summary(self) -> None:
        selected = []
        for key, label, combo, _var in self._iter_link_targets():
            if self._get_linkage_var(key).get() and self._combo_has_real_options(combo):
                selected.append(label)
        if selected:
            self.linkage_summary_var.set("联动：" + "、".join(selected))
        else:
            self.linkage_summary_var.set("联动：未开启")

    def open_linkage_dialog(self) -> None:
        win = tk.Toplevel(self)
        win.title("联动设置")
        win.transient(self.winfo_toplevel())
        win.grab_set()
        win.resizable(False, False)

        ttk.Label(
            win,
            text="勾选后：手动切换“衣服或者其他时间端”时，下面这些选项会各自切到下一项。",
            wraplength=360,
            justify="left",
        ).pack(fill="x", padx=12, pady=(12, 8))

        frame = ttk.LabelFrame(win, text="可联动项目")
        frame.pack(fill="both", expand=True, padx=12, pady=(0, 8))

        targets = list(self._iter_link_targets())
        if not targets:
            ttk.Label(frame, text="请先加载 LSF 项目。").pack(anchor="w", padx=8, pady=8)
        else:
            for key, label, combo, _var in targets:
                bool_var = self._get_linkage_var(key)
                available = self._combo_has_real_options(combo)
                text = label if available else f"{label}（当前无可用选项）"
                cb = ttk.Checkbutton(frame, text=text, variable=bool_var, command=self._update_linkage_summary)
                cb.pack(anchor="w", padx=8, pady=3)
                if not available:
                    cb.state(["disabled"])

        btns = ttk.Frame(win)
        btns.pack(fill="x", padx=12, pady=(0, 12))

        def select_all_available() -> None:
            for key, _label, combo, _var in self._iter_link_targets():
                if self._combo_has_real_options(combo):
                    self._get_linkage_var(key).set(True)
            self._update_linkage_summary()

        def clear_all() -> None:
            for key, _label, _combo, _var in self._iter_link_targets():
                self._get_linkage_var(key).set(False)
            self._update_linkage_summary()

        ttk.Button(btns, text="全选可用", command=select_all_available).pack(side="left")
        ttk.Button(btns, text="清空", command=clear_all).pack(side="left", padx=(8, 0))
        ttk.Button(btns, text="关闭", command=win.destroy).pack(side="right")
        self._place_popup_like_left_panel(win)

    def _advance_enabled_linkage_controls(self) -> int:
        """按当前“联动设置”推进已勾选的表情/红晕/特殊/圣光选项。"""
        changed = 0
        for key, _label, combo, var in self._iter_link_targets():
            if self._get_linkage_var(key).get() and self._combo_has_real_options(combo):
                if self._advance_combobox(combo, var, skip_none_choices=True):
                    changed += 1
        return changed

    def _enabled_linkage_labels(self) -> list[str]:
        labels: list[str] = []
        for key, label, combo, _var in self._iter_link_targets():
            if self._get_linkage_var(key).get() and self._combo_has_real_options(combo):
                labels.append(label)
        return labels

    def _automation_linkage_step_count_for_scene(self, scene: LSFScene) -> int:
        """自动化测试启用联动时，每个 LSF 至少循环到能覆盖已勾选联动项一轮。"""
        max_count = 1
        for i, (_name, options) in enumerate(scene.expression_groups, start=1):
            key = f"expression_{i}"
            if self._get_linkage_var(key).get():
                values = self._values_from_labels([x.label for x in options], real_only=True)
                max_count = max(max_count, len(values))
        for i, (_name, options) in enumerate(scene.blush_groups, start=1):
            key = f"blush_{i}"
            if self._get_linkage_var(key).get():
                values = self._values_from_labels([x.label for x in options], real_only=True)
                max_count = max(max_count, len(values))
        for i, (_name, options) in enumerate(scene.special_groups, start=1):
            key = f"special_{i}"
            if self._get_linkage_var(key).get():
                values = self._values_from_labels([x.label for x in options], real_only=True)
                max_count = max(max_count, len(values))
        if self._get_linkage_var("holy").get():
            values = self._values_from_labels([x.label for x in scene.holy_options], real_only=True)
            max_count = max(max_count, len(values))
        return max(1, max_count)

    def _on_body_selected(self) -> None:
        self._advance_enabled_linkage_controls()
        self.refresh_preview()

    def _pick_lsf_dir(self) -> None:
        folder = filedialog.askdirectory()
        if folder:
            self.lsf_input_var.set(folder)
            self.png_var.set(folder)

    def _pick_png(self) -> None:
        folder = filedialog.askdirectory()
        if folder:
            self.png_var.set(folder)

    def _on_dir_changed(self) -> None:
        lsf_count = count_dir_files(self.lsf_input_var.get(), ("*.lsf",))
        png_count = count_dir_files(self.png_var.get(), ("*.png",))
        self.stats_var.set(f"当前目录统计：LSF {lsf_count} 个，PNG {png_count} 个")

    def _clear_current_preview_image(self) -> None:
        self.current_image = None
        if hasattr(self, "preview"):
            self.preview.show_image(None)

    def _release_loaded_resources(self) -> dict[str, object | None]:
        # 切换目录时只把旧资源从前台摘下，不在主线程立即清理。
        return self._detach_loaded_resources()

    def load_project(self) -> None:
        old_resources = self._release_loaded_resources()
        try:
            self.lsf_files = collect_input_files(self.lsf_input_var.get(), ("*.lsf",))
            if not self.lsf_files:
                raise ProjectError("请先选择包含 LSF 的目录。")
            if not self.png_var.get().strip():
                self.png_var.set(self.lsf_input_var.get().strip())
            if not self.png_var.get().strip():
                raise ProjectError("请先选择 PNG 目录。")
            self.resolver = PNGResolver(self.png_var.get().strip())
            self._on_dir_changed()
            scene_names = [p.name for p in self.lsf_files]
            self.scene_combo["values"] = scene_names
            self.scene_var.set(scene_names[0])
            self._load_selected_scene()
            self._schedule_background_resource_cleanup(old_resources)
        except Exception as exc:
            self._schedule_background_resource_cleanup(old_resources)
            messagebox.showerror("加载失败", str(exc))

    def _has_real_body_options(self) -> bool:
        if not self.scene:
            return False
        return any(len(opt.records) > 0 and not opt.key.startswith("__none") for opt in self.scene.body_options)

    def _apply_body_visibility(self) -> None:
        # 没有真正“衣服/时间端”选项时，不显示这个空下拉框，避免遮住下面的表情选项。
        # 注意：Tkinter 的 pack(before=...) 要求 before 目标当前已经由 pack 管理；
        # 之前在重新显示 body_label 时把 before 指向已被 pack_forget 的 body_combo，
        # 会触发 “isn't packed” 错误。这里统一以一直存在的 group_controls_frame
        # 作为锚点，并且 pack_forget 前先确认控件确实由 pack 管理。
        if self._has_real_body_options():
            if not self.body_label.winfo_manager():
                self.body_label.pack(anchor="w", padx=8, pady=(8, 2), before=self.group_controls_frame)
            if not self.body_combo.winfo_manager():
                self.body_combo.pack(fill="x", padx=8, before=self.group_controls_frame)
            self.body_combo.state(["!disabled", "readonly"])
        else:
            self.body_combo.state(["disabled"])
            if self.body_combo.winfo_manager() == "pack":
                self.body_combo.pack_forget()
            if self.body_label.winfo_manager() == "pack":
                self.body_label.pack_forget()

    def _apply_group_to_combo(self, combo: ttk.Combobox, var: tk.StringVar, groups, idx: int, none_label: str) -> None:
        if idx < len(groups):
            _group_name, options = groups[idx]
            combo["values"] = [x.label for x in options]
            if len(options) > 1:
                var.set(options[1].label)
            else:
                var.set(options[0].label if options else "")
            combo.state(["!disabled", "readonly"])
        else:
            combo["values"] = [none_label]
            var.set(none_label)
            combo.state(["disabled"])

    def _load_selected_scene(self, refresh: bool = True) -> None:
        try:
            selected = self.scene_var.get().strip()
            if not selected:
                return
            path = next((p for p in self.lsf_files if p.name == selected), None)
            if not path:
                return
            self._clear_current_preview_image()
            self.scene = analyze_lsf_scene(parse_lsf_file(path))
            self.body_combo["values"] = [x.label for x in self.scene.body_options]
            if len(self.scene.body_options) > 1 and self.scene.body_options[0].key == "__none__":
                self.body_var.set(self.scene.body_options[1].label)
            else:
                self.body_var.set(self.scene.body_options[0].label if self.scene.body_options else "")
            self._apply_body_visibility()

            self._rebuild_group_controls(self.scene.expression_groups, self.scene.blush_groups, self.scene.special_groups)

            self.holy_combo["values"] = [x.label for x in self.scene.holy_options]
            self.holy_var.set(self.scene.holy_options[0].label if self.scene.holy_options else "")
            if len(self.scene.holy_options) <= 1:
                self.holy_combo.state(["disabled"])
            else:
                self.holy_combo.state(["!disabled", "readonly"])
            self._update_linkage_summary()
            if refresh:
                self.refresh_preview()
        except Exception as exc:
            messagebox.showerror("读取 LSF 失败", str(exc))

    def _find_option(self, options: list[LSFOption], selected_label: str) -> Optional[LSFOption]:
        for item in options:
            if item.label == selected_label:
                return item
        return options[0] if options else None

    def refresh_preview(self) -> None:
        if not self.scene or not self.resolver:
            return
        body = self._find_option(self.scene.body_options, self.body_var.get())
        exprs: list[Optional[LSFOption]] = []
        for i, (_name, opts) in enumerate(self.scene.expression_groups):
            if i < len(self.expression_vars):
                opt = self._find_option(opts, self.expression_vars[i].get())
                exprs.append(None if opt and opt.key == "__none__" else opt)
        blushes: list[Optional[LSFOption]] = []
        for i, (_name, opts) in enumerate(self.scene.blush_groups):
            if i < len(self.blush_vars):
                opt = self._find_option(opts, self.blush_vars[i].get())
                blushes.append(None if opt and opt.key == "__none__" else opt)
        specials: list[Optional[LSFOption]] = []
        for i, (_name, opts) in enumerate(self.scene.special_groups):
            if i < len(self.special_vars):
                opt = self._find_option(opts, self.special_vars[i].get())
                specials.append(None if opt and opt.key == "__none__" else opt)
        holy = self._find_option(self.scene.holy_options, self.holy_var.get())
        image, warnings, records = compose_lsf_scene(
            self.scene,
            self.resolver,
            body if body and body.records is not None else None,
            exprs,
            blushes,
            None if holy and holy.key == "__none__" else holy,
            specials,
            runtime_workers=CPU_LOGICAL_THREADS,
        )
        self.current_image = image
        self.preview.show_image(image)

        lines = [
            f"LSF: {self.scene.project.lsf_path.name}",
            f"画布: {self.scene.project.canvas_width} x {self.scene.project.canvas_height}",
            f"已加载 LSF 数: {len(self.lsf_files)}",
            f"已索引 PNG 数: {len(self.resolver.by_stem) if self.resolver else 0}",
            CPU_INFO_TEXT,
            f"当前默认工作线程: {DEFAULT_THREAD_COUNT}",
            f"运行时预览 PNG 解码线程: {CPU_LOGICAL_THREADS}",
            f"衣服或者其他时间端: {body.label if body else '(无)'}",
        ]
        for i, expr in enumerate(exprs, start=1):
            lines.append(f"表情{i}: {expr.label if expr else '(无表情)'}")
        for i, blush in enumerate(blushes, start=1):
            lines.append(f"红晕{i}: {blush.label if blush else '(无红晕)'}")
        for i, sp in enumerate(specials, start=1):
            lines.append(f"特殊{i}: {sp.label if sp else '(无特殊)'}")
        lines += [
            f"圣光: {holy.label if holy and holy.key != '__none__' else '(无圣光)'}",
            f"当前合成图层数: {len(records)}",
            "",
            "分析结果:",
            *[f"  - {n}" for n in self.scene.notes],
        ]
        if warnings:
            lines += ["", "警告:", *[f"  - {w}" for w in warnings]]
        self._set_info(lines)

    def export_current(self) -> None:
        if self.current_image is None:
            messagebox.showinfo("提示", "没有可导出的预览图。")
            return
        out = filedialog.asksaveasfilename(defaultextension=".png", filetypes=[("PNG files", "*.png")])
        if out:
            self.current_image.save(out)
            messagebox.showinfo("完成", f"已导出: {out}")


    def _target_values_from_combo(self, combo: ttk.Combobox, *, real_only: bool = True) -> list[str]:
        values = [str(v) for v in list(combo["values"] or [])]
        if real_only:
            real = [v for v in values if not self._label_is_none_choice(v)]
            return real if real else values
        return values

    def _advance_label_value(self, values: list[str], current: str, *, skip_none_choices: bool = True) -> str:
        if not values:
            return current
        usable = values
        if skip_none_choices and len(values) > 1:
            real = [v for v in values if not self._label_is_none_choice(v)]
            if real:
                usable = real
        try:
            current_idx = values.index(current)
        except ValueError:
            current_idx = -1
        usable_indices = [values.index(v) for v in usable if v in values]
        if not usable_indices:
            return current
        if current_idx in usable_indices:
            pos = usable_indices.index(current_idx)
            return values[usable_indices[(pos + 1) % len(usable_indices)]]
        next_idx = next((i for i in usable_indices if i > current_idx), usable_indices[0])
        return values[next_idx]

    def _iter_batch_targets(self):
        yield "body", "衣服或者其他时间端", self.body_combo, self.body_var
        yield from self._iter_link_targets()

    def _current_lsf_selection(self) -> dict[str, str]:
        selection: dict[str, str] = {"body": self.body_var.get(), "holy": self.holy_var.get()}
        for i, var in enumerate(self.expression_vars, start=1):
            selection[f"expression_{i}"] = var.get()
        for i, var in enumerate(self.blush_vars, start=1):
            selection[f"blush_{i}"] = var.get()
        for i, var in enumerate(self.special_vars, start=1):
            selection[f"special_{i}"] = var.get()
        return selection

    def _compose_lsf_selection(self, selection: dict[str, str]) -> tuple[Image.Image, list[str], list[LSFOption]]:
        if not self.scene or not self.resolver:
            raise ProjectError("请先加载 LSF 项目。")
        body = self._find_option(self.scene.body_options, selection.get("body", self.body_var.get()))
        exprs: list[Optional[LSFOption]] = []
        for i, (_name, opts) in enumerate(self.scene.expression_groups, start=1):
            opt = self._find_option(opts, selection.get(f"expression_{i}", self.expression_vars[i - 1].get() if i - 1 < len(self.expression_vars) else ""))
            exprs.append(None if opt and opt.key == "__none__" else opt)
        blushes: list[Optional[LSFOption]] = []
        for i, (_name, opts) in enumerate(self.scene.blush_groups, start=1):
            opt = self._find_option(opts, selection.get(f"blush_{i}", self.blush_vars[i - 1].get() if i - 1 < len(self.blush_vars) else ""))
            blushes.append(None if opt and opt.key == "__none__" else opt)
        specials: list[Optional[LSFOption]] = []
        for i, (_name, opts) in enumerate(self.scene.special_groups, start=1):
            opt = self._find_option(opts, selection.get(f"special_{i}", self.special_vars[i - 1].get() if i - 1 < len(self.special_vars) else ""))
            specials.append(None if opt and opt.key == "__none__" else opt)
        holy = self._find_option(self.scene.holy_options, selection.get("holy", self.holy_var.get()))
        image, warnings, records = compose_lsf_scene(
            self.scene,
            self.resolver,
            body if body and body.records is not None else None,
            exprs,
            blushes,
            None if holy and holy.key == "__none__" else holy,
            specials,
        )
        return image, warnings, records  # type: ignore[return-value]

    def _selection_filename(self, index: int, selection: dict[str, str], selected_keys: list[str]) -> str:
        scene_stem = self.scene.project.stem if self.scene else "scene"
        parts = [safe_filename_part(scene_stem, 40), f"{index:04d}"]
        for key, label, _combo, _var in self._iter_batch_targets():
            if key in selected_keys:
                parts.append(safe_filename_part(selection.get(key, ""), 32))
        return "__".join([p for p in parts if p])

    def _values_from_labels(self, values: list[str], *, real_only: bool = True) -> list[str]:
        values = [str(v) for v in values]
        if real_only:
            real = [v for v in values if not self._label_is_none_choice(v)]
            return real if real else values
        return values

    def _lsf_targets_for_scene(self, scene: LSFScene) -> list[tuple[str, str, list[str]]]:
        targets: list[tuple[str, str, list[str]]] = []
        if any(len(opt.records) > 0 and not opt.key.startswith("__none") for opt in scene.body_options):
            targets.append(("body", "衣服或者其他时间端", [x.label for x in scene.body_options]))
        for i, (_name, options) in enumerate(scene.expression_groups, start=1):
            targets.append((f"expression_{i}", f"表情{i}", [x.label for x in options]))
        for i, (_name, options) in enumerate(scene.blush_groups, start=1):
            targets.append((f"blush_{i}", f"红晕{i}", [x.label for x in options]))
        for i, (_name, options) in enumerate(scene.special_groups, start=1):
            targets.append((f"special_{i}", f"特殊{i}", [x.label for x in options]))
        targets.append(("holy", "圣光", [x.label for x in scene.holy_options]))
        return targets

    def _default_lsf_selection_for_scene(self, scene: LSFScene) -> dict[str, str]:
        selection: dict[str, str] = {}
        if scene.body_options:
            if len(scene.body_options) > 1 and scene.body_options[0].key == "__none__":
                selection["body"] = scene.body_options[1].label
            else:
                selection["body"] = scene.body_options[0].label
        for i, (_name, options) in enumerate(scene.expression_groups, start=1):
            if options:
                selection[f"expression_{i}"] = options[1].label if len(options) > 1 else options[0].label
        for i, (_name, options) in enumerate(scene.blush_groups, start=1):
            if options:
                selection[f"blush_{i}"] = options[0].label
        for i, (_name, options) in enumerate(scene.special_groups, start=1):
            if options:
                selection[f"special_{i}"] = options[0].label
        if scene.holy_options:
            selection["holy"] = scene.holy_options[0].label
        return selection

    def _compose_lsf_selection_for_scene(self, scene: LSFScene, selection: dict[str, str]) -> tuple[Image.Image, list[str], list[LSFOption]]:
        if not self.resolver:
            raise ProjectError("请先加载 PNG 目录。")
        body = self._find_option(scene.body_options, selection.get("body", ""))
        exprs: list[Optional[LSFOption]] = []
        for i, (_name, opts) in enumerate(scene.expression_groups, start=1):
            opt = self._find_option(opts, selection.get(f"expression_{i}", ""))
            exprs.append(None if opt and opt.key == "__none__" else opt)
        blushes: list[Optional[LSFOption]] = []
        for i, (_name, opts) in enumerate(scene.blush_groups, start=1):
            opt = self._find_option(opts, selection.get(f"blush_{i}", ""))
            blushes.append(None if opt and opt.key == "__none__" else opt)
        specials: list[Optional[LSFOption]] = []
        for i, (_name, opts) in enumerate(scene.special_groups, start=1):
            opt = self._find_option(opts, selection.get(f"special_{i}", ""))
            specials.append(None if opt and opt.key == "__none__" else opt)
        holy = self._find_option(scene.holy_options, selection.get("holy", ""))
        image, warnings, records = compose_lsf_scene(
            scene,
            self.resolver,
            body if body and body.records is not None else None,
            exprs,
            blushes,
            None if holy and holy.key == "__none__" else holy,
            specials,
        )
        return image, warnings, records  # type: ignore[return-value]

    def _selection_filename_for_scene(self, scene: LSFScene, index: int, selection: dict[str, str], selected_keys: list[str]) -> str:
        parts = [safe_filename_part(scene.project.stem, 40), f"{index:04d}"]
        for key, _label, _values in self._lsf_targets_for_scene(scene):
            if key in selected_keys:
                parts.append(safe_filename_part(selection.get(key, ""), 32))
        return "__".join([p for p in parts if p])

    def _estimate_lsf_scene_export(self, scene: LSFScene, selected_keys: list[str], mode: str) -> int:
        targets = self._lsf_targets_for_scene(scene)
        valid_keys = [k for k in selected_keys if any(t[0] == k for t in targets)]
        if mode == "product":
            estimate = 1
            any_selected = False
            for key, _label, values in targets:
                if key in valid_keys:
                    any_selected = True
                    estimate *= max(1, len(self._values_from_labels(values, real_only=True)))
            return estimate if any_selected else 1
        if "body" in valid_keys:
            body_values = next((values for key, _label, values in targets if key == "body"), [])
            return max(1, len(self._values_from_labels(body_values, real_only=True)))
        counts = [len(self._values_from_labels(values, real_only=True)) for key, _label, values in targets if key in valid_keys]
        return max(counts or [1])

    def _iter_lsf_scene_export_jobs(self, scene: LSFScene, selected_keys: list[str], mode: str, current: Optional[dict[str, str]] = None):
        current = dict(current or self._default_lsf_selection_for_scene(scene))
        targets = self._lsf_targets_for_scene(scene)
        valid_selected_keys = [k for k in selected_keys if any(t[0] == k for t in targets)]
        filename_keys = valid_selected_keys or ["body"]

        if mode == "product":
            value_lists: list[tuple[str, list[str]]] = []
            for key, _label, values in targets:
                if key in valid_selected_keys:
                    vals = self._values_from_labels(values, real_only=True)
                    if vals:
                        value_lists.append((key, vals))
            combos = itertools.product(*[vals for _key, vals in value_lists]) if value_lists else [()]
            for idx, values in enumerate(combos, start=1):
                selection = dict(current)
                for (key, _vals), value in zip(value_lists, values):
                    selection[key] = value
                yield idx, scene, selection, filename_keys
        else:
            count = self._estimate_lsf_scene_export(scene, valid_selected_keys, mode)
            selection = dict(current)
            for idx in range(1, count + 1):
                yield idx, scene, dict(selection), filename_keys
                if idx < count:
                    for key, _label, values in targets:
                        if key in valid_selected_keys:
                            selection[key] = self._advance_label_value(values, selection.get(key, ""), skip_none_choices=True)

    def _collect_lsf_batch_jobs(self, selected_keys: list[str], mode: str, scope: str):
        if not self.scene or not self.resolver:
            raise ProjectError("请先加载 LSF 项目。")
        if scope == "directory":
            jobs = []
            for path in self.lsf_files:
                scene = analyze_lsf_scene(parse_lsf_file(path))
                jobs.extend(self._iter_lsf_scene_export_jobs(scene, selected_keys, mode))
            return jobs
        return list(self._iter_lsf_scene_export_jobs(self.scene, selected_keys, mode, self._current_lsf_selection()))

    def _export_lsf_batch_job(self, out_dir: Path, job, filename_lock: threading.Lock, reserved_paths: set[str]) -> int:
        idx, scene, selection, filename_keys = job
        img, warnings, _records = self._compose_lsf_selection_for_scene(scene, selection)
        filename = self._selection_filename_for_scene(scene, idx, selection, filename_keys)
        with filename_lock:
            out_path = make_unique_png_path_reserved(out_dir, filename, reserved_paths)
        img.save(out_path)
        return len(warnings)

    def _run_lsf_batch_export_threaded(
        self,
        out_dir: Path,
        selected_keys: list[str],
        mode: str,
        scope: str = "current",
        thread_count: int = 4,
        progress_callback=None,
    ) -> tuple[int, int]:
        if not self.scene or not self.resolver:
            raise ProjectError("请先加载 LSF 项目。")
        out_dir.mkdir(parents=True, exist_ok=True)
        jobs = self._collect_lsf_batch_jobs(selected_keys, mode, scope)
        total = len(jobs)
        if progress_callback:
            progress_callback(0, total, 0)
        if not jobs:
            return 0, 0

        max_workers = normalize_thread_count(thread_count)
        filename_lock = threading.Lock()
        reserved_paths: set[str] = set()
        completed = 0
        warnings_total = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._export_lsf_batch_job, out_dir, job, filename_lock, reserved_paths)
                for job in jobs
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    warnings_total += future.result()
                except Exception:
                    for f in futures:
                        f.cancel()
                    raise
                completed += 1
                if progress_callback:
                    progress_callback(completed, total, warnings_total)
        return completed, warnings_total

    def _run_lsf_batch_export_for_scene(self, scene: LSFScene, out_dir: Path, selected_keys: list[str], mode: str, current: Optional[dict[str, str]] = None) -> tuple[int, int]:
        current = dict(current or self._default_lsf_selection_for_scene(scene))
        targets = self._lsf_targets_for_scene(scene)
        selected_keys = [k for k in selected_keys if any(t[0] == k for t in targets)]
        warnings_total = 0
        exported = 0

        if mode == "product":
            value_lists: list[tuple[str, list[str]]] = []
            for key, _label, values in targets:
                if key in selected_keys:
                    vals = self._values_from_labels(values, real_only=True)
                    if vals:
                        value_lists.append((key, vals))
            combos = itertools.product(*[vals for _key, vals in value_lists]) if value_lists else [()]
            for idx, values in enumerate(combos, start=1):
                selection = dict(current)
                for (key, _vals), value in zip(value_lists, values):
                    selection[key] = value
                img, warnings, _records = self._compose_lsf_selection_for_scene(scene, selection)
                warnings_total += len(warnings)
                filename = self._selection_filename_for_scene(scene, idx, selection, selected_keys or ["body"])
                img.save(make_unique_png_path(out_dir, filename))
                exported += 1
        else:
            count = self._estimate_lsf_scene_export(scene, selected_keys, mode)
            selection = dict(current)
            for idx in range(1, count + 1):
                img, warnings, _records = self._compose_lsf_selection_for_scene(scene, selection)
                warnings_total += len(warnings)
                filename = self._selection_filename_for_scene(scene, idx, selection, selected_keys or ["body"])
                img.save(make_unique_png_path(out_dir, filename))
                exported += 1
                if idx < count:
                    for key, _label, values in targets:
                        if key in selected_keys:
                            selection[key] = self._advance_label_value(values, selection.get(key, ""), skip_none_choices=True)
        return exported, warnings_total

    def _run_lsf_batch_export(self, out_dir: Path, selected_keys: list[str], mode: str, scope: str = "current") -> tuple[int, int]:
        if not self.scene or not self.resolver:
            raise ProjectError("请先加载 LSF 项目。")
        out_dir.mkdir(parents=True, exist_ok=True)
        if scope == "directory":
            exported = 0
            warnings_total = 0
            for path in self.lsf_files:
                scene = analyze_lsf_scene(parse_lsf_file(path))
                e, w = self._run_lsf_batch_export_for_scene(scene, out_dir, selected_keys, mode)
                exported += e
                warnings_total += w
            return exported, warnings_total
        return self._run_lsf_batch_export_for_scene(self.scene, out_dir, selected_keys, mode, self._current_lsf_selection())

    def open_supported_series_dialog(self) -> None:
        win = tk.Toplevel(self)
        win.title("支持系列")
        win.transient(self.winfo_toplevel())
        win.grab_set()
        win.resizable(False, False)

        ttk.Label(
            win,
            text=SUPPORTED_SERIES_TEXT,
            justify="left",
            anchor="w",
        ).pack(fill="x", padx=16, pady=(14, 12))

        btns = ttk.Frame(win)
        btns.pack(fill="x", padx=16, pady=(0, 14))
        ttk.Button(btns, text="确定", command=win.destroy, width=10).pack(side="right")
        self._place_popup_like_left_panel(win)

    def open_batch_export_dialog(self) -> None:
        if not self.scene or not self.resolver:
            messagebox.showinfo("提示", "请先加载 LSF 项目。")
            return

        win = tk.Toplevel(self)
        win.title("批量导出当前组合")
        win.transient(self.winfo_toplevel())
        win.grab_set()
        win.resizable(False, False)

        default_folder = Path(self.png_var.get().strip() or ".").expanduser() / "batch_export"
        out_var = tk.StringVar(value=str(default_folder))
        mode_var = tk.StringVar(value="sequence")
        scope_var = tk.StringVar(value="current")
        thread_var = tk.StringVar(value=DEFAULT_THREAD_COUNT)
        check_vars: dict[str, tk.BooleanVar] = {}

        ttk.Label(
            win,
            text="选择哪些下拉项参与批量导出。默认按当前“联动设置”勾选；也可以手动改。",
            wraplength=420,
            justify="left",
        ).pack(fill="x", padx=12, pady=(12, 8))

        out_frame = ttk.LabelFrame(win, text="输出目录")
        out_frame.pack(fill="x", padx=12, pady=(0, 8))
        row = ttk.Frame(out_frame)
        row.pack(fill="x", padx=8, pady=8)
        ttk.Entry(row, textvariable=out_var, width=44).pack(side="left", fill="x", expand=True)

        def choose_out_dir() -> None:
            folder = filedialog.askdirectory(parent=win)
            if folder:
                out_var.set(folder)

        def use_loaded_dir() -> None:
            folder = self.lsf_input_var.get().strip() or self.png_var.get().strip()
            if folder:
                out_var.set(folder)

        ttk.Button(row, text="选择", command=choose_out_dir, width=8).pack(side="left", padx=(6, 0))
        ttk.Button(row, text="当前目录", command=use_loaded_dir, width=10).pack(side="left", padx=(6, 0))

        mode_frame = ttk.LabelFrame(win, text="导出方式")
        mode_frame.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Radiobutton(
            mode_frame,
            text="联动序列：以当前选择为起点，逐项切换时间端；勾选项跟着下一项",
            variable=mode_var,
            value="sequence",
        ).pack(anchor="w", padx=8, pady=(6, 2))
        ttk.Radiobutton(
            mode_frame,
            text="全组合：把勾选项的所有真实选项全部排列组合导出",
            variable=mode_var,
            value="product",
        ).pack(anchor="w", padx=8, pady=(2, 6))

        scope_frame = ttk.LabelFrame(win, text="导出范围")
        scope_frame.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Radiobutton(
            scope_frame,
            text="只导出当前选中的 LSF",
            variable=scope_var,
            value="current",
        ).pack(anchor="w", padx=8, pady=(6, 2))
        ttk.Radiobutton(
            scope_frame,
            text="导出当前加载目录里的全部 LSF",
            variable=scope_var,
            value="directory",
        ).pack(anchor="w", padx=8, pady=(2, 6))

        thread_frame = ttk.LabelFrame(win, text="多线程")
        thread_frame.pack(fill="x", padx=12, pady=(0, 8))
        thread_row = ttk.Frame(thread_frame)
        thread_row.pack(fill="x", padx=8, pady=8)
        ttk.Label(thread_row, text="导出线程数量").pack(side="left")
        thread_combo = ttk.Combobox(
            thread_row,
            textvariable=thread_var,
            values=THREAD_COUNT_CHOICES,
            state="readonly",
            width=6,
        )
        thread_combo.pack(side="left", padx=(8, 8))
        ttk.Label(thread_row, text=f"{CPU_INFO_TEXT}；默认使用全部逻辑线程").pack(side="left")

        target_frame = ttk.LabelFrame(win, text="参与批量的选项")
        target_frame.pack(fill="both", expand=True, padx=12, pady=(0, 8))

        targets = list(self._iter_batch_targets())
        for key, label, combo, _var in targets:
            available = self._combo_has_real_options(combo)
            default_checked = key == "body" or (key in self.linkage_vars and self._get_linkage_var(key).get())
            var = tk.BooleanVar(value=bool(default_checked and available))
            check_vars[key] = var
            text = label if available else f"{label}（当前无可用选项）"
            cb = ttk.Checkbutton(target_frame, text=text, variable=var)
            cb.pack(anchor="w", padx=8, pady=2)
            if not available:
                cb.state(["disabled"])

        btns1 = ttk.Frame(win)
        btns1.pack(fill="x", padx=12, pady=(0, 8))

        def apply_linkage_setting() -> None:
            for key, _label, combo, _var in targets:
                if not self._combo_has_real_options(combo):
                    check_vars[key].set(False)
                elif key == "body":
                    check_vars[key].set(True)
                else:
                    check_vars[key].set(self._get_linkage_var(key).get())

        def select_all_available() -> None:
            for key, _label, combo, _var in targets:
                check_vars[key].set(self._combo_has_real_options(combo))

        def clear_all() -> None:
            for key in check_vars:
                check_vars[key].set(False)

        ttk.Button(btns1, text="按联动设置选择", command=apply_linkage_setting).pack(side="left")
        ttk.Button(btns1, text="全选可用", command=select_all_available).pack(side="left", padx=(8, 0))
        ttk.Button(btns1, text="清空", command=clear_all).pack(side="left", padx=(8, 0))

        progress_var = tk.DoubleVar(value=0.0)
        progress_text_var = tk.StringVar(value="进度：未开始")
        progress_frame = ttk.LabelFrame(win, text="导出进度")
        progress_frame.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Progressbar(progress_frame, maximum=100, variable=progress_var).pack(fill="x", padx=8, pady=(8, 4))
        ttk.Label(progress_frame, textvariable=progress_text_var, justify="left", anchor="w").pack(fill="x", padx=8, pady=(0, 8))

        btns2 = ttk.Frame(win)
        btns2.pack(fill="x", padx=12, pady=(0, 12))

        def start_export() -> None:
            out_text = out_var.get().strip()
            if not out_text:
                messagebox.showinfo("提示", "请选择输出目录。", parent=win)
                return
            out_dir = Path(out_text).expanduser()
            selected_keys = [key for key, var in check_vars.items() if var.get()]
            selected_mode = mode_var.get()
            selected_scope = scope_var.get()
            try:
                thread_count = int(thread_var.get() or DEFAULT_THREAD_COUNT)
            except Exception:
                thread_count = int(DEFAULT_THREAD_COUNT)

            estimate = 1
            if selected_mode == "product":
                for key, _label, combo, _var in targets:
                    if key in selected_keys:
                        estimate *= max(1, len(self._target_values_from_combo(combo, real_only=True)))
            else:
                if "body" in selected_keys:
                    estimate = max(1, len(self._target_values_from_combo(self.body_combo, real_only=True)))
                else:
                    estimate = max([len(self._target_values_from_combo(combo, real_only=True)) for key, _label, combo, _var in targets if key in selected_keys] or [1])
            if selected_scope == "directory":
                try:
                    estimate = sum(
                        self._estimate_lsf_scene_export(analyze_lsf_scene(parse_lsf_file(path)), selected_keys, selected_mode)
                        for path in self.lsf_files
                    )
                except Exception:
                    estimate = max(1, estimate) * max(1, len(self.lsf_files))
            if estimate > 800 and not messagebox.askyesno("确认", f"预计会导出约 {estimate} 张 PNG，是否继续？", parent=win):
                return

            export_queue: queue.Queue = queue.Queue()
            start_time = time.time()
            progress_var.set(0.0)
            progress_text_var.set(batch_progress_text(0, estimate, start_time))
            start_button.state(["disabled"])
            close_button.state(["disabled"])
            win.protocol("WM_DELETE_WINDOW", lambda: None)

            def progress_callback(done: int, total: int, warnings_count: int) -> None:
                export_queue.put(("progress", done, total, warnings_count))

            def worker() -> None:
                try:
                    exported, warnings_total = self._run_lsf_batch_export_threaded(
                        out_dir,
                        selected_keys,
                        selected_mode,
                        selected_scope,
                        thread_count,
                        progress_callback,
                    )
                    export_queue.put(("done", exported, warnings_total, time.time() - start_time))
                except Exception as exc:
                    export_queue.put(("error", str(exc)))

            def poll_queue() -> None:
                finished = None
                try:
                    while True:
                        message = export_queue.get_nowait()
                        kind = message[0]
                        if kind == "progress":
                            _kind, done, total, _warnings_count = message
                            percent = 100.0 if total <= 0 else done * 100.0 / total
                            progress_var.set(percent)
                            progress_text_var.set(batch_progress_text(done, total, start_time))
                        elif kind == "done":
                            finished = message
                            _kind, exported, _warnings_total, _elapsed = message
                            progress_var.set(100.0)
                            progress_text_var.set(batch_progress_text(exported, exported, start_time))
                        elif kind == "error":
                            finished = message
                            progress_text_var.set(f"导出失败：{message[1]}")
                except queue.Empty:
                    pass

                if finished is None:
                    win.after(100, poll_queue)
                    return

                start_button.state(["!disabled"])
                close_button.state(["!disabled"])
                win.protocol("WM_DELETE_WINDOW", win.destroy)
                if finished[0] == "done":
                    _kind, exported, warnings_total, elapsed = finished
                    messagebox.showinfo(
                        "完成",
                        f"已导出 {exported} 张 PNG。\n输出目录：{out_dir}\n警告数量：{warnings_total}\n用时：{format_duration(elapsed)}\n线程数量：{thread_count}",
                        parent=win,
                    )
                else:
                    messagebox.showerror("批量导出失败", finished[1], parent=win)

            threading.Thread(target=worker, daemon=True).start()
            poll_queue()

        start_button = ttk.Button(btns2, text="开始导出", command=start_export)
        start_button.pack(side="left")
        close_button = ttk.Button(btns2, text="关闭", command=win.destroy)
        close_button.pack(side="right")
        self._place_popup_like_left_panel(win)


    def _iter_clear_targets(self):
        # “人物或者场景” intentionally excluded: this button never switches LSF.
        yield "body", "衣服或者其他时间端", self.body_combo, self.body_var
        yield from self._iter_link_targets()

    def _combo_can_clear(self, combo: ttk.Combobox) -> bool:
        values = list(combo["values"] or [])
        return bool(values) and str(combo.cget("state")) != "disabled"

    def _clear_value_for_combo(self, values: list[str]) -> str:
        if not values:
            return ""
        for value in values:
            label = str(value)
            if self._label_is_none_choice(label) or label in {"默认", "槽位模式"}:
                return label
        return str(values[0])

    def _configured_clear_keys(self) -> set[str] | None:
        self.clear_settings = load_one_click_clear_settings()
        if not bool(self.clear_settings.get("configured", False)):
            return None
        selected = self.clear_settings.get("selected_keys", [])
        if not isinstance(selected, list):
            return set()
        return {str(x) for x in selected}

    def one_click_clear(self) -> None:
        if not self.scene:
            messagebox.showinfo("提示", "请先加载 LSF 项目。")
            return
        configured_keys = self._configured_clear_keys()
        changed = 0
        for key, _label, combo, var in self._iter_clear_targets():
            if configured_keys is not None and key not in configured_keys:
                continue
            if not self._combo_can_clear(combo):
                continue
            values = [str(v) for v in list(combo["values"] or [])]
            target = self._clear_value_for_combo(values)
            if target and var.get() != target:
                var.set(target)
                changed += 1
        if changed:
            self.refresh_preview()
        else:
            # Nothing changed, but refresh keeps the information panel consistent if
            # the current selection was already at the clear/default state.
            self.refresh_preview()

    def _show_one_click_clear_menu(self, event=None) -> None:
        menu = tk.Menu(self, tearoff=0)
        menu.add_command(label="清空设置...", command=self.open_one_click_clear_settings_dialog)
        try:
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            menu.grab_release()

    def open_one_click_clear_settings_dialog(self) -> None:
        if not self.scene:
            messagebox.showinfo("提示", "请先加载 LSF 项目。")
            return

        win = tk.Toplevel(self)
        win.title("一键清空设置")
        win.transient(self.winfo_toplevel())
        win.grab_set()
        win.resizable(False, False)

        ttk.Label(
            win,
            text="选择“一键清空”会清空哪些组合选项。人物或者场景不会被清空。默认是全部可用选项都清空；保存后会记住你的选择。",
            wraplength=420,
            justify="left",
        ).pack(fill="x", padx=12, pady=(12, 8))

        frame = ttk.LabelFrame(win, text="可清空项目")
        frame.pack(fill="both", expand=True, padx=12, pady=(0, 8))

        targets = list(self._iter_clear_targets())
        configured_keys = self._configured_clear_keys()
        check_vars: dict[str, tk.BooleanVar] = {}
        if not targets:
            ttk.Label(frame, text="请先加载 LSF 项目。").pack(anchor="w", padx=8, pady=8)
        else:
            for key, label, combo, _var in targets:
                available = self._combo_can_clear(combo)
                default_checked = bool(available) if configured_keys is None else (key in configured_keys and available)
                bool_var = tk.BooleanVar(value=default_checked)
                check_vars[key] = bool_var
                text = label if available else f"{label}（当前无可用选项）"
                cb = ttk.Checkbutton(frame, text=text, variable=bool_var)
                cb.pack(anchor="w", padx=8, pady=3)
                if not available:
                    cb.state(["disabled"])

        btns1 = ttk.Frame(win)
        btns1.pack(fill="x", padx=12, pady=(0, 8))

        def select_all_available() -> None:
            for key, _label, combo, _var in targets:
                if key in check_vars:
                    check_vars[key].set(self._combo_can_clear(combo))

        def clear_all() -> None:
            for var in check_vars.values():
                var.set(False)

        ttk.Button(btns1, text="全选可用", command=select_all_available).pack(side="left")
        ttk.Button(btns1, text="清空勾选", command=clear_all).pack(side="left", padx=(8, 0))

        btns2 = ttk.Frame(win)
        btns2.pack(fill="x", padx=12, pady=(0, 12))

        def save_settings() -> None:
            selected_keys = [key for key, var in check_vars.items() if var.get()]
            self.clear_settings = {"configured": True, "selected_keys": selected_keys}
            save_one_click_clear_settings(self.clear_settings)
            win.destroy()

        def restore_default() -> None:
            self.clear_settings = dict(DEFAULT_ONE_CLICK_CLEAR_SETTINGS)
            save_one_click_clear_settings(self.clear_settings)
            win.destroy()

        ttk.Button(btns2, text="恢复默认全清", command=restore_default).pack(side="left")
        ttk.Button(btns2, text="保存", command=save_settings).pack(side="right")
        ttk.Button(btns2, text="取消", command=win.destroy).pack(side="right", padx=(0, 8))
        self._place_popup_like_left_panel(win)


    def quick_start_automation_test(self) -> None:
        self.automation_settings = load_automation_settings()
        if not bool(self.automation_settings.get("has_run", False)):
            messagebox.showinfo("提示", "先进行一次软件自动化测试先。")
            return
        self.open_automation_test_dialog(auto_start=True)

    def open_automation_test_dialog(self, auto_start: bool = False) -> None:
        if not self.lsf_files or not list(self.scene_combo["values"] or []):
            messagebox.showinfo("提示", "请先加载 LSF 项目。")
            return

        win = tk.Toplevel(self)
        win.title("软件自动化测试")
        win.transient(self.winfo_toplevel())
        win.resizable(False, False)

        self.automation_settings = load_automation_settings()
        interval_var = tk.StringVar(value=str(self.automation_settings.get("interval_seconds", 0.8)))
        start_from_current_var = tk.BooleanVar(value=bool(self.automation_settings.get("start_from_current", False)))
        auto_pick_next_dir_var = tk.BooleanVar(value=bool(self.automation_settings.get("auto_pick_next_dir", False)))
        apply_linkage_var = tk.BooleanVar(value=bool(self.automation_settings.get("apply_linkage", True)))
        progress_var = tk.DoubleVar(value=0.0)
        status_var = tk.StringVar(value=f"待开始｜{CPU_INFO_TEXT}")

        state = {
            "running": False,
            "sequence": [],
            "index": 0,
            "start_time": 0.0,
            "after_id": None,
            "last_scene": "",
            "last_body": "",
        }

        ttk.Label(
            win,
            text="自动测试顺序：先加载第 1 个“人物或者场景”，再把“衣服或者其他时间端”从第 1 项切到最后 1 项；然后加载第 2 个“人物或者场景”继续测试。勾选“应用联动”后，会按当前联动设置同步切换表情/红晕/特殊/圣光；如果联动项数量更多，会循环衣服选项直到联动项也走完一轮。",
            wraplength=500,
            justify="left",
        ).pack(fill="x", padx=12, pady=(12, 8))

        setting_frame = ttk.LabelFrame(win, text="测试设置")
        setting_frame.pack(fill="x", padx=12, pady=(0, 8))

        row1 = ttk.Frame(setting_frame)
        row1.pack(fill="x", padx=8, pady=(8, 4))
        ttk.Label(row1, text="每次切换间隔（秒）").pack(side="left")
        ttk.Entry(row1, textvariable=interval_var, width=10).pack(side="left", padx=(8, 0))
        ttk.Label(row1, text="建议 0.3 - 2 秒；设太低会更吃 CPU").pack(side="left", padx=(8, 0))

        ttk.Checkbutton(
            setting_frame,
            text="从当前选中的人物/衣服开始测试（不勾选则从第一个人物、第一个衣服开始）",
            variable=start_from_current_var,
        ).pack(anchor="w", padx=8, pady=(2, 2))
        ttk.Checkbutton(
            setting_frame,
            text="应用当前“联动”设置（自动切换表情 / 红晕 / 特殊 / 圣光）",
            variable=apply_linkage_var,
        ).pack(anchor="w", padx=8, pady=(2, 2))
        ttk.Checkbutton(
            setting_frame,
            text="测试到最后一个选项后，自动打开 LSF 目录选择窗口",
            variable=auto_pick_next_dir_var,
        ).pack(anchor="w", padx=8, pady=(2, 8))

        progress_frame = ttk.LabelFrame(win, text="测试进度")
        progress_frame.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Progressbar(progress_frame, maximum=100, variable=progress_var).pack(fill="x", padx=8, pady=(8, 4))
        ttk.Label(progress_frame, textvariable=status_var, justify="left", anchor="w").pack(fill="x", padx=8, pady=(0, 8))

        btns = ttk.Frame(win)
        btns.pack(fill="x", padx=12, pady=(0, 12))

        def parse_interval_ms() -> int:
            try:
                seconds = float(interval_var.get().strip())
            except Exception:
                seconds = 0.8
            seconds = max(0.05, min(seconds, 3600.0))
            interval_var.set(str(seconds).rstrip("0").rstrip(".") if seconds != int(seconds) else str(int(seconds)))
            return int(seconds * 1000)

        def _body_labels_for_lsf(path: Path) -> list[str]:
            try:
                scene = analyze_lsf_scene(parse_lsf_file(path))
                labels = [str(x.label) for x in scene.body_options]
                return labels if labels else [""]
            except Exception:
                # 解析失败也保留一个测试步骤，让正式切换时显示原来的错误提示。
                return [""]

        def format_test_status(done: int, total: int, current_scene: str = "", current_body: str = "") -> str:
            total = max(0, int(total))
            done = max(0, min(int(done), total)) if total else int(done)
            percent = 100.0 if total <= 0 else done * 100.0 / total
            if done <= 0:
                interval_seconds = parse_interval_ms() / 1000.0
                eta = format_duration(interval_seconds * max(0, total - done))
            elif done >= total:
                eta = "00:00"
            else:
                elapsed = max(0.0, time.time() - float(state.get("start_time") or time.time()))
                eta = format_duration((elapsed / done) * (total - done))
            name_part = ""
            if current_scene or current_body:
                name_part = f"｜当前：{current_scene}"
                if current_body:
                    name_part += f" / {current_body}"
            linkage_part = ""
            if apply_linkage_var.get():
                labels = self._enabled_linkage_labels()
                if labels:
                    linkage_part = "｜联动：" + "、".join(labels)
            return f"已切换文件数：{done}/{total} ({percent:.1f}%)｜预计剩余时间：{eta}{name_part}{linkage_part}"

        def save_current_automation_settings() -> None:
            try:
                interval_seconds = float(interval_var.get().strip())
            except Exception:
                interval_seconds = 0.8
            interval_seconds = max(0.05, min(interval_seconds, 3600.0))
            self.automation_settings = {
                "has_run": True,
                "interval_seconds": interval_seconds,
                "start_from_current": bool(start_from_current_var.get()),
                "auto_pick_next_dir": bool(auto_pick_next_dir_var.get()),
                "apply_linkage": bool(apply_linkage_var.get()),
            }
            save_automation_settings(self.automation_settings)

        def build_sequence() -> list[dict[str, object]]:
            scene_values = [str(v) for v in list(self.scene_combo["values"] or [])]
            if not scene_values:
                return []

            scene_start_idx = 0
            current_body = self.body_var.get()
            if start_from_current_var.get():
                try:
                    scene_start_idx = scene_values.index(self.scene_var.get())
                except ValueError:
                    scene_start_idx = 0

            sequence: list[dict[str, object]] = []
            path_by_name = {p.name: p for p in self.lsf_files}
            use_linkage = bool(apply_linkage_var.get()) and any(var.get() for var in self.linkage_vars.values())
            for scene_idx, scene_name in enumerate(scene_values[scene_start_idx:], start=scene_start_idx):
                path = path_by_name.get(scene_name)
                if path is None:
                    continue
                try:
                    scene_for_count = analyze_lsf_scene(parse_lsf_file(path))
                    bodies = [str(x.label) for x in scene_for_count.body_options] or [""]
                    linkage_steps = self._automation_linkage_step_count_for_scene(scene_for_count) if use_linkage else 1
                except Exception:
                    # 解析失败也保留一个测试步骤，让正式切换时显示原来的错误提示。
                    bodies = [""]
                    linkage_steps = 1

                body_start_idx = 0
                if start_from_current_var.get() and scene_idx == scene_start_idx:
                    try:
                        body_start_idx = bodies.index(current_body)
                    except ValueError:
                        body_start_idx = 0

                if use_linkage:
                    # 联动模式：衣服/时间端可循环，用来带动已勾选的表情/红晕/特殊/圣光走完一轮。
                    step_count = max(1, len(bodies), linkage_steps)
                    for offset in range(step_count):
                        body_idx = (body_start_idx + offset) % len(bodies)
                        body_label = bodies[body_idx]
                        sequence.append({
                            "scene": scene_name,
                            "body": body_label,
                            "scene_index": scene_idx + 1,
                            "scene_total": len(scene_values),
                            "body_index": body_idx + 1,
                            "body_total": len(bodies),
                            "link_step": offset,
                            "linkage_enabled": True,
                        })
                else:
                    for body_idx, body_label in enumerate(bodies[body_start_idx:], start=body_start_idx):
                        sequence.append({
                            "scene": scene_name,
                            "body": body_label,
                            "scene_index": scene_idx + 1,
                            "scene_total": len(scene_values),
                            "body_index": body_idx + 1,
                            "body_total": len(bodies),
                            "link_step": 0,
                            "linkage_enabled": False,
                        })
            return sequence

        def finish_test() -> None:
            state["running"] = False
            state["after_id"] = None
            total = len(state.get("sequence") or [])
            done = min(int(state.get("index") or 0), total)
            progress_var.set(100.0 if total else 0.0)
            status_var.set(format_test_status(done, total, str(state.get("last_scene") or ""), str(state.get("last_body") or "")) + "｜完成")
            start_button.state(["!disabled"])
            stop_button.state(["disabled"])
            close_button.state(["!disabled"])
            if auto_pick_next_dir_var.get():
                win.destroy()
                self.after(120, self._pick_lsf_dir)

        def stop_test() -> None:
            state["running"] = False
            after_id = state.get("after_id")
            if after_id:
                try:
                    win.after_cancel(after_id)
                except Exception:
                    pass
            state["after_id"] = None
            total = len(state.get("sequence") or [])
            done = min(int(state.get("index") or 0), total)
            status_var.set(format_test_status(done, total, str(state.get("last_scene") or ""), str(state.get("last_body") or "")) + "｜已停止")
            start_button.state(["!disabled"])
            stop_button.state(["disabled"])
            close_button.state(["!disabled"])

        def run_next() -> None:
            if not state.get("running"):
                return
            sequence = list(state.get("sequence") or [])
            total = len(sequence)
            idx = int(state.get("index") or 0)
            if idx >= total:
                finish_test()
                return

            item = sequence[idx]
            scene_name = str(item.get("scene", ""))
            body_label = str(item.get("body", ""))
            state["last_scene"] = scene_name
            state["last_body"] = body_label

            # 场景变化时先加载这个 LSF，但不立刻刷新；随后切到指定衣服/时间端并刷新一次。
            if self.scene_var.get() != scene_name or self.scene is None or self.scene.project.lsf_path.name != scene_name:
                self.scene_var.set(scene_name)
                self._load_selected_scene(refresh=False)
            if body_label:
                self.body_var.set(body_label)
            # 自动化测试现在会真正应用“联动设置”：第 1 步保留当前/默认表情，
            # 从第 2 步开始每次推进已勾选的联动项，避免跳过初始表情。
            if bool(item.get("linkage_enabled")) and int(item.get("link_step") or 0) > 0:
                self._advance_enabled_linkage_controls()
            self.refresh_preview()

            idx += 1
            state["index"] = idx

            percent = 100.0 if total <= 0 else idx * 100.0 / total
            progress_var.set(percent)
            status_var.set(format_test_status(idx, total, scene_name, body_label))

            if idx >= total:
                finish_test()
            else:
                state["after_id"] = win.after(parse_interval_ms(), run_next)

        def start_test() -> None:
            sequence = build_sequence()
            if not sequence:
                messagebox.showinfo("提示", "没有可测试的 LSF / 衣服选项。", parent=win)
                return
            parse_interval_ms()
            save_current_automation_settings()
            state["sequence"] = sequence
            state["index"] = 0
            state["start_time"] = time.time()
            state["last_scene"] = ""
            state["last_body"] = ""
            state["running"] = True
            progress_var.set(0.0)
            status_var.set(format_test_status(0, len(sequence)))
            start_button.state(["disabled"])
            stop_button.state(["!disabled"])
            close_button.state(["disabled"])
            run_next()

        start_button = ttk.Button(btns, text="开始测试", command=start_test)
        start_button.pack(side="left")
        stop_button = ttk.Button(btns, text="停止", command=stop_test)
        stop_button.pack(side="left", padx=(8, 0))
        stop_button.state(["disabled"])
        close_button = ttk.Button(btns, text="关闭", command=win.destroy)
        close_button.pack(side="right")
        win.protocol("WM_DELETE_WINDOW", lambda: stop_test() or win.destroy())
        self._place_popup_like_left_panel(win)
        if auto_start:
            win.after(120, start_test)


class JSONTab(BaseTab):
    def __init__(self, master: tk.Misc):
        super().__init__(master, "JSON")
        self.json_files: list[Path] = []
        self.scene: Optional[JSONScene] = None
        self.resolver: Optional[PNGResolver] = None
        self.current_image: Optional[Image.Image] = None

        self.json_input_var = tk.StringVar()
        self.png_var = tk.StringVar()
        self.scene_var = tk.StringVar()
        self.body_var = tk.StringVar()
        self.linkage_vars: dict[str, tk.BooleanVar] = {}
        self.linkage_summary_var = tk.StringVar(value="联动：未开启")
        self.expression_var = tk.StringVar()
        self.blush_var = tk.StringVar()
        self.stats_var = tk.StringVar(value="当前目录统计：JSON 0 个，PNG 0 个")
        self._build_ui()

    def _build_ui(self) -> None:
        input_box = ttk.LabelFrame(self.left, text="输入")
        input_box.pack(fill="x", pady=(0, 8))
        self._path_row(input_box, "JSON 目录", self.json_input_var, self._pick_json_dir, self._on_dir_changed)
        self._path_row(input_box, "PNG 目录", self.png_var, self._pick_png, self._on_dir_changed)
        self._compact_button_bar(input_box, [
            ("加载 JSON 项目", self.load_project),
            ("导出当前 PNG", self.export_current),
            ("批量导出当前组合", self.open_batch_export_dialog),
        ])

        stats_box = ttk.LabelFrame(self.left, text="目录统计")
        stats_box.pack(fill="x", pady=(0, 8))
        ttk.Label(stats_box, textvariable=self.stats_var, justify="left", anchor="w").pack(fill="x", padx=8, pady=8)

        linkage_box = ttk.LabelFrame(self.left, text="联动")
        linkage_box.pack(fill="x", pady=(0, 8))
        ttk.Button(linkage_box, text="联动设置", command=self.open_linkage_dialog).pack(fill="x", padx=8, pady=(8, 2))
        ttk.Label(linkage_box, textvariable=self.linkage_summary_var, justify="left", anchor="w").pack(fill="x", padx=8, pady=(0, 8))

        options = ttk.LabelFrame(self.left, text="组合选项")
        options.pack(fill="x")
        ttk.Label(options, text="人物或者场景").pack(anchor="w", padx=8, pady=(8, 2))
        self.scene_combo = ttk.Combobox(options, textvariable=self.scene_var, state="readonly", width=48)
        self.scene_combo.pack(fill="x", padx=8)
        self.scene_combo.bind("<<ComboboxSelected>>", lambda e: self._load_selected_scene())

        self.body_label = ttk.Label(options, text="衣服或者其他时间端")
        self.body_label.pack(anchor="w", padx=8, pady=(8, 2))
        self.body_combo = ttk.Combobox(options, textvariable=self.body_var, state="readonly", width=48)
        self.body_combo.pack(fill="x", padx=8)
        self.body_combo.bind("<<ComboboxSelected>>", lambda e: self._on_body_selected())

        ttk.Label(options, text="表情").pack(anchor="w", padx=8, pady=(8, 2))
        self.expression_combo = ttk.Combobox(options, textvariable=self.expression_var, state="readonly", width=48)
        self.expression_combo.pack(fill="x", padx=8)
        self.expression_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_preview())

        ttk.Label(options, text="红晕").pack(anchor="w", padx=8, pady=(8, 2))
        self.blush_combo = ttk.Combobox(options, textvariable=self.blush_var, state="readonly", width=48)
        self.blush_combo.pack(fill="x", padx=8, pady=(0, 8))
        self.blush_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_preview())

        self.create_info_box()
        self.create_preview_area()

    def _label_is_none_choice(self, label: str) -> bool:
        return label.startswith("(") or label.startswith("原图")

    def _advance_combobox(self, combo: ttk.Combobox, var: tk.StringVar, *, skip_none_choices: bool = False) -> bool:
        values = list(combo["values"] or [])
        if not values or str(combo.cget("state")) == "disabled":
            return False

        current = var.get()
        try:
            current_idx = values.index(current)
        except ValueError:
            current_idx = -1

        usable_indices = list(range(len(values)))
        if skip_none_choices and len(values) > 1:
            real_indices = [i for i, label in enumerate(values) if not self._label_is_none_choice(str(label))]
            if real_indices:
                usable_indices = real_indices

        if current_idx in usable_indices:
            pos = usable_indices.index(current_idx)
            next_idx = usable_indices[(pos + 1) % len(usable_indices)]
        else:
            next_idx = next((i for i in usable_indices if i > current_idx), usable_indices[0])

        var.set(values[next_idx])
        return True

    def _get_linkage_var(self, key: str) -> tk.BooleanVar:
        if key not in self.linkage_vars:
            self.linkage_vars[key] = tk.BooleanVar(value=False)
        return self.linkage_vars[key]

    def _iter_link_targets(self):
        yield "expression", "表情", self.expression_combo, self.expression_var
        yield "blush", "红晕", self.blush_combo, self.blush_var

    def _combo_has_real_options(self, combo: ttk.Combobox) -> bool:
        values = list(combo["values"] or [])
        if str(combo.cget("state")) == "disabled":
            return False
        return any(not self._label_is_none_choice(str(v)) for v in values)

    def _update_linkage_summary(self) -> None:
        selected = []
        for key, label, combo, _var in self._iter_link_targets():
            if self._get_linkage_var(key).get() and self._combo_has_real_options(combo):
                selected.append(label)
        if selected:
            self.linkage_summary_var.set("联动：" + "、".join(selected))
        else:
            self.linkage_summary_var.set("联动：未开启")

    def open_linkage_dialog(self) -> None:
        win = tk.Toplevel(self)
        win.title("联动设置")
        win.transient(self.winfo_toplevel())
        win.grab_set()
        win.resizable(False, False)

        ttk.Label(
            win,
            text="勾选后：手动切换“衣服或者其他时间端”时，下面这些选项会各自切到下一项。",
            wraplength=360,
            justify="left",
        ).pack(fill="x", padx=12, pady=(12, 8))

        frame = ttk.LabelFrame(win, text="可联动项目")
        frame.pack(fill="both", expand=True, padx=12, pady=(0, 8))

        for key, label, combo, _var in self._iter_link_targets():
            bool_var = self._get_linkage_var(key)
            available = self._combo_has_real_options(combo)
            text = label if available else f"{label}（当前无可用选项）"
            cb = ttk.Checkbutton(frame, text=text, variable=bool_var, command=self._update_linkage_summary)
            cb.pack(anchor="w", padx=8, pady=3)
            if not available:
                cb.state(["disabled"])

        btns = ttk.Frame(win)
        btns.pack(fill="x", padx=12, pady=(0, 12))

        def select_all_available() -> None:
            for key, _label, combo, _var in self._iter_link_targets():
                if self._combo_has_real_options(combo):
                    self._get_linkage_var(key).set(True)
            self._update_linkage_summary()

        def clear_all() -> None:
            for key, _label, _combo, _var in self._iter_link_targets():
                self._get_linkage_var(key).set(False)
            self._update_linkage_summary()

        ttk.Button(btns, text="全选可用", command=select_all_available).pack(side="left")
        ttk.Button(btns, text="清空", command=clear_all).pack(side="left", padx=(8, 0))
        ttk.Button(btns, text="关闭", command=win.destroy).pack(side="right")
        self._place_popup_like_left_panel(win)

    def _on_body_selected(self) -> None:
        for key, _label, combo, var in self._iter_link_targets():
            if self._get_linkage_var(key).get() and self._combo_has_real_options(combo):
                self._advance_combobox(combo, var, skip_none_choices=True)
        self.refresh_preview()

    def _pick_json_dir(self) -> None:
        folder = filedialog.askdirectory()
        if folder:
            self.json_input_var.set(folder)
            self.png_var.set(folder)

    def _pick_png(self) -> None:
        folder = filedialog.askdirectory()
        if folder:
            self.png_var.set(folder)

    def _on_dir_changed(self) -> None:
        json_count = count_dir_files(self.json_input_var.get(), ("*.json",))
        png_count = count_dir_files(self.png_var.get(), ("*.png",))
        self.stats_var.set(f"当前目录统计：JSON {json_count} 个，PNG {png_count} 个")

    def _clear_current_preview_image(self) -> None:
        self.current_image = None
        if hasattr(self, "preview"):
            self.preview.show_image(None)

    def _release_loaded_resources(self) -> dict[str, object | None]:
        # 切换目录时只把旧资源从前台摘下，不在主线程立即清理。
        return self._detach_loaded_resources()

    def load_project(self) -> None:
        old_resources = self._release_loaded_resources()
        try:
            self.json_files = collect_input_files(self.json_input_var.get(), ("*.json",))
            if not self.json_files:
                raise ProjectError("请先选择包含 JSON 的目录。")
            if not self.png_var.get().strip():
                self.png_var.set(self.json_input_var.get().strip())
            if not self.png_var.get().strip():
                raise ProjectError("请先选择 PNG 目录。")
            self.resolver = PNGResolver(self.png_var.get().strip())
            self._on_dir_changed()
            scene_names = [p.name for p in self.json_files]
            self.scene_combo["values"] = scene_names
            self.scene_var.set(scene_names[0])
            self._load_selected_scene()
            self._schedule_background_resource_cleanup(old_resources)
        except Exception as exc:
            self._schedule_background_resource_cleanup(old_resources)
            messagebox.showerror("加载失败", str(exc))

    def _load_selected_scene(self) -> None:
        try:
            selected = self.scene_var.get().strip()
            if not selected:
                return
            path = next((p for p in self.json_files if p.name == selected), None)
            if not path:
                return
            self._clear_current_preview_image()
            self.scene = analyze_json_scene(parse_json_project(path))
            self.body_combo["values"] = [x.label for x in self.scene.body_options]
            self.expression_combo["values"] = [x.label for x in self.scene.expression_options]
            self.blush_combo["values"] = [x.label for x in self.scene.blush_options]
            if len(self.scene.body_options) > 1 and self.scene.body_options[0].key == "__none__":
                self.body_var.set(self.scene.body_options[1].label)
            else:
                self.body_var.set(self.scene.body_options[0].label if self.scene.body_options else "")
            if len(self.scene.expression_options) > 1:
                self.expression_var.set(self.scene.expression_options[1].label)
            else:
                self.expression_var.set(self.scene.expression_options[0].label if self.scene.expression_options else "")
            self.blush_var.set(self.scene.blush_options[0].label if self.scene.blush_options else "")
            self._update_linkage_summary()
            self.refresh_preview()
        except Exception as exc:
            messagebox.showerror("读取 JSON 失败", str(exc))

    def _find_option(self, options: list[LSFOption], selected_label: str) -> Optional[LSFOption]:
        for item in options:
            if item.label == selected_label:
                return item
        return options[0] if options else None

    def refresh_preview(self) -> None:
        if not self.scene or not self.resolver:
            return
        body = self._find_option(self.scene.body_options, self.body_var.get())
        expr = self._find_option(self.scene.expression_options, self.expression_var.get())
        blush = self._find_option(self.scene.blush_options, self.blush_var.get())
        image, warnings, layers = compose_json_scene(
            self.scene,
            self.resolver,
            body if body and body.records is not None else None,
            None if expr and expr.key == "__none__" else expr,
            None if blush and blush.key == "__none__" else blush,
            runtime_workers=CPU_LOGICAL_THREADS,
        )
        self.current_image = image
        self.preview.show_image(image)
        lines = [
            f"JSON: {self.scene.project.json_path.name}",
            f"画布: {self.scene.project.canvas_width} x {self.scene.project.canvas_height}",
            f"已加载 JSON 数: {len(self.json_files)}",
            f"已索引 PNG 数: {len(self.resolver.by_stem) if self.resolver else 0}",
            CPU_INFO_TEXT,
            f"当前默认工作线程: {DEFAULT_THREAD_COUNT}",
            f"运行时预览 PNG 解码线程: {CPU_LOGICAL_THREADS}",
            f"衣服或者其他: {body.label if body else '(无)'}",
            f"表情: {expr.label if expr else '(无表情)'}",
            f"红晕: {blush.label if blush else '(无红晕)'}",
            f"当前合成图层数: {len(layers)}",
            "",
            "分析结果:",
            *[f"  - {n}" for n in self.scene.notes],
        ]
        if warnings:
            lines += ["", "警告:", *[f"  - {w}" for w in warnings]]
        self._set_info(lines)

    def export_current(self) -> None:
        if self.current_image is None:
            messagebox.showinfo("提示", "没有可导出的预览图。")
            return
        out = filedialog.asksaveasfilename(defaultextension=".png", filetypes=[("PNG files", "*.png")])
        if out:
            self.current_image.save(out)
            messagebox.showinfo("完成", f"已导出: {out}")


    def _target_values_from_combo(self, combo: ttk.Combobox, *, real_only: bool = True) -> list[str]:
        values = [str(v) for v in list(combo["values"] or [])]
        if real_only:
            real = [v for v in values if not self._label_is_none_choice(v)]
            return real if real else values
        return values

    def _advance_label_value(self, values: list[str], current: str, *, skip_none_choices: bool = True) -> str:
        if not values:
            return current
        usable = values
        if skip_none_choices and len(values) > 1:
            real = [v for v in values if not self._label_is_none_choice(v)]
            if real:
                usable = real
        try:
            current_idx = values.index(current)
        except ValueError:
            current_idx = -1
        usable_indices = [values.index(v) for v in usable if v in values]
        if not usable_indices:
            return current
        if current_idx in usable_indices:
            pos = usable_indices.index(current_idx)
            return values[usable_indices[(pos + 1) % len(usable_indices)]]
        next_idx = next((i for i in usable_indices if i > current_idx), usable_indices[0])
        return values[next_idx]

    def _iter_batch_targets(self):
        yield "body", "衣服或者其他时间端", self.body_combo, self.body_var
        yield from self._iter_link_targets()

    def _current_json_selection(self) -> dict[str, str]:
        return {
            "body": self.body_var.get(),
            "expression": self.expression_var.get(),
            "blush": self.blush_var.get(),
        }

    def _compose_json_selection(self, selection: dict[str, str]) -> tuple[Image.Image, list[str], list]:
        if not self.scene or not self.resolver:
            raise ProjectError("请先加载 JSON 项目。")
        body = self._find_option(self.scene.body_options, selection.get("body", self.body_var.get()))
        expr = self._find_option(self.scene.expression_options, selection.get("expression", self.expression_var.get()))
        blush = self._find_option(self.scene.blush_options, selection.get("blush", self.blush_var.get()))
        return compose_json_scene(
            self.scene,
            self.resolver,
            body if body and body.records is not None else None,
            None if expr and expr.key == "__none__" else expr,
            None if blush and blush.key == "__none__" else blush,
        )

    def _selection_filename(self, index: int, selection: dict[str, str], selected_keys: list[str]) -> str:
        scene_stem = self.scene.project.stem if self.scene else "scene"
        parts = [safe_filename_part(scene_stem, 40), f"{index:04d}"]
        for key, label, _combo, _var in self._iter_batch_targets():
            if key in selected_keys:
                parts.append(safe_filename_part(selection.get(key, ""), 32))
        return "__".join([p for p in parts if p])

    def _values_from_labels(self, values: list[str], *, real_only: bool = True) -> list[str]:
        values = [str(v) for v in values]
        if real_only:
            real = [v for v in values if not self._label_is_none_choice(v)]
            return real if real else values
        return values

    def _json_targets_for_scene(self, scene: JSONScene) -> list[tuple[str, str, list[str]]]:
        return [
            ("body", "衣服或者其他时间端", [x.label for x in scene.body_options]),
            ("expression", "表情", [x.label for x in scene.expression_options]),
            ("blush", "红晕", [x.label for x in scene.blush_options]),
        ]

    def _default_json_selection_for_scene(self, scene: JSONScene) -> dict[str, str]:
        selection: dict[str, str] = {}
        if scene.body_options:
            if len(scene.body_options) > 1 and scene.body_options[0].key == "__none__":
                selection["body"] = scene.body_options[1].label
            else:
                selection["body"] = scene.body_options[0].label
        if scene.expression_options:
            selection["expression"] = scene.expression_options[1].label if len(scene.expression_options) > 1 else scene.expression_options[0].label
        if scene.blush_options:
            selection["blush"] = scene.blush_options[0].label
        return selection

    def _compose_json_selection_for_scene(self, scene: JSONScene, selection: dict[str, str]) -> tuple[Image.Image, list[str], list]:
        if not self.resolver:
            raise ProjectError("请先加载 PNG 目录。")
        body = self._find_option(scene.body_options, selection.get("body", ""))
        expr = self._find_option(scene.expression_options, selection.get("expression", ""))
        blush = self._find_option(scene.blush_options, selection.get("blush", ""))
        return compose_json_scene(
            scene,
            self.resolver,
            body if body and body.records is not None else None,
            None if expr and expr.key == "__none__" else expr,
            None if blush and blush.key == "__none__" else blush,
        )

    def _selection_filename_for_json_scene(self, scene: JSONScene, index: int, selection: dict[str, str], selected_keys: list[str]) -> str:
        parts = [safe_filename_part(scene.project.stem, 40), f"{index:04d}"]
        for key, _label, _values in self._json_targets_for_scene(scene):
            if key in selected_keys:
                parts.append(safe_filename_part(selection.get(key, ""), 32))
        return "__".join([p for p in parts if p])

    def _estimate_json_scene_export(self, scene: JSONScene, selected_keys: list[str], mode: str) -> int:
        targets = self._json_targets_for_scene(scene)
        valid_keys = [k for k in selected_keys if any(t[0] == k for t in targets)]
        if mode == "product":
            estimate = 1
            any_selected = False
            for key, _label, values in targets:
                if key in valid_keys:
                    any_selected = True
                    estimate *= max(1, len(self._values_from_labels(values, real_only=True)))
            return estimate if any_selected else 1
        if "body" in valid_keys:
            body_values = next((values for key, _label, values in targets if key == "body"), [])
            return max(1, len(self._values_from_labels(body_values, real_only=True)))
        counts = [len(self._values_from_labels(values, real_only=True)) for key, _label, values in targets if key in valid_keys]
        return max(counts or [1])

    def _iter_json_scene_export_jobs(self, scene: JSONScene, selected_keys: list[str], mode: str, current: Optional[dict[str, str]] = None):
        current = dict(current or self._default_json_selection_for_scene(scene))
        targets = self._json_targets_for_scene(scene)
        valid_selected_keys = [k for k in selected_keys if any(t[0] == k for t in targets)]
        filename_keys = valid_selected_keys or ["body"]

        if mode == "product":
            value_lists: list[tuple[str, list[str]]] = []
            for key, _label, values in targets:
                if key in valid_selected_keys:
                    vals = self._values_from_labels(values, real_only=True)
                    if vals:
                        value_lists.append((key, vals))
            combos = itertools.product(*[vals for _key, vals in value_lists]) if value_lists else [()]
            for idx, values in enumerate(combos, start=1):
                selection = dict(current)
                for (key, _vals), value in zip(value_lists, values):
                    selection[key] = value
                yield idx, scene, selection, filename_keys
        else:
            count = self._estimate_json_scene_export(scene, valid_selected_keys, mode)
            selection = dict(current)
            for idx in range(1, count + 1):
                yield idx, scene, dict(selection), filename_keys
                if idx < count:
                    for key, _label, values in targets:
                        if key in valid_selected_keys:
                            selection[key] = self._advance_label_value(values, selection.get(key, ""), skip_none_choices=True)

    def _collect_json_batch_jobs(self, selected_keys: list[str], mode: str, scope: str):
        if not self.scene or not self.resolver:
            raise ProjectError("请先加载 JSON 项目。")
        if scope == "directory":
            jobs = []
            for path in self.json_files:
                scene = analyze_json_scene(parse_json_project(path))
                jobs.extend(self._iter_json_scene_export_jobs(scene, selected_keys, mode))
            return jobs
        return list(self._iter_json_scene_export_jobs(self.scene, selected_keys, mode, self._current_json_selection()))

    def _export_json_batch_job(self, out_dir: Path, job, filename_lock: threading.Lock, reserved_paths: set[str]) -> int:
        idx, scene, selection, filename_keys = job
        img, warnings, _layers = self._compose_json_selection_for_scene(scene, selection)
        filename = self._selection_filename_for_json_scene(scene, idx, selection, filename_keys)
        with filename_lock:
            out_path = make_unique_png_path_reserved(out_dir, filename, reserved_paths)
        img.save(out_path)
        return len(warnings)

    def _run_json_batch_export_threaded(
        self,
        out_dir: Path,
        selected_keys: list[str],
        mode: str,
        scope: str = "current",
        thread_count: int = 4,
        progress_callback=None,
    ) -> tuple[int, int]:
        if not self.scene or not self.resolver:
            raise ProjectError("请先加载 JSON 项目。")
        out_dir.mkdir(parents=True, exist_ok=True)
        jobs = self._collect_json_batch_jobs(selected_keys, mode, scope)
        total = len(jobs)
        if progress_callback:
            progress_callback(0, total, 0)
        if not jobs:
            return 0, 0

        max_workers = normalize_thread_count(thread_count)
        filename_lock = threading.Lock()
        reserved_paths: set[str] = set()
        completed = 0
        warnings_total = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._export_json_batch_job, out_dir, job, filename_lock, reserved_paths)
                for job in jobs
            ]
            for future in concurrent.futures.as_completed(futures):
                try:
                    warnings_total += future.result()
                except Exception:
                    for f in futures:
                        f.cancel()
                    raise
                completed += 1
                if progress_callback:
                    progress_callback(completed, total, warnings_total)
        return completed, warnings_total

    def _run_json_batch_export_for_scene(self, scene: JSONScene, out_dir: Path, selected_keys: list[str], mode: str, current: Optional[dict[str, str]] = None) -> tuple[int, int]:
        current = dict(current or self._default_json_selection_for_scene(scene))
        targets = self._json_targets_for_scene(scene)
        selected_keys = [k for k in selected_keys if any(t[0] == k for t in targets)]
        warnings_total = 0
        exported = 0

        if mode == "product":
            value_lists: list[tuple[str, list[str]]] = []
            for key, _label, values in targets:
                if key in selected_keys:
                    vals = self._values_from_labels(values, real_only=True)
                    if vals:
                        value_lists.append((key, vals))
            combos = itertools.product(*[vals for _key, vals in value_lists]) if value_lists else [()]
            for idx, values in enumerate(combos, start=1):
                selection = dict(current)
                for (key, _vals), value in zip(value_lists, values):
                    selection[key] = value
                img, warnings, _layers = self._compose_json_selection_for_scene(scene, selection)
                warnings_total += len(warnings)
                filename = self._selection_filename_for_json_scene(scene, idx, selection, selected_keys or ["body"])
                img.save(make_unique_png_path(out_dir, filename))
                exported += 1
        else:
            count = self._estimate_json_scene_export(scene, selected_keys, mode)
            selection = dict(current)
            for idx in range(1, count + 1):
                img, warnings, _layers = self._compose_json_selection_for_scene(scene, selection)
                warnings_total += len(warnings)
                filename = self._selection_filename_for_json_scene(scene, idx, selection, selected_keys or ["body"])
                img.save(make_unique_png_path(out_dir, filename))
                exported += 1
                if idx < count:
                    for key, _label, values in targets:
                        if key in selected_keys:
                            selection[key] = self._advance_label_value(values, selection.get(key, ""), skip_none_choices=True)
        return exported, warnings_total

    def _run_json_batch_export(self, out_dir: Path, selected_keys: list[str], mode: str, scope: str = "current") -> tuple[int, int]:
        if not self.scene or not self.resolver:
            raise ProjectError("请先加载 JSON 项目。")
        out_dir.mkdir(parents=True, exist_ok=True)
        if scope == "directory":
            exported = 0
            warnings_total = 0
            for path in self.json_files:
                scene = analyze_json_scene(parse_json_project(path))
                e, w = self._run_json_batch_export_for_scene(scene, out_dir, selected_keys, mode)
                exported += e
                warnings_total += w
            return exported, warnings_total
        return self._run_json_batch_export_for_scene(self.scene, out_dir, selected_keys, mode, self._current_json_selection())

    def open_batch_export_dialog(self) -> None:
        if not self.scene or not self.resolver:
            messagebox.showinfo("提示", "请先加载 JSON 项目。")
            return

        win = tk.Toplevel(self)
        win.title("批量导出当前组合")
        win.transient(self.winfo_toplevel())
        win.grab_set()
        win.resizable(False, False)

        default_folder = Path(self.png_var.get().strip() or ".").expanduser() / "batch_export"
        out_var = tk.StringVar(value=str(default_folder))
        mode_var = tk.StringVar(value="sequence")
        scope_var = tk.StringVar(value="current")
        thread_var = tk.StringVar(value=DEFAULT_THREAD_COUNT)
        check_vars: dict[str, tk.BooleanVar] = {}

        ttk.Label(
            win,
            text="选择哪些下拉项参与批量导出。默认按当前“联动设置”勾选；也可以手动改。",
            wraplength=420,
            justify="left",
        ).pack(fill="x", padx=12, pady=(12, 8))

        out_frame = ttk.LabelFrame(win, text="输出目录")
        out_frame.pack(fill="x", padx=12, pady=(0, 8))
        row = ttk.Frame(out_frame)
        row.pack(fill="x", padx=8, pady=8)
        ttk.Entry(row, textvariable=out_var, width=44).pack(side="left", fill="x", expand=True)

        def choose_out_dir() -> None:
            folder = filedialog.askdirectory(parent=win)
            if folder:
                out_var.set(folder)

        def use_loaded_dir() -> None:
            folder = self.json_input_var.get().strip() or self.png_var.get().strip()
            if folder:
                out_var.set(folder)

        ttk.Button(row, text="选择", command=choose_out_dir, width=8).pack(side="left", padx=(6, 0))
        ttk.Button(row, text="当前目录", command=use_loaded_dir, width=10).pack(side="left", padx=(6, 0))

        mode_frame = ttk.LabelFrame(win, text="导出方式")
        mode_frame.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Radiobutton(mode_frame, text="联动序列：以当前选择为起点，逐项切换时间端；勾选项跟着下一项", variable=mode_var, value="sequence").pack(anchor="w", padx=8, pady=(6, 2))
        ttk.Radiobutton(mode_frame, text="全组合：把勾选项的所有真实选项全部排列组合导出", variable=mode_var, value="product").pack(anchor="w", padx=8, pady=(2, 6))

        scope_frame = ttk.LabelFrame(win, text="导出范围")
        scope_frame.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Radiobutton(scope_frame, text="只导出当前选中的 JSON", variable=scope_var, value="current").pack(anchor="w", padx=8, pady=(6, 2))
        ttk.Radiobutton(scope_frame, text="导出当前加载目录里的全部 JSON", variable=scope_var, value="directory").pack(anchor="w", padx=8, pady=(2, 6))

        thread_frame = ttk.LabelFrame(win, text="多线程")
        thread_frame.pack(fill="x", padx=12, pady=(0, 8))
        thread_row = ttk.Frame(thread_frame)
        thread_row.pack(fill="x", padx=8, pady=8)
        ttk.Label(thread_row, text="导出线程数量").pack(side="left")
        thread_combo = ttk.Combobox(
            thread_row,
            textvariable=thread_var,
            values=THREAD_COUNT_CHOICES,
            state="readonly",
            width=6,
        )
        thread_combo.pack(side="left", padx=(8, 8))
        ttk.Label(thread_row, text=f"{CPU_INFO_TEXT}；默认使用全部逻辑线程").pack(side="left")

        target_frame = ttk.LabelFrame(win, text="参与批量的选项")
        target_frame.pack(fill="both", expand=True, padx=12, pady=(0, 8))

        targets = list(self._iter_batch_targets())
        for key, label, combo, _var in targets:
            available = self._combo_has_real_options(combo)
            default_checked = key == "body" or (key in self.linkage_vars and self._get_linkage_var(key).get())
            var = tk.BooleanVar(value=bool(default_checked and available))
            check_vars[key] = var
            text = label if available else f"{label}（当前无可用选项）"
            cb = ttk.Checkbutton(target_frame, text=text, variable=var)
            cb.pack(anchor="w", padx=8, pady=2)
            if not available:
                cb.state(["disabled"])

        btns1 = ttk.Frame(win)
        btns1.pack(fill="x", padx=12, pady=(0, 8))

        def apply_linkage_setting() -> None:
            for key, _label, combo, _var in targets:
                if not self._combo_has_real_options(combo):
                    check_vars[key].set(False)
                elif key == "body":
                    check_vars[key].set(True)
                else:
                    check_vars[key].set(self._get_linkage_var(key).get())

        def select_all_available() -> None:
            for key, _label, combo, _var in targets:
                check_vars[key].set(self._combo_has_real_options(combo))

        def clear_all() -> None:
            for key in check_vars:
                check_vars[key].set(False)

        ttk.Button(btns1, text="按联动设置选择", command=apply_linkage_setting).pack(side="left")
        ttk.Button(btns1, text="全选可用", command=select_all_available).pack(side="left", padx=(8, 0))
        ttk.Button(btns1, text="清空", command=clear_all).pack(side="left", padx=(8, 0))

        progress_var = tk.DoubleVar(value=0.0)
        progress_text_var = tk.StringVar(value="进度：未开始")
        progress_frame = ttk.LabelFrame(win, text="导出进度")
        progress_frame.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Progressbar(progress_frame, maximum=100, variable=progress_var).pack(fill="x", padx=8, pady=(8, 4))
        ttk.Label(progress_frame, textvariable=progress_text_var, justify="left", anchor="w").pack(fill="x", padx=8, pady=(0, 8))

        btns2 = ttk.Frame(win)
        btns2.pack(fill="x", padx=12, pady=(0, 12))

        def start_export() -> None:
            out_text = out_var.get().strip()
            if not out_text:
                messagebox.showinfo("提示", "请选择输出目录。", parent=win)
                return
            out_dir = Path(out_text).expanduser()
            selected_keys = [key for key, var in check_vars.items() if var.get()]
            selected_mode = mode_var.get()
            selected_scope = scope_var.get()
            try:
                thread_count = int(thread_var.get() or DEFAULT_THREAD_COUNT)
            except Exception:
                thread_count = int(DEFAULT_THREAD_COUNT)

            estimate = 1
            if selected_mode == "product":
                for key, _label, combo, _var in targets:
                    if key in selected_keys:
                        estimate *= max(1, len(self._target_values_from_combo(combo, real_only=True)))
            else:
                if "body" in selected_keys:
                    estimate = max(1, len(self._target_values_from_combo(self.body_combo, real_only=True)))
                else:
                    estimate = max([len(self._target_values_from_combo(combo, real_only=True)) for key, _label, combo, _var in targets if key in selected_keys] or [1])
            if selected_scope == "directory":
                try:
                    estimate = sum(
                        self._estimate_json_scene_export(analyze_json_scene(parse_json_project(path)), selected_keys, selected_mode)
                        for path in self.json_files
                    )
                except Exception:
                    estimate = max(1, estimate) * max(1, len(self.json_files))
            if estimate > 800 and not messagebox.askyesno("确认", f"预计会导出约 {estimate} 张 PNG，是否继续？", parent=win):
                return

            export_queue: queue.Queue = queue.Queue()
            start_time = time.time()
            progress_var.set(0.0)
            progress_text_var.set(batch_progress_text(0, estimate, start_time))
            start_button.state(["disabled"])
            close_button.state(["disabled"])
            win.protocol("WM_DELETE_WINDOW", lambda: None)

            def progress_callback(done: int, total: int, warnings_count: int) -> None:
                export_queue.put(("progress", done, total, warnings_count))

            def worker() -> None:
                try:
                    exported, warnings_total = self._run_json_batch_export_threaded(
                        out_dir,
                        selected_keys,
                        selected_mode,
                        selected_scope,
                        thread_count,
                        progress_callback,
                    )
                    export_queue.put(("done", exported, warnings_total, time.time() - start_time))
                except Exception as exc:
                    export_queue.put(("error", str(exc)))

            def poll_queue() -> None:
                finished = None
                try:
                    while True:
                        message = export_queue.get_nowait()
                        kind = message[0]
                        if kind == "progress":
                            _kind, done, total, _warnings_count = message
                            percent = 100.0 if total <= 0 else done * 100.0 / total
                            progress_var.set(percent)
                            progress_text_var.set(batch_progress_text(done, total, start_time))
                        elif kind == "done":
                            finished = message
                            _kind, exported, _warnings_total, _elapsed = message
                            progress_var.set(100.0)
                            progress_text_var.set(batch_progress_text(exported, exported, start_time))
                        elif kind == "error":
                            finished = message
                            progress_text_var.set(f"导出失败：{message[1]}")
                except queue.Empty:
                    pass

                if finished is None:
                    win.after(100, poll_queue)
                    return

                start_button.state(["!disabled"])
                close_button.state(["!disabled"])
                win.protocol("WM_DELETE_WINDOW", win.destroy)
                if finished[0] == "done":
                    _kind, exported, warnings_total, elapsed = finished
                    messagebox.showinfo(
                        "完成",
                        f"已导出 {exported} 张 PNG。\n输出目录：{out_dir}\n警告数量：{warnings_total}\n用时：{format_duration(elapsed)}\n线程数量：{thread_count}",
                        parent=win,
                    )
                else:
                    messagebox.showerror("批量导出失败", finished[1], parent=win)

            threading.Thread(target=worker, daemon=True).start()
            poll_queue()

        start_button = ttk.Button(btns2, text="开始导出", command=start_export)
        start_button.pack(side="left")
        close_button = ttk.Button(btns2, text="关闭", command=win.destroy)
        close_button.pack(side="right")
        self._place_popup_like_left_panel(win)


def center_main_window(root: tk.Tk, width: int = 1435, height: int = 936) -> None:
    """根据使用者当前屏幕分辨率，将主窗口自动居中。"""

    def apply() -> None:
        root.update_idletasks()
        screen_w = root.winfo_screenwidth()
        screen_h = root.winfo_screenheight()
        x = max(0, (screen_w - width) // 2)
        y = max(0, (screen_h - height) // 2)
        root.geometry(f"{width}x{height}+{x}+{y}")

    apply()
    # Windows 下窗口创建后可能被系统再次摆放，延迟校准一次，确保启动时居中。
    root.after(50, apply)


class App(ttk.Frame):
    def __init__(self, master: tk.Tk):
        super().__init__(master)
        master.title(TITLE)
        apply_window_icon(master)
        center_main_window(master, 1435, 936)
        self.pack(fill="both", expand=True)
        # 只保留主功能界面，不再显示顶部功能标签。
        LSFTab(self).pack(fill="both", expand=True)


def run_app() -> None:
    root = tk.Tk()
    App(root)
    root.mainloop()
