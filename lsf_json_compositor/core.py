from __future__ import annotations

import concurrent.futures
import json
import re
import struct
import threading
import time
from collections import OrderedDict, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from PIL import Image

TEXT_ENCODINGS = ("utf-8", "utf-8-sig", "cp932", "shift_jis", "gbk", "utf-16")


class ProjectError(Exception):
    pass


def natural_sort_key(text: str):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", text)]


def read_json_any(path: Path) -> Any:
    raw = path.read_bytes()
    for enc in TEXT_ENCODINGS:
        try:
            return json.loads(raw.decode(enc))
        except Exception:
            continue
    return json.loads(raw.decode("utf-8", errors="replace"))


def collect_input_files(input_text: str, patterns: tuple[str, ...]) -> list[Path]:
    text = (input_text or "").strip()
    if not text:
        return []
    p = Path(text).expanduser()
    results: list[Path] = []
    if p.is_dir():
        for pattern in patterns:
            results.extend(sorted(p.glob(pattern), key=lambda x: natural_sort_key(x.name)))
    elif p.is_file():
        results.append(p)
    unique: list[Path] = []
    seen: set[str] = set()
    for path in results:
        key = str(path.resolve()).lower() if path.exists() else str(path).lower()
        if key not in seen:
            unique.append(path)
            seen.add(key)
    return unique


@dataclass(slots=True)
class LSFRecord:
    index: int
    name: str
    left: int
    top: int
    right: int
    bottom: int
    unk1: int
    unk2: int
    tag: int
    unk3: int
    unk4: int

    @property
    def width(self) -> int:
        return max(0, self.right - self.left)

    @property
    def height(self) -> int:
        return max(0, self.bottom - self.top)

    @property
    def area(self) -> int:
        return self.width * self.height

    @property
    def slot_code(self) -> int:
        return self.tag & 0xFF

    @property
    def variant_code(self) -> int:
        return (self.tag >> 8) & 0xFF

    @property
    def tag_label(self) -> str:
        return f"{self.slot_code:02X}-{self.variant_code:02X}"

    @property
    def label(self) -> str:
        return f"{self.name} [{self.width}x{self.height}, tag={self.tag_label}]"


@dataclass(slots=True)
class LSFProject:
    lsf_path: Path
    canvas_width: int
    canvas_height: int
    records: list[LSFRecord]
    header_canvas1: tuple[int, int]
    header_canvas2: tuple[int, int]

    @property
    def stem(self) -> str:
        return self.lsf_path.stem


@dataclass(slots=True)
class LSFOption:
    key: str
    label: str
    records: list[LSFRecord]


@dataclass(slots=True)
class LSFScene:
    project: LSFProject
    fixed_records: list[LSFRecord]
    body_options: list[LSFOption]
    expression_groups: list[tuple[str, list[LSFOption]]]
    blush_groups: list[tuple[str, list[LSFOption]]]
    special_groups: list[tuple[str, list[LSFOption]]]
    holy_options: list[LSFOption]
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class JSONLayer:
    layer_id: int
    name: str
    group_id: Optional[int]
    group_label: str
    left: int
    top: int
    width: int
    height: int
    visible: bool
    draw_index: int

    @property
    def area(self) -> int:
        return max(0, self.width) * max(0, self.height)

    @property
    def label(self) -> str:
        return f"{self.name} [{self.layer_id}] ({self.width}x{self.height})"


@dataclass(slots=True)
class JSONProject:
    json_path: Path
    canvas_width: int
    canvas_height: int
    groups: dict[str, list[JSONLayer]]
    fixed_layers: list[JSONLayer] = field(default_factory=list)

    @property
    def stem(self) -> str:
        return self.json_path.stem


@dataclass(slots=True)
class JSONScene:
    project: JSONProject
    fixed_layers: list[JSONLayer]
    body_options: list[LSFOption]
    expression_options: list[LSFOption]
    blush_options: list[LSFOption]
    notes: list[str] = field(default_factory=list)


def parse_lsf_file(path: str | Path) -> LSFProject:
    path = Path(path)
    data = path.read_bytes()
    if len(data) < 28 or data[:4] != b"LSF\x00":
        raise ProjectError("不是有效的 LSF 文件。")

    _sig, _v1, _v2, _reserved0, count, c1w, _c1w_hi, c1h, _c1h_hi, c2w, _c2w_hi, c2h, _c2h_hi = struct.unpack("<4s12H", data[:28])

    records: list[LSFRecord] = []
    offset = 28
    rec_size = 164
    for idx in range(count):
        chunk = data[offset:offset + rec_size]
        if len(chunk) < rec_size:
            break
        name = chunk[:128].split(b"\0", 1)[0].decode("utf-8", errors="ignore")
        vals = struct.unpack("<9I", chunk[128:])
        records.append(
            LSFRecord(
                index=idx,
                name=name,
                left=vals[0],
                top=vals[1],
                right=vals[2],
                bottom=vals[3],
                unk1=vals[4],
                unk2=vals[5],
                tag=vals[6],
                unk3=vals[7],
                unk4=vals[8],
            )
        )
        offset += rec_size

    if not records:
        raise ProjectError("LSF 中没有可用记录。")

    max_right = max(r.right for r in records)
    max_bottom = max(r.bottom for r in records)
    canvas_width = max(c1w, c2w, max_right)
    canvas_height = max(c1h, c2h, max_bottom)

    return LSFProject(
        lsf_path=path,
        canvas_width=canvas_width,
        canvas_height=canvas_height,
        records=records,
        header_canvas1=(c1w, c1h),
        header_canvas2=(c2w, c2h),
    )


def _first_int(d: dict[str, Any], keys: list[str], default: int = 0) -> int:
    for key in keys:
        if key in d and d[key] is not None:
            try:
                return int(d[key])
            except Exception:
                continue
    return default


def _first_bool(d: dict[str, Any], keys: list[str], default: bool = True) -> bool:
    for key in keys:
        if key in d and d[key] is not None:
            value = d[key]
            if isinstance(value, bool):
                return value
            if isinstance(value, (int, float)):
                return bool(value)
            if isinstance(value, str):
                return value.strip().lower() not in {"0", "false", "no", "off"}
    return default


def _first_str(d: dict[str, Any], keys: list[str], default: str = "") -> str:
    for key in keys:
        if key in d and d[key] is not None:
            return str(d[key])
    return default


def parse_json_project(path: str | Path) -> JSONProject:
    path = Path(path)
    data = read_json_any(path)

    items: list[dict[str, Any]]
    if isinstance(data, list):
        items = [x for x in data if isinstance(x, dict)]
        root = {}
    elif isinstance(data, dict):
        root = data
        if isinstance(data.get("layers"), list):
            items = [x for x in data["layers"] if isinstance(x, dict)]
        elif isinstance(data.get("items"), list):
            items = [x for x in data["items"] if isinstance(x, dict)]
        else:
            items = [x for x in data.values() if isinstance(x, dict) and ("layer_id" in x or "id" in x)]
    else:
        raise ProjectError("JSON 结构不受支持。")

    if not items:
        raise ProjectError("JSON 中没有可用图层。")

    child_to_parent: dict[int, Optional[int]] = {}
    raw_map: dict[int, dict[str, Any]] = {}
    parent_to_children: dict[int, list[int]] = defaultdict(list)

    canvas_width = _first_int(root, ["canvas_width", "width", "w"], 0)
    canvas_height = _first_int(root, ["canvas_height", "height", "h"], 0)

    for idx, item in enumerate(items):
        layer_id = _first_int(item, ["layer_id", "id"], idx)
        group_id_raw = item.get("group_layer_id", item.get("group_id", item.get("parent_id")))
        try:
            group_id = int(group_id_raw) if group_id_raw is not None else None
        except Exception:
            group_id = None

        raw_map[layer_id] = item
        child_to_parent[layer_id] = group_id
        if group_id is not None:
            parent_to_children[group_id].append(layer_id)
        canvas_width = max(canvas_width, _first_int(item, ["right"], 0), _first_int(item, ["left", "x"], 0) + _first_int(item, ["width", "w"], 0))
        canvas_height = max(canvas_height, _first_int(item, ["bottom"], 0), _first_int(item, ["top", "y"], 0) + _first_int(item, ["height", "h"], 0))

    def top_label(layer_id: int) -> str:
        seen = set()
        current = layer_id
        label = _first_str(raw_map.get(layer_id, {}), ["name", "label", "title"], str(layer_id))
        while current in child_to_parent and child_to_parent[current] is not None:
            parent = child_to_parent[current]
            if parent in seen or parent not in raw_map:
                break
            seen.add(parent)
            current = parent
            parent_name = _first_str(raw_map[parent], ["name", "label", "title"], str(parent))
            if parent_name:
                label = parent_name
        return label or "Layers"

    layers: list[JSONLayer] = []
    for idx, item in enumerate(items):
        layer_id = _first_int(item, ["layer_id", "id"], idx)
        if layer_id in parent_to_children:
            continue
        left = _first_int(item, ["left", "x"], 0)
        top = _first_int(item, ["top", "y"], 0)
        width = _first_int(item, ["width", "w"], max(0, _first_int(item, ["right"], 0) - left))
        height = _first_int(item, ["height", "h"], max(0, _first_int(item, ["bottom"], 0) - top))
        visible = _first_bool(item, ["visible", "is_visible"], True)
        group_id_raw = item.get("group_layer_id", item.get("group_id", item.get("parent_id")))
        try:
            group_id = int(group_id_raw) if group_id_raw is not None else None
        except Exception:
            group_id = None

        layers.append(
            JSONLayer(
                layer_id=layer_id,
                name=_first_str(item, ["name", "label", "title"], str(layer_id)),
                group_id=group_id,
                group_label=top_label(layer_id),
                left=left,
                top=top,
                width=width,
                height=height,
                visible=visible,
                draw_index=idx,
            )
        )

    groups: dict[str, list[JSONLayer]] = defaultdict(list)
    fixed_layers: list[JSONLayer] = []
    for layer in sorted(layers, key=lambda x: x.draw_index):
        groups[layer.group_label].append(layer)

    final_groups: dict[str, list[JSONLayer]] = {}
    for label, items_in_group in sorted(groups.items(), key=lambda kv: natural_sort_key(kv[0])):
        unique_ids = {x.layer_id for x in items_in_group}
        if len(unique_ids) == 1:
            layer = items_in_group[0]
            if layer.visible:
                fixed_layers.append(layer)
            else:
                final_groups[label] = items_in_group
            continue
        visible_items = [x for x in items_in_group if x.visible]
        if len(visible_items) == 1 and len(items_in_group) == 1:
            fixed_layers.extend(visible_items)
        else:
            final_groups[label] = sorted(items_in_group, key=lambda x: (x.draw_index, natural_sort_key(x.name)))

    if canvas_width <= 0 or canvas_height <= 0:
        canvas_width = max((x.left + x.width for x in layers), default=1500)
        canvas_height = max((x.top + x.height for x in layers), default=2500)

    return JSONProject(
        json_path=path,
        canvas_width=canvas_width,
        canvas_height=canvas_height,
        groups=final_groups,
        fixed_layers=sorted(fixed_layers, key=lambda x: x.draw_index),
    )


class PNGResolver:
    # 预览时只缓存最近使用的少量 PNG。旧版使用装饰在类方法上的
    # lru_cache(maxsize=4096)，缓存会跨 PNGResolver 实例保留：用户切换目录后，
    # 上一个目录加载过的图片仍被全局缓存引用，导致内存持续增长。
    # 现在改为每个 PNGResolver 实例独立的、可分批释放的 LRU 缓存。GUI 切换目录后，
    # 会先完成新目录加载，再把旧目录缓存丢给后台线程慢慢清理，避免加载新目录时卡顿。
    DEFAULT_IMAGE_CACHE_SIZE = 128

    def __init__(self, png_dir: str | Path, image_cache_size: int = DEFAULT_IMAGE_CACHE_SIZE):
        self.png_dir = Path(png_dir).expanduser().resolve()
        if not self.png_dir.exists():
            raise ProjectError("PNG 目录不存在。")
        self.by_stem: dict[str, Path] = {}
        self.by_suffix: dict[str, Path] = {}
        self._image_cache_size = max(1, int(image_cache_size))
        self._image_cache: OrderedDict[str, Image.Image] = OrderedDict()
        self._image_cache_lock = threading.RLock()
        self._build_index()

    def _build_index(self) -> None:
        for path in self.png_dir.glob("*.png"):
            stem_lower = path.stem.lower()
            self.by_stem[stem_lower] = path
            m = re.search(r"(?:_|-)(\d+)$", path.stem)
            if m:
                self.by_suffix[m.group(1)] = path

    def find_for_lsf(self, record_name: str) -> Optional[Path]:
        return self.by_stem.get(record_name.lower())

    def find_for_json_layer(self, scene_stem: str, layer_id: int) -> Optional[Path]:
        candidates = [
            f"{scene_stem}_{layer_id}".lower(),
            f"{scene_stem}-{layer_id}".lower(),
            f"{layer_id}".lower(),
        ]
        for c in candidates:
            if c in self.by_stem:
                return self.by_stem[c]
        return self.by_suffix.get(str(layer_id))

    def _load_rgba_uncached(self, path_str: str) -> Image.Image:
        # 使用 with 立即关闭文件句柄，只把 convert 后的 RGBA 图像交给缓存。
        with Image.open(path_str) as img:
            return img.convert("RGBA")

    def load_rgba(self, path_str: str) -> Image.Image:
        key = str(path_str)
        with self._image_cache_lock:
            cached = self._image_cache.get(key)
            if cached is not None:
                self._image_cache.move_to_end(key)
                return cached

        image = self._load_rgba_uncached(key)

        with self._image_cache_lock:
            # 其他线程可能刚刚加载了同一张图，优先复用已有对象。
            cached = self._image_cache.get(key)
            if cached is not None:
                self._image_cache.move_to_end(key)
                return cached
            self._image_cache[key] = image
            self._image_cache.move_to_end(key)
            while len(self._image_cache) > self._image_cache_size:
                self._image_cache.popitem(last=False)
        return image

    def clear_cache(self) -> None:
        with self._image_cache_lock:
            self._image_cache.clear()

    def clear_cache_gradually(self, batch_size: int = 8, delay_seconds: float = 0.02) -> None:
        """分批释放缓存，给后台清理线程使用，避免一次性释放大量图片造成前台卡顿。"""
        batch_size = max(1, int(batch_size))
        delay_seconds = max(0.0, float(delay_seconds))
        while True:
            victims: list[tuple[str, Image.Image]] = []
            with self._image_cache_lock:
                for _ in range(batch_size):
                    if not self._image_cache:
                        break
                    victims.append(self._image_cache.popitem(last=False))
            if not victims:
                break
            # 离开锁后再释放 Image 对象，避免阻塞正在使用同一 resolver 的操作。
            victims.clear()
            if delay_seconds:
                time.sleep(delay_seconds)

def _record_group_by_tag(records: list[LSFRecord]) -> dict[int, dict[int, list[LSFRecord]]]:
    slots: dict[int, dict[int, list[LSFRecord]]] = defaultdict(lambda: defaultdict(list))
    for rec in sorted(records, key=lambda r: (r.index, natural_sort_key(r.name))):
        slots[rec.slot_code][rec.variant_code].append(rec)
    return slots


def _label_from_records(prefix: str, variant_code: int, recs: list[LSFRecord]) -> str:
    if recs:
        return recs[0].name
    return f"{prefix} {variant_code:02X}"



def _record_suffix_num(name: str) -> int:
    m = re.search(r"(\d+)$", name or "")
    return int(m.group(1)) if m else -1



def _choose_slot00_body_display_record(recs: list[LSFRecord], canvas_area: int = 0) -> Optional[LSFRecord]:
    """
    Pick a good display label source for ADV/EV-style body variants.

    Priority:
    1) if a variant contains mid==3 helper records, use the first following mid==0 record
       as the display layer (for example EV_A02_019 / 021 / 023 / 025)
    2) otherwise use the last mid==0 body record in index order
       (for example EV_A02_013 / 015 or EV_D07_001 ~ 007)
    """
    if not recs:
        return None
    ordered = sorted(recs, key=lambda r: r.index)

    saw_mid3 = False
    for r in ordered:
        if r.slot_code == 0 and ((r.tag >> 16) & 0xFF) == 3:
            saw_mid3 = True
            continue
        if saw_mid3 and r.slot_code == 0 and ((r.tag >> 16) & 0xFF) == 0:
            return r

    mid0 = [r for r in ordered if r.slot_code == 0 and ((r.tag >> 16) & 0xFF) == 0]
    if mid0:
        return mid0[-1]
    return ordered[0]


def _choose_non_adv_body_display_record(
    exact_records: list[LSFRecord],
    body_slots: set[int],
    slot_meta: dict[int, dict[str, float | int]],
) -> Optional[LSFRecord]:
    """
    Pick a display label source for non-ADV standing portrait style body combinations.

    When multiple body slots are merged, we should prefer the record coming from the
    slot that actually carries the visible outfit/arm variant, rather than a large
    shared torso/base layer. Example: for 01_Tsugumi we want labels like
    01_Tsugumi_002 / 003 / 004 / 007 ... instead of repeated 01_Tsugumi_006.
    """
    exact_records = [r for r in exact_records if not _is_helper_mask_record(r)] or exact_records
    if not exact_records:
        return None

    # Prefer records from body slots with more variants; these are usually the
    # real selectable outfit/arm layer slots.
    ordered_slots = sorted(
        body_slots,
        key=lambda s: (
            int(slot_meta.get(s, {}).get("variant_count", 0)),
            -float(slot_meta.get(s, {}).get("max_ratio", 0.0)),
            s,
        ),
        reverse=True,
    )
    by_slot = {}
    for r in exact_records:
        by_slot.setdefault(r.slot_code, []).append(r)

    for slot in ordered_slots:
        if slot in by_slot:
            # Within the preferred slot, choose the smaller/more specific visible layer first.
            recs = sorted(by_slot[slot], key=lambda r: (r.area, r.index, natural_sort_key(r.name)))
            return recs[0]

    return sorted(exact_records, key=lambda r: (r.area, r.index, natural_sort_key(r.name)))[0]


def _filter_body_records_for_display(recs: list[LSFRecord]) -> list[LSFRecord]:
    """
    Drop mid==3 helper mask layers from body composition.
    They are usually white/black assist masks; if composited normally they can
    become opaque rectangles over the face.
    """
    filtered = [r for r in recs if not _is_helper_mask_record(r)]
    return filtered if filtered else recs




def _split_common_body_keys(
    common_keys: set[tuple],
    canvas_area: int,
) -> tuple[set[tuple], set[tuple]]:
    """Split shared body records into fixed layers vs hidden-by-default overlays.

    ADV/EV CGs often place helper masks or large local white cover layers in slot00
    and re-use them across several time/body variants. If those records are promoted
    to fixed layers, the preview opens with a white rectangle/large white body block.

    Keep only truly full-canvas shared layers as fixed. Helper-mask records and
    partial slot00 overlays are hidden by default; they can still be represented by
    the normal variant option if that variant explicitly needs a visible counterpart.
    """
    fixed_keys: set[tuple] = set()
    hidden_keys: set[tuple] = set()
    for key in common_keys:
        _name, left, top, right, bottom, slot_code, mid_code = key
        area = max(0, right - left) * max(0, bottom - top)
        ratio = area / max(1, canvas_area)

        # mid==3 is an engine helper/mask. Never make it a fixed normal layer.
        if mid_code == 3:
            hidden_keys.add(key)
            continue

        # For ADV/EV slot00, shared partial overlays are usually mask/cover pairs.
        # A10 has EV_A10_028/029 here; promoting them caused the default white block.
        if slot_code == 0 and ratio < 0.95:
            hidden_keys.add(key)
            continue

        fixed_keys.add(key)
    return fixed_keys, hidden_keys



def _mid_code(rec: LSFRecord) -> int:
    return (rec.tag >> 16) & 0xFF


def _is_helper_mask_record(rec: LSFRecord) -> bool:
    """mid==3 is used by this engine for rectangular helper/mask layers.

    These layers are useful for the original renderer, but when composited as normal
    PNG layers they often become the white rectangles that cover the character's face.
    """
    return _mid_code(rec) == 3


def _unique_record_names(recs: list[LSFRecord]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for r in recs:
        if r.name not in seen:
            seen.add(r.name)
            names.append(r.name)
    return names


def _format_adv_time_label(
    variant_code: int,
    visible_recs: list[LSFRecord],
    fixed_records: list[LSFRecord],
    canvas_area: int,
) -> str:
    """Create stable, unique-ish labels for ADV/EV slot00 time/body variants.

    Tk comboboxes select by label text. Duplicate labels such as five rows all named
    EV_D10_047 make every click resolve to the first option. Prefixing with the
    variant/time number prevents that and makes the time-end list readable.
    """
    names = _unique_record_names(sorted(visible_recs, key=lambda r: (-r.area, r.index, natural_sort_key(r.name))))
    if not names:
        base_candidates = [
            r for r in fixed_records
            if r.slot_code == 0 and _mid_code(r) == 0 and r.area >= canvas_area * 0.80
        ]
        names = _unique_record_names(sorted(base_candidates, key=lambda r: (r.index, natural_sort_key(r.name))))

    if names:
        shown = " + ".join(names[:3])
        if len(names) > 3:
            shown += f" + ...({len(names)})"
        return f"时间端 {variant_code:02X}: {shown}"
    return f"时间端 {variant_code:02X}"


def _make_option_labels_unique(options: list[LSFOption]) -> None:
    """Ensure each option has a distinct label for reliable combobox selection."""
    counts: dict[str, int] = {}
    for opt in options:
        base = opt.label
        counts[base] = counts.get(base, 0) + 1
        if counts[base] > 1:
            opt.label = f"{base} [{counts[base]}]"


def _is_single_base_face_overlay_scene(
    project: LSFProject,
    slots: dict[int, dict[int, list[LSFRecord]]],
    canvas_area: int,
) -> bool:
    """Detect EV CGs that are one full base PNG plus small face overlay/mask PNGs.

    The samples EV_B10 / EV_B11 / EV_C13 / EV_D12 / EV_E07 have a single full-scene
    slot00 record and then slot0A/slot0B facial cover records. Treating those face
    records as the default body option makes the preview open with a white block on
    the face, so in this mode the default body option must be the plain base image.
    """
    slot0 = slots.get(0, {})
    if len(slot0) != 1:
        return False

    full_base = [
        r
        for recs in slot0.values()
        for r in recs
        if _mid_code(r) == 0 and r.area >= canvas_area * 0.80
    ]
    if not full_base:
        return False

    other_records = [
        r
        for slot, variants in slots.items()
        for recs in variants.values()
        for r in recs
        if r not in full_base
    ]
    if not other_records:
        return False

    # Must contain the known facial overlay slots. This avoids catching ordinary
    # single-background packages that only have unrelated foreground effects.
    if not any(slot in slots for slot in (0x0A, 0x0B)):
        return False

    # The rest should be small/medium overlays around the upper half of the canvas.
    for r in other_records:
        if r.area > canvas_area * 0.12:
            return False
        if r.top > project.canvas_height * 0.58:
            return False
    return True


def _build_single_base_face_overlay_scene(
    project: LSFProject,
    slots: dict[int, dict[int, list[LSFRecord]]],
    canvas_area: int,
) -> tuple[
    list[LSFRecord],
    list[LSFOption],
    list[tuple[str, list[LSFOption]]],
    list[tuple[str, list[LSFOption]]],
    list[tuple[str, list[LSFOption]]],
    list[str],
]:
    """Build controls for the single-base + face-overlay EV pattern.

    Files such as EV_B07 contain one full base CG in slot00 plus small face
    difference layers in slot0A/slot0B.  Earlier versions put those small layers
    into the body/time combobox, so the UI looked like it had no expression
    controls.  Keep the full CG as the fixed base image and expose slot0A as
    表情, slot0B as 红晕/脸部附加差分, and any other non-zero slots as 特殊.
    """
    fixed_records: list[LSFRecord] = []
    body_options: list[LSFOption] = [LSFOption("__none__", "原图", [])]
    expression_groups: list[tuple[str, list[LSFOption]]] = []
    blush_groups: list[tuple[str, list[LSFOption]]] = []
    special_groups: list[tuple[str, list[LSFOption]]] = []
    notes: list[str] = ["识别为单张 EV 底图 + 脸部差分，已将 slot0A/slot0B 拆成表情/红晕选项。"]

    for recs in slots.get(0, {}).values():
        for r in sorted(recs, key=lambda x: x.index):
            if _mid_code(r) == 0 and r.area >= canvas_area * 0.80:
                fixed_records.append(r)

    def build_slot_options(slot: int, none_label: str, key_prefix: str) -> list[LSFOption]:
        options: list[LSFOption] = [LSFOption("__none__", none_label, [])]
        seen_options: set[tuple] = set()
        for variant, recs in sorted(slots.get(slot, {}).items()):
            # Do not expose engine helper masks. They are the usual source of
            # opaque white rectangles over the face.
            visible = [r for r in sorted(recs, key=lambda x: x.index) if not _is_helper_mask_record(r)]
            if not visible:
                continue
            sig = tuple((r.name, r.left, r.top, r.right, r.bottom, _mid_code(r)) for r in visible)
            if sig in seen_options:
                continue
            seen_options.add(sig)
            label = visible[0].name if len(visible) == 1 else " + ".join(r.name for r in visible)
            options.append(LSFOption(f"{key_prefix}_{slot:02X}_{variant:02X}", label, visible))
        return options

    expr_index = 1
    blush_index = 1
    special_index = 1
    for slot in sorted(s for s in slots.keys() if s != 0):
        if slot == 0x0A:
            opts = build_slot_options(slot, "(无表情)", "expr_face")
            if len(opts) > 1:
                expression_groups.append((f"表情{expr_index}", opts))
                expr_index += 1
        elif slot == 0x0B:
            opts = build_slot_options(slot, "(无红晕)", "blush_face")
            if len(opts) > 1:
                blush_groups.append((f"红晕{blush_index}", opts))
                blush_index += 1
        else:
            opts = build_slot_options(slot, "(无特殊)", "special_face")
            if len(opts) > 1:
                special_groups.append((f"特殊{special_index}", opts))
                special_index += 1

    return fixed_records, body_options, expression_groups, blush_groups, special_groups, notes




def analyze_lsf_scene(project: LSFProject) -> LSFScene:
    slots = _record_group_by_tag(project.records)
    fixed_records: list[LSFRecord] = []
    body_options: list[LSFOption] = []
    holy_options: list[LSFOption] = [LSFOption("__none__", "(无圣光)", [])]
    notes: list[str] = []
    canvas_area = max(1, project.canvas_width * project.canvas_height)

    slot_meta: dict[int, dict[str, float | int]] = {}
    for slot, variants in slots.items():
        all_recs = [r for recs in variants.values() for r in recs]
        max_area = max((r.area for r in all_recs), default=0)
        avg_area = sum((r.area for r in all_recs), 0) / max(1, len(all_recs))
        avg_center_x = sum(((r.left + r.right) / 2 for r in all_recs), 0.0) / max(1, len(all_recs))
        avg_center_y = sum(((r.top + r.bottom) / 2 for r in all_recs), 0.0) / max(1, len(all_recs))
        slot_meta[slot] = {
            "variant_count": len(variants),
            "record_count": len(all_recs),
            "max_ratio": max_area / canvas_area,
            "avg_ratio": avg_area / canvas_area,
            "avg_center_x": avg_center_x,
            "avg_center_y": avg_center_y,
        }

    slot_ids = sorted(slots.keys())
    expression_groups: list[tuple[str, list[LSFOption]]] = []
    blush_groups: list[tuple[str, list[LSFOption]]] = []
    special_groups: list[tuple[str, list[LSFOption]]] = []

    known_standing_stems = {
        "01_Tsugumi",
        "02_Haruna",
        "03_Yachiyo",
        "04_Suzu",
        "05_Nanase",
        "06_Tsugumi_you",
        "07_Haruna_you",
        "08_Suzu_you",
    }

    # EV CGs like EV_B10 / EV_B11 / EV_C13 / EV_D12 / EV_E07 are one full base
    # image plus small face overlay/mask records. The old heuristic treated the
    # face overlay as the default body option, so the preview opened with a white
    # rectangle covering the face. Build them as: fixed full image + optional face
    # overlays, with the default option set to the plain original image.
    if _is_single_base_face_overlay_scene(project, slots, canvas_area):
        (
            fixed_records,
            body_options,
            expression_groups,
            blush_groups,
            special_groups,
            extra_notes,
        ) = _build_single_base_face_overlay_scene(project, slots, canvas_area)
        notes.extend(extra_notes)

    # Background-like package: only one slot and all variants are large full-scene choices.
    elif len(slot_ids) == 1 and slot_ids[0] == 0 and slot_meta[0]["variant_count"] > 1:
        for variant, recs in sorted(slots[0].items()):
            body_options.append(LSFOption(f"body_{variant:02X}", _label_from_records("背景", variant, recs), sorted(recs, key=lambda r: r.index)))
        notes.append("识别为背景/单槽多变体 LSF，已将 slot 00 作为衣服或者其他时间端选项。")
    else:
        body_slots: set[int] = set()
        expr_slots: set[int] = set()
        blush_slots: set[int] = set()
        holy_slots: set[int] = set()
        special_slots: set[int] = set()

        # ADV/EV CGs and单人立绘都会出现 slot00 多变体，不能只靠这一条判断。
        # 对于窄而高的 standing portrait（如 01_Tsugumi / 02_Haruna），如果误判成 ADV，
        # slot03/04/05 这些身体槽位会被拆散，导致手臂/衣服叠层顺序错误。
        adv_mode = (
            0 in slots
            and int(slot_meta.get(0, {}).get("variant_count", 0)) >= 2
            and project.canvas_width >= project.canvas_height * 0.70
        )
        known_standing_stems = {
            "01_Tsugumi",
            "02_Haruna",
            "03_Yachiyo",
            "04_Suzu",
            "05_Nanase",
            "06_Tsugumi_you",
            "07_Haruna_you",
            "08_Suzu_you",
        }

        portrait_mode = (
            (
                not adv_mode
                and project.canvas_height >= project.canvas_width * 1.7
                and 1 in slots
                and 2 in slots
                and (3 in slots or 4 in slots)
                and max(slot_ids or [0]) <= 8
            )
            or project.stem in known_standing_stems
        )

        if portrait_mode:
            for s in (0, 3, 4):
                if s in slots:
                    body_slots.add(s)

        # Strong pattern from known ADV/EV samples.
        if adv_mode:
            body_slots.add(0)
            for base_slot in range(0x0A, 0xF0, 0x0A):
                expr_slot = base_slot
                blush_slot = base_slot + 1
                if expr_slot in slots and int(slot_meta.get(expr_slot, {}).get("variant_count", 0)) >= 2:
                    expr_slots.add(expr_slot)
                if blush_slot in slots and int(slot_meta.get(blush_slot, {}).get("variant_count", 0)) >= 1:
                    blush_slots.add(blush_slot)
            if 0xFF in slots and int(slot_meta.get(0xFF, {}).get("variant_count", 0)) >= 1:
                holy_slots.add(0xFF)

        # Older single-character style only when not in ADV/EV mode.
        if not adv_mode:
            if 3 in slots and int(slot_meta.get(3, {}).get("variant_count", 0)) >= 2:
                body_slots.add(3)
            if 1 in slots and int(slot_meta.get(1, {}).get("variant_count", 0)) >= 2:
                expr_slots.add(1)
            if 2 in slots and int(slot_meta.get(2, {}).get("variant_count", 0)) >= 1:
                blush_slots.add(2)

            if project.stem in known_standing_stems:
                if 1 in slots:
                    expr_slots = {1}
                if 2 in slots:
                    blush_slots = {2}

            # 单人立绘类 LSF 常把“手臂/袖子/外套叠层”分到额外槽位里。
            # 这些槽位如果不并入 body_slots，就会导致手臂被躯干盖住，
            # 出现“手在后面”的问题（例如 01_Tsugumi）。
            for slot in slot_ids:
                if slot in body_slots | expr_slots | blush_slots | holy_slots:
                    continue
                meta = slot_meta[slot]
                variant_count = int(meta["variant_count"])
                max_ratio = float(meta["max_ratio"])
                avg_center_y = float(meta.get("avg_center_y", 0.0))
                # 规则：多变体、不是典型脸部槽、面积中等以上或中心不在脸部区域，
                # 就当成身体附加槽并进 body_slots。
                if (
                    variant_count >= 2
                    and (
                        max_ratio >= 0.015
                        or avg_center_y >= project.canvas_height * 0.35
                    )
                ):
                    body_slots.add(slot)

        # Additional heuristic fallback.
        for slot in slot_ids:
            meta = slot_meta[slot]
            variant_count = int(meta["variant_count"])
            max_ratio = float(meta["max_ratio"])
            if slot == 0xFF:
                holy_slots.add(slot)
            elif slot in expr_slots or slot in blush_slots or slot in body_slots:
                continue
            elif adv_mode and 0 < slot < 0x0A:
                # ADV/EV mode sometimes uses extra local overlay slots here (for example EV_B05 slot 01).
                # Do not misclassify them as facial expression groups; expose them as separate special groups.
                if variant_count >= 2:
                    special_slots.add(slot)
            elif variant_count >= 2 and max_ratio >= 0.12:
                body_slots.add(slot)
            elif variant_count >= 5 and max_ratio < 0.12:
                expr_slots.add(slot)
            elif 2 <= variant_count <= 4 and max_ratio < 0.08:
                blush_slots.add(slot)

        # If still no body slot, choose the largest multi-variant slot except dedicated slots.
        if not body_slots:
            multi_slots = [s for s in slot_ids if int(slot_meta[s]["variant_count"]) >= 2 and s not in expr_slots | blush_slots | holy_slots | special_slots]
            if multi_slots:
                largest_slot = max(multi_slots, key=lambda s: (float(slot_meta[s]["max_ratio"]), int(slot_meta[s]["variant_count"])))
                body_slots.add(largest_slot)

        expr_slots -= body_slots | holy_slots | special_slots
        blush_slots -= body_slots | expr_slots | holy_slots | special_slots
        holy_slots -= body_slots | expr_slots | blush_slots | special_slots
        special_slots -= body_slots | expr_slots | blush_slots | holy_slots

        def _slot_order_value(slot: int) -> float:
            return float(slot_meta.get(slot, {}).get("avg_center_x", 0.0))

        def _rect_iou(a: LSFRecord, b: LSFRecord) -> float:
            x1 = max(a.left, b.left)
            y1 = max(a.top, b.top)
            x2 = min(a.right, b.right)
            y2 = min(a.bottom, b.bottom)
            inter = max(0, x2 - x1) * max(0, y2 - y1)
            if inter <= 0:
                return 0.0
            area_a = max(1, a.width * a.height)
            area_b = max(1, b.width * b.height)
            return inter / float(area_a + area_b - inter)

        def _build_group_options(slot_list: list[int], none_label: str, kind_prefix: str) -> list[tuple[str, list[LSFOption]]]:
            groups: list[tuple[str, list[LSFOption]]] = []
            for idx, slot in enumerate(sorted(slot_list, key=_slot_order_value)):
                options: list[LSFOption] = [LSFOption("__none__", none_label, [])]

                if kind_prefix == "特殊":
                    slot_records = [r for recs in slots[slot].values() for r in recs]
                    visible_records = [r for r in slot_records if ((r.tag >> 16) & 0xFF) == 0]

                    visible_unique: list[LSFRecord] = []
                    seen_visible: set[tuple] = set()
                    for r in sorted(visible_records, key=lambda x: natural_sort_key(x.name)):
                        key = (r.name, r.left, r.top, r.right, r.bottom)
                        if key not in seen_visible:
                            seen_visible.add(key)
                            visible_unique.append(r)

                    if visible_unique:
                        # 对于特殊组，只按真正可见的主图层建立独立选项。
                        # 不再自动把 mid!=0 的辅助层挂回去，避免像 EV_B05 这类文件里
                        # 选择 EV_B05_021 时又把 EV_B05_020 一起叠上，造成“粘在一起”。
                        for i, vis in enumerate(visible_unique):
                            options.append(LSFOption(f"{kind_prefix}_{slot:02X}_{i + 1:02X}", vis.name, [vis]))
                        groups.append((f"{kind_prefix}{idx + 1}", options))
                        continue

                for variant, recs in sorted(slots[slot].items()):
                    recs_sorted = sorted(recs, key=lambda r: r.index)
                    label = _label_from_records(kind_prefix, variant, recs_sorted)
                    options.append(LSFOption(f"{kind_prefix}_{slot:02X}_{variant:02X}", label, recs_sorted))
                groups.append((f"{kind_prefix}{idx + 1}", options))
            return groups

        expression_groups = _build_group_options(sorted(expr_slots), "(无表情)", "表情")
        blush_groups = _build_group_options(sorted(blush_slots), "(无红晕)", "红晕")
        special_groups = _build_group_options(sorted(special_slots), "(无特殊)", "特殊")

        for slot in sorted(holy_slots):
            for variant, recs in sorted(slots[slot].items()):
                holy_options.append(LSFOption(f"holy_{slot:02X}_{variant:02X}", _label_from_records("圣光", variant, recs), sorted(recs, key=lambda r: r.index)))


        # Build body options.
        if body_slots:
            if portrait_mode:
                # Standing portrait mode.
                slot0_variants = {k: sorted(v, key=lambda r: r.index) for k, v in slots.get(0, {}).items()}
                slot3_variants = {k: sorted(v, key=lambda r: r.index) for k, v in slots.get(3, {}).items()}
                slot4_variants = {k: sorted(v, key=lambda r: r.index) for k, v in slots.get(4, {}).items()}
                slot5_variants = {k: sorted(v, key=lambda r: r.index) for k, v in slots.get(5, {}).items()}

                def _dedupe_records(recs: list[LSFRecord]) -> list[LSFRecord]:
                    out: list[LSFRecord] = []
                    seen: set[tuple] = set()
                    for r in recs:
                        key = (r.name, r.index)
                        if key not in seen:
                            seen.add(key)
                            out.append(r)
                    return out

                name_to_records: dict[str, list[LSFRecord]] = {}
                for r in project.records:
                    name_to_records.setdefault(r.name, []).append(r)

                def _named_bundle(names: list[str]) -> list[LSFRecord]:
                    recs: list[LSFRecord] = []
                    seen: set[tuple] = set()
                    for name in names:
                        for r in sorted(name_to_records.get(name, []), key=lambda x: x.index):
                            key = (r.name, r.index)
                            if key not in seen:
                                seen.add(key)
                                recs.append(r)
                    return recs

                # Detailed standing-portrait profiles reconstructed from the provided st package.
                standing_profiles: dict[str, dict[str, object]] = {
                    # 按 3.zip / 3(2).zip / 3(5).zip 的 st 单人立绘 LSF 顺序整理。
                    # 组合选项固定为：衣服或者其他时间端 / 表情1 / 红晕1 / 饰品 / 圣光
                    # 展示路线固定为：先裸体，裸体动作展示完，再下一件衣服。
                    "01_Tsugumi": {
                        "body": [
                            ("裸体 / 动作1", ["01_Tsugumi_001", "01_Tsugumi_002"]),
                            ("裸体 / 动作2", ["01_Tsugumi_001", "01_Tsugumi_003"]),
                            ("裸体 / 动作3", ["01_Tsugumi_001", "01_Tsugumi_004"]),
                            ("服装1 / 动作1", ["01_Tsugumi_001", "01_Tsugumi_006", "01_Tsugumi_007"]),
                            ("服装1 / 动作2", ["01_Tsugumi_001", "01_Tsugumi_006", "01_Tsugumi_008"]),
                            ("服装1 / 动作3", ["01_Tsugumi_001", "01_Tsugumi_006", "01_Tsugumi_009"]),
                            ("服装2 / 动作1", ["01_Tsugumi_001", "01_Tsugumi_010", "01_Tsugumi_011"]),
                            ("服装2 / 动作2", ["01_Tsugumi_001", "01_Tsugumi_010", "01_Tsugumi_012"]),
                            ("服装2 / 动作3", ["01_Tsugumi_001", "01_Tsugumi_010", "01_Tsugumi_013"]),
                        ],
                        "acc": [
                            ("(无饰品)", []),
                            ("01_Tsugumi_005 + 01_Tsugumi_014", ["01_Tsugumi_005", "01_Tsugumi_014"]),
                        ],
                    },
                    "02_Haruna": {
                        "body": [
                            ("裸体 / 动作1", ["02_Haruna_001", "02_Haruna_003"]),
                            ("裸体 / 动作2", ["02_Haruna_001", "02_Haruna_004"]),
                            ("裸体 / 动作3", ["02_Haruna_001", "02_Haruna_005"]),
                            ("服装1 / 动作1", ["02_Haruna_001", "02_Haruna_006", "02_Haruna_007", "02_Haruna_008"]),
                            ("服装1 / 动作2", ["02_Haruna_001", "02_Haruna_006", "02_Haruna_007", "02_Haruna_009"]),
                            ("服装1 / 动作3", ["02_Haruna_001", "02_Haruna_006", "02_Haruna_007", "02_Haruna_010"]),
                            ("服装2 / 动作1", ["02_Haruna_002", "02_Haruna_011", "02_Haruna_012"]),
                            ("服装2 / 动作2", ["02_Haruna_002", "02_Haruna_011", "02_Haruna_013"]),
                            ("服装2 / 动作3", ["02_Haruna_002", "02_Haruna_011", "02_Haruna_014"]),
                            ("服装3 / 动作1", ["02_Haruna_002", "02_Haruna_016", "02_Haruna_015"]),
                            ("服装3 / 动作2", ["02_Haruna_002", "02_Haruna_016", "02_Haruna_017"]),
                            ("服装3 / 动作3", ["02_Haruna_002", "02_Haruna_016", "02_Haruna_018"]),
                        ],
                        "acc": [
                            ("(无饰品)", []),
                            ("02_Haruna_019", ["02_Haruna_019"]),
                            ("02_Haruna_020", ["02_Haruna_020"]),
                        ],
                    },
                    "03_Yachiyo": {
                        "body": [
                            ("裸体 / 动作1", ["03_Yachiyo_001", "03_Yachiyo_002"]),
                            ("裸体 / 动作2", ["03_Yachiyo_001", "03_Yachiyo_003"]),
                            ("服装1 / 动作1", ["03_Yachiyo_001", "03_Yachiyo_004", "03_Yachiyo_005"]),
                            ("服装1 / 动作2", ["03_Yachiyo_001", "03_Yachiyo_004", "03_Yachiyo_006"]),
                            ("服装2 / 动作1", ["03_Yachiyo_001", "03_Yachiyo_007", "03_Yachiyo_002"]),
                            ("服装2 / 动作2", ["03_Yachiyo_001", "03_Yachiyo_007", "03_Yachiyo_008"]),
                            ("服装3 / 动作1", ["03_Yachiyo_009"]),
                        ],
                        "acc": [
                            ("(无饰品)", []),
                            ("03_Yachiyo_010", ["03_Yachiyo_010"]),
                        ],
                    },
                    "04_Suzu": {
                        "body": [
                            ("裸体 / 动作1", ["04_Suzu_001", "04_Suzu_002"]),
                            ("裸体 / 动作2", ["04_Suzu_001", "04_Suzu_003"]),
                            ("服装1 / 动作1", ["04_Suzu_001", "04_Suzu_004", "04_Suzu_013", "04_Suzu_005", "04_Suzu_007"]),
                            ("服装1 / 动作2", ["04_Suzu_001", "04_Suzu_004", "04_Suzu_013", "04_Suzu_006", "04_Suzu_008"]),
                            ("服装2 / 动作1", ["04_Suzu_001", "04_Suzu_009", "04_Suzu_013", "04_Suzu_010"]),
                            ("服装2 / 动作2", ["04_Suzu_001", "04_Suzu_009", "04_Suzu_013", "04_Suzu_011"]),
                        ],
                        "acc": [
                            ("(无饰品)", []),
                            ("04_Suzu_012", ["04_Suzu_012"]),
                        ],
                    },
                    "05_Nanase": {
                        "body": [
                            ("裸体 / 动作1", ["05_Nanase_001", "05_Nanase_002"]),
                            ("裸体 / 动作2", ["05_Nanase_001", "05_Nanase_003"]),
                            ("服装1 / 动作1", ["05_Nanase_001", "05_Nanase_004", "05_Nanase_013"]),
                            ("服装1 / 动作2", ["05_Nanase_001", "05_Nanase_004", "05_Nanase_011"]),
                            ("服装1 / 动作3", ["05_Nanase_001", "05_Nanase_004", "05_Nanase_009"]),
                            ("服装1 / 动作4", ["05_Nanase_001", "05_Nanase_004", "05_Nanase_008"]),
                            ("服装2 / 动作1", ["05_Nanase_001", "05_Nanase_005", "05_Nanase_012"]),
                            ("服装2 / 动作2", ["05_Nanase_001", "05_Nanase_005", "05_Nanase_010"]),
                            ("服装2 / 动作3", ["05_Nanase_001", "05_Nanase_005", "05_Nanase_007"]),
                            ("服装2 / 动作4", ["05_Nanase_001", "05_Nanase_005", "05_Nanase_006"]),
                        ],
                        "acc": [
                            ("(无饰品)", []),
                        ],
                    },
                    "06_Tsugumi_you": {
                        "body": [
                            ("裸体 / 动作1", ["06_Tsugumi_you_001"]),
                            ("裸体 / 动作2", ["06_Tsugumi_you_002"]),
                            ("服装1 / 动作1", ["06_Tsugumi_you_001", "06_Tsugumi_you_023"]),
                            ("服装2 / 动作1", ["06_Tsugumi_you_002", "06_Tsugumi_you_024"]),
                        ],
                        "acc": [
                            ("(无饰品)", []),
                            ("06_Tsugumi_you_025", ["06_Tsugumi_you_025"]),
                        ],
                    },
                    "07_Haruna_you": {
                        "body": [
                            ("裸体 / 动作1", ["07_Haruna_you_001"]),
                            ("服装1 / 动作1", ["07_Haruna_you_001", "07_Haruna_you_002"]),
                            ("服装2 / 动作1", ["07_Haruna_you_001", "07_Haruna_you_003"]),
                        ],
                        "acc": [
                            ("(无饰品)", []),
                            ("07_Haruna_you_004", ["07_Haruna_you_004"]),
                        ],
                    },
                    "08_Suzu_you": {
                        "body": [
                            ("裸体 / 动作1", ["08_Suzu_you_001"]),
                            ("服装1 / 动作1", ["08_Suzu_you_001", "08_Suzu_you_002"]),
                        ],
                        "acc": [
                            ("(无饰品)", []),
                            ("08_Suzu_you_003", ["08_Suzu_you_003"]),
                        ],
                    },
                }

                if project.stem in standing_profiles:
                    profile = standing_profiles[project.stem]

                    # 对 st 目录这批单人立绘，组合选项应固定成：
                    # 衣服或者其他时间端 / 表情1 / 红晕1 / 饰品 / 圣光
                    # 不能再把早先启发式识别出的“特殊1/特殊2...”带进来。
                    special_groups = []

                    body_options = []
                    for idx_profile, (label, names) in enumerate(profile["body"], start=1):
                        recs = _named_bundle(list(names))
                        if recs:
                            body_options.append(LSFOption(f"body_profile_{idx_profile:02d}", label, recs))

                    acc_entries = profile.get("acc", [])
                    accessory_options: list[LSFOption] = []
                    for idx_acc, (label, names) in enumerate(acc_entries, start=1):
                        recs = _named_bundle(list(names))
                        key = "__none__" if idx_acc == 1 and not names else f"acc_profile_{idx_acc:02d}"
                        accessory_options.append(LSFOption(key, label, recs))
                    if len(accessory_options) > 1:
                        special_groups.append(("饰品", accessory_options))

                    # st 单人立绘里：
                    # slot01 = 表情
                    # slot02 = 红晕
                    # slot05 = 饰品
                    # 这些都不应混入 fixed_records。
                    fixed_records = [r for r in fixed_records if r.slot_code not in {1, 2, 5}]

                    if 1 in slots:
                        expr_slots = {1}
                    else:
                        expr_slots = set()
                    if 2 in slots:
                        blush_slots = {2}
                    else:
                        blush_slots = set()

                    # 这里必须重建一次表情/红晕组。
                    # 因为前面 generic 路径已经先构建过 expression_groups / blush_groups，
                    # 如果不在 standing profile 分支里重建，界面就会继续显示 0 组，
                    # 导致看起来“没有表情”“没有红晕”。
                    expression_groups = _build_group_options(sorted(expr_slots), "(无表情)", "表情")
                    blush_groups = _build_group_options(sorted(blush_slots), "(无红晕)", "红晕")

                    body_slots |= {s for s in (0, 3, 4) if s in slots}
                    # slot05 只以“饰品”形式暴露；同时要把它标记为已处理，
                    # 否则后面的“未处理槽位 -> 固定层”流程又会把饰品塞回 fixed_records。
                    special_slots = {5} if 5 in slots else set()
                else:
                    # Generic standing portrait fallback.
                    def _family(v: int) -> int:
                        return 0 if v < 10 else v // 10

                    slot0_keys = sorted(slot0_variants.keys())
                    slot3_keys = sorted(slot3_variants.keys())
                    slot4_keys = sorted(slot4_variants.keys())

                    def _pick_base_variant(fam: int, fallback_variant: int | None = None) -> Optional[int]:
                        if not slot0_keys:
                            return None
                        if fam > 0 and fam in slot0_variants:
                            return fam
                        if fallback_variant is not None and fallback_variant in slot0_variants:
                            return fallback_variant
                        return slot0_keys[0]

                    def _pick_slot3_for_family(fam: int) -> Optional[int]:
                        if not slot3_keys or fam <= 0:
                            return None
                        for cand in (fam, fam * 10 + 1):
                            if cand in slot3_variants:
                                return cand
                        same_family = [k for k in slot3_keys if _family(k) == fam]
                        if same_family:
                            return same_family[0]
                        return None

                    # Put nude/body family first, then the rest of outfits in family order.
                    family_groups: dict[int, list[int]] = {}
                    for v in slot4_keys:
                        family_groups.setdefault(_family(v), []).append(v)
                    for fam in sorted(family_groups):
                        for v in sorted(family_groups[fam]):
                            recs: list[LSFRecord] = []
                            base_v = _pick_base_variant(fam, fallback_variant=v)
                            if base_v is not None:
                                recs.extend(slot0_variants.get(base_v, []))
                            slot3_v = _pick_slot3_for_family(fam)
                            if slot3_v is not None:
                                recs.extend(slot3_variants.get(slot3_v, []))
                            recs.extend(slot4_variants.get(v, []))
                            recs = _dedupe_records(recs)
                            if recs:
                                outfit_idx = fam if fam > 0 else 0
                                action_idx = sorted(family_groups[fam]).index(v) + 1
                                outfit_label = "裸体" if outfit_idx == 0 else f"服装{outfit_idx}"
                                label = f"{outfit_label} / 动作{action_idx}"
                                body_options.append(LSFOption(f"body_{v:02X}", label, recs))

                    # Any slot00 variants that are not already represented become standalone body options.
                    represented = {r.name for opt in body_options for r in opt.records}
                    for base_v in slot0_keys:
                        recs = _dedupe_records(list(slot0_variants[base_v]))
                        if recs and not all(r.name in represented for r in recs):
                            body_options.insert(0, LSFOption(f"body_base_{base_v:02X}", f"裸体 / 动作{base_v}", recs))

                    accessory_options: list[LSFOption] = [LSFOption("__none__", "(无饰品)", [])]
                    if slot5_variants:
                        for variant, recs in sorted(slot5_variants.items()):
                            recs_sorted = _dedupe_records(sorted(recs, key=lambda r: r.index))
                            if recs_sorted:
                                accessory_options.append(LSFOption(f"acc_{variant:02X}", recs_sorted[0].name, recs_sorted))
                        if len(accessory_options) > 1:
                            special_groups.append(("饰品", accessory_options))

                    # adv/gfx/face 这类包常见结构：
                    #   slot00：无衣服/基础脸部底图（有些角色没有）
                    #   slot03：衣服/身体底图或服装覆盖层
                    #   slot01：表情
                    #   slot02：红晕
                    # 旧的 generic standing fallback 只认 slot04 作为动作层；
                    # 当没有 slot04 时，slot03 没有被生成到“衣服或者其他时间端”，
                    # 界面就只剩“默认”空层，预览里只会看到眼睛/嘴巴等表情差分。
                    # adv/gfx/face 另一种结构：
                    #   slot03 = 真正的身体/衣服底图
                    #   slot04 = 帽子/头饰/局部覆盖层（有时还夹着 mid==3 遮罩）
                    # v16 只在“没有 slot04”时才把 slot03 重建成身体选项；
                    # 但 FD2 的 01_Kagome 同时存在 slot03 和 slot04，结果程序把 slot04 当成身体，
                    # 默认只合成帽子/脸部局部层，身体丢失。这里改成：只要 slot03 明显是大面积身体层，
                    # 就让 slot03 主导“衣服或者其他时间端”，slot04 作为可选饰品/覆盖层。
                    slot3_drives_face_body = (
                        bool(slot3_variants)
                        and (
                            not slot4_variants
                            or (
                                len(slot3_variants) >= 2
                                and float(slot_meta.get(3, {}).get("max_ratio", 0.0)) >= 0.18
                                and (
                                    not slot0_variants
                                    or float(slot_meta.get(0, {}).get("max_ratio", 0.0)) < 0.18
                                )
                                and len(slot4_variants) <= 3
                            )
                        )
                    )
                    if slot3_drives_face_body:
                        rebuilt_body: list[LSFOption] = []

                        def _records_label(recs: list[LSFRecord], fallback: str) -> str:
                            if len(recs) == 1:
                                return recs[0].name
                            if recs:
                                return " + ".join(r.name for r in recs[:3])
                            return fallback

                        base_variants_sorted = sorted(
                            slot0_variants.items(),
                            key=lambda kv: (min((r.index for r in kv[1]), default=999999), kv[0]),
                        )
                        slot3_variants_sorted = sorted(
                            slot3_variants.items(),
                            key=lambda kv: kv[0],
                        )

                        # 先放基础底图。若没有 slot00，则直接把 slot03 的每个变体当作完整身体底图。
                        default_base: list[LSFRecord] = []
                        if base_variants_sorted:
                            first_base_v, first_base_recs = base_variants_sorted[0]
                            default_base = _dedupe_records(sorted(first_base_recs, key=lambda r: r.index))
                            for base_v, base_recs in base_variants_sorted:
                                recs = _dedupe_records(sorted(base_recs, key=lambda r: r.index))
                                if recs:
                                    rebuilt_body.append(LSFOption(f"body_base_{base_v:02X}", _records_label(recs, f"基础 {base_v:02X}"), recs))

                        for slot3_v, slot3_recs in slot3_variants_sorted:
                            recs = []
                            # 有基础底图时，slot03 多数是服装/身体覆盖层；需要叠在基础底图上。
                            # 没有基础底图时，slot03 自身就是完整底图。
                            if default_base:
                                recs.extend(default_base)
                            recs.extend(sorted(slot3_recs, key=lambda r: r.index))
                            recs = _dedupe_records(recs)
                            if recs:
                                label = _records_label(sorted(slot3_recs, key=lambda r: r.index), f"服装/身体 {slot3_v:02X}")
                                rebuilt_body.append(LSFOption(f"body_slot3_{slot3_v:02X}", label, recs))

                        if rebuilt_body:
                            body_options = rebuilt_body
                            notes.append("检测到 adv/gfx/face 结构，已将 slot03 作为衣服或者其他时间端。")

                            # 如果 slot04 同时存在，通常是帽子/头饰/局部覆盖层。
                            # mid==3 记录按普通 RGBA 合成会出现白块/遮罩，作为饰品时也要过滤掉。
                            if slot4_variants:
                                accessory_options: list[LSFOption] = [LSFOption("__none__", "(无饰品)", [])]
                                for variant, recs in sorted(slot4_variants.items()):
                                    visible = [
                                        r for r in _dedupe_records(sorted(recs, key=lambda r: r.index))
                                        if not _is_helper_mask_record(r)
                                    ]
                                    if visible:
                                        label = visible[0].name if len(visible) == 1 else " + ".join(r.name for r in visible[:3])
                                        accessory_options.append(LSFOption(f"acc_slot4_{variant:02X}", label, visible))
                                if len(accessory_options) > 1:
                                    special_groups.append(("饰品", accessory_options))
                                    special_slots.add(4)

                    unique_body: list[LSFOption] = []
                    seen_body: set[tuple[str, ...]] = set()
                    for opt in body_options:
                        sig = tuple(sorted(r.name for r in opt.records))
                        if sig not in seen_body:
                            seen_body.add(sig)
                            unique_body.append(opt)
                    body_options = unique_body

                    body_slots |= {s for s in (0, 3, 4) if s in slots}
                    special_slots |= {5} if 5 in slots else set()

            else:
                common_body_keys: set[tuple] = set()
                forced_fixed_body_keys: set[tuple] = set()

                for slot in sorted(body_slots):
                    variants = slots[slot]
                    if slot == 0 and 0 in variants and len(variants) > 1:
                        v0_recs = sorted(variants[0], key=lambda r: r.index)
                        if v0_recs and max((r.area for r in v0_recs), default=0) >= canvas_area * 0.5:
                            for r in v0_recs:
                                forced_fixed_body_keys.add((r.name, r.left, r.top, r.right, r.bottom, r.slot_code, ((r.tag >> 16) & 0xFF)))

                    variant_lists = [
                        sorted(recs, key=lambda r: r.index)
                        for variant, recs in sorted(variants.items())
                        if not (slot == 0 and variant == 0 and len(variants) > 1)
                    ]
                    if len(variant_lists) < 2:
                        continue
                    key_sets = []
                    for recs in variant_lists:
                        key_sets.append({(r.name, r.left, r.top, r.right, r.bottom, r.slot_code, ((r.tag >> 16) & 0xFF)) for r in recs})
                    shared = set.intersection(*key_sets) if key_sets else set()
                    common_body_keys |= shared

                fixed_body_keys, hidden_body_keys = _split_common_body_keys(common_body_keys, canvas_area)
                fixed_body_keys |= forced_fixed_body_keys

                if fixed_body_keys:
                    for slot in sorted(body_slots):
                        variants = slots[slot]
                        for recs in variants.values():
                            for r in recs:
                                key = (r.name, r.left, r.top, r.right, r.bottom, r.slot_code, ((r.tag >> 16) & 0xFF))
                                if key in fixed_body_keys:
                                    fixed_records.append(r)

                body_variants = sorted({
                    variant
                    for slot in body_slots
                    for variant in slots[slot].keys()
                    if not (slot == 0 and variant == 0 and len(slots[slot]) > 1)
                })
                for variant_code in body_variants:
                    chosen_original: list[LSFRecord] = []
                    chosen_filtered: list[LSFRecord] = []
                    exact_match_records: list[LSFRecord] = []
                    for slot in sorted(body_slots):
                        variants = slots[slot]
                        used_exact = False
                        if variant_code in variants:
                            source_recs = variants[variant_code]
                            used_exact = True
                        else:
                            fallback_candidates = [v for v in sorted(variants.keys()) if not (slot == 0 and v == 0 and len(variants) > 1)]
                            fallback_variant = fallback_candidates[0] if fallback_candidates else sorted(variants.keys())[0]
                            source_recs = variants[fallback_variant]
                        for r in source_recs:
                            key = (r.name, r.left, r.top, r.right, r.bottom, r.slot_code, ((r.tag >> 16) & 0xFF))
                            if key not in fixed_body_keys and key not in hidden_body_keys:
                                chosen_original.append(r)
                                if used_exact:
                                    exact_match_records.append(r)

                    chosen_original = sorted(chosen_original, key=lambda r: r.index)
                    chosen_filtered = _filter_body_records_for_display(chosen_original)
                    # 身体层内部优先按面积从大到小绘制，再画局部手臂/袖子/前景覆盖层。
                    chosen_filtered = sorted(chosen_filtered, key=lambda r: (-r.area, r.index, natural_sort_key(r.name)))
                    if adv_mode:
                        label = _format_adv_time_label(variant_code, chosen_filtered, fixed_records, canvas_area)
                    else:
                        display_rec = _choose_non_adv_body_display_record(exact_match_records, body_slots, slot_meta)
                        label = display_rec.name if display_rec else _label_from_records("身体", variant_code, chosen_filtered)
                    body_options.append(LSFOption(f"body_{variant_code:02X}", label, chosen_filtered))


        # st/0、st/3 这批单人立绘还有一种结构：
        #   slot03 = 完整身体/衣服
        #   slot04 = 帽子/头饰（常伴随 mid==3 辅助遮罩）
        #   slot05/slot06 = 头顶小物件/鸟/漫符/特殊效果
        # 旧启发式只在窄高 portrait_mode 下处理；但 st/3 的 01_Kagome 画布很宽，
        # 因此没有进入 portrait_mode，slot04 被提升成固定层、slot05 被并进身体、slot06 被当成表情2。
        # 这里新增 st_layered_face_mode：只要是 slot01 表情 + slot02 红晕 + slot03 大身体，
        # 且额外高号槽位位于头顶附近，就把这些槽统一作为可选特殊层，默认不叠加。
        st_layered_face_mode = (
            1 in slots
            and 3 in slots
            and int(slot_meta.get(1, {}).get("variant_count", 0)) >= 5
            and float(slot_meta.get(1, {}).get("max_ratio", 0.0)) <= 0.10
            and float(slot_meta.get(3, {}).get("max_ratio", 0.0)) >= 0.35
            and max(slot_ids or [0]) <= 8
            and any(s >= 4 for s in slot_ids)
        )
        if (portrait_mode or st_layered_face_mode) and project.stem not in known_standing_stems:
            forced_special_slots: set[int] = set()
            for _slot in sorted(slot_ids):
                if _slot in {0, 1, 2, 3, 0xFF}:
                    continue
                _meta = slot_meta.get(_slot, {})
                _max_ratio = float(_meta.get("max_ratio", 0.0))
                _avg_y = float(_meta.get("avg_center_y", project.canvas_height))
                _variant_count = int(_meta.get("variant_count", 0))

                # slot04 这类帽子/头饰面积可能比较大（st/3 约 18%），
                # 但中心仍在头部区域且变体很少；slot05/06 等头顶小效果面积更小。
                # 它们都不该默认固定叠加，也不该混入身体或表情。
                if (
                    (_slot == 4 and _variant_count <= 3 and _max_ratio <= 0.25 and _avg_y <= project.canvas_height * 0.45)
                    or (_slot >= 5 and _max_ratio <= 0.04 and _avg_y <= project.canvas_height * 0.28)
                ):
                    forced_special_slots.add(_slot)

            if forced_special_slots:
                def _remove_groups_for_slots(groups: list[tuple[str, list[LSFOption]]], remove_slots: set[int]) -> list[tuple[str, list[LSFOption]]]:
                    kept: list[tuple[str, list[LSFOption]]] = []
                    for _name, _opts in groups:
                        _slots_in_group = {
                            r.slot_code
                            for opt in _opts
                            for r in opt.records
                        }
                        if _slots_in_group and _slots_in_group <= remove_slots:
                            continue
                        kept.append((_name, _opts))
                    return kept

                expression_groups = _remove_groups_for_slots(expression_groups, forced_special_slots)
                blush_groups = _remove_groups_for_slots(blush_groups, forced_special_slots)
                special_groups = _remove_groups_for_slots(special_groups, forced_special_slots)

                # 这些槽可能已经被早前的身体构建流程并进 body_options。
                # 例如 st/3 的 slot05 = 头顶鸟身体，会被误合到“衣服或者其他时间端”，
                # 导致一打开就显示鸟。这里把它们从身体选项里剥离，只保留在特殊层里。
                for _opt in body_options:
                    _opt.records[:] = [r for r in _opt.records if r.slot_code not in forced_special_slots]
                body_options = [
                    _opt for _opt in body_options
                    if _opt.records or _opt.key.startswith("__none") or _opt.key == "body_default"
                ]

                _existing_special_slots = {
                    r.slot_code
                    for _name, _opts in special_groups
                    for opt in _opts
                    for r in opt.records
                }

                def _add_special_group_for_slot(_slot: int, _group_label: str) -> None:
                    if _slot in _existing_special_slots:
                        return
                    _opts: list[LSFOption] = [LSFOption("__none__", f"(无{_group_label})", [])]
                    for _variant, _recs in sorted(slots[_slot].items()):
                        _visible = [
                            r for r in sorted(_recs, key=lambda x: x.index)
                            if not _is_helper_mask_record(r)
                        ]
                        if not _visible:
                            continue
                        # 同一变体内的多个小件应一起切换，例如 slot05 的头顶鸟 + 头发/额外小件。
                        _label = _visible[0].name if len(_visible) == 1 else " + ".join(r.name for r in _visible[:3])
                        _opts.append(LSFOption(f"special_slot{_slot:02X}_{_variant:02X}", _label, _visible))
                    if len(_opts) > 1:
                        special_groups.append((_group_label, _opts))
                        _existing_special_slots.add(_slot)

                for _slot in sorted(forced_special_slots):
                    if _slot == 4:
                        _label = "饰品"
                    elif _slot == 5:
                        _label = "特殊1"
                    else:
                        _label = f"特殊{_slot - 4}"
                    _add_special_group_for_slot(_slot, _label)

                special_slots |= forced_special_slots
                expr_slots -= forced_special_slots
                blush_slots -= forced_special_slots
                body_slots -= forced_special_slots
                notes.append("检测到 st 单人立绘头顶饰品/特殊效果槽，已改为可选特殊层，默认不叠加。")


        used_slots = body_slots | expr_slots | blush_slots | holy_slots | special_slots
        for slot in sorted(slot_ids):
            if slot in used_slots:
                continue
            for recs in slots[slot].values():
                fixed_records.extend(recs)


    # ADV/EV 有些文件（例如 EV_B12）是：
    #   slot00 variant 0 = 完整底图
    #   slot00 variant 2 = 局部透明覆盖层（不是完整时间端）
    # v8 会因为只有一个 body option 而默认叠加该覆盖层，造成“时间端”打开就变了。
    # 如果已经有固定完整底图、body 中没有“原图/空选项”，并且 slot00 缺少 variant 1，
    # 就在最前面补一个默认原图选项；它用专门 key，避免 GUI 自动跳到第二项。
    if 0 in slots:
        slot0_variants = set(slots.get(0, {}).keys())
        represented_slot0_variants = {
            r.variant_code
            for opt in body_options
            for r in opt.records
            if r.slot_code == 0
        }
        has_fixed_full_slot0_base = any(
            r.slot_code == 0 and _mid_code(r) == 0 and r.area >= canvas_area * 0.80
            for r in fixed_records
        )
        has_empty_body_option = any(len(opt.records) == 0 for opt in body_options)
        has_slot0_body_option = any(
            any(r.slot_code == 0 for r in opt.records)
            for opt in body_options
        )
        nonzero_slot0_variants = [v for v in slot0_variants if v != 0]
        if (
            has_fixed_full_slot0_base
            and has_slot0_body_option
            and not has_empty_body_option
            and nonzero_slot0_variants
            and 1 not in represented_slot0_variants
        ):
            body_options.insert(0, LSFOption("__none_default__", "原图（不叠加时间端/覆盖层）", []))
            for opt in body_options[1:]:
                if opt.label.startswith("时间端 "):
                    opt.label = opt.label.replace("时间端 ", "覆盖层 ", 1)
            notes.append("检测到完整底图 + 局部时间端覆盖层，默认显示原图。")


    # 有些小物件/鸟类资源会把同一张主体图重复挂在多个变体上。
    # 公共主体已被提升为 fixed_records 后，剩下的 body_options 全是空记录，
    # 界面会显示“身体 01 / 身体 02”但切换没有任何变化。这里折叠成一个“默认”。
    if body_options and all(len(opt.records) == 0 for opt in body_options):
        # 单张完整底图 + 表情/红晕差分的 EV 场景没有衣服/时间端可切换，保留“原图”语义；
        # 其他重复主体资源才折叠为“默认”。
        if any("单张 EV 底图 + 脸部差分" in n for n in notes):
            body_options = [LSFOption("__none__", "原图", [])]
        else:
            body_options = [LSFOption("body_default", "默认", [])]
            notes.append("检测到重复主体变体，已折叠为空默认选项。")

    if not body_options:
        body_options.append(LSFOption("body_default", "默认", []))

    if project.stem in known_standing_stems:
        fixed_records = [r for r in fixed_records if r.slot_code not in {1, 2, 5}]

    # 安全兜底：辅助遮罩 mid==3 不作为固定层显示；已归入特殊/饰品的槽位也不固定叠加。
    if "special_slots" in locals():
        fixed_records = [r for r in fixed_records if r.slot_code not in special_slots]
    fixed_records = [r for r in fixed_records if not _is_helper_mask_record(r)]

    _make_option_labels_unique(body_options)
    for _group_name, _opts in expression_groups:
        _make_option_labels_unique(_opts)
    for _group_name, _opts in blush_groups:
        _make_option_labels_unique(_opts)
    for _group_name, _opts in special_groups:
        _make_option_labels_unique(_opts)
    _make_option_labels_unique(holy_options)

    # Dedupe by actual rendered layer, not by LSF row index. Some EV files repeat
    # the same full background record in several variants; drawing it repeatedly is
    # harmless visually, but it confuses the layer count and option analysis.
    fixed_records = sorted({(r.name, r.left, r.top, r.right, r.bottom, r.slot_code, _mid_code(r)): r for r in fixed_records}.values(), key=lambda r: r.index)
    expr_count = sum(max(0, len(opts) - 1) for _, opts in expression_groups)
    blush_count = sum(max(0, len(opts) - 1) for _, opts in blush_groups)
    special_count = sum(max(0, len(opts) - 1) for _, opts in special_groups)
    notes.append(f"固定图层: {len(fixed_records)}")
    notes.append(f"衣服或者其他时间端选项: {len(body_options)}")
    notes.append(f"表情组选项: {expr_count}")
    notes.append(f"红晕组选项: {blush_count}")
    notes.append(f"特殊组选项: {special_count}")
    notes.append(f"圣光选项: {max(0, len(holy_options) - 1)}")
    if "hidden_body_keys" in locals() and hidden_body_keys:
        notes.append(f"已默认隐藏共享附加层: {len(hidden_body_keys)}")
    return LSFScene(
        project=project,
        fixed_records=fixed_records,
        body_options=body_options,
        expression_groups=expression_groups,
        blush_groups=blush_groups,
        special_groups=special_groups,
        holy_options=holy_options,
        notes=notes,
    )




def _normalize_runtime_worker_count(value: int | None, item_count: int) -> int:
    """预览/运行时合成用的工作线程数。只并行 PNG 解码读取，最终叠图仍保持原顺序。"""
    if item_count <= 1:
        return 1
    try:
        workers = int(value or 1)
    except Exception:
        workers = 1
    return max(1, min(workers, item_count))


def _load_rgba_paths_parallel(
    resolver: PNGResolver,
    paths: list[Path],
    runtime_workers: int | None = 1,
) -> list[Image.Image]:
    """按 paths 顺序返回 RGBA 图片；读取/解码阶段可使用多个 CPU 线程。"""
    workers = _normalize_runtime_worker_count(runtime_workers, len(paths))
    if workers <= 1:
        return [resolver.load_rgba(str(path)) for path in paths]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers, thread_name_prefix="preview-png") as executor:
        return list(executor.map(lambda p: resolver.load_rgba(str(p)), paths))

def compose_lsf_scene(
    scene: LSFScene,
    resolver: PNGResolver,
    body_option: Optional[LSFOption],
    expression_options: list[Optional[LSFOption]] | None,
    blush_options: list[Optional[LSFOption]] | None,
    holy_option: Optional[LSFOption] = None,
    special_options: list[Optional[LSFOption]] | None = None,
    runtime_workers: int | None = 1,
) -> tuple[Image.Image, list[str], list[LSFRecord]]:
    selected_records: list[LSFRecord] = []
    selected_records.extend(scene.fixed_records)
    if body_option:
        selected_records.extend(body_option.records)
    for expr in expression_options or []:
        if expr:
            selected_records.extend(expr.records)
    for blush in blush_options or []:
        if blush:
            selected_records.extend(blush.records)
    for sp in special_options or []:
        if sp:
            selected_records.extend(sp.records)
    if holy_option:
        selected_records.extend(holy_option.records)

    # 保持“固定层 -> 身体 -> 表情 -> 红晕 -> 特殊 -> 圣光”的追加顺序，
    # 同时只做去重，不再按 index 全局重排。
    # 之前全局按 index 排序会把局部前景手臂层重新排到大身体底图前面，
    # 导致 01_Tsugumi 这类文件里手臂被压到后面。
    deduped: list[LSFRecord] = []
    seen_keys: set[tuple[str, int]] = set()
    for r in selected_records:
        key = (r.name, r.index)
        if key not in seen_keys:
            seen_keys.add(key)
            deduped.append(r)
    selected_records = deduped

    canvas = Image.new("RGBA", (scene.project.canvas_width, scene.project.canvas_height), (0, 0, 0, 0))
    warnings: list[str] = []
    drawable_records: list[LSFRecord] = []
    drawable_paths: list[Path] = []
    for rec in selected_records:
        img_path = resolver.find_for_lsf(rec.name)
        if not img_path:
            warnings.append(f"缺少 PNG: {rec.name}.png")
            continue
        drawable_records.append(rec)
        drawable_paths.append(img_path)

    # 运行时预览合成：PNG 读取/解码阶段并行使用 CPU 线程；
    # alpha_composite 仍按原图层顺序执行，避免图层前后关系错乱。
    drawable_images = _load_rgba_paths_parallel(resolver, drawable_paths, runtime_workers)
    for rec, img in zip(drawable_records, drawable_images):
        canvas.alpha_composite(img, (rec.left, rec.top))
    return canvas, warnings, selected_records


def _classify_json_group(label: str) -> str:
    s = (label or "").lower()
    if any(k in s for k in ["頬", "脸红", "紅", "blush", "cheek"]):
        return "blush"
    if any(k in s for k in ["表情", "expression", "face", "mouth", "eye", "目", "口"]):
        return "expression"
    if any(k in s for k in ["身体", "衣", "服", "pose", "body", "武器", "差分", "bg", "背景"]):
        return "body"
    return "body"


def analyze_json_scene(project: JSONProject) -> JSONScene:
    fixed_layers = list(project.fixed_layers)
    body_options: list[LSFOption] = []
    expression_options: list[LSFOption] = [LSFOption("__none__", "(无表情)", [])]
    blush_options: list[LSFOption] = [LSFOption("__none__", "(无红晕)", [])]
    notes: list[str] = []

    for label, layers in project.groups.items():
        kind = _classify_json_group(label)
        if kind == "expression":
            for layer in layers:
                expression_options.append(LSFOption(f"expr_{layer.layer_id}", layer.label, [layer]))
        elif kind == "blush":
            for layer in layers:
                blush_options.append(LSFOption(f"blush_{layer.layer_id}", layer.label, [layer]))
        else:
            for layer in layers:
                body_options.append(LSFOption(f"body_{layer.layer_id}", layer.label, [layer]))


    if not body_options:
        body_options.append(LSFOption("body_default", "默认", []))
    notes.append(f"固定图层: {len(fixed_layers)}")
    notes.append(f"衣服或者其他选项: {len(body_options)}")
    notes.append(f"表情选项: {max(0, len(expression_options) - 1)}")
    notes.append(f"红晕选项: {max(0, len(blush_options) - 1)}")
    return JSONScene(
        project=project,
        fixed_layers=fixed_layers,
        body_options=body_options,
        expression_options=expression_options,
        blush_options=blush_options,
        notes=notes,
    )


def compose_json_scene(
    scene: JSONScene,
    resolver: PNGResolver,
    body_option: Optional[LSFOption],
    expression_option: Optional[LSFOption],
    blush_option: Optional[LSFOption],
    runtime_workers: int | None = 1,
) -> tuple[Image.Image, list[str], list[JSONLayer]]:
    canvas = Image.new("RGBA", (scene.project.canvas_width, scene.project.canvas_height), (0, 0, 0, 0))
    warnings: list[str] = []
    layers = list(scene.fixed_layers)
    if body_option:
        layers.extend(body_option.records)  # type: ignore[arg-type]
    if expression_option:
        layers.extend(expression_option.records)  # type: ignore[arg-type]
    if blush_option:
        layers.extend(blush_option.records)  # type: ignore[arg-type]
    layers = sorted({(x.layer_id, x.draw_index): x for x in layers}.values(), key=lambda x: x.draw_index)

    drawable_layers: list[JSONLayer] = []
    drawable_paths: list[Path] = []
    for layer in layers:
        path = resolver.find_for_json_layer(scene.project.stem, layer.layer_id)
        if not path:
            warnings.append(f"缺少 PNG: layer_id={layer.layer_id}")
            continue
        drawable_layers.append(layer)
        drawable_paths.append(path)

    drawable_images = _load_rgba_paths_parallel(resolver, drawable_paths, runtime_workers)
    for layer, img in zip(drawable_layers, drawable_images):
        canvas.alpha_composite(img, (layer.left, layer.top))
    return canvas, warnings, layers