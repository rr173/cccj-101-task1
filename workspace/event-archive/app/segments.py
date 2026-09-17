"""分段存储。

分段按 "事件时间窗口 + 窗口内序号" 组织：
    seg-<window_start_ms>-<ordinal>.seg

迟到数据（窗口还在 / 尚未冻结）按业务窗口落位，保持按事件时间可查询；
时钟回拨的旧数据不回写已封存分段，落入当前活动窗口，并打 rollback 标。

每个分段是一串带 CRC 的帧 + END 结束帧。END 帧存在说明是干净封存；
缺失则是崩溃留下的活动分段，重启时截断尾部撕裂帧后补封。
已封存分段若校验失败则整体隔离（重命名到 quarantine/），影响范围
不超过该分段，并通过 catalog 报告可继续处理的 WAL 位置。
"""
from __future__ import annotations

import hashlib
import os
import shutil
import time
from typing import Any

from . import framing
from .catalog import Catalog


class SegmentError(Exception):
    pass


class OpenSegment:
    """一个正在写的分段；内存中维护校验和与统计，落盘由 fsync 保证。"""

    def __init__(self, seg_id: int, path: str, window_start: int,
                 ordinal: int):
        self.seg_id = seg_id
        self.path = path
        self.window_start = window_start
        self.ordinal = ordinal
        self.f = open(path, "ab")
        self.sha = hashlib.sha256()
        self.size = self.f.tell()
        self.event_count = 0
        self.first_wal: int | None = None
        self.last_wal: int | None = None
        self.min_ts: int | None = None
        self.max_ts: int | None = None

    def append(self, framed: bytes, env: dict[str, Any]) -> tuple[int, int]:
        offset = self.size
        self.f.write(framed)
        self.f.flush()
        os.fsync(self.f.fileno())
        self.sha.update(framed)
        self.size += len(framed)
        self.event_count += 1
        ws = env["wal_seq"]
        self.first_wal = ws if self.first_wal is None else min(self.first_wal, ws)
        self.last_wal = ws if self.last_wal is None else max(self.last_wal, ws)
        ts = env["event_ts"]
        self.min_ts = ts if self.min_ts is None else min(self.min_ts, ts)
        self.max_ts = ts if self.max_ts is None else max(self.max_ts, ts)
        return offset, len(framed)

    def seal(self) -> str:
        end = framing.encode(b"", end=True)
        self.f.write(end)
        self.f.flush()
        os.fsync(self.f.fileno())
        self.sha.update(end)
        self.size += len(end)
        self.f.close()
        return self.sha.hexdigest()

    def close(self) -> None:
        try:
            self.f.close()
        except OSError:
            pass


class SegmentManager:
    def __init__(self, root: str, catalog: Catalog,
                 window_ms: int, rotate_count: int, max_open: int):
        self.root = root
        self.seg_dir = os.path.join(root, "segments")
        self.q_dir = os.path.join(root, "quarantine")
        os.makedirs(self.seg_dir, exist_ok=True)
        os.makedirs(self.q_dir, exist_ok=True)
        self.cat = catalog
        self.window_ms = window_ms
        self.rotate_count = rotate_count
        self.max_open = max_open
        self.open: dict[int, OpenSegment] = {}  # window_start -> writer

    # ---- 路径与命名 -----------------------------------------------
    def _path(self, window_start: int, ordinal: int) -> str:
        return os.path.join(
            self.seg_dir, f"seg-{window_start}-{ordinal:06d}.seg")

    def window_of(self, event_ts: int) -> int:
        return (event_ts // self.window_ms) * self.window_ms

    # ---- 写入 -----------------------------------------------------
    def _open_segment(self, window_start: int) -> OpenSegment:
        existing = self.open.get(window_start)
        if existing:
            if existing.f.closed:
                # 恢复流程封存了文件，内存句柄已失效
                self.open.pop(window_start, None)
            elif existing.event_count >= self.rotate_count:
                self._seal(existing)
                self.open.pop(window_start, None)
            else:
                return existing

        ordinal = self.cat.max_ordinal_for_window(window_start) + 1
        path = self._path(window_start, ordinal)
        seg_id = self.cat.upsert_open_segment(
            os.path.basename(path), window_start, ordinal, path)
        writer = OpenSegment(seg_id, path, window_start, ordinal)
        self.open[window_start] = writer
        self._evict_lru()
        return writer

    def _evict_lru(self) -> None:
        """打开句柄过多时，封存最老的窗口，不影响数据（新窗口可再开）。"""
        while len(self.open) > self.max_open:
            oldest = min(self.open, key=lambda w: self.open[w].last_wal or 0)
            self._seal(self.open.pop(oldest))

    def write_envelope(self, env: dict[str, Any], payload: bytes
                       ) -> tuple[int, int, int]:
        """写一条信封。时钟回拨的记录强制落到当前窗口。"""
        ts = env["event_ts"]
        window = self.window_of(int(time.time() * 1000)) \
            if env["flags"].get("rollback") else self.window_of(ts)
        writer = self._open_segment(window)
        framed = framing.encode(payload)
        offset, length = writer.append(framed, env)
        return writer.seg_id, offset, length

    def _seal(self, writer: OpenSegment) -> None:
        if writer.event_count == 0:
            writer.close()
            os.path.exists(writer.path) and os.remove(writer.path)
            return
        digest = writer.seal()
        self.cat.seal_segment(
            writer.seg_id, digest, writer.size, writer.event_count,
            writer.first_wal, writer.last_wal, writer.min_ts, writer.max_ts)

    def seal_all(self) -> None:
        for w in list(self.open.values()):
            self._seal(w)
        self.open.clear()

    def flush_stats(self) -> None:
        for w in self.open.values():
            self.cat.update_open_stats(
                w.seg_id, w.size, w.event_count, w.first_wal, w.last_wal,
                w.min_ts, w.max_ts)

    # ---- 启动恢复 -------------------------------------------------
    def recover_tail(self) -> None:
        """处理数据库里仍为 open 的分段（上次进程没走到封存）。"""
        for row in self.cat.list_open_segments():
            path = row["path"]
            self._recover_one(row, path)
        self.open.clear()

    def _recover_one(self, row, path: str) -> None:
        if not os.path.exists(path):
            self.cat.mark_quarantined(row["id"])
            return
        good_size, count, min_ts, max_ts, first_wal, last_wal = \
            self._scan_tail(path)
        if good_size < os.path.getsize(path):
            # 活动分段尾部撕裂：截断到最后一条完整帧，并移除被截断
            # 事件的索引行——已确认副本仍在 WAL 中，随后重放重建
            with open(path, "r+b") as f:
                f.truncate(good_size)
                f.flush()
                os.fsync(f.fileno())
            self.cat.delete_events_from_offset(row["id"], good_size)
        if count == 0:
            os.remove(path)
            self.cat.delete_segment(row["id"])
            return
        self._seal_existing(row, good_size, count, min_ts, max_ts,
                            first_wal, last_wal)

    @staticmethod
    def _scan_tail(path: str):
        """返回 (好字节数, 事件数, min_ts, max_ts, first_wal, last_wal)。

        已见 END 帧也算好（说明上次已封存，这里只是重新统计）。
        """
        import json
        good = 0
        count = 0
        min_ts = max_ts = first_wal = last_wal = None
        with open(path, "rb") as f:
            try:
                for start, end, raw, is_end in framing.iter_frames(path):
                    if is_end:
                        good = end
                        break
                    env = json.loads(raw.decode())
                    count += 1
                    ts = env["event_ts"]
                    ws = env["wal_seq"]
                    min_ts = ts if min_ts is None else min(min_ts, ts)
                    max_ts = ts if max_ts is None else max(max_ts, ts)
                    first_wal = ws if first_wal is None \
                        else min(first_wal, ws)
                    last_wal = ws if last_wal is None else max(last_wal, ws)
                    good = end
            except framing.FrameError:
                pass
        return good, count, min_ts, max_ts, first_wal, last_wal

    def _seal_existing(self, row, size: int, count: int, min_ts, max_ts,
                       first_wal, last_wal) -> None:
        """补写 END 帧并把 open 分段封存（digest 覆盖补写后的文件）。"""
        path = row["path"]
        sha = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                sha.update(chunk)
        with open(path, "ab") as f:
            end = framing.encode(b"", end=True)
            f.write(end)
            f.flush()
            os.fsync(f.fileno())
            sha.update(end)
            total = size + len(end)
        first_wal = first_wal if first_wal is not None \
            else row["first_wal_seq"]
        self.cat.seal_segment(row["id"], sha.hexdigest(), total, count,
                              first_wal, last_wal, min_ts, max_ts)

    # ---- 校验与隔离 -----------------------------------------------
    def verify_sealed(self) -> list[dict]:
        """校验所有已封存分段，损坏的隔离。返回隔离报告。"""
        reports = []
        for row in self.cat.list_segments("sealed"):
            report = self._verify_row(row)
            if report:
                reports.append(report)
        return reports

    def _verify_row(self, row) -> dict | None:
        path = row["path"]
        if not os.path.exists(path):
            return self._quarantine(row, "分段文件缺失")
        good_offset = 0
        good_count = 0
        try:
            saw_end = False
            for start, end, raw, is_end in framing.iter_frames(path):
                if is_end:
                    saw_end = True
                    break
                good_offset = end
                good_count += 1
        except framing.FrameError as e:
            # 封存文件损坏：截断点之后不可信，整体隔离影响范围
            suspect = self.cat.mark_suspect_range(row["id"], good_offset)
            return self._quarantine(
                row, f"{e}；可继续位置 offset={good_offset}，"
                     f"其前 {good_count} 帧可读，受影响事件 {suspect} 条")
        if not saw_end:
            return self._quarantine(row, "缺少 END 结束帧")
        return None

    def _quarantine(self, row, reason: str) -> dict:
        src = row["path"]
        dst = os.path.join(
            self.q_dir, f"{row['name']}.{int(time.time())}.bad")
        if os.path.exists(src):
            shutil.move(src, dst)
        self.cat.mark_quarantined(row["id"])
        # 可继续处理位置：隔离分段最后一条完好 WAL 序列号，
        # 上层从 last_wal_seq+1 起重放即可重建
        return {
            "segment": row["name"],
            "reason": reason,
            "quarantine_path": dst,
            "resume_after_wal_seq": row["last_wal_seq"],
        }
