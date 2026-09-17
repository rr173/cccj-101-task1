"""预写日志（WAL）。

每条记录在追加后都调用 fsync，应用收到写入确认即代表落盘。
记录按单调递增的序列号编号；节点重启后先扫描 WAL 找到
高水位（durable_hwm），再把 hwm 之后的记录重放进分段，
做到确认后不丢、未确认的不承诺。

WAL 文件按大小轮转：wal-0000000001.log ...
"""
from __future__ import annotations

import os
import re
import struct

from . import framing

WAL_RE = re.compile(r"wal-(\d{10})\.log$")


class WAL:
    def __init__(self, dirpath: str, max_size: int = 128 * 1024 * 1024):
        self.dir = dirpath
        self.max_size = max_size
        os.makedirs(dirpath, exist_ok=True)
        self._files = self._list()
        if not self._files:
            self._files = [1]
        self._seg = self._files[-1]
        self._path = self._name(self._seg)
        self._f = open(self._path, "ab")
        self._size = self._f.tell()

    # ---- 基础维护 -------------------------------------------------
    def _name(self, seg: int) -> str:
        return os.path.join(self.dir, f"wal-{seg:010d}.log")

    def _list(self) -> list[int]:
        out = []
        for n in os.listdir(self.dir):
            m = WAL_RE.match(n)
            if m:
                out.append(int(m.group(1)))
        return sorted(out)

    def close(self) -> None:
        self._f.flush()
        os.fsync(self._f.fileno())
        self._f.close()

    # ---- 写入 -----------------------------------------------------
    def append(self, payload: bytes, seq: int) -> None:
        """追加一帧。seq 由调用方保证单调递增（写锁保护）。"""
        data = framing.encode(self._wrap(seq, payload))
        if self._size > 0 and self._size + len(data) > self.max_size:
            self._rotate()
        self._f.write(data)
        self._f.flush()
        os.fsync(self._f.fileno())
        self._size += len(data)

    def _rotate(self) -> None:
        self._f.flush()
        os.fsync(self._f.fileno())
        self._f.close()
        self._seg += 1
        self._path = self._name(self._seg)
        self._f = open(self._path, "ab")
        self._size = 0

    @staticmethod
    def _wrap(seq: int, payload: bytes) -> bytes:
        return struct.pack("<Q", seq) + payload

    @staticmethod
    def _unwrap(raw: bytes) -> tuple[int, bytes]:
        return struct.unpack("<Q", raw[:8])[0], raw[8:]

    # ---- 读取 -----------------------------------------------------
    def scan(self) -> tuple[int, list[tuple[int, bytes]]]:
        """扫描全部 WAL 文件。

        返回 (扫描到的最大序列号, [(seq, payload)...])，顺序即文件顺序。
        撕裂的最后一帧按 "未写入成功" 忽略（没有 fsync 确认过）。
        """
        records: list[tuple[int, bytes]] = []
        max_seq = 0
        for seg in self._list() or self._files:
            path = self._name(seg)
            try:
                for _start, _end, raw, is_end in framing.iter_frames(path):
                    seq, payload = self._unwrap(raw)
                    records.append((seq, payload))
                    max_seq = max(max_seq, seq)
                    if is_end:
                        break
            except framing.FrameError:
                # 活动 WAL 的尾部撕裂属于崩溃现场，丢弃尾部未确认帧；
                # 已封存文件不该有尾巴，由上层在分段侧处理隔离。
                break
        return max_seq, records

    # ---- 回收 -----------------------------------------------------
    def gc(self, durable_hwm: int) -> None:
        """删除所有记录都 <= durable_hwm 的 WAL 文件。

        损坏文件保留交人工处理；最新（活动）文件始终保留。
        """
        segs = self._list()
        for seg in segs[:-1]:
            path = self._name(seg)
            last = 0
            keep_corrupt = False
            try:
                for _s, _e, raw, is_end in framing.iter_frames(path):
                    last = max(last, self._unwrap(raw)[0])
                    if is_end:
                        break
            except framing.FrameError:
                keep_corrupt = True
            if not keep_corrupt and last and last <= durable_hwm:
                os.remove(path)
