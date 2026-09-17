"""归档服务主体：启动恢复、写入分类、后台刷盘、冻结视图、查询与回放。

顺序与并发性：
  * 接收线程在 ing_lock 内完成 "分类 -> 分配 WAL 序列号 -> fsync 追加"，
    因此每台设备看到的分类顺序与 WAL 顺序一致（业务顺序不被破坏）。
  * 唯一的刷盘线程按 WAL 顺序把信封写进分段并登记 catalog，
    durable_hwm 只单调向前。
  * 冻结时等待刷盘排空并封存全部分段，冻结后新数据进入新分段，
    物理上不可能混入已冻结批次。
"""
from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from typing import Any

from . import framing
from .catalog import Catalog
from .segments import SegmentManager
from .wal import WAL

META_HWM = "durable_hwm"
META_SEQ = "wal_seq_counter"


class ArchiveService:
    def __init__(self, data_dir: str, *, window_ms: int = 3_600_000,
                 rotate_count: int = 100_000, max_open: int = 16,
                 late_grace_ms: int = 300_000,
                 flush_interval: float = 0.5,
                 wal_max_size: int = 128 * 1024 * 1024):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.window_ms = window_ms
        self.late_grace_ms = late_grace_ms
        self.flush_interval = flush_interval

        self.cat = Catalog(os.path.join(data_dir, "catalog.db"))
        self.wal = WAL(os.path.join(data_dir, "wal"), max_size=wal_max_size)
        self.segs = SegmentManager(
            os.path.join(data_dir), self.cat, window_ms,
            rotate_count, max_open)

        self.ing_lock = threading.RLock()
        # 分段持久化锁：刷盘线程、freeze、rebuild 都经它串行写分段，
        # 避免多个线程交叉写同一活动分段句柄。
        self.persist_lock = threading.RLock()
        self.cond = threading.Condition(self.ing_lock)
        self.pending: deque[tuple[int, bytes]] = deque()
        # 已确认（已 fsync）的业务键：
        # 唯一索引只能在落分段后拦截重复，内存集合保证同一批/刷盘前
        # 瞬间到达的重发也被识别为幂等重复。
        self.seen: set[tuple[str, int]] = set()
        self.seq_counter = int(self.cat.get_meta(META_SEQ, "0"))
        self.durable_hwm = int(self.cat.get_meta(META_HWM, "0"))
        self.stop_event = threading.Event()
        self._flusher: threading.Thread | None = None
        self.last_reports: list[dict] = []

    # ================= 启动 / 恢复 =================================
    def boot(self) -> list[dict]:
        """崩溃恢复，返回隔离/恢复报告。必须在 HTTP 服务启动前调用。"""
        # 1) 活动分段：截断尾部撕裂帧并补封
        self.segs.recover_tail()
        # 2) 封存分段校验，损坏的整体隔离
        reports = self.segs.verify_sealed()
        # 3) 扫描 WAL，重放所有未进分段的记录（确认过的不丢）。
        #    boot 在刷盘线程启动前执行，仍持持久化锁以统一写段路径。
        scanned_max, records = self.wal.scan()
        self.seq_counter = max(self.seq_counter, scanned_max)
        with self.persist_lock:
            for seq, payload in records:
                env = json.loads(payload.decode())
                if self.cat.event_exists(env["device_id"], env["seq"]):
                    continue
                self._persist_one(seq, payload, env)
            self.segs.seal_all()
            # 4) 重建出来的新分段再校验一次
            reports += self.segs.verify_sealed()
        # 5) 高水位按健康分段的实际进度推进；存在隔离分段时，
        #    其所需 WAL 必须保留作为重建来源，回收上界相应收窄。
        self.durable_hwm = max(self.durable_hwm,
                               self.cat.max_wal_seq_healthy())
        self._persist_hwm()
        self.wal.gc(self._wal_gc_hwm())
        self.seen = self.cat.all_event_keys()
        self.last_reports = reports
        return reports

    def _wal_gc_hwm(self) -> int:
        """WAL 回收上界：不能越过最早隔离分段所需的 WAL。"""
        floor = self.cat.min_first_wal_with_quarantine()
        if floor is None:
            return self.durable_hwm
        return min(self.durable_hwm, max(0, floor - 1))

    def start_flusher(self) -> None:
        self._flusher = threading.Thread(
            target=self._flush_loop, name="flusher", daemon=True)
        self._flusher.start()

    def shutdown(self) -> None:
        self.stop_event.set()
        with self.cond:
            self.cond.notify_all()
        if self._flusher:
            self._flusher.join(timeout=10)
        # 刷盘线程已退出；排空残留并封存
        with self.persist_lock:
            batch = self._take_batch()
            if batch:
                self._persist_batch_locked(batch)
            self.segs.seal_all()
        self.wal.close()
        self.cat.close()

    # ================= 写入分类 ====================================
    def ingest(self, device_id: str, seq: int, event_ts: int | None,
               data: Any) -> dict:
        if not isinstance(device_id, str) or not device_id:
            raise ValueError("device_id 必须是非空字符串")
        if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0:
            raise ValueError("seq 必须是非负整数")
        now = int(time.time() * 1000)
        if event_ts is None:
            event_ts = now
        if not isinstance(event_ts, int) or isinstance(event_ts, bool):
            raise ValueError("event_ts 必须是整数毫秒时间戳")

        with self.ing_lock:
            key = (device_id, seq)
            # 重复检测：内存集合覆盖 "已 fsync 但尚未落分段" 的窗口，
            # 数据库 (device_id,seq) 唯一索引是跨重启的最终事实来源。
            if key in self.seen or self.cat.event_exists(device_id, seq):
                existing = self.cat.get_event(device_id, seq)
                return {"device_id": device_id, "seq": seq,
                        "accepted": False, "duplicate": True,
                        "flags": {"duplicate": True},
                        "wal_seq": existing["wal_seq"] if existing else None}

            wm = self.cat.get_device(device_id)
            flags: dict[str, Any] = {}
            if wm is not None:
                if seq > wm["max_seq"] + 1:
                    flags["gap_fill"] = True
                if now - event_ts > self.late_grace_ms:
                    flags["late"] = True
                if event_ts < wm["max_event_ts"] - self.late_grace_ms:
                    # 序列号往前走、事件时间却大幅倒退：设备时钟回拨
                    flags["rollback"] = True

            self.seq_counter += 1
            wal_seq = self.seq_counter
            env = {"device_id": device_id, "seq": seq, "event_ts": event_ts,
                   "wal_seq": wal_seq, "flags": flags,
                   "ingest_ts": now, "data": data}
            payload = json.dumps(env, separators=(",", ":"),
                                 ensure_ascii=False).encode()
            # 先落 WAL 并 fsync，成功才算确认；未 fsync 的崩溃帧重启丢弃
            self.wal.append(payload, wal_seq)
            self.cat.set_meta(META_SEQ, str(wal_seq))
            self.cat.put_device(device_id, seq, event_ts)
            self.seen.add(key)
            self.pending.append((wal_seq, payload))
            self.cond.notify()

        return {"device_id": device_id, "seq": seq, "accepted": True,
                "duplicate": False, "flags": flags, "wal_seq": wal_seq}

    # ================= 刷盘循环 ====================================
    def _flush_loop(self) -> None:
        while not self.stop_event.is_set():
            # 取批与持久化在同一把 persist_lock 内完成，
            # 保证冻结时不存在 "已取出但未写盘" 的悬空批次。
            with self.persist_lock:
                batch = self._take_batch()
                if batch:
                    self._persist_batch_locked(batch)
                else:
                    self.segs.flush_stats()
            with self.cond:
                self.cond.wait(self.flush_interval / 2)

    def _take_batch(self, max_size: int = 500) -> list[tuple[int, bytes]]:
        with self.ing_lock:
            batch = []
            while self.pending and len(batch) < max_size:
                batch.append(self.pending.popleft())
            return batch

    def _persist_batch_locked(self, batch: list[tuple[int, bytes]]) -> None:
        """调用方必须持有 persist_lock。"""
        max_seq = self.durable_hwm
        for seq, payload in batch:
            env = json.loads(payload.decode())
            if self.cat.event_exists(env["device_id"], env["seq"]):
                max_seq = max(max_seq, seq)
                continue
            self._persist_one(seq, payload, env)
            max_seq = max(max_seq, seq)
        self.durable_hwm = max(self.durable_hwm, max_seq)
        self._persist_hwm()
        self.segs.flush_stats()

    def _persist_one(self, seq: int, payload: bytes, env: dict) -> None:
        seg_id, offset, length = self.segs.write_envelope(env, payload)
        self.cat.insert_event(
            device_id=env["device_id"], seq=env["seq"],
            event_ts=env["event_ts"], wal_seq=seq, segment_id=seg_id,
            status="ok", flags=env.get("flags", {}),
            offset=offset, length=length)

    def _persist_hwm(self) -> None:
        self.cat.set_meta(META_HWM, str(self.durable_hwm))

    def sync(self, timeout: float = 30.0) -> bool:
        """等待所有已确认写入进入分段（供测试 / 优雅停机使用）。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self.ing_lock:
                target = self.seq_counter
                if not self.pending and self.durable_hwm >= target:
                    return True
            time.sleep(0.02)
        return False

    # ================= 冻结视图 ====================================
    def freeze(self, name: str, note: str = "") -> dict:
        # 固定锁序 persist_lock -> ing_lock；持锁期间接收侧可继续往
        # WAL 追加（那只短暂需要 ing_lock），但它们的 pending 在
        # 封存动作之后才被取走，因此只会进入冻结点之后的新分段。
        with self.persist_lock, self.ing_lock:
            if self.cat.get_snapshot(name):
                raise ValueError(f"冻结视图 {name} 已存在")
            batch = list(self.pending)
            self.pending.clear()
            self._persist_batch_locked(batch)
            self.segs.seal_all()
            hwm = self.durable_hwm
            self.cat.create_snapshot(name, hwm, note)
        snap = self.cat.get_snapshot(name)
        return self._snapshot_view(snap)

    def _snapshot_view(self, snap) -> dict:
        segs = self.cat.snapshot_segments(snap["wal_hwm"])
        return {
            "name": snap["name"],
            "wal_hwm": snap["wal_hwm"],
            "created_at": snap["created_at"],
            "note": snap["note"],
            "segments": [
                {"name": r["name"], "event_count": r["event_count"],
                 "sha256": r["sha256"],
                 "window_start": r["window_start"]}
                for r in segs],
            "segment_count": len(segs),
        }

    def list_snapshots(self) -> list[dict]:
        return [self._snapshot_view(s)
                for s in self.cat.list_snapshots()]

    def get_snapshot(self, name: str) -> dict | None:
        snap = self.cat.get_snapshot(name)
        return self._snapshot_view(snap) if snap else None

    # ================= 回放 ========================================
    def replay(self, name: str, cursor: int | None, limit: int) -> dict:
        snap = self.cat.get_snapshot(name)
        if not snap:
            raise KeyError(name)
        hwm = snap["wal_hwm"]
        rows = self.cat.events_by_wal_range(cursor, hwm, limit)
        events = []
        for r in rows:
            item = self._read_event(r)
            if item is not None:
                events.append(item)
        next_cursor = rows[-1]["wal_seq"] if rows else cursor
        done = not rows or (rows[-1]["wal_seq"] >= hwm)
        return {"snapshot": name, "hwm": hwm, "cursor": cursor,
                "next_cursor": next_cursor, "done": done,
                "events": events}

    # ================= 查询 ========================================
    def query(self, device_id: str, ts_from: int | None, ts_to: int | None,
              limit: int) -> dict:
        rows = self.cat.query_events(device_id, ts_from, ts_to, limit)
        events = []
        for r in rows:
            item = self._read_event(r)
            if item is not None:
                events.append(item)
        return {"device_id": device_id, "events": events,
                "count": len(events)}

    def _read_event(self, row) -> dict | None:
        """从分段文件指定偏移读出原始帧；读不出则标记隔离，不拖垮整查。"""
        seg = self.cat.get_segment_by_id(row["segment_id"])
        item = {
            "device_id": row["device_id"], "seq": row["seq"],
            "event_ts": row["event_ts"], "wal_seq": row["wal_seq"],
            "status": row["status"], "flags": json.loads(row["flags"]),
            "ingest_ts": row["ingest_ts"], "data": None}
        if not seg or seg["state"] == "quarantined":
            item["status"] = "quarantined"
            return item
        try:
            with open(seg["path"], "rb") as f:
                f.seek(row["offset"])
                frame = framing.read_frame(f)
                if frame is None:
                    raise framing.FrameError("偏移处无帧")
                env = json.loads(frame[0].decode())
                item["data"] = env.get("data")
        except (OSError, framing.FrameError, ValueError) as e:
            item["status"] = "unreadable"
            item["error"] = str(e)
        return item

    # ================= 运维接口 ====================================
    def devices(self) -> list[dict]:
        return [dict(r) for r in self.cat.list_devices()]

    def segments(self) -> list[dict]:
        out = []
        for r in self.cat.list_segments():
            out.append({k: r[k] for k in (
                "name", "window_start", "ordinal", "state", "size",
                "event_count", "first_wal_seq", "last_wal_seq",
                "min_event_ts", "max_event_ts", "sha256")})
        return out

    def verify(self) -> list[dict]:
        reports = self.segs.verify_sealed()
        self.last_reports += reports
        return reports

    def rebuild_quarantined(self) -> dict:
        """从 WAL 重建被隔离分段的数据。

        隔离分段的 catalog 行保留为 quarantined（损坏原文件仍在
        quarantine/ 下），其事件索引删除后按 WAL 记录重新落入全新分段。
        全程串行执行，重建期间接收侧的新数据继续进活动分段。
        """
        with self.persist_lock, self.ing_lock:
            keys = self.cat.delete_quarantined_events()
            _max_seq, records = self.wal.scan()
            n = 0
            for seq, payload in records:
                env = json.loads(payload.decode())
                if (env["device_id"], env["seq"]) not in keys:
                    continue
                # 与正常刷盘完全一致：写分段帧 + 登记唯一索引
                self._persist_one(seq, payload, env)
                n += 1
            self.segs.seal_all()
            self.durable_hwm = max(self.durable_hwm,
                                   self.cat.max_wal_seq_healthy())
            self._persist_hwm()
            self.wal.gc(self._wal_gc_hwm())
        return {"rebuilt_events": n,
                "quarantined_keys": len(keys)}

    def stats(self) -> dict:
        return {
            "wal_seq_counter": self.seq_counter,
            "durable_hwm": self.durable_hwm,
            "pending": len(self.pending),
            "events": self.cat.count_events(),
            "segments": {
                "open": len(self.cat.list_segments("open")),
                "sealed": len(self.cat.list_segments("sealed")),
                "quarantined": len(self.cat.list_segments("quarantined"))},
            "last_reports": self.last_reports[-10:],
        }
