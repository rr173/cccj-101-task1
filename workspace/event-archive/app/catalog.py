"""SQLite 目录：分段元数据、事件索引（去重）、冻结视图、设备水位。

单连接 + 显式锁串行化写入，WAL 模式保证读写不互斥。
events 表上的唯一约束 (device_id, seq) 是 "重复检测" 的最终事实来源，
即使来源离线数小时后批量补传，也能识别重发的旧序号。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Iterable

SCHEMA = """
CREATE TABLE IF NOT EXISTS segments (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL UNIQUE,
    window_start  INTEGER NOT NULL,   -- 事件时间窗口起点(ms)
    ordinal       INTEGER NOT NULL,   -- 同窗口内的分段序号
    state         TEXT NOT NULL,      -- open|sealed|quarantined
    path          TEXT NOT NULL,
    sha256        TEXT,
    size          INTEGER NOT NULL DEFAULT 0,
    event_count   INTEGER NOT NULL DEFAULT 0,
    first_wal_seq INTEGER,
    last_wal_seq  INTEGER,
    min_event_ts  INTEGER,
    max_event_ts  INTEGER,
    created_at    INTEGER NOT NULL,
    sealed_at     INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_seg_window_ord
    ON segments(window_start, ordinal);

CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    event_ts     INTEGER NOT NULL,
    wal_seq      INTEGER NOT NULL UNIQUE,
    segment_id   INTEGER REFERENCES segments(id),
    status       TEXT NOT NULL,       -- ok|suspect|quarantined
    flags        TEXT NOT NULL,       -- JSON: late|rollback|gap_fill
    offset       INTEGER,
    length       INTEGER,
    ingest_ts    INTEGER NOT NULL,
    UNIQUE(device_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_events_query
    ON events(device_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_events_seg ON events(segment_id);

CREATE TABLE IF NOT EXISTS device_wm (
    device_id  TEXT PRIMARY KEY,
    max_seq    INTEGER NOT NULL,
    max_event_ts INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    name        TEXT PRIMARY KEY,
    wal_hwm     INTEGER NOT NULL,
    created_at  INTEGER NOT NULL,
    note        TEXT
);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Catalog:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    # ---- meta -----------------------------------------------------
    def get_meta(self, key: str, default: str | None = None) -> str | None:
        with self.lock:
            row = self.conn.execute(
                "SELECT value FROM meta WHERE key=?", (key,)).fetchone()
            return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self.lock:
            self.conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value))
            self.conn.commit()

    # ---- segments -------------------------------------------------
    def get_segment_by_name(self, name: str) -> sqlite3.Row | None:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM segments WHERE name=?", (name,)).fetchone()

    def get_segment_by_id(self, seg_id: int) -> sqlite3.Row | None:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM segments WHERE id=?", (seg_id,)).fetchone()

    def upsert_open_segment(self, name: str, window_start: int,
                            ordinal: int, path: str) -> int:
        with self.lock:
            row = self.get_segment_by_name(name)
            if row:
                return row["id"]
            cur = self.conn.execute(
                "INSERT INTO segments(name,window_start,ordinal,state,path,"
                "created_at) VALUES(?,?,?,'open',?,?)",
                (name, window_start, ordinal, path, int(time.time() * 1000)))
            self.conn.commit()
            return cur.lastrowid

    def list_open_segments(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM segments WHERE state='open' ORDER BY id"
            ).fetchall()

    def list_segments(self, state: str | None = None) -> list[sqlite3.Row]:
        with self.lock:
            if state:
                return self.conn.execute(
                    "SELECT * FROM segments WHERE state=? ORDER BY id",
                    (state,)).fetchall()
            return self.conn.execute(
                "SELECT * FROM segments ORDER BY id").fetchall()

    def seal_segment(self, seg_id: int, digest: str, size: int,
                     count: int, first_seq: int | None, last_seq: int | None,
                     min_ts: int | None, max_ts: int | None) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE segments SET state='sealed', sha256=?, size=?, "
                "event_count=?, first_wal_seq=?, last_wal_seq=?, "
                "min_event_ts=?, max_event_ts=?, sealed_at=? WHERE id=?",
                (digest, size, count, first_seq, last_seq, min_ts, max_ts,
                 int(time.time() * 1000), seg_id))
            self.conn.commit()

    def update_open_stats(self, seg_id: int, size: int, count: int,
                          first_seq: int | None, last_seq: int | None,
                          min_ts: int | None, max_ts: int | None) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE segments SET size=?, event_count=?, "
                "first_wal_seq=COALESCE(first_wal_seq,?), "
                "last_wal_seq=?, min_event_ts=COALESCE(min_event_ts,?), "
                "max_event_ts=? WHERE id=?",
                (size, count, first_seq, last_seq, min_ts, max_ts, seg_id))
            self.conn.commit()

    def mark_quarantined(self, seg_id: int) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE segments SET state='quarantined' WHERE id=?", (seg_id,))
            # 分段内所有事件都视为受影响（suspect=损坏点之后，
            # quarantined=其余）；重建按整个分段从 WAL 重做
            self.conn.execute(
                "UPDATE events SET status='quarantined' WHERE segment_id=?",
                (seg_id,))
            self.conn.commit()

    def max_ordinal_for_window(self, window_start: int) -> int:
        with self.lock:
            row = self.conn.execute(
                "SELECT MAX(ordinal) m FROM segments "
                "WHERE window_start=?", (window_start,)).fetchone()
            return row["m"] if row["m"] is not None else -1

    # ---- events ---------------------------------------------------
    def insert_event(self, *, device_id: str, seq: int, event_ts: int,
                     wal_seq: int, segment_id: int, status: str,
                     flags: dict[str, Any], offset: int, length: int) -> bool:
        """返回 True=插入成功；False=(device_id,seq) 重复。"""
        with self.lock:
            try:
                self.conn.execute(
                    "INSERT INTO events(device_id,seq,event_ts,wal_seq,"
                    "segment_id,status,flags,offset,length,ingest_ts) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (device_id, seq, event_ts, wal_seq, segment_id, status,
                     json.dumps(flags, sort_keys=True), offset, length,
                     int(time.time() * 1000)))
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                self.conn.rollback()
                return False

    def all_event_keys(self) -> set[tuple[str, int]]:
        with self.lock:
            return {(r["device_id"], r["seq"])
                    for r in self.conn.execute(
                        "SELECT device_id, seq FROM events")}

    def delete_segment(self, seg_id: int) -> None:
        with self.lock:
            self.conn.execute("DELETE FROM events WHERE segment_id=?",
                              (seg_id,))
            self.conn.execute("DELETE FROM segments WHERE id=?", (seg_id,))
            self.conn.commit()

    def delete_events_from_offset(self, segment_id: int,
                                  from_offset: int) -> int:
        """活动分段尾部被截断时，删除对应事件索引以便从 WAL 重放。"""
        with self.lock:
            cur = self.conn.execute(
                "DELETE FROM events WHERE segment_id=? AND offset>=?",
                (segment_id, from_offset))
            self.conn.commit()
            return cur.rowcount

    def delete_quarantined_events(self) -> set[tuple[str, int]]:
        """删除隔离分段的索引行并返回其业务键（供从 WAL 重建）。"""
        with self.lock:
            rows = self.conn.execute(
                "SELECT device_id, seq FROM events "
                "WHERE segment_id IN ("
                "SELECT id FROM segments WHERE state='quarantined')"
            ).fetchall()
            self.conn.execute(
                "DELETE FROM events WHERE segment_id IN ("
                "SELECT id FROM segments WHERE state='quarantined')")
            self.conn.commit()
            return {(r["device_id"], r["seq"]) for r in rows}

    def event_exists(self, device_id: str, seq: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM events WHERE device_id=? AND seq=? LIMIT 1",
                (device_id, seq)).fetchone()
            return row is not None

    def get_event(self, device_id: str, seq: int) -> sqlite3.Row | None:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM events WHERE device_id=? AND seq=?",
                (device_id, seq)).fetchone()

    def query_events(self, device_id: str, ts_from: int | None,
                     ts_to: int | None, limit: int) -> list[sqlite3.Row]:
        with self.lock:
            sql = ("SELECT * FROM events WHERE device_id=? "
                   "AND status!='quarantined'")
            args: list[Any] = [device_id]
            if ts_from is not None:
                sql += " AND event_ts>=?"
                args.append(ts_from)
            if ts_to is not None:
                sql += " AND event_ts<=?"
                args.append(ts_to)
            sql += " ORDER BY seq ASC LIMIT ?"
            args.append(limit)
            return self.conn.execute(sql, args).fetchall()

    def mark_suspect_range(self, segment_id: int, from_offset: int) -> int:
        with self.lock:
            cur = self.conn.execute(
                "UPDATE events SET status='suspect' WHERE segment_id=? "
                "AND offset>=? AND status='ok'", (segment_id, from_offset))
            self.conn.commit()
            return cur.rowcount

    def count_events(self) -> int:
        with self.lock:
            return self.conn.execute(
                "SELECT COUNT(*) c FROM events").fetchone()["c"]

    def min_first_wal_with_quarantine(self) -> int | None:
        """存在隔离分段时，返回其中最早的 first_wal_seq。

        这些 WAL 是重建隔离数据的来源，在重建完成前不得回收。
        """
        with self.lock:
            row = self.conn.execute(
                "SELECT MIN(first_wal_seq) m FROM segments "
                "WHERE state='quarantined' AND first_wal_seq IS NOT NULL"
            ).fetchone()
            return row["m"]

    def max_wal_seq_healthy(self) -> int:
        """健康（非隔离）分段中已持久化的最大 WAL 序列号，用于 WAL 回收。"""
        with self.lock:
            row = self.conn.execute(
                "SELECT COALESCE(MAX(last_wal_seq),0) m FROM segments "
                "WHERE state!='quarantined'").fetchone()
            return row["m"]

    # ---- device watermarks ---------------------------------------
    def get_device(self, device_id: str) -> sqlite3.Row | None:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM device_wm WHERE device_id=?", (device_id,)
            ).fetchone()

    def put_device(self, device_id: str, max_seq: int,
                   max_event_ts: int) -> None:
        with self.lock:
            self.conn.execute(
                "INSERT INTO device_wm(device_id,max_seq,max_event_ts) "
                "VALUES(?,?,?) ON CONFLICT(device_id) DO UPDATE SET "
                "max_seq=MAX(excluded.max_seq,device_wm.max_seq), "
                "max_event_ts=MAX(excluded.max_event_ts,device_wm.max_event_ts)",
                (device_id, max_seq, max_event_ts))
            self.conn.commit()

    def list_devices(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM device_wm ORDER BY device_id").fetchall()

    # ---- snapshots ------------------------------------------------
    def create_snapshot(self, name: str, wal_hwm: int, note: str) -> None:
        with self.lock:
            self.conn.execute(
                "INSERT INTO snapshots(name,wal_hwm,created_at,note) "
                "VALUES(?,?,?,?)",
                (name, wal_hwm, int(time.time() * 1000), note))
            self.conn.commit()

    def get_snapshot(self, name: str) -> sqlite3.Row | None:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM snapshots WHERE name=?", (name,)).fetchone()

    def list_snapshots(self) -> list[sqlite3.Row]:
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM snapshots ORDER BY created_at").fetchall()

    def snapshot_segments(self, wal_hwm: int) -> list[sqlite3.Row]:
        """冻结视图覆盖的分段：封存且 WAL 范围与 hwm 相交。"""
        with self.lock:
            return self.conn.execute(
                "SELECT * FROM segments WHERE state='sealed' AND "
                "first_wal_seq IS NOT NULL AND first_wal_seq<=? "
                "ORDER BY window_start, ordinal", (wal_hwm,)).fetchall()

    def events_by_wal_range(self, lo: int | None, hi: int,
                            limit: int) -> list[sqlite3.Row]:
        with self.lock:
            if lo is None:
                return self.conn.execute(
                    "SELECT * FROM events WHERE wal_seq<=? AND "
                    "status!='quarantined' ORDER BY wal_seq LIMIT ?",
                    (hi, limit)).fetchall()
            return self.conn.execute(
                "SELECT * FROM events WHERE wal_seq>? AND wal_seq<=? AND "
                "status!='quarantined' ORDER BY wal_seq LIMIT ?",
                (lo, hi, limit)).fetchall()
