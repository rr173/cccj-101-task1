#!/usr/bin/env python3
"""端到端验证。

本地运行（自动拉起/杀掉服务进程，需要能访问数据目录）：
    python3 tests/e2e.py

容器内运行（对接已启动的 archive 服务，不做 kill/restart/损坏注入）：
    EA_BASE_URL=http://archive:8080 python3 tests/e2e.py --remote

退出码非 0 表示有断言失败。
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = os.environ.get("EA_BASE_URL")
DATA_DIR = os.environ.get("EA_DATA_DIR", "/tmp/ea-e2e-data")
REMOTE = "--remote" in sys.argv
ROOT = Path(__file__).resolve().parents[1]

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def call(method: str, path: str, body=None, expect_error: bool = False):
    url = BASE.rstrip("/") + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as e:
        payload = json.loads(e.read() or b"{}")
        if expect_error:
            return e.code, payload
        raise RuntimeError(f"{method} {path} -> {e.code} {payload}")


def wait_healthy(timeout: float = 15) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            call("GET", "/healthz")
            return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError("服务未在规定时间内就绪")


_proc = None


def start_server(kill: bool = False):
    global _proc
    env = dict(os.environ, EA_DATA_DIR=DATA_DIR, EA_PORT="8080",
               EA_HOST="127.0.0.1", EA_FLUSH_INTERVAL="0.2",
               EA_WINDOW_MS="3600000", EA_LATE_GRACE_MS="300000",
               PYTHONPATH=str(ROOT))
    _proc = subprocess.Popen(
        [sys.executable, "-m", "app.main"], cwd=ROOT, env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_healthy()
    return _proc


def stop_server(graceful: bool = True) -> str:
    global _proc
    if not _proc:
        return ""
    if graceful:
        _proc.send_signal(signal.SIGTERM)
    else:
        _proc.send_signal(signal.SIGKILL)
    code = _proc.wait(timeout=20)
    out = ""
    try:
        out = _proc.stdout.read() if _proc.stdout else ""
    except Exception:  # noqa: BLE001
        pass
    _proc = None
    if code != 0 and graceful:
        print(f"  服务退出码 {code}\n{out[-2000:]}")
    return out


def flush() -> None:
    status, body = call("POST", "/v1/admin/flush", {"timeout": 30})
    assert body.get("flushed"), f"刷盘未排空: {body}"


# ----------------------------------------------------------------------
def case_basic_classification():
    print("\n[1] 正常写入、迟到、重复、时钟回拨、补传跳号")
    now = int(time.time() * 1000)

    _, r1 = call("POST", "/v1/events",
                 {"device_id": "dev-A", "seq": 1,
                  "event_ts": now, "data": {"v": 1}})
    a1 = r1["results"][0]
    check("正常写入 accepted", a1["accepted"] and not a1["flags"])

    # 完全重复（同设备同序号，哪怕 data 不同）
    _, r2 = call("POST", "/v1/events",
                 {"device_id": "dev-A", "seq": 1,
                  "event_ts": now, "data": {"v": 999}})
    a2 = r2["results"][0]
    check("重复写入被识别且幂等（不分配新 WAL 序列号）",
          (not a2["accepted"]) and a2["duplicate"]
          and a2["wal_seq"] is None, str(a2))
    # 落分段后的重复则直接返回原始 WAL 序列号（跨重启同样成立）
    flush()
    _, r2b = call("POST", "/v1/events",
                  {"device_id": "dev-A", "seq": 1,
                   "event_ts": now, "data": {"v": 999}})
    a2b = r2b["results"][0]
    check("落盘后的重复返回原始 WAL 序列号",
          (not a2b["accepted"]) and a2b["wal_seq"] == a1["wal_seq"],
          str(a2b))

    # 迟到：seq 回退补一个旧序号、事件时间在 1 小时前
    late_ts = now - 3_600_000
    _, r3 = call("POST", "/v1/events",
                 {"device_id": "dev-A", "seq": 0,
                  "event_ts": late_ts, "data": {"v": "late"}})
    a3 = r3["results"][0]
    check("迟到数据打 late 标且仍接收",
          a3["accepted"] and a3["flags"].get("late"))

    # 跳号补传：直接发 seq=10
    _, r4 = call("POST", "/v1/events",
                 {"device_id": "dev-A", "seq": 10,
                  "event_ts": now + 1000, "data": {"v": 10}})
    a4 = r4["results"][0]
    check("跳号补传打 gap_fill 标", a4["accepted"]
          and a4["flags"].get("gap_fill"))

    # 时钟回拨：seq 继续前进，但事件时间比水位早很多
    _, r5 = call("POST", "/v1/events",
                 {"device_id": "dev-A", "seq": 11,
                  "event_ts": now - 7_200_000, "data": {"v": "clockback"}})
    a5 = r5["results"][0]
    check("时钟回拨打 rollback 标", a5["accepted"]
          and a5["flags"].get("rollback"))

    # 离线批量补传：另一台设备一批事件，乱序发送
    batch = [{"device_id": "dev-B", "seq": i,
              "event_ts": now - 2 * 3600_000 + i * 1000,
              "data": {"i": i}} for i in range(20, -1, -1)]
    _, rb = call("POST", "/v1/events", {"events": batch})
    check("批量补传全部接收", len(rb["results"]) == 21
          and all(x["accepted"] for x in rb["results"]),
          str(rb.get("errors")))
    # 再发其中两条 -> 全是重复
    _, rb2 = call("POST", "/v1/events", {"events": batch[5:7]})
    check("补传批次中的重发被去重",
          all((not x["accepted"]) and x["duplicate"]
              for x in rb2["results"]))

    flush()

    # 查询按业务序列号排序，迟到的 seq=0 在最前
    _, q = call("GET", "/v1/events?device=dev-A&limit=100")
    seqs = [e["seq"] for e in q["events"]]
    check("查询结果按设备业务序号排序",
          seqs == sorted(seqs) and seqs[:2] == [0, 1], str(seqs))
    check("迟到事件载荷完整",
          next(e["data"] == {"v": "late"} for e in q["events"]
               if e["seq"] == 0))
    check("重复事件未产生第二条", len(seqs) == len(set(seqs)) == 4)
    _, qb = call("GET", "/v1/events?device=dev-B&limit=100")
    check("设备 B 补传 21 条且按序号排序",
          [e["seq"] for e in qb["events"]] == list(range(21)))


def case_freeze_replay():
    print("\n[2] 冻结视图与回放，且回放不混入新数据")
    now = int(time.time() * 1000)
    _, fr = call("POST", "/v1/snapshots",
                 {"name": "sp-1", "note": "事故复盘点"})
    check("冻结视图创建成功", fr["wal_hwm"] > 0
          and fr["segment_count"] >= 1, str(fr))
    frozen_segments = {s["name"] for s in fr["segments"]}
    frozen_hwm = fr["wal_hwm"]

    # 冻结后新到数据
    _, ing = call("POST", "/v1/events", {"events": [
        {"device_id": "dev-A", "seq": 20, "event_ts": now,
         "data": {"v": "after-freeze"}},
        {"device_id": "dev-C", "seq": 1, "event_ts": now,
         "data": {"v": "new-device"}},
    ]})
    check("冻结后新数据照常接收",
          all(r["accepted"] for r in ing["results"]))
    flush()

    # 回放分页走完全部，全部 wal_seq <= hwm
    cursor = None
    replayed = []
    pages = 0
    while True:
        path = f"/v1/replay/sp-1?limit=7"
        if cursor is not None:
            path += f"&cursor={cursor}"
        _, page = call("GET", path)
        pages += 1
        replayed += page["events"]
        check(f"第 {pages} 页不超过冻结 HWM",
              all(e["wal_seq"] <= frozen_hwm for e in page["events"]),
              f"hwm={frozen_hwm}")
        if page["done"]:
            break
        cursor = page["next_cursor"]
    check("回放分页覆盖冻结时刻全部事件",
          pages > 1 and len(replayed) == 25,  # 4(dev-A) + 21(dev-B)
          f"pages={pages} n={len(replayed)}")
    keys = {(e["device_id"], e["seq"]) for e in replayed}
    check("回放不含冻结后新数据",
          ("dev-A", 20) not in keys and ("dev-C", 1) not in keys)

    # 冻结后写入落到了不同分段（物理隔离）：冻结后新数据所在分段
    # 不在冻结清单中（可能仍为 open，也可能已封存）
    _, segs = call("GET", "/v1/segments")
    post_freeze_segments = [s for s in segs["segments"]
                            if s["last_wal_seq"] is not None
                            and s["last_wal_seq"] > frozen_hwm]
    check("冻结后数据落入新分段",
          len(post_freeze_segments) >= 1
          and all(s["name"] not in frozen_segments
                  for s in post_freeze_segments),
          str(post_freeze_segments))

    # 实时查询能看到新数据
    _, q = call("GET", "/v1/events?device=dev-C&limit=10")
    check("实时查询不受冻结影响", len(q["events"]) == 1
          and q["events"][0]["data"] == {"v": "new-device"})

    # 视图不可重名
    code, _ = call("POST", "/v1/snapshots", {"name": "sp-1"},
                   expect_error=True)
    check("冻结视图名唯一", code == 400)
    _, lst = call("GET", "/v1/snapshots")
    check("视图可列出", any(s["name"] == "sp-1" for s in lst["snapshots"]))


def case_restart_graceful():
    print("\n[3] 优雅重启后数据仍在、视图仍在")
    if not REMOTE:
        stop_server(graceful=True)
        start_server()
    _, q = call("GET", "/v1/events?device=dev-B&limit=100")
    check("重启后补传数据不丢", len(q["events"]) == 21)
    _, s = call("GET", "/v1/snapshots/sp-1")
    check("重启后冻结视图仍可回放", s.get("wal_hwm", 0) > 0)


def case_crash_durability():
    print("\n[4] kill -9 崩溃：已确认数据不丢")
    if REMOTE:
        print("  SKIP  远程模式不杀进程")
        return
    now = int(time.time() * 1000)
    # 发一批后立即 kill -9，不做 flush（部分可能还只在 WAL 里）
    _, r = call("POST", "/v1/events", {"events": [
        {"device_id": "dev-D", "seq": i, "event_ts": now + i,
         "data": {"i": i}} for i in range(50)]})
    confirmed = [x for x in r["results"] if x["accepted"]]
    check("崩溃前 50 条全部已确认", len(confirmed) == 50)
    stop_server(graceful=False)
    time.sleep(0.5)
    start_server()
    flush()
    _, q = call("GET", "/v1/events?device=dev-D&limit=100")
    check("崩溃恢复后 50 条已确认数据全部可查",
          len(q["events"]) == 50, f"n={len(q['events'])}")
    check("数据按业务序号有序",
          [e["seq"] for e in q["events"]] == list(range(50)))


def case_corruption():
    print("\n[5] 分段损坏：隔离影响范围并给出续传位置")
    if REMOTE:
        print("  SKIP  远程模式不注入磁盘损坏")
        return
    stop_server(graceful=True)

    # 选一个较大的封存分段注入损坏；优先选含 dev-D（崩溃恢复写入）
    # 的最新分段，避免影响其它断言依赖的 dev-B 数据
    seg_dir = Path(DATA_DIR) / "segments"
    candidates = sorted(seg_dir.glob("seg-*.seg"),
                        key=lambda p: p.stat().st_size, reverse=True)
    target = next((p for p in candidates if p.stat().st_size > 300), None)
    check("找到可注入损坏的分段", target is not None)
    if target is None:
        start_server()
        return
    size = target.stat().st_size
    with open(target, "r+b") as f:
        f.seek(120)
        f.write(b"\xde\xad\xbe\xef" * 8)
    print(f"  注入损坏: {target.name} size={size}")

    start_server()
    _, st = call("GET", "/v1/segments")
    quarantined = [s for s in st["segments"] if s["state"] == "quarantined"]
    check("损坏分段被隔离", any(
        Path(s["name"]).name == target.name for s in quarantined),
        str([s["name"] for s in quarantined]))

    bad_files = list((Path(DATA_DIR) / "quarantine").glob("*.bad"))
    check("损坏文件移入 quarantine 目录", len(bad_files) >= 1)

    # 其他分段查询正常，损坏分段事件带状态而非整服务失败
    _, qb = call("GET", "/v1/events?device=dev-B&limit=100")
    healthy = [e for e in qb["events"] if e["status"] == "ok"]
    check("未损坏设备数据完全不受影响", len(healthy) == 21,
          f"{[e['status'] for e in qb['events']]}")

    # 续传位置 & WAL 重建：隔离分段的数据从 WAL 重放进新分段
    _, stats = call("GET", "/v1/stats")
    check("服务仍可继续写入（续传位置已推进）", stats["durable_hwm"] > 0)
    _, rb = call("POST", "/v1/admin/rebuild", {})
    check("从 WAL 重建隔离分段数据", rb["rebuilt_events"] > 0
          and rb["quarantined_keys"] > 0, str(rb))
    flush()
    _, qd = call("GET", "/v1/events?device=dev-D&limit=100")
    check("重建后崩溃恢复批次数据可重新查询",
          len(qd["events"]) == 50
          and all(e["status"] == "ok" for e in qd["events"]),
          f"n={len(qd['events'])}")
    now = int(time.time() * 1000)
    _, post = call("POST", "/v1/events",
                   {"device_id": "dev-E", "seq": 0,
                    "event_ts": now, "data": {"after": "corruption"}})
    check("损坏隔离后新写入正常", post["results"][0]["accepted"])
    flush()
    _, qe = call("GET", "/v1/events?device=dev-E&limit=10")
    check("损坏后新数据可查询", len(qe["events"]) == 1
          and qe["events"][0]["status"] == "ok")

    # 再次重启确认隔离状态持久、无重复膨胀
    stop_server(graceful=True)
    start_server()
    _, qb2 = call("GET", "/v1/events?device=dev-B&limit=100")
    check("二次重启后无重复、总数稳定", len(qb2["events"]) == 21)


def main() -> int:
    global BASE
    if REMOTE:
        BASE = BASE or "http://127.0.0.1:8080"
        wait_healthy()
    else:
        if os.path.exists(DATA_DIR):
            import shutil
            shutil.rmtree(DATA_DIR)
        BASE = "http://127.0.0.1:8080"
        start_server()

    try:
        case_basic_classification()
        case_freeze_replay()
        case_restart_graceful()
        case_crash_durability()
        case_corruption()
    finally:
        if not REMOTE:
            stop_server(graceful=True)

    print(f"\n==== 结果: {PASS} 通过, {FAIL} 失败 ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
