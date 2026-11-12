#!/usr/bin/env python3
"""Executable acceptance assertions for the research-results harvester.

Runs fully offline: each scenario spins up the mock server on an ephemeral
localhost port and exercises the collector against it. Backoff waits use
scaled-down sleeps, never real long waits.

Usage:  python tests/acceptance.py
Exit code 0 = all checks passed.
"""
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from server.mock_server import make_server          # noqa: E402
from collector.collector import Collector           # noqa: E402
from collector import db                            # noqa: E402

T0 = 1_700_000_000.0
T1 = T0 + 60.0
T2 = T0 + 120.0

FAILURES = []


def check(cond, label, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f"  -- {detail}" if (detail and not cond) else ""))
    if not cond:
        FAILURES.append(label)


def print_report(rep):
    iv = rep["confirmed_interval"]
    ivs = f"[{iv[0]}, {iv[1]}]" if iv else "-"
    print(f"  report {rep['tenant']}: confirmed={ivs}"
          f" rounds={rep['rounds_completed']} versions={rep['versions']}"
          f" accessions={rep['accessions']} tombstones={rep['tombstones']}"
          f" pages={rep['pages']}")


def post(base, path, obj):
    req = urllib.request.Request(
        base + path, data=json.dumps(obj).encode(),
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def get(base, path):
    with urllib.request.urlopen(base + path, timeout=10) as resp:
        return json.loads(resp.read().decode())


class Env:
    """One mock server + one temp db directory per scenario."""

    def __init__(self):
        self.httpd, self.state = make_server("127.0.0.1", 0)
        self.port = self.httpd.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(
            target=self.httpd.serve_forever,
            kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()
        self.tmp = tempfile.mkdtemp(prefix="harvest-acc-")
        self.db_path = os.path.join(self.tmp, "harvest.db")

    def control(self, path, obj=None):
        return post(self.base, "/control" + path, obj or {})

    def requests(self, tenant=None):
        q = f"?tenant={tenant}" if tenant else ""
        return get(self.base, "/control/requests" + q)["requests"]

    def seed(self, n, modified_at, prefix="a"):
        for i in range(n):
            self.state.add_version(f"{prefix}{i:04d}",
                                   modified_at=modified_at, payload={"i": i})

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=3)


def pages_confirmed(db_path, tenant=None):
    if not os.path.exists(db_path):
        return 0
    try:
        conn = sqlite3.connect(db_path, timeout=1)
        try:
            if tenant:
                row = conn.execute(
                    "SELECT pages_confirmed FROM scan_state WHERE tenant=?",
                    (tenant,)).fetchone()
                return row[0] if row else 0
            row = conn.execute(
                "SELECT COALESCE(SUM(pages_confirmed),0) FROM scan_state"
            ).fetchone()
            return row[0] or 0
        finally:
            conn.close()
    except sqlite3.Error:
        return 0


# ---------------------------------------------------------------------------
def scenario_same_second_cross_page():
    """Hundreds of records sharing one modified_at must survive pagination."""
    print("== T1 same-second records across pages: no loss")
    env = Env()
    try:
        env.seed(250, T0)
        c = Collector(env.base, env.db_path, ["t1"], page_size=60,
                      retry_scale=0.01)
        res = c.run(rounds=1)
        conn = db.connect(env.db_path)
        rep = db.report(conn, "t1")
        print_report(rep)
        check(res["t1"].status == "ok", "T1: tenant completes")
        check(rep["versions"] == 250 and rep["accessions"] == 250,
              "T1: all 250 same-second records collected across pages",
              f"versions={rep['versions']}")
        check(rep["confirmed_interval"] == [0.0, T0],
              "T1: confirmed interval is [0, T0]")
        check(rep["pages"] == 5, "T1: 5 pages for 250 records @ page size 60",
              f"pages={rep['pages']}")
        conn.close()
    finally:
        env.close()


def scenario_cursor_expiry_resume():
    """Expired cursor -> re-open from persisted composite boundary, no gap."""
    print("== T2 cursor expiry: resume from composite boundary without gap")
    env = Env()
    try:
        env.seed(120, T0, "t0-")
        env.seed(30, T1, "t1-")
        env.control("/trigger", {"tenant": "t1", "after_requests": 2,
                                 "action": {"type": "expire_cursors"}})
        c = Collector(env.base, env.db_path, ["t1"], page_size=50,
                      retry_scale=0.01)
        res = c.run(rounds=1)
        conn = db.connect(env.db_path)
        rep = db.report(conn, "t1")
        print_report(rep)
        check(res["t1"].status == "ok", "T2: tenant completes after expiry")
        check(c.reopens["t1"] >= 1,
              "T2: collector re-opened the scan from persisted boundary")
        check(rep["versions"] == 150 and rep["accessions"] == 150,
              "T2: no gap and no duplicate after resume",
              f"versions={rep['versions']}")
        reqs = env.requests("t1")
        check(any(r["status"] == 410 for r in reqs),
              "T2: server answered 410 for the expired cursor")
        check(any("after_accession=t0-0049" in r["path"] and "until=" in r["path"]
                  for r in reqs),
              "T2: resume used persisted composite boundary (modified_at, accession)")
        n410 = conn.execute(
            "SELECT COUNT(*) c FROM raw_pages WHERE tenant='t1' AND status=410"
        ).fetchone()["c"]
        check(n410 == 1, "T2: 410 response stored in raw store", f"n={n410}")
        accs = {r["accession"] for r in conn.execute(
            "SELECT DISTINCT accession FROM result_versions WHERE tenant='t1'")}
        expect = ({f"t0-{i:04d}" for i in range(120)}
                  | {f"t1-{i:04d}" for i in range(30)})
        check(accs == expect, "T2: exact accession set collected")
        conn.close()
    finally:
        env.close()


def scenario_429_retry_and_tenant_isolation():
    """429 -> wait Retry-After; the throttled tenant must not slow others."""
    print("== T3 429: Retry-After honoured, other tenant not slowed")
    env = Env()
    try:
        env.seed(120, T0)
        env.control("/latency", {"seconds": 0.03})
        env.control("/fault", {"tenant": "A", "kind": 429, "times": 1,
                               "retry_after": 5.0})
        c = Collector(env.base, env.db_path, ["A", "B"], page_size=10,
                      in_flight=2, retry_scale=0.05)  # 5s -> 0.25s real
        res = c.run(rounds=1)
        conn = db.connect(env.db_path)
        ra, rb = db.report(conn, "A"), db.report(conn, "B")
        print_report(ra)
        print_report(rb)
        check(res["A"].status == "ok" and res["B"].status == "ok",
              "T3: both tenants complete")
        check(ra["versions"] == 120 and rb["versions"] == 120,
              "T3: both tenants collected everything")
        s429 = [e for e in c.sleeper.log
                if e["tenant"] == "A" and e["reason"] == "429"]
        check(len(s429) == 1 and s429[0]["seconds"] == 5.0,
              "T3: collector waited exactly Retry-After (5s, scaled)",
              f"log={s429}")
        reqs = env.requests()
        a429 = [r for r in reqs if r["tenant"] == "A" and r["status"] == 429]
        check(len(a429) == 1, "T3: exactly one 429 issued")
        t429 = a429[0]["ts"]
        a_next = [r for r in reqs if r["tenant"] == "A" and r["ts"] > t429]
        check(a_next and a_next[0]["ts"] - t429 >= 0.2,
              "T3: A actually waited before retrying",
              f"delta={a_next[0]['ts'] - t429 if a_next else None}")
        check(any(r["tenant"] == "B" and t429 <= r["ts"] <= t429 + 0.25
                  for r in reqs),
              "T3: tenant B kept progressing during A's backoff")
        a_paths = [r["path"] for r in reqs if r["tenant"] == "A"]
        check(len(a_paths) >= 2 and a_paths[0] == a_paths[1],
              "T3: the 429'd request was retried unchanged")
        check(env.state.max_inflight <= 2,
              "T3: in-flight limit respected",
              f"max_inflight={env.state.max_inflight}")
        conn.close()
    finally:
        env.close()


def scenario_503_recovery():
    """Short 503 bursts are retried and the scan recovers."""
    print("== T4 503: transient retries and recovery")
    env = Env()
    try:
        env.seed(25, T0)
        env.control("/fault", {"tenant": "t1", "kind": 503, "times": 3,
                               "retry_after": 0.5})
        c = Collector(env.base, env.db_path, ["t1"], page_size=10,
                      retry_scale=0.05)
        res = c.run(rounds=1)
        conn = db.connect(env.db_path)
        rep = db.report(conn, "t1")
        print_report(rep)
        s503 = [e for e in c.sleeper.log if e["reason"] == "503"]
        check(res["t1"].status == "ok", "T4: recovered from 503 burst")
        check(len(s503) == 3 and all(e["seconds"] == 0.5 for e in s503),
              "T4: backed off per Retry-After on each 503", f"log={s503}")
        check(rep["versions"] == 25, "T4: all records collected after recovery")
        n503 = len([r for r in env.requests("t1") if r["status"] == 503])
        check(n503 == 3, "T4: server saw exactly 3 transient failures")
        conn.close()
    finally:
        env.close()


def scenario_401_stops_tenant():
    """401 -> the tenant stops immediately and sends no further requests."""
    print("== T5 401: tenant stops, no further requests")
    env = Env()
    try:
        env.seed(100, T0)
        env.control("/trigger", {"tenant": "A", "after_requests": 3,
                                 "action": {"type": "fault", "kind": 401,
                                            "times": None, "retry_after": 0}})
        c = Collector(env.base, env.db_path, ["A", "B"], page_size=10,
                      retry_scale=0.01)
        res = c.run(rounds=1)
        conn = db.connect(env.db_path)
        ra, rb = db.report(conn, "A"), db.report(conn, "B")
        print_report(ra)
        print_report(rb)
        check(res["A"].status == "stopped_401", "T5: tenant A stopped on 401")
        check(res["B"].status == "ok" and rb["versions"] == 100,
              "T5: tenant B unaffected")
        reqs_a = env.requests("A")
        idx401 = [i for i, r in enumerate(reqs_a) if r["status"] == 401]
        check(len(idx401) == 1 and idx401[0] == len(reqs_a) - 1,
              "T5: zero requests from A after the 401",
              f"requests={len(reqs_a)}")
        check(ra["versions"] == 20,
              "T5: A's confirmed pages persisted before the stop",
              f"versions={ra['versions']}")
        conn.close()
    finally:
        env.close()


def scenario_kill_and_restart():
    """SIGKILL mid-write, then restart: state is consistent and converges."""
    print("== T6 kill during write, then restart")
    env = Env()
    try:
        for i in range(200):
            env.state.add_version(f"k{i:04d}",
                                  modified_at=T0 + (i % 4) * 60.0,
                                  payload={"i": i})
        env.control("/latency", {"seconds": 0.15})
        cmd = [sys.executable, os.path.join(ROOT, "run_fetch.py"),
               "--server", env.base, "--db", env.db_path,
               "--tenants", "k1", "--rounds", "1",
               "--page-size", "20", "--retry-scale", "0.01"]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        pages = 0
        deadline = time.time() + 30
        while time.time() < deadline:
            pages = pages_confirmed(env.db_path, "k1")
            if pages >= 3:
                break
            time.sleep(0.05)
        check(pages >= 3, "T6: collector committed >=3 pages before kill",
              f"pages={pages}")
        proc.kill()
        proc.communicate(timeout=10)

        conn = db.connect(env.db_path)
        st = db.load_scan_state(conn, "k1")
        wm = db.load_watermark(conn, "k1")
        versions = conn.execute(
            "SELECT COUNT(*) c FROM result_versions WHERE tenant='k1'"
        ).fetchone()["c"]
        committed = st["pages_confirmed"] if st else 0
        check(st is not None and st["status"] == "in_progress",
              "T6: scan_state persisted as in_progress after SIGKILL")
        check(wm is None,
              "T6: high watermark NOT advanced before round completion")
        check(versions == committed * 20,
              "T6: page writes and pagination progress committed atomically",
              f"versions={versions} pages={committed}")
        check(committed < 10, "T6: killed before the round completed",
              f"pages={committed}")
        conn.close()

        res = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        check(res.returncode == 0, "T6: restarted run exits 0",
              (res.stdout or "")[-400:])
        conn = db.connect(env.db_path)
        rep = db.report(conn, "k1")
        print_report(rep)
        wm = db.load_watermark(conn, "k1")
        check(rep["versions"] == 200 and rep["accessions"] == 200,
              "T6: converged to the full dataset after restart",
              f"versions={rep['versions']}")
        check(wm and wm["high_modified_at"] == T0 + 3 * 60.0,
              "T6: high watermark == max modified_at")
        accs = {r["accession"] for r in conn.execute(
            "SELECT DISTINCT accession FROM result_versions WHERE tenant='k1'")}
        check(accs == {f"k{i:04d}" for i in range(200)},
              "T6: no gaps after restart")
        check(rep["pages"] >= 10,
              "T6: all pages fetched (retransmission absorbed)",
              f"pages={rep['pages']}")
        conn.close()
    finally:
        env.close()


def scenario_convergence_next_round():
    """Writes/retractions during a scan stay out of it; next round converges."""
    print("== T7 mid-scan writes isolated; next round converges")
    env = Env()
    try:
        env.seed(50, T0, "a")  # a0000..a0049
        actions = [{"type": "time", "set": T2}]
        actions += [{"type": "record", "accession": f"r-new-{i}",
                     "modified_at": T2, "payload": {"i": i}} for i in range(5)]
        actions.append({"type": "record", "accession": "r-back",
                        "modified_at": T0, "payload": {"backdated": True}})
        actions.append({"type": "retract", "accession": "a0007",
                        "modified_at": T2})
        env.control("/trigger", {"tenant": "t1", "after_requests": 2,
                                 "action": actions})
        c = Collector(env.base, env.db_path, ["t1"], page_size=20,
                      retry_scale=0.01)
        res = c.run(rounds=2)
        conn = db.connect(env.db_path)
        rep = db.report(conn, "t1")
        print_report(rep)
        check(res["t1"].status == "ok", "T7: two rounds complete")
        check(rep["rounds_completed"] == 2, "T7: two rounds recorded")
        check(rep["versions"] == 57,
              "T7: 50 originals + 5 new + 1 backdated + 1 tombstone",
              f"versions={rep['versions']}")
        check(rep["tombstones"] == 1, "T7: tombstone stored")
        check(rep["confirmed_interval"][1] == T2,
              "T7: high watermark advanced to T2")
        leak = conn.execute(
            "SELECT COUNT(*) c FROM result_versions v"
            " JOIN raw_pages p ON v.raw_page_id = p.id"
            " WHERE p.scan_until=? AND (v.accession LIKE 'r-new-%'"
            " OR v.accession='r-back'"
            " OR (v.accession='a0007' AND v.revision=2))", (T0,)).fetchone()["c"]
        check(leak == 0,
              "T7: writes during the scan did not enter the frozen scan view")
        rb = conn.execute(
            "SELECT COUNT(*) c FROM result_versions WHERE accession='r-back'"
        ).fetchone()["c"]
        check(rb == 1, "T7: backdated write converged in the next round")
        cur = conn.execute(
            "SELECT retracted FROM result_versions WHERE accession='a0007'"
            " ORDER BY revision DESC LIMIT 1").fetchone()
        check(cur and cur["retracted"] == 1,
              "T7: retraction is the current version of a0007")
        conn.close()
    finally:
        env.close()


def scenario_cancel_and_resume():
    """cancel() stops promptly; a fresh collector resumes from persisted state."""
    print("== T8 cancel and restart; in-flight limit enforced")
    env = Env()
    try:
        env.seed(100, T0)
        env.control("/latency", {"seconds": 0.05})
        c = Collector(env.base, env.db_path, ["c1", "c2"], page_size=10,
                      in_flight=1, retry_scale=0.01)
        th = threading.Thread(target=lambda: c.run(rounds=1), daemon=True)
        th.start()
        deadline = time.time() + 15
        pages = 0
        while time.time() < deadline:
            pages = pages_confirmed(env.db_path)
            if pages >= 3:
                break
            time.sleep(0.05)
        check(pages >= 3, "T8: progress before cancel", f"pages={pages}")
        c.cancel()
        th.join(timeout=15)
        check(not th.is_alive(), "T8: collector stopped promptly after cancel")
        check(c.results and all(r.status == "cancelled"
                                for r in c.results.values()),
              "T8: tenants report cancelled",
              f"statuses={[r.status for r in c.results.values()]}")
        check(env.state.max_inflight == 1,
              "T8: in-flight limit of 1 respected",
              f"max_inflight={env.state.max_inflight}")
        conn = db.connect(env.db_path)
        inprog = conn.execute(
            "SELECT COUNT(*) c FROM scan_state WHERE status='in_progress'"
        ).fetchone()["c"]
        check(inprog >= 1, "T8: interrupted round persisted for resume")
        conn.close()

        c2 = Collector(env.base, env.db_path, ["c1", "c2"], page_size=10,
                       in_flight=2, retry_scale=0.01)
        res2 = c2.run(rounds=1)
        conn = db.connect(env.db_path)
        r1, r2 = db.report(conn, "c1"), db.report(conn, "c2")
        print_report(r1)
        print_report(r2)
        check(res2["c1"].status == "ok" and res2["c2"].status == "ok",
              "T8: restarted collector completes")
        check(r1["versions"] == 100 and r2["versions"] == 100,
              "T8: both tenants converged after resume")
        conn.close()
    finally:
        env.close()


def main():
    scenarios = [
        scenario_same_second_cross_page,
        scenario_cursor_expiry_resume,
        scenario_429_retry_and_tenant_isolation,
        scenario_503_recovery,
        scenario_401_stops_tenant,
        scenario_kill_and_restart,
        scenario_convergence_next_round,
        scenario_cancel_and_resume,
    ]
    started = time.time()
    for fn in scenarios:
        fn()
    print("=" * 70)
    if FAILURES:
        print(f"{len(FAILURES)} CHECK(S) FAILED:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print(f"ALL ACCEPTANCE CHECKS PASSED in {time.time() - started:.1f}s")
    print("\nOffline reproducible commands:")
    print("  python server/mock_server.py --port 8734")
    print("  python run_fetch.py --server http://127.0.0.1:8734 \\")
    print("      --db out/harvest.db --tenants alpha,beta --rounds 2")
    print("  python tests/acceptance.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
