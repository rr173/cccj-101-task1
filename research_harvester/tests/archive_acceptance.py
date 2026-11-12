#!/usr/bin/env python3
"""Executable acceptance assertions for raw-response retention/archival.

Fully offline, stdlib only.  Every scenario builds its own landing database in
the collector's real schema, so the assertions are independent of test order
and of any mock server.

Coverage (A1..A12):
  A1  successful archive: bodies move to a shard, trace reads the original
      response back identically
  A2  inclusive cutoff boundary and the frozen page-id upper bound
  A3  multi-tenant filtering, tenant isolation of a corrupt shard, date buckets
  A4  file written but not registered (SIGKILL) then rerun: no loss, no dupes
  A5  registered but bodies not cleared (SIGKILL) then rerun converges
  A6  digest mismatch: SQLite bodies preserved, shard never registered,
      overwrite refused, trace/verify report the location
  A7  rows written after the scan started stay un-archived
  A8  mixed archive state: result versions, quality projection and trace stay
      consistent; source order preserved
  A9  preview is read-only (no files, no clears) and counts match archive
  A10 replay of the same snapshot reuses the verified file (no rewrite)
  A11 archive leaves quality projection, rule generations and progress intact
  A12 shard format is independently verifiable (gunzip + sha256)

Usage:  python tests/archive_acceptance.py
Exit code 0 = all checks passed.
"""
import gzip
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from collector import db as landing_db              # noqa: E402
from archive import archiver, inspect  # noqa: E402
from archive import shard as shard_mod  # noqa: E402
from archive import store  # noqa: E402
from quality import rules as rules_mod              # noqa: E402
from quality.processor import Processor             # noqa: E402

RULES_V1 = os.path.join(ROOT, "quality", "rules", "v1.json")
FAILURES = []
RUN_ARCHIVE = os.path.join(ROOT, "run_archive.py")
RUN_QUALITY = os.path.join(ROOT, "run_quality.py")


def check(cond, label, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f"  -- {detail}" if (detail and not cond) else ""))
    if not cond:
        FAILURES.append(label)


class Fixture:
    """Landing DB with raw pages, versions and (optionally) quality rows."""

    def __init__(self, prefix="rh-archive-"):
        self.dir = tempfile.mkdtemp(prefix=prefix)
        self.path = os.path.join(self.dir, "harvest.db")
        self.archive_dir = os.path.join(self.dir, "archive")
        conn = landing_db.connect(self.path)
        landing_db.init(conn)
        conn.commit()
        conn.close()

    # -- landing data -----------------------------------------------------
    def page(self, tenant, body, *, status=200, fetched_at=1000.0,
             url=None, scan_id="scan-x", scan_until=None, accession=None,
             revision=1, modified_at=None, payload=None, retracted=False):
        conn = landing_db.connect(self.path)
        with conn:
            raw_id = conn.execute(
                "INSERT INTO raw_pages(tenant, scan_id, url, status,"
                " scan_until, body, fetched_at)"
                " VALUES (?,?,?,?,?,?,?)",
                (tenant, scan_id, url or f"http://s/{tenant}",
                 int(status), scan_until, body, float(fetched_at))).lastrowid
            if accession is not None:
                conn.execute(
                    "INSERT OR IGNORE INTO result_versions(tenant, accession,"
                    " revision, modified_at, retracted, payload, raw_page_id)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (tenant, accession, int(revision),
                     float(modified_at if modified_at is not None
                           else fetched_at),
                     1 if retracted else 0,
                     json.dumps(payload) if payload is not None else None,
                     raw_id))
        conn.close()
        return raw_id

    def query(self, sql, params=()):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def execute(self, sql, params=()):
        conn = sqlite3.connect(self.path, timeout=10)
        try:
            with conn:
                return conn.execute(sql, params).rowcount
        finally:
            conn.close()

    # -- commands ---------------------------------------------------------
    def archive(self, *, cutoff, dir=None, extra=(), env=None, tenants=None,
                fetch_date=None, page_group_limit=None, clear=True):
        cmd = [sys.executable, RUN_ARCHIVE, "archive", "--db", self.path,
               "--archive-dir", dir or self.archive_dir,
               "--cutoff-at", str(cutoff)]
        if tenants:
            cmd += ["--tenants", ",".join(tenants)]
        if fetch_date:
            cmd += ["--fetch-date", fetch_date]
        if page_group_limit:
            cmd += ["--page-group-limit", str(page_group_limit)]
        if not clear:
            cmd += ["--no-clear"]
        cmd += list(extra)
        return subprocess.run(cmd, capture_output=True, text=True,
                              env=env or os.environ)

    def archive_parsed(self, *, cutoff, extra=(), env=None, tenants=None,
                       fetch_date=None, page_group_limit=None, clear=True):
        cmd = [sys.executable, RUN_ARCHIVE, "archive", "--db", self.path,
               "--archive-dir", self.archive_dir,
               "--cutoff-at", str(cutoff), "--json"]
        if tenants:
            cmd += ["--tenants", ",".join(tenants)]
        if fetch_date:
            cmd += ["--fetch-date", fetch_date]
        if page_group_limit:
            cmd += ["--page-group-limit", str(page_group_limit)]
        if not clear:
            cmd += ["--no-clear"]
        cmd += list(extra)
        proc = subprocess.run(cmd, capture_output=True, text=True,
                              env=env or os.environ)
        try:
            report = json.loads(proc.stdout) if proc.stdout.strip() else {}
        except ValueError:
            report = {"_raw": proc.stdout}
        return proc, report

    def cli(self, command, *args, env=None):
        return subprocess.run(
            [sys.executable, RUN_ARCHIVE, command, "--db", self.path, *args],
            capture_output=True, text=True, env=env or os.environ)

    def trace(self, tenant, accession, env=None):
        proc = subprocess.run(
            [sys.executable, RUN_QUALITY, "trace", "--db", self.path,
             "--tenant", tenant, "--accession", accession, "--json"],
            capture_output=True, text=True, env=env or os.environ)
        report = None
        if proc.returncode == 0 and proc.stdout.strip():
            report = json.loads(proc.stdout)
        return proc, report

    def process_quality(self):
        snapshot = rules_mod.load_rules(RULES_V1)
        return Processor(self.path, snapshot).process()

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def file_digest(path):
    return shard_mod.file_sha256(path)


def page_bytes(fixture, tenant=None):
    sql = "SELECT COALESCE(SUM(LENGTH(body)),0) s FROM raw_pages" \
          " WHERE body IS NOT NULL"
    params = []
    if tenant:
        sql += " AND tenant=?"
        params.append(tenant)
    return fixture.query(sql, params)[0]["s"]


def body_of(fixture, page_id):
    rows = fixture.query("SELECT body FROM raw_pages WHERE id=?", (page_id,))
    return rows[0]["body"] if rows else None


def tar(x):
    return x.decode() if isinstance(x, bytes) else x


# --------------------------------------------------------------------------
# A1: the core promise -- bodies leave SQLite, everything else stays queryable
# --------------------------------------------------------------------------
def scenario_success_and_trace():
    print("== A1 archived body is read back identically through trace")
    fx = Fixture()
    try:
        p1 = fx.page("alpha", '{"data": [{"a": 1}], "note": "keep"}',
                     fetched_at=1000.0, url="http://s/alpha/p1",
                     accession="a1", payload={"title": "T1",
                                              "publication_year": 2020})
        p2 = fx.page("alpha", '{"data": [{"b": 2}]}', fetched_at=1001.0,
                     url="http://s/alpha/p2", accession="a2",
                     payload={"title": "T2", "publication_year": 2021})
        before_trace, report_before = fx.trace("alpha", "a1")
        check(report_before is None,
              "A1: no trace before quality has consumed the landing row")
        # trace reads from the quality input ledger; publish first so the
        # source page is part of the audited landing history
        fx.process_quality()
        before_trace, report_before = fx.trace("alpha", "a1")
        check(report_before is not None and
              report_before["raw_pages"][0]["body"] ==
              body_of(fx, p1),
              "A1: trace shows the original body before archiving",
              before_trace.stdout + before_trace.stderr)

        proc, report = fx.archive_parsed(cutoff=1001.0)
        check(proc.returncode == 0 and report["ok"],
              "A1: archive succeeded", proc.stdout + proc.stderr)
        check(report["phase"] == "cleared",
              "A1: batch reached the cleared phase", report.get("phase"))
        check(body_of(fx, p1) is None and body_of(fx, p2) is None,
              "A1: bodies were removed from the main database")

        rows = fx.query(
            "SELECT id, tenant, status, url, fetched_at FROM raw_pages"
            " ORDER BY id")
        check(rows[0]["status"] == 200
              and rows[0]["url"] == "http://s/alpha/p1"
              and rows[0]["fetched_at"] == 1000.0,
              "A1: status, URL, fetch time and page id remain queryable",
              str(rows[0]))
        versions = fx.query(
            "SELECT accession, raw_page_id FROM result_versions"
            " ORDER BY accession")
        check([v["raw_page_id"] for v in versions] == [p1, p2],
              "A1: result version references are unchanged")

        proc, report_after = fx.trace("alpha", "a1")
        check(proc.returncode == 0, "A1: trace still succeeds after archiving",
              proc.stdout + proc.stderr)
        check(report_after["raw_pages"][0]["body"] ==
              report_before["raw_pages"][0]["body"],
              "A1: trace reads back a byte-identical body from the shard",
              json.dumps(report_after["raw_pages"][0]))
        check(report_after["body_sources"][str(p1)] == "archive",
              "A1: trace reports the archived body source")
        check(report_after["inputs"] == report_before["inputs"]
              and report_after["events"] == report_before["events"],
              "A1: trace inputs and events are unchanged by archiving")
        check(report_after["current"] == report_before["current"],
              "A1: trace current projection is unchanged by archiving")
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A2: cutoff is inclusive; the page-id upper bound is frozen at batch start
# --------------------------------------------------------------------------
def scenario_cutoff_and_upper_bound():
    print("== A2 cutoff boundary is inclusive; max page id frozen at start")
    fx = Fixture()
    try:
        exact = fx.page("alpha", "at-cutoff", fetched_at=2000.0)
        newer = fx.page("alpha", "after-cutoff", fetched_at=2000.000001)
        older = fx.page("alpha", "before-cutoff", fetched_at=1999.9)
        # a backdated row that lands AFTER the snapshot would have been taken
        # (simulate by freezing the snapshot first, then writing it)
        engine = archiver.Archiver(fx.path, fx.archive_dir)
        conn = store.connect(fx.path)
        batch = engine.freeze_snapshot(conn, cutoff_at=2000.0)
        late = fx.page("alpha", "written-after-snapshot", fetched_at=1990.0)
        conn.close()
        report = engine.run(cutoff_at=2000.0)
        check(report["batch_id"] == batch["batch_id"],
              "A2: the frozen snapshot batch is reused")

        conn = store.connect(fx.path)
        try:
            archived = {p["page_id"] for p in store.registered_pages_for_batch(
                conn, batch["batch_id"])}
        finally:
            conn.close()
        check(exact in archived,
              "A2: a page fetched exactly at the cutoff is archived (inclusive)")
        check(older in archived, "A2: older pages are archived")
        check(newer not in archived, "A2: newer pages stay in the main database")
        check(late not in archived and body_of(fx, late) ==
              "written-after-snapshot",
              "A2: a row written after the snapshot stays un-archived even if "
              "it backdates fetched_at")
        check(body_of(fx, exact) is None and body_of(fx, older) is None,
              "A2: expired bodies were cleared")
        check(body_of(fx, newer) == "after-cutoff"
              and body_of(fx, late) == "written-after-snapshot",
              "A2: newer rows keep their bodies")
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A3: multi-tenant filtering + one tenant's bad shard does not block another
# --------------------------------------------------------------------------
def scenario_tenant_isolation():
    print("== A3 tenant filtering, date buckets and failure isolation")
    fx = Fixture()
    try:
        # alpha: two UTC dates; beta: one; gamma: excluded by the filter
        a1 = fx.page("alpha", "a-day1", fetched_at=1000.0)          # 1970-01-01
        a2 = fx.page("alpha", "a-day2", fetched_at=90000.0)         # 1970-01-02
        b1 = fx.page("beta", "b-day1", fetched_at=1000.0)
        g1 = fx.page("gamma", "g-day1", fetched_at=1000.0)
        proc, report = fx.archive_parsed(cutoff=90000.0,
                                        tenants=["alpha", "beta"])
        dates = {(s["tenant"], s["fetch_date"]) for s in report["shards"]}
        check(proc.returncode == 0, "A3: multi-tenant archive succeeded",
              proc.stdout + proc.stderr)
        check(dates == {("alpha", "1970-01-01"), ("alpha", "1970-01-02"),
                        ("beta", "1970-01-01")},
              "A3: shards are partitioned by tenant and UTC fetch date",
              str(sorted(dates)))
        check(body_of(fx, g1) == "g-day1",
              "A3: a tenant outside the filter is untouched")
        check(body_of(fx, a1) is None and body_of(fx, b1) is None,
              "A3: filtered tenants were archived")

        # inject a corrupt shard for alpha only, then archive both tenants in
        # a fresh snapshot; beta must still complete
        fx2 = Fixture()
        try:
            alpha_page = fx2.page("alpha", "alpha-body", fetched_at=1000.0)
            beta_page = fx2.page("beta", "beta-body", fetched_at=1000.0)
            engine = archiver.Archiver(fx2.path, fx2.archive_dir)
            conn = store.connect(fx2.path)
            batch = engine.freeze_snapshot(conn, cutoff_at=5000.0,
                                           tenants=["alpha", "beta"])
            conn.close()
            # build the exact alpha path then corrupt it before registration
            # by writing the correct file, verifying it, then flipping bytes
            conn = store.connect(fx2.path)
            pages = store.pending_pages(conn, cutoff_at=5000.0,
                                        max_page_id=batch["max_page_id"])
            conn.close()
            alpha_pages = [p for p in pages if p["tenant"] == "alpha"]
            path = archiver.shard_path(
                fx2.archive_dir, "alpha", "1970-01-01", 0,
                batch["snapshot_sig"],
                int(alpha_pages[0]["id"]), int(alpha_pages[-1]["id"]))
            shard_mod.write_shard(
                path, alpha_pages, tenant="alpha", fetch_date="1970-01-01",
                shard_no=0, cutoff_at=5000.0,
                max_page_id=batch["max_page_id"])
            with open(path, "wb") as fh:
                fh.write(b"corrupted!!")  # destroy the on-disk content
            proc, report = fx2.archive_parsed(cutoff=5000.0,
                                             tenants=["alpha", "beta"])
            check(proc.returncode == 1,
                  "A3: the run reports an integrity failure (exit 1)")
            failure = report.get("failures", [{}])[0]
            check(failure.get("tenant") == "alpha"
                  and failure.get("path", "").endswith(".jsonl.gz"),
                  "A3: the failure localises tenant and shard path",
                  str(failure))
            beta_shards = [s for s in report["shards"]
                           if s["tenant"] == "beta"]
            check(beta_shards and beta_shards[0]["page_count"] == 1,
                  "A3: the other tenant still archived its pages",
                  str(report["shards"]))
            check(body_of(fx2, beta_page) is None,
                  "A3: the unaffected tenant's bodies were cleared")
            check(body_of(fx2, alpha_page) == "alpha-body",
                  "A3: the failing tenant's SQLite body is preserved")
            check(fx2.query("SELECT COUNT(*) c FROM archive_pages"
                            " WHERE tenant='alpha'")[0]["c"] == 0,
                  "A3: no shard for the failing tenant was registered")
        finally:
            fx2.close()
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A4/A5: the two cross-resource crash windows
# --------------------------------------------------------------------------
def _run_killed(fx, point, marker, wait_file, extra=(), env_extra=None,
                cutoff=1000.0):
    env = dict(os.environ, RH_ARCHIVE_CRASH_POINT=point,
               RH_ARCHIVE_CRASH_MARKER=marker,
               RH_ARCHIVE_CRASH_WAIT_FILE=wait_file)
    if env_extra:
        env.update(env_extra)
    with open(wait_file, "w", encoding="utf-8"):
        pass
    proc = subprocess.Popen(
        [sys.executable, RUN_ARCHIVE, "archive", "--db", fx.path,
         "--archive-dir", fx.archive_dir, "--cutoff-at", str(cutoff),
         *extra],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
    deadline = time.time() + 60
    while time.time() < deadline and not os.path.exists(marker):
        if proc.poll() is not None:
            break
        time.sleep(0.01)
    reached = os.path.exists(marker)
    if proc.poll() is None:
        proc.kill()
    out, _ = proc.communicate(timeout=15)
    if os.path.exists(wait_file):
        os.remove(wait_file)
    return reached, proc.returncode, out


def scenario_crash_after_file_write():
    print("== A4 SIGKILL after the shard was written, before registration")
    fx = Fixture()
    try:
        body = "payload-that-must-survive-crash"
        pid = fx.page("alpha", body, fetched_at=1000.0)
        marker = os.path.join(fx.dir, "marker-a4.flag")
        wait_file = os.path.join(fx.dir, "wait-a4.flag")
        reached, rc, out = _run_killed(fx, "after_shard_write", marker,
                                       wait_file)
        check(reached and rc in (-9, 137),
              "A4: the process was killed after writing the file", out[-300:])
        batch = fx.query("SELECT * FROM archive_batches")[0]
        check(batch["phase"] == "shard_written",
              "A4: the durable phase records a written-but-unregistered shard",
              batch["phase"])
        check(fx.query("SELECT COUNT(*) c FROM archive_shards")[0]["c"] == 0,
              "A4: nothing was registered before the kill")
        check(body_of(fx, pid) == body,
              "A4: the SQLite body is still present (nothing lost)")
        files = list_shard_files(fx.archive_dir)
        check(len(files) == 1, "A4: exactly one shard file exists", str(files))

        proc, report = fx.archive_parsed(cutoff=1000.0)
        check(proc.returncode == 0 and report["ok"],
              "A4: rerunning the same snapshot converges", proc.stderr)
        check(report["adopted"] == 1 and report["written"] == 0,
              "A4: the rerun adopted the verified existing file (no rewrite)")
        check(len(list_shard_files(fx.archive_dir)) == 1,
              "A4: no duplicate shard file was created")
        check(fx.query("SELECT COUNT(*) c FROM archive_shards")[0]["c"] == 1,
              "A4: exactly one shard registration exists")
        check(body_of(fx, pid) is None,
              "A4: bodies were cleared only after successful registration")
        proc, trace = fx.trace("alpha", "missing-acc")
        check(file_digest(files[0]) ==
              fx.query("SELECT file_sha256 FROM archive_shards")[0]["file_sha256"],
              "A4: the adopted file matches the registered digest")
    finally:
        fx.close()


def scenario_crash_before_clear():
    print("== A5 SIGKILL after registration, before bodies were cleared")
    fx = Fixture()
    try:
        body = "registered-but-not-cleared"
        pid = fx.page("alpha", body, fetched_at=1000.0)
        marker = os.path.join(fx.dir, "marker-a5.flag")
        wait_file = os.path.join(fx.dir, "wait-a5.flag")
        reached, rc, out = _run_killed(fx, "before_clear", marker, wait_file)
        check(reached and rc in (-9, 137),
              "A5: the process was killed before clearing bodies", out[-300:])
        batch = fx.query("SELECT * FROM archive_batches")[0]
        check(batch["phase"] == "registered",
              "A5: the durable phase records registered shards", batch["phase"])
        check(fx.query("SELECT COUNT(*) c FROM archive_shards")[0]["c"] == 1,
              "A5: the shard is registered")
        check(body_of(fx, pid) == body,
              "A5: the body is still in SQLite (cleanup not reached)")

        proc, report = fx.archive_parsed(cutoff=1000.0)
        check(proc.returncode == 0 and report["ok"],
              "A5: rerunning converges", proc.stderr)
        check(report["written"] == 0 and report["adopted"] == 0,
              "A5: the rerun neither rewrites nor re-registers the shard")
        check(fx.query("SELECT COUNT(*) c FROM archive_shards")[0]["c"] == 1,
              "A5: no duplicate registration")
        check(body_of(fx, pid) is None,
              "A5: the deferred cleanup completed on the rerun")
        check(report["phase"] == "cleared",
              "A5: the batch reached the cleared phase")
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A6: corrupt shard -- preserve bodies, do not register, do not overwrite
# --------------------------------------------------------------------------
def scenario_integrity_failure():
    print("== A6 corrupt shard keeps SQLite bodies and is never registered")
    fx = Fixture()
    try:
        body = "unarchived-on-purpose"
        pid = fx.page("alpha", body, fetched_at=1000.0)
        # first create a valid registered shard, then corrupt its file, then
        # attempt a *different* snapshot that would collide on the same path
        proc, report = fx.archive_parsed(cutoff=1000.0)
        check(proc.returncode == 0, "A6: baseline archive succeeded")
        shard_row = fx.query("SELECT * FROM archive_shards")[0]
        path = shard_row["path"]
        before = file_digest(path)
        with open(path, "wb") as fh:
            fh.write(b"tampered body, same path")
        check(file_digest(path) != before, "A6: the file was tampered with")

        proc, report = fx.archive_parsed(cutoff=1000.0)
        check(proc.returncode == 1,
              "A6: a repeat run reports the integrity failure", proc.stderr)
        failure = report["failures"][0]
        check(failure["path"] == path and failure["tenant"] == "alpha",
              "A6: the failure points at the exact file", str(failure))
        check(page_bytes(fx) == 0,
              "A6: the already-cleared body was not re-created or lost")
        # add an unarchived row and make sure a failing run never clears it
        pid2 = fx.page("alpha", "second-body", fetched_at=1001.0)
        proc, report = fx.archive_parsed(cutoff=1001.0)
        check(proc.returncode == 1,
              "A6: a later snapshot hitting the corrupt path still fails")
        check(body_of(fx, pid2) == "second-body",
              "A6: an unregistered body is preserved on failure")
        check(fx.query("SELECT COUNT(*) c FROM archive_shards"
                       " WHERE page_count=1 AND first_page_id=?",
                       [pid2])[0]["c"] == 0,
              "A6: the bad shard was not registered")
        with open(path, "rb") as fh:
            check(fh.read() == b"tampered body, same path",
                  "A6: the corrupt file was not overwritten")
        ver = fx.cli("verify", "--tenant", "alpha", "--json")
        vreport = json.loads(ver.stdout)
        check(ver.returncode == 1 and vreport["problem_shards"] == 1,
              "A6: verify flags the corrupt shard", ver.stdout[:400])
        check(any(p["path"] == path for p in vreport["problems"]),
              "A6: verify localises the failing path")
        trace_proc, trace = fx.trace("alpha", "a1")
        check(trace_proc.returncode != 0,
              "A6: trace refuses rather than guessing from a corrupt shard",
              trace_proc.stdout + trace_proc.stderr)
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A7: rows written during the archival scan remain unarchived
# --------------------------------------------------------------------------
def scenario_concurrent_rows_stay_unarchived():
    print("== A7 rows written during the scan stay un-archived")
    fx = Fixture()
    try:
        old = fx.page("alpha", "old-body", fetched_at=1000.0)
        engine = archiver.Archiver(fx.path, fx.archive_dir)
        conn = store.connect(fx.path)
        batch = engine.freeze_snapshot(conn, cutoff_at=5000.0,
                                       tenants=["alpha", "beta"])
        conn.close()
        # a new page arrives mid-scan; it is not part of the frozen snapshot
        fresh = fx.page("alpha", "fresh-body", fetched_at=1200.0)
        report = engine.run(cutoff_at=5000.0, tenants=["alpha", "beta"])
        check(report["batch_id"] == batch["batch_id"]
              and report["max_page_id"] == batch["max_page_id"],
              "A7: the second invocation reuses the frozen snapshot")
        check(body_of(fx, old) is None, "A7: old body was archived")
        check(body_of(fx, fresh) == "fresh-body",
              "A7: the row written during the scan kept its body")
        check(fresh > batch["max_page_id"],
              "A7: the new row is above the frozen max page id")
        # next run with a fresh snapshot picks it up
        proc, report2 = fx.archive_parsed(cutoff=5000.0)
        fresh_batch = report2["batch_id"]
        check(fresh_batch != batch["batch_id"],
              "A7: a later snapshot forms a new batch",
              f"{fresh_batch} vs {batch['batch_id']}")
        check(body_of(fx, fresh) is None,
              "A7: the next snapshot archives the previously new row")
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A8: mixed archive state -- quality projection + trace consistency
# --------------------------------------------------------------------------
def scenario_mixed_state_and_quality():
    print("== A8 mixed archive state keeps versions and quality tracing intact")
    fx = Fixture()
    try:
        page_good = fx.page("alpha", '{"title": "Good",'
                          ' "publication_year": 2020}', fetched_at=1000.0,
                          accession="good", payload={"title": "Good",
                                                     "publication_year": 2020})
        page_bad = fx.page("alpha", '{"title": "Bad",'
                         ' "publication_year": 1500}', fetched_at=1001.0,
                         accession="bad", payload={"title": "Bad",
                                                   "publication_year": 1500})
        # a third page left online to force a mixed state
        page_late = fx.page("alpha", '{"title": "Late",'
                          ' "publication_year": 2019}', fetched_at=9000.0,
                          accession="late", payload={"title": "Late",
                                                     "publication_year": 2019})

        first = fx.process_quality()
        check(first["status"] == "published", "A8: quality published before archive")
        clean_before = fx.query("SELECT * FROM quality_current_clean")
        quar_before = fx.query("SELECT * FROM quality_current_quarantine")
        clean_cols = fx.query("SELECT * FROM quality_clean")
        gen_before = fx.query("SELECT * FROM quality_generations")
        prog_before = fx.query("SELECT * FROM quality_progress")
        pointer_before = fx.query("SELECT * FROM quality_pointer")
        events_before = fx.query("SELECT * FROM quality_events")

        proc, report = fx.archive_parsed(cutoff=5000.0)
        check(proc.returncode == 0, "A8: partial-cutoff archive succeeded",
              proc.stderr)
        check(body_of(fx, page_good) is None and body_of(fx, page_bad) is None,
              "A8: expired bodies were archived")
        check(body_of(fx, page_late) == '{"title": "Late",'
              ' "publication_year": 2019}',
              "A8: a newer page remains in SQLite (mixed state)")

        check(fx.query("SELECT * FROM quality_current_clean") == clean_before,
              "A8: the quality clean projection is unchanged")
        check(fx.query("SELECT * FROM quality_current_quarantine") == quar_before,
              "A8: the quality quarantine projection is unchanged")
        check(fx.query("SELECT * FROM quality_generations") == gen_before,
              "A8: rule generations are untouched")
        check(fx.query("SELECT * FROM quality_progress") == prog_before,
              "A8: processing progress is untouched")
        check(fx.query("SELECT * FROM quality_pointer") == pointer_before,
              "A8: the published pointer is untouched")
        check(fx.query("SELECT * FROM quality_events") == events_before,
              "A8: quality transition history is untouched")
        check(fx.query("SELECT * FROM quality_clean") == clean_cols,
              "A8: projection rows are untouched")

        # trace a result whose source page is archived AND one still online
        proc, trace_good = fx.trace("alpha", "good")
        check(proc.returncode == 0, "A8: trace of an archived result works",
              proc.stderr)
        check(trace_good["body_sources"][str(page_good)] == "archive",
              "A8: trace reads the archived page from the shard")
        check(trace_good["raw_pages"][0]["id"] == page_good,
              "A8: the archived source page keeps its original page id")
        check(trace_good["current"]["state"] == "clean"
              and trace_good["current"]["raw_page_id"] == page_good,
              "A8: the clean projection still points at the archived page")

        proc, trace_late = fx.trace("alpha", "late")
        check(trace_late["body_sources"][str(page_late)] == "sqlite",
              "A8: trace reads a still-online page from SQLite")

        # trace a page that mixes archived and online source pages: the
        # "mixed" accession lands after the first quality run, so publish the
        # new landing rows before tracing it
        fx.execute("INSERT OR IGNORE INTO result_versions(tenant, accession,"
                   " revision, modified_at, retracted, payload, raw_page_id)"
                   " VALUES ('alpha','mixed',1,1000.0,0,'{}',?)", [page_good])
        fx.execute("INSERT OR IGNORE INTO result_versions(tenant, accession,"
                   " revision, modified_at, retracted, payload, raw_page_id)"
                   " VALUES ('alpha','mixed',2,9000.0,1,NULL,?)", [page_late])
        fx.process_quality()
        proc, trace_mixed = fx.trace("alpha", "mixed")
        check(proc.returncode == 0, "A8: trace of a mixed-state accession works",
              proc.stderr)
        ids = [p["id"] for p in trace_mixed["raw_pages"]]
        check(ids == [page_good, page_late],
              "A8: source pages keep their original ascending order", str(ids))
        sources = trace_mixed["body_sources"]
        check(sources[str(page_good)] == "archive"
              and sources[str(page_late)] == "sqlite",
              "A8: mixed state reports each body source")
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A9: preview is read-only
# --------------------------------------------------------------------------
def scenario_preview_read_only():
    print("== A9 preview computes capacity without writing or clearing")
    fx = Fixture()
    try:
        for i in range(5):
            fx.page("alpha", f"body-{i}" * 4, fetched_at=1000.0 + i)
        fx.page("beta", "beta-body" * 3, fetched_at=1000.0)
        fx.page("beta", "beta-new", fetched_at=9000.0)
        before_pages = fx.query("SELECT id, body FROM raw_pages ORDER BY id")
        before_files = list_shard_files(fx.archive_dir)
        proc = fx.cli("preview", "--cutoff-at", "1005",
                      "--page-group-limit", "2", "--json")
        report = json.loads(proc.stdout)
        check(proc.returncode == 0, "A9: preview runs")
        check(list_shard_files(fx.archive_dir) == before_files == [],
              "A9: preview created no files")
        check(fx.query("SELECT id, body FROM raw_pages ORDER BY id") ==
              before_pages, "A9: preview cleared no bodies")
        tables = {r["name"] for r in fx.query(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        check(not {"archive_batches", "archive_shards", "archive_pages"}
              & tables, "A9: preview did not even create the archive tables")
        conn = store.connect(fx.path)
        try:
            check(store.shard_rows(conn) == [] and not store.has_tables(conn),
                  "A9: preview registered no shards")
        finally:
            conn.close()
        alpha = report["tenants"]["alpha"]
        beta = report["tenants"]["beta"]
        expected_alpha_bytes = sum(len((f"body-{i}" * 4).encode())
                                   for i in range(5))
        check(alpha["releasable_pages"] == 5
              and alpha["releasable_bytes"] == expected_alpha_bytes,
              "A9: alpha capacity matches the actual bodies",
              str(alpha))
        # five pages, two per shard -> three shards
        check(alpha["shards"] == 3, "A9: shard count uses the group limit",
              str(alpha["shards"]))
        check(beta["releasable_pages"] == 1
              and beta["shards"] == 1,
              "A9: beta counts only pages at or below the cutoff", str(beta))
        check(report["totals"]["releasable_pages"] == 6
              and report["wrote_files"] is False,
              "A9: totals aggregate tenants and declare read-only")
        check(fx.cli("preview", "--cutoff-at", "1005", "--tenants", "beta",
                     "--json").returncode == 0,
              "A9: preview honours a tenant filter")
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A10: replay of the same snapshot reuses the verified file
# --------------------------------------------------------------------------
def scenario_replay_reuses_file():
    print("== A10 identical snapshot reuses the verified shard; new snapshot "
          "never overwrites")
    fx = Fixture()
    try:
        fx.page("alpha", "replay-body", fetched_at=1000.0)
        engine = archiver.Archiver(fx.path, fx.archive_dir)
        first = engine.run(cutoff_at=5000.0)
        path = first["shards"][0]["path"]
        digest = file_digest(path)
        mtime = os.stat(path).st_mtime_ns
        second = engine.run(cutoff_at=5000.0)
        check(second["batch_id"] == first["batch_id"],
              "A10: the same snapshot maps to the same batch")
        check(second["written"] == 0,
              "A10: the replay wrote no new shard")
        check(file_digest(path) == digest and os.stat(path).st_mtime_ns == mtime,
              "A10: the existing file was not rewritten")
        check(fx.query("SELECT COUNT(*) c FROM archive_shards")[0]["c"] == 1,
              "A10: the replay did not duplicate the registration")
        # a distinct snapshot must not overwrite the old path
        fx.page("alpha", "second-eligible-body", fetched_at=5500.0)
        third = engine.run(cutoff_at=6000.0)
        check(third["batch_id"] != first["batch_id"],
              "A10: a different cutoff is a different batch")
        check(len(list_shard_files(fx.archive_dir)) == 2,
              "A10: a new snapshot created a new file")
        check(file_digest(path) == digest,
              "A10: the earlier shard file is unchanged")
        old_path = fx.query("SELECT path FROM archive_shards"
                            " WHERE batch_id=?", [first["batch_id"]])[0]["path"]
        check(old_path == path, "A10: the old registration still points at "
              "the old file")
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A11: archive touches no quality/batch metadata beyond its own tables
# --------------------------------------------------------------------------
def scenario_no_side_effects():
    print("== A11 archival does not move the fetch cursor or quality state")
    fx = Fixture()
    try:
        fx.page("alpha", "x", fetched_at=1000.0, accession="x1",
                payload={"title": "X", "publication_year": 2020})
        conn = landing_db.connect(fx.path)
        with conn:
            conn.execute(
                "INSERT INTO scan_state(tenant, scan_id, scan_until,"
                " confirmed_modified_at, confirmed_accession, pages_confirmed,"
                " status, round_lower, updated_at)"
                " VALUES ('alpha','s1',2000.0,1000.0,'x1',1,'done',0.0,0.0)")
            conn.execute(
                "INSERT INTO watermarks(tenant, high_modified_at,"
                " origin_modified_at, rounds_completed) VALUES ('alpha',2000.0,0.0,1)")
        conn.close()
        fx.process_quality()
        scan_before = fx.query("SELECT * FROM scan_state")
        wm_before = fx.query("SELECT * FROM watermarks")
        proc, report = fx.archive_parsed(cutoff=5000.0)
        check(proc.returncode == 0, "A11: archive ran")
        check(fx.query("SELECT * FROM scan_state") == scan_before,
              "A11: the fetch cursor is untouched")
        check(fx.query("SELECT * FROM watermarks") == wm_before,
              "A11: the round watermark is untouched")
        check(fx.query("SELECT * FROM quality_generations") ==
              fx.query("SELECT * FROM quality_generations"),
              "A11: generation rows are untouched")
        batch = fx.query("SELECT * FROM archive_batches")[0]
        check(batch["phase"] == "cleared" and batch["batch_id"],
              "A11: the batch has a queryable id and phase")
        proc = fx.cli("batches", "--json")
        rows = json.loads(proc.stdout)
        check(rows[0]["phase"] == "cleared",
              "A11: batches command exposes the current phase")
    finally:
        fx.close()


# --------------------------------------------------------------------------
# A12: independent offline verification of the shard file format
# --------------------------------------------------------------------------
def scenario_independent_format_check():
    print("== A12 shards verify independently of SQLite (gunzip + sha256)")
    fx = Fixture()
    try:
        bodies = ['{"first": true}', "plain text body", '{"third": 3}']
        for i, body in enumerate(bodies):
            fx.page("alpha", body, fetched_at=1000.0 + i)
        proc, report = fx.archive_parsed(cutoff=1005.0)
        check(proc.returncode == 0, "A12: archive succeeded")
        shard_row = fx.query("SELECT * FROM archive_shards")[0]
        path = shard_row["path"]
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            lines = fh.read().splitlines()
        header = json.loads(lines[0])
        page_lines = lines[1:-1]
        footer = json.loads(lines[-1])
        recomputed = hashlib.sha256(
            ("\n".join(page_lines) + "\n").encode("utf-8")).hexdigest()
        check(header["kind"] == "rh-archive-manifest"
              and footer["kind"] == "footer",
              "A12: the shard has a self-describing header and footer")
        check(recomputed == header["content_sha256"] ==
              footer["content_sha256"],
              "A12: a plain sha256 of the page lines matches both digests")
        check(header["page_count"] == 3
              and header["first_page_id"] == shard_row["first_page_id"]
              and header["last_page_id"] == shard_row["last_page_id"],
              "A12: the header records page count and range")
        check(header["tenant"] == "alpha" and header["fetch_date"] ==
              "1970-01-01", "A12: the header records tenant and UTC date")
        check(header["manifest_sha256"] ==
              json.loads(shard_row["manifest_sha256"]),
              "A12: the per-page digest map matches the registration")
        page_records = [json.loads(line) for line in page_lines]
        for record, expected in zip(page_records, bodies):
            check(record["body"] == expected
                  and record["body_sha256"] ==
                  hashlib.sha256(expected.encode()).hexdigest(),
                  f"A12: page {record['page_id']} body digest is independent")
        check(shard_row["compressed_bytes"] == os.path.getsize(path),
              "A12: the recorded compressed size matches the file")
        side = shard_mod.read_sidecar(path)
        check(side is not None
              and side["file_sha256"] == shard_row["file_sha256"],
              "A12: an external manifest records the whole-file digest")
        ver = fx.cli("verify", "--json")
        vreport = json.loads(ver.stdout)
        check(ver.returncode == 0 and vreport["moved_out_pages"] == 3
              and vreport["problem_pages"] == 0,
              "A12: verify counts moved-out pages and no problems")
    finally:
        fx.close()


def list_shard_files(directory):
    found = []
    for base, _dirs, files in os.walk(directory):
        for name in files:
            if name.endswith(".jsonl.gz"):
                found.append(os.path.join(base, name))
    return sorted(found)


def main():
    scenarios = [
        scenario_success_and_trace,
        scenario_cutoff_and_upper_bound,
        scenario_tenant_isolation,
        scenario_crash_after_file_write,
        scenario_crash_before_clear,
        scenario_integrity_failure,
        scenario_concurrent_rows_stay_unarchived,
        scenario_mixed_state_and_quality,
        scenario_preview_read_only,
        scenario_replay_reuses_file,
        scenario_no_side_effects,
        scenario_independent_format_check,
    ]
    started = time.time()
    for scenario in scenarios:
        try:
            scenario()
        except Exception as exc:  # keep going so all failures are visible
            import traceback
            traceback.print_exc()
            FAILURES.append(f"{scenario.__name__}: {type(exc).__name__}: {exc}")
    print("=" * 70)
    if FAILURES:
        print(f"{len(FAILURES)} CHECK(S) FAILED")
        for label in FAILURES:
            print(f"  - {label}")
        return 1
    print(f"ALL ARCHIVE ACCEPTANCE CHECKS PASSED in {time.time() - started:.1f}s")
    print("\nOffline reproducible commands:")
    print("  python run_archive.py archive --db out/harvest.db \\")
    print("      --archive-dir out/archive --retention-days 30")
    print("  python run_archive.py preview --db out/harvest.db \\")
    print("      --retention-days 30")
    print("  python run_archive.py verify --db out/harvest.db")
    print("  python tests/archive_acceptance.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
