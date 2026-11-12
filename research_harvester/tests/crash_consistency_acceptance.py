#!/usr/bin/env python3
"""Crash-consistency acceptance tests for the quality publish gate.

These tests pin the fix for the data/history divergence window: the processor
commits summary counts in one transaction and writes the transition history in
a later one, so a hard kill between them used to leave a staging generation
that ``publish`` happily made visible *without* its audit history.

Faults are real SIGKILL signals delivered to a worker subprocess at three
deterministic commit boundaries (test-only barriers, inert unless
``RH_QUALITY_CRASH_POINT`` is set):

  after_counts_commit  counts durable, history transaction not started
  stage_events_txn     inside the history transaction, before its commit
  publish_txn          inside the final publish transaction, pointer not flipped

The suite also covers zero-event batches, legacy staging rows that predate the
completeness marker, and a regression of the core quality/DOI-conflict logic.

Usage:  python tests/crash_consistency_acceptance.py
Exit code 0 = all checks passed.  Fully offline, stdlib only.
"""
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
from quality import rules as rules_mod              # noqa: E402
from quality import store                            # noqa: E402
from quality.processor import Processor              # noqa: E402

RULES_V1 = os.path.join(ROOT, "quality", "rules", "v1.json")
FAILURES = []
CRASH_POINTS = ("after_counts_commit", "stage_events_txn", "publish_txn")


def check(cond, label, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f"  -- {detail}" if (detail and not cond) else ""))
    if not cond:
        FAILURES.append(label)


class Landing:
    def __init__(self, prefix="rh-crash-"):
        self.dir = tempfile.mkdtemp(prefix=prefix)
        self.path = os.path.join(self.dir, "harvest.db")
        conn = landing_db.connect(self.path)
        landing_db.init(conn)
        conn.commit()
        conn.close()

    def version(self, accession, payload, *, tenant="alpha", revision=1,
                retracted=False, modified_at=1.0):
        conn = landing_db.connect(self.path)
        with conn:
            raw = conn.execute(
                "INSERT INTO raw_pages(tenant, scan_id, url, status,"
                " scan_until, body, fetched_at)"
                " VALUES (?,?,?,200,NULL,'{}',?)",
                (tenant, f"scan-{accession}", f"memory://{accession}",
                 float(modified_at))).lastrowid
            conn.execute(
                "INSERT INTO result_versions(tenant, accession, revision,"
                " modified_at, retracted, payload, raw_page_id)"
                " VALUES (?,?,?,?,?,?,?)",
                (tenant, accession, int(revision), float(modified_at),
                 1 if retracted else 0,
                 json.dumps(payload) if payload is not None else None, raw))
        conn.close()

    def write_rules(self, version, **overrides):
        with open(RULES_V1, "r", encoding="utf-8") as fh:
            config = json.load(fh)
        config["rule_version"] = version
        for dotted, value in overrides.items():
            section, _, key = dotted.partition("__")
            if key:
                config.setdefault(section, {})[key] = value
            else:
                config[section] = value
        path = os.path.join(self.dir, f"rules-{version}.json")
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(config, fh, indent=2)
        return path

    def query(self, sql, params=()):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def cli(self, *args):
        return subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"), *args],
            capture_output=True, text=True)

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def process(landing, rules_path=RULES_V1, **kwargs):
    snapshot = rules_mod.load_rules(rules_path)
    return Processor(landing.path, snapshot).process(**kwargs)


def kill_at_barrier(landing, rules_path, point, *extra_args):
    """Run the CLI in a subprocess and SIGKILL it at a named barrier."""
    marker = os.path.join(landing.dir, f"marker-{point}.flag")
    wait_file = os.path.join(landing.dir, f"wait-{point}.flag")
    with open(wait_file, "w", encoding="utf-8"):
        pass
    env = dict(os.environ,
               RH_QUALITY_CRASH_POINT=point,
               RH_QUALITY_CRASH_MARKER=marker,
               RH_QUALITY_CRASH_WAIT_FILE=wait_file)
    cmd = [sys.executable, os.path.join(ROOT, "run_quality.py"), "process",
           "--db", landing.path, "--rules", rules_path, *extra_args]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True, env=env)
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


def staging_row(landing):
    rows = landing.query(
        "SELECT * FROM quality_generations WHERE status='staging'"
        " ORDER BY generation_id DESC LIMIT 1")
    return rows[0] if rows else None


def trace(landing, accession, tenant="alpha"):
    out = landing.cli("trace", "--db", landing.path, "--tenant", tenant,
                      "--accession", accession, "--json")
    assert out.returncode == 0, out.stdout + out.stderr
    return json.loads(out.stdout)


# --------------------------------------------------------------------------
# C1: the exact reported window -- kill after the counts commit, before the
# transition history is written.  publish must refuse; resume rebuilds and the
# full before/after history is preserved; repeated recovery never duplicates.
# --------------------------------------------------------------------------
def scenario_kill_after_counts_commit():
    print("== C1 SIGKILL after counts commit: publish refused, resume rebuilds")
    landing = Landing()
    try:
        landing.version("p2030", {"title": "Future work",
                                  "publication_year": 2030})
        first = process(landing)
        check(first["status"] == "published", "C1: initial v1 run published")
        gen_before = landing.query(
            "SELECT generation_id FROM quality_generations"
            " WHERE status='published'")[0]["generation_id"]
        quar = landing.query(
            "SELECT rule_ids FROM quality_current_quarantine"
            " WHERE accession='p2030'")[0]
        check(json.loads(quar["rule_ids"]) == ["R-YEAR-003"],
              "C1: year 2030 quarantined under v1")

        v2 = landing.write_rules("v2", publication_year__max=2035)
        reached, rc, out = kill_at_barrier(
            landing, v2, "after_counts_commit", "--reevaluate")
        check(reached and rc == -9,
              "C1: worker was SIGKILLed at the counts/history gap", out[-300:])

        st = staging_row(landing)
        check(st is not None and st["counts"] is not None
              and st["build_phase"] == "building",
              "C1: counts committed but marker still 'building'",
              str({k: st.get(k) for k in ("counts", "build_phase")}
                  if st else None))
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_pending_events")[0]["c"] == 0,
              "C1: no transition history was made durable")
        check(landing.query(
            "SELECT generation_id FROM quality_pointer WHERE id=1")[0]
              ["generation_id"] == gen_before,
              "C1: published pointer unchanged after the kill")

        pub = landing.cli("publish", "--db", landing.path)
        check(pub.returncode == 4,
              "C1: publish refuses the history-less staging (was exit 0)",
              pub.stdout + pub.stderr)
        check("resume" in pub.stderr,
              "C1: refusal names the operator recovery command")
        report = trace(landing, "p2030")
        check(report["current"] and report["current"]["state"] == "quarantine",
              "C1: readers still see the old quarantine, not the new clean row")
        check([e["event_type"] for e in report["events"]]
              == ["entered_quarantine"],
              "C1: no fabricated recovery event hides the missing history",
              str([e["event_type"] for e in report["events"]]))

        resumed = landing.cli("process", "--db", landing.path,
                              "--rules", v2, "--resume")
        check(resumed.returncode == 0, "C1: resume completes",
              resumed.stdout + resumed.stderr)
        report = trace(landing, "p2030")
        check(report["current"]["state"] == "clean"
              and report["current"]["rule_version"] == "v2",
              "C1: recovered result is clean under the batch's v2 rules")
        types = [(e["event_type"], e["rule_version_before"],
                  e["rule_version"]) for e in report["events"]]
        check(types == [("entered_quarantine", None, "v1"),
                        ("recovered", "v1", "v2")],
              "C1: full before/after reasons retained with rule versions",
              str(types))
        before_details = report["events"][-1]["details"]
        if isinstance(before_details, str):
            before_details = json.loads(before_details)
        before_reason = before_details["before"]["reasons"][0]
        check(before_reason["rule_id"] == "R-YEAR-003"
              and before_reason["value"] == 2030,
              "C1: prior quarantine reason survives the rebuild")
        progress = landing.query(
            "SELECT * FROM quality_progress WHERE tenant='alpha'")[0]
        check(progress["inputs_seen"] == 1,
              "C1: input progress advanced exactly once",
              str(dict(progress)))

        # repeated recovery must be idempotent
        again = landing.cli("process", "--db", landing.path,
                            "--rules", v2, "--resume")
        check(again.returncode == 0, "C1: second resume is safe")
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_events")[0]["c"] == 2,
              "C1: repeated resume adds no duplicate history")
        republish = landing.cli("publish", "--db", landing.path)
        check(republish.returncode == 0
              and "no staging" in republish.stdout,
              "C1: publish after completion reports nothing left to do")
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_events")[0]["c"] == 2,
              "C1: repeated publish adds no duplicate history")
    finally:
        landing.close()


# --------------------------------------------------------------------------
# C2: kill inside the history transaction.  SQLite rolls the uncommitted event
# rows and marker back together; resume rebuilds from the stored snapshot.
# --------------------------------------------------------------------------
def scenario_kill_during_history_transaction():
    print("== C2 SIGKILL inside stage_events transaction: rollback + rebuild")
    landing = Landing()
    try:
        landing.version("p2030", {"title": "Future work",
                                  "publication_year": 2030})
        process(landing)
        v2 = landing.write_rules("v2", publication_year__max=2035)
        reached, rc, out = kill_at_barrier(
            landing, v2, "stage_events_txn", "--reevaluate")
        check(reached and rc == -9,
              "C2: worker killed inside the history transaction", out[-300:])
        st = staging_row(landing)
        check(st["build_phase"] == "building",
              "C2: uncommitted marker rolled back to 'building'")
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_pending_events")[0]["c"] == 0,
              "C2: uncommitted event rows were rolled back atomically")
        check(landing.cli("publish", "--db", landing.path).returncode == 4,
              "C2: rolled-back staging cannot be published")

        resumed = landing.cli("process", "--db", landing.path,
                              "--rules", v2, "--resume")
        check(resumed.returncode == 0, "C2: resume rebuilds the batch",
              resumed.stdout + resumed.stderr)
        report = trace(landing, "p2030")
        types = [(e["event_type"], e["rule_version_before"],
                  e["rule_version"]) for e in report["events"]]
        check(types == [("entered_quarantine", None, "v1"),
                        ("recovered", "v1", "v2")],
              "C2: rebuilt history carries both verdicts", str(types))
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_generations"
            " WHERE status='staging'")[0]["c"] == 0,
              "C2: no staging residue after resume")
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_events")[0]["c"] == 2,
              "C2: exactly one rebuilt recovery event, no duplication")
    finally:
        landing.close()


# --------------------------------------------------------------------------
# C3: kill inside the final publish transaction.  The history already
# committed (events_staged), so the batch is genuinely deliverable: a plain
# publish (or resume) finishes it, and retrying never writes history twice.
# --------------------------------------------------------------------------
def scenario_kill_during_publish_transaction():
    print("== C3 SIGKILL inside publish transaction: idempotent completion")
    landing = Landing()
    try:
        landing.version("p2030", {"title": "Future work",
                                  "publication_year": 2030})
        process(landing)
        gen_before = landing.query(
            "SELECT generation_id FROM quality_generations"
            " WHERE status='published'")[0]["generation_id"]
        v2 = landing.write_rules("v2", publication_year__max=2035)
        reached, rc, out = kill_at_barrier(
            landing, v2, "publish_txn", "--reevaluate")
        check(reached and rc == -9,
              "C3: worker killed inside the publish transaction", out[-300:])
        st = staging_row(landing)
        check(st["build_phase"] == "events_staged",
              "C3: history marker committed before the publish txn")
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_pending_events")[0]["c"] == 1,
              "C3: staged history survived the kill")
        check(landing.query(
            "SELECT generation_id FROM quality_pointer WHERE id=1")[0]
              ["generation_id"] == gen_before,
              "C3: pointer did not flip; old generation still visible")

        pub = landing.cli("publish", "--db", landing.path)
        check(pub.returncode == 0 and "published" in pub.stdout,
              "C3: a durable, fully-staged batch publishes directly",
              pub.stdout + pub.stderr)
        report = trace(landing, "p2030")
        check(report["current"]["state"] == "clean",
              "C3: new projection visible after completion")
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_events")[0]["c"] == 2,
              "C3: history promoted exactly once")
        progress = landing.query(
            "SELECT inputs_seen, last_rowid FROM quality_progress"
            " WHERE tenant='alpha'")[0]
        check(progress["inputs_seen"] == 1 and progress["last_rowid"] == 1,
              "C3: restarted publish advanced the reading progress",
              str(dict(progress)))

        # resume against the now-published batch and a second publish must not
        # re-promote the same events
        again = landing.cli("process", "--db", landing.path,
                            "--rules", v2, "--resume")
        check(again.returncode == 0, "C3: resume after publish is safe")
        republish = landing.cli("publish", "--db", landing.path)
        check(republish.returncode == 0, "C3: repeated publish is a no-op")
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_events")[0]["c"] == 2,
              "C3: no duplicate history from repeated recovery actions")
    finally:
        landing.close()


# --------------------------------------------------------------------------
# C4: a legitimate batch with zero transition events must not be mistaken for
# an incomplete one.  Completeness comes from the marker, not event count.
# --------------------------------------------------------------------------
def scenario_zero_event_batch_publishes():
    print("== C4 zero-event batch is not blocked by the completeness gate")
    landing = Landing()
    try:
        landing.version("stable", {"title": "Stable",
                                   "publication_year": 2020})
        v2 = landing.write_rules("v2", publication_year__max=2035)
        process(landing, v2)                       # published clean under v2
        # re-evaluate with the same contract: every verdict is identical, so
        # the event set is empty even though a full generation is rebuilt
        result = process(landing, v2, reevaluate=True)
        check(result["status"] == "published"
              and result["events"] == 0,
              "C4: zero-event re-evaluation still publishes",
              str(result.get("status")))
        gen = landing.query(
            "SELECT build_phase FROM quality_generations"
            " ORDER BY generation_id DESC LIMIT 1")[0]
        check(gen["build_phase"] == "events_staged",
              "C4: empty history still carries the explicit ready marker")
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_current_clean"
            " WHERE accession='stable'")[0]["c"] == 1,
              "C4: result remains visible and clean")
        # ordinary no-op run stays a no-op
        noop = process(landing, v2)
        check(noop["status"] == "noop",
              "C4: subsequent run with no changes is a no-op")
    finally:
        landing.close()


# --------------------------------------------------------------------------
# C5: legacy databases.  Old staging rows have a NULL marker.  Three shapes --
# summary only, half a tenant, fully populated -- must all be refused by
# publish and rebuilt by resume; already-published rows stay untouched.
# --------------------------------------------------------------------------
def _null_phase_staging(landing, gid):
    conn = store.connect(landing.path)
    try:
        with conn:
            conn.execute(
                "UPDATE quality_generations SET build_phase=NULL")
    finally:
        conn.close()


def scenario_legacy_staging_shapes():
    print("== C5 legacy NULL-marker staging is never auto-passed")
    # --- 5a: summary counts only, no history ---
    landing = Landing(prefix="rh-leg-a-")
    try:
        landing.version("leg", {"title": "Legit", "publication_year": 2020})
        process(landing)
        landing.version("new1", {"title": "New", "publication_year": 2021})
        staged = process(landing, stage_only=True)
        gid = staged["generation_id"]
        conn = store.connect(landing.path)
        with conn:
            conn.execute(
                "DELETE FROM quality_pending_events WHERE generation_id=?",
                (gid,))
        conn.close()
        _null_phase_staging(landing, gid)
        row = staging_row(landing)
        check(row["counts"] is not None and row["build_phase"] is None,
              "C5a: legacy staging has counts but no marker")
        pub = landing.cli("publish", "--db", landing.path)
        check(pub.returncode == 4 and "legacy" in pub.stderr,
              "C5a: summary-only legacy staging is refused", pub.stderr)
        resumed = landing.cli("process", "--db", landing.path,
                              "--rules", RULES_V1, "--resume")
        check(resumed.returncode == 0, "C5a: resume rebuilds summary-only batch",
              resumed.stdout + resumed.stderr)
        events = landing.query(
            "SELECT accession, event_type FROM quality_events"
            " ORDER BY event_id")
        check(("new1", "entered_clean") in [(e["accession"], e["event_type"])
                                            for e in events],
              "C5a: rebuild restores the missing history")
    finally:
        landing.close()

    # --- 5b: half a tenant's projection rows, no history ---
    landing = Landing(prefix="rh-leg-b-")
    try:
        landing.version("leg", {"title": "Legit",
                                "publication_year": 2020}, tenant="alpha")
        landing.version("b1", {"title": "Beta",
                               "publication_year": 2019}, tenant="beta")
        process(landing)
        landing.version("b2", {"title": "Beta two",
                               "publication_year": 2018}, tenant="beta")
        staged = process(landing, stage_only=True)
        gid = staged["generation_id"]
        conn = store.connect(landing.path)
        with conn:
            conn.execute(
                "DELETE FROM quality_clean WHERE generation_id=?"
                " AND tenant='beta'", (gid,))
            conn.execute(
                "DELETE FROM quality_pending_events WHERE generation_id=?",
                (gid,))
        conn.close()
        _null_phase_staging(landing, gid)
        check(landing.cli("publish", "--db", landing.path).returncode == 4,
              "C5b: half-tenant legacy staging is refused")
        resumed = landing.cli("process", "--db", landing.path,
                              "--rules", RULES_V1, "--resume")
        check(resumed.returncode == 0, "C5b: resume repairs the partial tenant",
              resumed.stdout + resumed.stderr)
        visible = {(r["tenant"], r["accession"]) for r in landing.query(
            "SELECT tenant, accession FROM quality_current_clean")}
        check(visible == {("alpha", "leg"), ("beta", "b1"), ("beta", "b2")},
              "C5b: rebuilt projection covers every tenant", str(visible))
    finally:
        landing.close()

    # --- 5c: fully populated legacy staging, still no marker ---
    landing = Landing(prefix="rh-leg-c-")
    try:
        landing.version("leg", {"title": "Legit", "publication_year": 2020})
        process(landing)
        landing.version("new2", {"title": "New two", "publication_year": 2022})
        staged = process(landing, stage_only=True)
        gid = staged["generation_id"]
        pending_before = landing.query(
            "SELECT COUNT(*) c FROM quality_pending_events"
            " WHERE generation_id=?", (gid,))[0]["c"]
        check(pending_before >= 1, "C5c: legacy staging really has history rows")
        _null_phase_staging(landing, gid)
        check(landing.cli("publish", "--db", landing.path).returncode == 4,
              "C5c: even complete-looking legacy staging is not inferred ready")
        resumed = landing.cli("process", "--db", landing.path,
                              "--rules", RULES_V1, "--resume")
        check(resumed.returncode == 0, "C5c: resume rebuilds and publishes",
              resumed.stdout + resumed.stderr)
        check(landing.query(
            "SELECT COUNT(*) c FROM quality_events WHERE accession='new2'"
        )[0]["c"] == 1,
              "C5c: rebuilt history has no duplicate entries")
        # already-published generations keep their original (NULL) phase; the
        # migration never stamps historical rows as complete
        legacy_published = landing.query(
            "SELECT status, build_phase FROM quality_generations"
            " WHERE status IN ('published','retired')")
        check(all(r["build_phase"] in ("events_staged", None)
                  for r in legacy_published),
              "C5c: published rows are left as the migration found them",
              str(legacy_published))
    finally:
        landing.close()


# --------------------------------------------------------------------------
# C6: regression -- core quarantine judgement and DOI conflict correction keep
# working through a crashed-and-resumed batch.
# --------------------------------------------------------------------------
def scenario_quality_and_conflict_regression():
    print("== C6 quality verdicts and DOI conflict correction survive recovery")
    landing = Landing()
    try:
        landing.version("good", {"title": "Good", "publication_year": 2020,
                                 "doi": "https://doi.org/10.7000/abc"})
        # 2030 violates the v1 *upper* bound, so relaxing the max releases it
        landing.version("bad", {"title": "Bad", "publication_year": 2030})
        landing.version("dup-a", {"title": "A", "publication_year": 2020,
                                  "doi": "10.9000/SAME"})
        landing.version("dup-b", {"title": "B", "publication_year": 2020,
                                  "doi": "https://doi.org/10.9000/same"})
        process(landing)
        rows = landing.query(
            "SELECT accession, rule_ids FROM quality_current_quarantine")
        quar = {r["accession"]: json.loads(r["rule_ids"]) for r in rows}
        check(quar.get("bad") == ["R-YEAR-003"],
              "C6: out-of-range year still quarantined", str(quar))
        check(sorted(quar.get("dup-a", []) + quar.get("dup-b", []))
              == ["R-DOI-004", "R-DOI-004"],
              "C6: DOI collision still quarantines both sides", str(quar))

        v2 = landing.write_rules("v2", publication_year__max=2035)
        reached, rc, out = kill_at_barrier(
            landing, v2, "after_counts_commit", "--reevaluate")
        check(reached and rc == -9, "C6: batch interrupted at the crash window",
              out[-300:])
        resumed = landing.cli("process", "--db", landing.path,
                              "--rules", v2, "--resume")
        check(resumed.returncode == 0, "C6: interrupted batch recovered",
              resumed.stdout + resumed.stderr)

        # year rule relaxed -> bad recovers; DOI conflict is unaffected
        clean = {r["accession"] for r in landing.query(
            "SELECT accession FROM quality_current_clean")}
        check({"good", "bad"} <= clean,
              "C6: year quarantine released after rule relaxation", str(clean))
        quar_after = {r["accession"] for r in landing.query(
            "SELECT accession FROM quality_current_quarantine")}
        check({"dup-a", "dup-b"} <= quar_after,
              "C6: DOI conflict correction logic intact after recovery",
              str(quar_after))
        bad_events = [e["event_type"] for e in
                      trace(landing, "bad")["events"]]
        check(bad_events == ["entered_quarantine", "recovered"],
              "C6: recovered record keeps its full transition history",
              str(bad_events))

        # correcting one DOI releases the other on the next normal run
        landing.version("dup-b", {"title": "B fixed",
                                  "publication_year": 2020,
                                  "doi": "10.9000/other"}, revision=2,
                        modified_at=2.0)
        fixed = process(landing, v2)
        check(fixed["status"] == "published", "C6: follow-up run publishes")
        clean = {r["accession"] for r in landing.query(
            "SELECT accession FROM quality_current_clean")}
        check("dup-a" in clean and "dup-b" in clean,
              "C6: conflict auto-releases once the collision is removed",
              str(clean))
    finally:
        landing.close()


def main():
    scenarios = [
        scenario_kill_after_counts_commit,
        scenario_kill_during_history_transaction,
        scenario_kill_during_publish_transaction,
        scenario_zero_event_batch_publishes,
        scenario_legacy_staging_shapes,
        scenario_quality_and_conflict_regression,
    ]
    started = time.time()
    for fn in scenarios:
        fn()
    print("=" * 70)
    if FAILURES:
        print(f"{len(FAILURES)} CHECK(S) FAILED:")
        for name in FAILURES:
            print(f"  - {name}")
        return 1
    print(f"ALL CRASH-CONSISTENCY CHECKS PASSED in "
          f"{time.time() - started:.1f}s")
    print("\nOffline reproducible commands:")
    print("  python tests/crash_consistency_acceptance.py")
    print("  python tests/quality_acceptance.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
