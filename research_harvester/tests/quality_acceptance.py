#!/usr/bin/env python3
"""Executable acceptance assertions for the quality isolation layer.

Fully offline.  Landing data is constructed directly in the collector's
landing schema (raw_pages / result_versions) -- no network, no dependency on
the mock server -- so each scenario is deterministic and independently
reproducible.

Usage:  python tests/quality_acceptance.py
Exit code 0 = all checks passed.
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


def check(cond, label, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f"  -- {detail}" if (detail and not cond) else ""))
    if not cond:
        FAILURES.append(label)


class Landing:
    """Minimal landing-db fixture builder (collector schema, real tables)."""

    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="quality-acc-")
        self.path = os.path.join(self.dir, "harvest.db")
        conn = landing_db.connect(self.path)
        landing_db.init(conn)
        conn.commit()
        conn.close()

    def raw_page(self, tenant="alpha"):
        conn = landing_db.connect(self.path)
        with conn:
            cur = conn.execute(
                "INSERT INTO raw_pages(tenant, scan_id, url, status,"
                " scan_until, body, fetched_at)"
                " VALUES (?,?,?,200,NULL,?,?)",
                (tenant, f"scan-{tenant}", f"memory://{tenant}",
                 "{}", time.time()))
            raw_id = cur.lastrowid
        conn.close()
        return raw_id

    def versions_bulk(self, tenant, items, raw_page_id=None, modified_at=1.0):
        """items: iterable of (accession, revision, payload|None, retracted)."""
        raw_page_id = raw_page_id or self.raw_page(tenant)
        conn = landing_db.connect(self.path)
        with conn:
            conn.executemany(
                "INSERT INTO result_versions(tenant, accession, revision,"
                " modified_at, retracted, payload, raw_page_id)"
                " VALUES (?,?,?,?,?,?,?)",
                [(tenant, accession, int(revision), float(modified_at),
                 1 if retracted else 0,
                 json.dumps(payload) if payload is not None else None,
                 raw_page_id)
                 for accession, revision, payload, retracted in items])
        conn.close()
        return raw_page_id

    def version(self, accession, revision, payload=None, *, tenant="alpha",
                retracted=False, raw_page_id=None, modified_at=1.0):
        raw_page_id = raw_page_id or self.raw_page(tenant)
        conn = landing_db.connect(self.path)
        with conn:
            conn.execute(
                "INSERT INTO result_versions(tenant, accession,"
                " revision, modified_at, retracted, payload, raw_page_id)"
                " VALUES (?,?,?,?,?,?,?)",
                (tenant, accession, int(revision), float(modified_at),
                 1 if retracted else 0,
                 json.dumps(payload) if payload is not None else None,
                 raw_page_id))
        conn.close()
        return raw_page_id

    def query(self, sql, params=()):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def write_rules(base_path, version, *, directory=None, **overrides):
    with open(base_path, "r", encoding="utf-8") as fh:
        config = json.load(fh)
    config["rule_version"] = version
    for dotted, value in overrides.items():
        section, _, key = dotted.partition("__")
        if key:
            config.setdefault(section, {})[key] = value
        else:
            config[section] = value
    path = os.path.join(directory or os.path.dirname(base_path),
                        f"_tmp_{version}_{len(overrides)}_{time.time_ns()}.json")
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2)
    return path


def process(landing, rules_path=RULES_V1, **kwargs):
    snapshot = rules_mod.load_rules(rules_path)
    init_kwargs = {k: v for k, v in kwargs.items() if k == "on_tenant_applied"}
    return Processor(landing.path, snapshot, **init_kwargs).process(**kwargs)


def current_rows(landing):
    return {
        "clean": landing.query("SELECT * FROM quality_current_clean"
                               " ORDER BY tenant, accession"),
        "quarantine": landing.query("SELECT * FROM quality_current_quarantine"
                                    " ORDER BY tenant, accession"),
        "retracted": landing.query("SELECT * FROM quality_current_retracted"
                                   " ORDER BY tenant, accession"),
    }


def by_accession(rows, tenant="alpha"):
    return {r["accession"]: r for r in rows if r["tenant"] == tenant}


def scenario_invalid_year_isolated():
    print("== Q1 invalid year quarantined, valid records published")
    landing = Landing()
    try:
        raw_bad = landing.version("bad-year", 1,
                                  {"title": "Bad", "publication_year": 1200})
        landing.version("missing-year", 1, {"title": "No year"})
        landing.version("bad-title", 1, {"title": "   ",
                                         "publication_year": 2020})
        landing.version("bad-doi", 1, {"title": "DOI", "publication_year": 2021,
                                       "doi": "not-a-doi"})
        landing.version("space-doi", 1, {"title": "Spaced",
                                         "publication_year": 2021,
                                         "doi": "10.1000/ab cd"})
        landing.version("good-1", 1, {"title": "Fine", "publication_year": 2020,
                                      "doi": "https://doi.org/10.1000/XYZ"})
        landing.version("good-2", 1, {"title": "No DOI",
                                      "publication_year": 1990})
        landing.version("nimble", 1, None, retracted=False)  # payload missing

        result = process(landing)
        check(result["status"] == "published", "Q1: generation published")
        rows = current_rows(landing)
        clean = by_accession(rows["clean"])
        quar = by_accession(rows["quarantine"])
        check(set(clean) == {"good-1", "good-2"},
              "Q1: only valid records are clean",
              f"clean={sorted(clean)}")
        check(set(quar) == {"bad-year", "missing-year", "bad-title", "bad-doi",
                            "nimble", "space-doi"},
              "Q1: invalid records quarantined", f"quarantine={sorted(quar)}")
        check(quar["bad-year"]["rule_ids"] == json.dumps(["R-YEAR-003"]),
              "Q1: invalid year carries rule R-YEAR-003",
              quar["bad-year"]["rule_ids"])
        original = json.loads(quar["bad-year"]["original"])
        check(original.get("publication_year") == 1200,
              "Q1: quarantine keeps the original value")
        check(quar["bad-year"]["raw_page_id"] == raw_bad,
              "Q1: quarantine records the source raw_page_id")
        reason = json.loads(quar["bad-year"]["reasons"])[0]
        check(reason["field"] == "publication_year" and reason["value"] == 1200,
              "Q1: reason identifies field and offending value")
        check(json.loads(quar["missing-year"]["rule_ids"]) == ["R-YEAR-001"],
              "Q1: missing year -> R-YEAR-001")
        check(json.loads(quar["bad-title"]["rule_ids"]) == ["R-TITLE-003"],
              "Q1: whitespace title -> R-TITLE-003")
        check(json.loads(quar["bad-doi"]["rule_ids"]) == ["R-DOI-003"],
              "Q1: malformed doi -> R-DOI-003")
        check(json.loads(quar["nimble"]["rule_ids"]) == ["R-PAYLOAD-001"],
              "Q1: missing payload -> R-PAYLOAD-001")
        check(clean["good-1"]["doi"] == "10.1000/xyz",
              "Q1: clean doi is normalized (lowercase, prefix stripped)")
        check(clean["good-2"]["doi"] is None,
              "Q1: absent doi is allowed")
        status = store.connect(landing.path)
        try:
            report = store.status(status)
        finally:
            status.close()
        check(report["counts"]["clean"] == 2
              and report["counts"]["quarantine"] == 6,
              "Q1: counts expose clean/quarantine/retracted",
              str(report["counts"]))
        check(report["rule_version"] == "v1",
              "Q1: status reports the published rule version")
    finally:
        landing.close()


def scenario_newer_bad_revision_masks():
    print("== Q2 newer bad revision masks older clean content")
    landing = Landing()
    try:
        landing.version("acc", 1, {"title": "Original Good",
                                   "publication_year": 2020,
                                   "doi": "10.2000/ok"})
        process(landing)
        before = by_accession(current_rows(landing)["clean"])
        check("acc" in before, "Q2: v1 is clean before the bad revision")

        landing.version("acc", 2, {"title": "Broken", "publication_year": "x"},
                        modified_at=2.0)
        result = process(landing)
        rows = current_rows(landing)
        check("acc" not in by_accession(rows["clean"]),
              "Q2: older clean content is NOT carried forward")
        quar = by_accession(rows["quarantine"])["acc"]
        check(quar["revision"] == 2, "Q2: quarantine tracks the newest revision")
        check(json.loads(quar["rule_ids"]) == ["R-YEAR-002"],
              "Q2: invalid year type -> R-YEAR-002")
        events = landing.query(
            "SELECT * FROM quality_events WHERE accession='acc'"
            " ORDER BY event_id")
        check(events[-1]["event_type"] == "quarantined"
              and events[-1]["from_state"] == "clean"
              and events[-1]["to_state"] == "quarantine",
              "Q2: transition clean -> quarantine recorded",
              str(events[-1]))

        landing.version("acc", 3, None, retracted=True, modified_at=3.0)
        process(landing)
        rows = current_rows(landing)
        check("acc" not in by_accession(rows["clean"])
              and "acc" not in by_accession(rows["quarantine"]),
              "Q2: retraction is invisible downstream")
        check("acc" in by_accession(rows["retracted"]),
              "Q2: retraction wins and is tracked as retracted")
        check(result["status"] == "published", "Q2: each run published")
    finally:
        landing.close()


def scenario_doi_conflict_and_release():
    print("== Q3 DOI normalization -> mutual conflict; fix/retract releases")
    landing = Landing()
    try:
        landing.version("doi-a", 1, {"title": "A", "publication_year": 2020,
                                     "doi": "10.5555/ABC"})
        landing.version("doi-b", 1, {"title": "B", "publication_year": 2020,
                                     "doi": "https://doi.org/10.5555/abc"})
        landing.version("doi-c", 1, {"title": "C", "publication_year": 2020,
                                     "doi": "  10.5555/abc  "})
        landing.version("unrelated", 1, {"title": "U",
                                         "publication_year": 2020})
        process(landing)
        rows = current_rows(landing)
        clean = by_accession(rows["clean"])
        quar = by_accession(rows["quarantine"])
        check(set(clean) == {"unrelated"}, "Q3: conflicting DOIs not clean")
        check(set(quar) == {"doi-a", "doi-b", "doi-c"},
              "Q3: all three colliding results quarantined (both directions)")
        for acc in quar:
            check(json.loads(quar[acc]["rule_ids"]) == ["R-DOI-004"],
                  f"Q3: {acc} quarantined by R-DOI-004")

        # one side fixes its DOI: the remaining two are still colliding
        landing.version("doi-b", 2, {"title": "B", "publication_year": 2020,
                                     "doi": "10.5555/def"}, modified_at=2.0)
        process(landing)
        rows = current_rows(landing)
        clean = by_accession(rows["clean"])
        quar = by_accession(rows["quarantine"])
        check("doi-b" in clean and {"doi-a", "doi-c"} <= set(quar),
              "Q3: fixing one of three leaves the remaining pair conflicted",
              f"clean={sorted(clean)} quarantine={sorted(quar)}")

        # fixing the second side releases the last remaining result
        landing.version("doi-c", 2, {"title": "C", "publication_year": 2021,
                                     "doi": "10.5555/xyz"}, modified_at=3.0)
        process(landing)
        rows = current_rows(landing)
        clean = by_accession(rows["clean"])
        quar = by_accession(rows["quarantine"])
        check(set(clean) == {"unrelated", "doi-a", "doi-b", "doi-c"},
              "Q3: fixing the last colliding side releases the remaining result",
              f"clean={sorted(clean)}")
        check(not quar, "Q3: no conflict quarantine left")
        events = landing.query(
            "SELECT * FROM quality_events WHERE accession='doi-a'"
            " ORDER BY event_id")
        check(any(e["event_type"] == "conflict_released" for e in events),
              "Q3: conflict release transition is recorded")

        # a two-way conflict released by retraction of the other side
        landing.version("doi-b", 3, {"title": "B", "publication_year": 2020,
                                     "doi": "10.5555/ghi"}, modified_at=4.0)
        landing.version("doi-c", 3, {"title": "C", "publication_year": 2021,
                                     "doi": "10.5555/GHI"}, modified_at=4.0)
        process(landing)
        rows = current_rows(landing)
        quar = by_accession(rows["quarantine"])
        clean = by_accession(rows["clean"])
        check("doi-b" in quar and "doi-c" in quar,
              "Q3: new normalized collision quarantines both sides")
        check("doi-a" in clean, "Q3: unrelated result unaffected")
        landing.version("doi-b", 4, None, retracted=True, modified_at=5.0)
        process(landing)
        rows = current_rows(landing)
        check("doi-c" in by_accession(rows["clean"]),
              "Q3: retracting one side releases the other")
        check("doi-b" in by_accession(rows["retracted"]),
              "Q3: retracted result stays retracted")
        landing.version("doi-a", 2, None, retracted=True, modified_at=6.0)
        process(landing)
        rows = current_rows(landing)
        check("doi-a" not in by_accession(rows["clean"])
              and "doi-a" in by_accession(rows["retracted"]),
              "Q3: retracted result is invisible downstream")
    finally:
        landing.close()


def scenario_tenant_isolation():
    print("== Q4 tenants are independent")
    landing = Landing()
    try:
        landing.version("shared", 1, {"title": "Alpha copy",
                                      "publication_year": 2020,
                                      "doi": "10.9000/SAME"}, tenant="alpha")
        landing.version("shared", 1, {"title": "Beta copy",
                                      "publication_year": 2020,
                                      "doi": "10.9000/same"}, tenant="beta")
        process(landing)
        rows = current_rows(landing)
        check("shared" in by_accession(rows["clean"], "alpha"),
              "Q4: alpha result clean despite beta using the same DOI")
        check("shared" in by_accession(rows["clean"], "beta"),
              "Q4: beta result clean despite alpha using the same DOI")
        check(not by_accession(rows["quarantine"], "alpha"),
              "Q4: alpha has no quarantine")
        counts = landing.query(
            "SELECT tenant, COUNT(*) c FROM quality_current_clean"
            " GROUP BY tenant ORDER BY tenant")
        check([(c["tenant"], c["c"]) for c in counts]
              == [("alpha", 1), ("beta", 1)],
              "Q4: counts retain both tenants")
    finally:
        landing.close()


def scenario_late_and_smaller_revision():
    print("== Q5 late rows and smaller revisions are detected")
    landing = Landing()
    try:
        for i in range(4):
            landing.version(f"a{i}", i + 1, {"title": f"A{i}",
                                             "publication_year": 2020},
                            modified_at=1.0 + i)
        process(landing)
        progress = landing.query("SELECT * FROM quality_progress"
                                 " WHERE tenant='alpha'")[0]
        check(progress["inputs_seen"] == 4, "Q5: initial inputs recorded")

        # a late-arriving accession whose revision is below the tenant max
        landing.version("late-small", 1, {"title": "Late",
                                          "publication_year": 2021},
                        modified_at=0.5)
        # a later physical row for an existing accession carrying a *smaller*
        # revision (out-of-order landing must still be recognised as input)
        landing.version("a1", 3, {"title": "", "publication_year": 2020},
                        modified_at=1.0)
        # a row that reuses a freed low rowid *below* the tenant watermark
        conn = landing_db.connect(landing.path)
        with conn:
            freed = conn.execute(
                "SELECT rowid, accession FROM result_versions"
                " WHERE tenant='alpha' ORDER BY rowid LIMIT 1").fetchone()
            conn.execute("DELETE FROM result_versions WHERE tenant='alpha'"
                         " AND accession=?", (freed["accession"],))
            raw = conn.execute(
                "INSERT INTO raw_pages(tenant, scan_id, url, status,"
                " scan_until, body, fetched_at) VALUES ('alpha',NULL,NULL,200,"
                "NULL,NULL,1)").lastrowid
            conn.execute(
                "INSERT INTO result_versions(rowid, tenant, accession,"
                " revision, modified_at, retracted, payload, raw_page_id)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (freed["rowid"], "alpha", "late-gap", 1, 0.25, 0,
                 json.dumps({"title": "Gap", "publication_year": 2018}), raw))
        conn.close()
        result = process(landing)
        check(result["status"] == "published", "Q5: late inputs published")
        check(result["new_inputs"] == 3,
              "Q5: both late rows plus the gap row counted as new inputs",
              str(result["new_inputs"]))
        rows = current_rows(landing)
        check("late-gap" in by_accession(rows["clean"]),
              "Q5: a row reusing a low rowid below the watermark is detected")
        check("late-small" in by_accession(rows["clean"]),
              "Q5: smaller-revision late accession is visible")
        check("a1" not in by_accession(rows["clean"]),
              "Q5: newest revision (3) is bad, so no clean row survives")
        check(by_accession(rows["quarantine"])["a1"]["revision"] == 3,
              "Q5: newest revision (3) is judged")
        progress = landing.query("SELECT * FROM quality_progress"
                                 " WHERE tenant='alpha'")[0]
        check(progress["inputs_seen"] == 7,
              "Q5: progress counted every landing identity, not max revision",
              str(progress["inputs_seen"]))
        noop = process(landing)
        check(noop["status"] == "noop",
              "Q5: re-running with no new rows is a no-op")
        check(len(current_rows(landing)["clean"]) ==
              len(rows["clean"]),
              "Q5: no duplicate projection rows on replay")

        # an in-place rewrite of an already absorbed identity is also caught
        conn = landing_db.connect(landing.path)
        with conn:
            row = conn.execute("SELECT rowid, raw_page_id FROM result_versions"
                               " WHERE tenant='alpha' AND accession='a2'").fetchone()
            conn.execute(
                "UPDATE result_versions SET payload=? WHERE rowid=?",
                (json.dumps({"title": "A2 rewritten",
                             "publication_year": 2020}), row["rowid"]))
        conn.close()
        rewrite = process(landing)
        clean = by_accession(current_rows(landing)["clean"])
        check(rewrite["status"] == "published"
              and clean["a2"]["title"] == "A2 rewritten",
              "Q5: in-place rewritten content is re-evaluated and published",
              rewrite.get("status", ""))
        final = process(landing)
        check(final["status"] == "noop",
              "Q5: after absorbing the rewrite the tenant is clean again")
    finally:
        landing.close()


def scenario_rule_upgrade_reevaluation():
    print("== Q6 rule upgrade re-evaluates and recovers quarantined data")
    landing = Landing()
    try:
        landing.version("future", 1, {"title": "Future work",
                                      "publication_year": 2031})
        landing.version("stable", 1, {"title": "Stable",
                                      "publication_year": 2020})
        process(landing)
        rows = current_rows(landing)
        check("future" in by_accession(rows["quarantine"]),
              "Q6: year 2031 quarantined under v1")
        gen1 = landing.query(
            "SELECT * FROM quality_generations WHERE status='published'")[0]
        check(gen1["rule_version"] == "v1", "Q6: published under v1")

        v2 = write_rules(RULES_V1, "v2", directory=landing.dir,
                         publication_year__max=2035)
        refused = subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"), "process",
             "--db", landing.path, "--rules", v2],
            capture_output=True, text=True)
        check(refused.returncode == 3
              and "reevaluate" in (refused.stderr or ""),
              "Q6: a new rule version is refused without explicit re-eval",
              refused.stderr)
        rows = current_rows(landing)
        check("future" in by_accession(rows["quarantine"]),
              "Q6: refused run changed nothing")

        run = subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"), "process",
             "--db", landing.path, "--rules", v2, "--reevaluate"],
            capture_output=True, text=True)
        check(run.returncode == 0, "Q6: explicit re-evaluation succeeds",
              run.stdout + run.stderr)
        rows = current_rows(landing)
        check("future" in by_accession(rows["clean"]),
              "Q6: formerly quarantined result recovered under v2")
        check(not by_accession(rows["quarantine"]),
              "Q6: stale quarantine state revoked")
        gen2 = landing.query(
            "SELECT * FROM quality_generations WHERE status='published'")[0]
        check(gen2["rule_version"] == "v2",
              "Q6: published generation carries the new rule version")
        events = landing.query(
            "SELECT * FROM quality_events WHERE accession='future'"
            " ORDER BY event_id")
        check(events[0]["to_state"] == "quarantine"
              and events[-1]["to_state"] == "clean",
              "Q6: before/after verdicts retained for audit", str(events))
        check(events[-1]["rule_version"] == "v2"
              and events[-1]["rule_version_before"] == "v1",
              "Q6: transition records both rule versions")
        before_details = json.loads(events[-1]["details"])["before"]
        check(before_details["state"] == "quarantine"
              and before_details["reasons"][0]["rule_id"] == "R-YEAR-003",
              "Q6: prior quarantine reason retained on recovery")
        os.remove(v2)
    finally:
        landing.close()


def scenario_rule_swap_during_run():
    print("== Q7 a rules file swapped mid-run cannot change this batch")
    landing = Landing()
    try:
        landing.version("alpha-future", 1,
                        {"title": "Alpha future", "publication_year": 2031},
                        tenant="alpha")
        landing.version("beta-future", 1,
                        {"title": "Beta future", "publication_year": 2031},
                        tenant="beta")
        rules_a = write_rules(RULES_V1, "swap-a", directory=landing.dir)
        rules_b = write_rules(RULES_V1, "swap-b", directory=landing.dir,
                              publication_year__max=2040)

        snapshot = rules_mod.load_rules(rules_a)
        calls = {"n": 0}

        def swap_after_first(tenant):
            calls["n"] += 1
            if calls["n"] == 1:
                # replace the batch's rules file on disk while processing
                with open(rules_b, "rb") as src, open(rules_a, "wb") as dst:
                    dst.write(src.read())

        result = Processor(landing.path, snapshot,
                           on_tenant_applied=swap_after_first).process()
        check(result["status"] == "published", "Q7: run completed")
        rows = current_rows(landing)
        check("alpha-future" in by_accession(rows["quarantine"], "alpha")
              and "beta-future" in by_accession(rows["quarantine"], "beta"),
              "Q7: both tenants judged with the snapshot read at start")
        check(not by_accession(rows["clean"], "alpha")
              and not by_accession(rows["clean"], "beta"),
              "Q7: year 2031 stays quarantined under the start snapshot")
        gen = landing.query(
            "SELECT * FROM quality_generations WHERE status='published'")[0]
        check(gen["rule_version"] == "swap-a",
              "Q7: published generation records the start rule version")
        check(json.loads(gen["rule_content"])["publication_year"]["max"] == 2026,
              "Q7: generation stores the exact snapshot used")
        os.remove(rules_a)
        os.remove(rules_b)
    finally:
        landing.close()


def scenario_crash_stage_resume_publish():
    print("== Q9 crash while staging: old generation intact; resume publishes once")
    landing = Landing()
    try:
        landing.version("alpha-1", 1, {"title": "Alpha", "publication_year": 2020},
                        tenant="alpha")
        landing.version("alpha-2", 1, {"title": "", "publication_year": 2020},
                        tenant="alpha")
        landing.version("beta-1", 1, {"title": "Beta", "publication_year": 2019},
                        tenant="beta")
        process(landing)
        published_before = landing.query(
            "SELECT generation_id, published_at FROM quality_generations"
            " WHERE status='published'")
        views_before = current_rows(landing)

        # new input, then crash after alpha's staging rows were written
        landing.version("beta-2", 2, {"title": "Beta two",
                                      "publication_year": 2018},
                        tenant="beta", modified_at=2.0)

        class Crash(Exception):
            pass

        def crash_after_first(tenant):
            raise Crash("simulated abrupt exit after " + tenant)

        snapshot = rules_mod.load_rules(RULES_V1)
        crashed = False
        try:
            Processor(landing.path, snapshot,
                      on_tenant_applied=crash_after_first).process()
        except Crash:
            crashed = True
        check(crashed, "Q9: simulated crash interrupted generation construction")
        rows = current_rows(landing)
        check(rows == views_before,
              "Q9: downstream still sees the complete old generation")
        check(landing.query("SELECT generation_id FROM quality_generations"
                            " WHERE status='published'")[0]["generation_id"]
              == published_before[0]["generation_id"],
              "Q9: published pointer not moved by the crash")
        staging = landing.query(
            "SELECT * FROM quality_generations WHERE status='staging'")
        check(len(staging) == 1 and staging[0]["published_at"] is None,
              "Q9: crashed generation remains unpublished")

        cmd = [sys.executable, os.path.join(ROOT, "run_quality.py"), "process",
               "--db", landing.path, "--rules", RULES_V1, "--resume"]
        resumed = subprocess.run(cmd, capture_output=True, text=True)
        check(resumed.returncode == 0, "Q9: restart with resume succeeds",
              resumed.stdout + resumed.stderr)
        rows = current_rows(landing)
        check("beta-2" in by_accession(rows["clean"], "beta"),
              "Q9: resumed generation publishes the new input")
        pub = landing.query(
            "SELECT * FROM quality_generations WHERE status='published'")
        check(len(pub) == 1, "Q9: exactly one published generation after resume")
        check(pub[0]["generation_id"] != published_before[0]["generation_id"],
              "Q9: the switch happened exactly once, to the resumed generation")

        before_events = len(landing.query("SELECT * FROM quality_events"))
        republish = store.connect(landing.path)
        try:
            for _ in range(2):
                out = store.publish_generation(republish, pub[0]["generation_id"])
                check(out.get("published") is False,
                      "Q9: re-publishing the same generation is a no-op")
        finally:
            republish.close()
        check(len(landing.query("SELECT * FROM quality_events")) == before_events,
              "Q9: no duplicate events from repeated publish")
        check(len(landing.query("SELECT * FROM quality_current_clean")) ==
              len(rows["clean"]),
              "Q9: repeated publish did not duplicate views")
    finally:
        landing.close()


def scenario_rule_version_reuse_conflict():
    print("== Q8 reusing a rule version with different content is refused")
    landing = Landing()
    try:
        landing.version("one", 1, {"title": "One", "publication_year": 2020})
        process(landing)
        forged = write_rules(RULES_V1, "v1", directory=landing.dir,
                             publication_year__max=2100)
        out = subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"), "process",
             "--db", landing.path, "--rules", forged],
            capture_output=True, text=True)
        check(out.returncode == 3 and "different content" in out.stderr,
              "Q8: same version with different content refused by default",
              out.stderr)
        forced = subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"), "process",
             "--db", landing.path, "--rules", forged, "--reevaluate"],
            capture_output=True, text=True)
        check(forced.returncode == 0,
              "Q8: explicit --reevaluate permits a deliberate reuse",
              forced.stdout + forced.stderr)
        gen = landing.query(
            "SELECT rule_sha256 FROM quality_generations"
            " WHERE status='published'")[0]
        snapshot = rules_mod.load_rules(forged)
        check(gen["rule_sha256"] == snapshot.sha256,
              "Q8: published generation records the exact new content hash")
        os.remove(forged)
    finally:
        landing.close()


def scenario_verification_blocks_publish():
    print("== Q10 inconsistent staging cannot publish; failure advances nothing")
    landing = Landing()
    try:
        landing.version("ok", 1, {"title": "Ok", "publication_year": 2020})
        process(landing)
        published = landing.query(
            "SELECT generation_id FROM quality_generations"
            " WHERE status='published'")[0]["generation_id"]
        views_before = current_rows(landing)

        landing.version("ok2", 1, {"title": "Ok2", "publication_year": 2021},
                        modified_at=2.0)
        snapshot = rules_mod.load_rules(RULES_V1)
        staged = Processor(landing.path, snapshot).process(stage_only=True)
        gen = staged["generation_id"]
        # corrupt the staged projection behind the processor's back
        conn = landing_db.connect(landing.path)
        with conn:
            conn.execute(
                "UPDATE quality_clean SET doi='dup', doi_key='dup'"
                " WHERE generation_id=?", (gen,))
        conn.close()
        out = subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"), "publish",
             "--db", landing.path], capture_output=True, text=True)
        check(out.returncode == 4, "Q10: publish refuses an inconsistent gen",
              out.stdout + out.stderr)
        check("refusing" in out.stderr, "Q10: refusal reason reported")
        active = landing.query(
            "SELECT generation_id FROM quality_generations"
            " WHERE status='published'")[0]["generation_id"]
        check(active == published, "Q10: published pointer did not move")
        check(current_rows(landing) == views_before,
              "Q10: readers still see the complete previous generation")

        # a failing processing run must also leave the published gen alone
        landing.version("bad-dup-a", 1, {"title": "A", "publication_year": 2020,
                                         "doi": "10.3131/CLASH"},
                        modified_at=3.0)
        landing.version("bad-dup-b", 1, {"title": "B", "publication_year": 2020,
                                         "doi": "10.3131/clash"},
                        modified_at=3.0)
        class Boom(Exception):
            pass

        def boom_after_first(tenant):
            raise Boom("injected processing failure")

        try:
            Processor(landing.path, snapshot,
                      on_tenant_applied=boom_after_first).process()
        except Boom:
            pass
        active = landing.query(
            "SELECT generation_id FROM quality_generations"
            " WHERE status='published'")[0]["generation_id"]
        check(active == published,
              "Q10: a failed processing run does not advance the published gen")
        check(current_rows(landing) == views_before,
              "Q10: failed run left readers on the old complete generation")
        left = landing.query(
            "SELECT generation_id, published_at FROM quality_generations"
            " WHERE status='staging'")
        check(len(left) == 1 and left[0]["published_at"] is None
              and left[0]["generation_id"] > published,
              "Q10: failed run only left unpublished staging behind",
              str(left))
        # discard and retry cleanly
        subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"),
             "discard-staging", "--db", landing.path],
            capture_output=True, text=True)
        landed_concurrent = landing.query(
            "SELECT generation_id FROM quality_generations"
            " WHERE status='staging'")
        check(not landed_concurrent, "Q10: staging can be safely discarded")
        result = process(landing)
        rows = current_rows(landing)
        check(result["status"] == "published"
              and set(by_accession(rows["quarantine"])) >= {"bad-dup-a",
                                                            "bad-dup-b"},
              "Q10: a clean rerun judges the new inputs correctly")
    finally:
        landing.close()


def scenario_crash_sigkill_old_generation():
    print("== Q11 real SIGKILL mid-build leaves the old generation whole")
    landing = Landing()
    try:
        tenants = [f"t{i:02d}" for i in range(20)]
        for tenant in tenants:
            landing.version("stable", 1, {"title": f"Stable {tenant}",
                                          "publication_year": 2020},
                            tenant=tenant)
        process(landing)
        gen_before = landing.query(
            "SELECT generation_id FROM quality_generations"
            " WHERE status='published'")[0]["generation_id"]
        views_before = current_rows(landing)

        # a lot of new input so the build takes long enough to interrupt
        for tenant in tenants:
            landing.versions_bulk(
                tenant,
                [(f"new{j:03d}", 1,
                  {"title": f"New {j}", "publication_year": 2000}, False)
                 for j in range(900)])
        cmd = [sys.executable, os.path.join(ROOT, "run_quality.py"), "process",
               "--db", landing.path, "--rules", RULES_V1]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        staging_seen = False
        applied_tenants = 0
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                staging = landing.query(
                    "SELECT * FROM quality_generations WHERE status='staging'")
            except sqlite3.Error:
                staging = []
            if staging:
                staging_seen = True
                applied_tenants = len(landing.query(
                    "SELECT DISTINCT tenant FROM quality_clean"
                    " WHERE generation_id=?", (staging[0]["generation_id"],)))
                # wait until at least one tenant's staging rows are in place
                if applied_tenants >= 1:
                    break
            time.sleep(0.01)
        proc.kill()
        proc.communicate(timeout=10)
        check(staging_seen, "Q11: a staging generation was being built")
        check(applied_tenants >= 1,
              "Q11: the kill landed after partial staging writes",
              f"tenants={applied_tenants}")

        rows = current_rows(landing)
        check(rows == views_before,
              "Q11: downstream readers still see the complete old generation")
        active = landing.query(
            "SELECT generation_id FROM quality_generations"
            " WHERE status='published'")[0]["generation_id"]
        check(active == gen_before,
              "Q11: published pointer unchanged after SIGKILL")
        staging = landing.query(
            "SELECT * FROM quality_generations WHERE status='staging'")
        check(len(staging) == 1,
              "Q11: interrupted generation is unpublished staging")

        resumed = subprocess.run(cmd + ["--resume"],
                                 capture_output=True, text=True, timeout=120)
        check(resumed.returncode == 0, "Q11: resume completes",
              resumed.stdout[-500:] + resumed.stderr[-500:])
        pub = landing.query(
            "SELECT COUNT(*) c FROM quality_generations WHERE status='published'")
        check(pub[0]["c"] == 1,
              "Q11: exactly one published generation after resume")
        active_after = landing.query(
            "SELECT generation_id FROM quality_generations"
            " WHERE status='published'")[0]["generation_id"]
        check(active_after != gen_before,
              "Q11: publish flipped exactly once to the resumed generation")
        rows = current_rows(landing)
        per_tenant = {}
        for row in rows["clean"]:
            per_tenant[row["tenant"]] = per_tenant.get(row["tenant"], 0) + 1
        check(all(per_tenant.get(t) == 901 for t in tenants),
              "Q11: every tenant's resumed projection is complete",
              str(sorted((t, per_tenant.get(t)) for t in tenants)[:5])
              + f" ... expected 901 each")
        check(not landing.query(
            "SELECT * FROM quality_generations WHERE status='staging'"),
            "Q11: no staging generation left behind")
    finally:
        landing.close()


def scenario_stage_only_visibility():
    print("== Q12 --stage-only / discard leaves readers on the published gen")
    landing = Landing()
    try:
        landing.version("one", 1, {"title": "One", "publication_year": 2020})
        process(landing)
        views_before = current_rows(landing)
        landing.version("two", 2, {"title": "Two", "publication_year": 2021},
                        modified_at=2.0)
        cmd = [sys.executable, os.path.join(ROOT, "run_quality.py"), "process",
               "--db", landing.path, "--rules", RULES_V1, "--stage-only"]
        staged = subprocess.run(cmd, capture_output=True, text=True)
        check(staged.returncode == 0, "Q12: stage-only succeeds",
              staged.stdout + staged.stderr)
        check(current_rows(landing) == views_before,
              "Q12: staged generation is invisible to readers")
        st = landing.query("SELECT * FROM quality_generations"
                           " WHERE status='staging'")
        check(len(st) == 1, "Q12: one staging generation exists")
        status_cmd = subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"), "status",
             "--db", landing.path], capture_output=True, text=True)
        check("staging generation" in status_cmd.stdout
              and "clean=1" in status_cmd.stdout,
              "Q12: status shows staging and published counts",
              status_cmd.stdout)
        disc = subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"),
             "discard-staging", "--db", landing.path],
            capture_output=True, text=True)
        check(disc.returncode == 0, "Q12: discard succeeds",
              disc.stdout + disc.stderr)
        check(current_rows(landing) == views_before,
              "Q12: discard leaves the published generation untouched")
        check(not landing.query(
            "SELECT * FROM quality_generations WHERE status='staging'"),
            "Q12: staging generation safely dropped")
        # the discarded input is still new, so a later run picks it up
        result = process(landing)
        check(result["status"] == "published"
              and "two" in by_accession(current_rows(landing)["clean"]),
              "Q12: discarded input is not lost; next run publishes it")
    finally:
        landing.close()


def scenario_identity_replay_no_duplicates():
    print("== Q13 replaying identical identities adds nothing")
    landing = Landing()
    try:
        landing.version("dup", 1, {"title": "Dup", "publication_year": 2020})
        landing.version("dup", 2, {"title": "", "publication_year": 2020},
                        modified_at=2.0)
        first = process(landing)
        counts1 = store.connect(landing.path)
        try:
            s1 = store.status(counts1)
        finally:
            counts1.close()
        second = process(landing)
        third = process(landing)
        counts3 = store.connect(landing.path)
        try:
            s3 = store.status(counts3)
        finally:
            counts3.close()
        check(first["status"] == "published"
              and second["status"] == "noop" and third["status"] == "noop",
              "Q13: replays are no-ops")
        check(s1["counts"] == s3["counts"], "Q13: counts unchanged on replay")
        check(len(landing.query("SELECT * FROM quality_clean")) ==
              len(landing.query("SELECT * FROM quality_current_clean")),
              "Q13: no retained duplicate clean rows")
        check(len(landing.query("SELECT * FROM quality_quarantine")) ==
              len(landing.query("SELECT * FROM quality_current_quarantine")),
              "Q13: no retained duplicate quarantine rows")
    finally:
        landing.close()


def scenario_landing_preserved():
    print("== Q14 existing collector tables/history are preserved")
    landing = Landing()
    try:
        landing.version("keep", 1, {"title": "Keep", "publication_year": 2020})
        before = {
            "result_versions": landing.query(
                "SELECT * FROM result_versions ORDER BY tenant, accession, revision"),
            "raw_pages": landing.query("SELECT * FROM raw_pages ORDER BY id"),
        }
        process(landing)
        after = {
            "result_versions": landing.query(
                "SELECT * FROM result_versions ORDER BY tenant, accession, revision"),
            "raw_pages": landing.query("SELECT * FROM raw_pages ORDER BY id"),
        }
        check(before == after,
              "Q14: landing tables byte-for-byte unchanged by the quality layer")
        check(landing.query("SELECT name FROM sqlite_master WHERE type='table'"
                            " AND name IN ('scan_state','watermarks')"),
              "Q14: collector bookkeeping tables still present")
        trace = subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"), "trace",
             "--db", landing.path, "--tenant", "alpha", "--accession", "keep"],
            capture_output=True, text=True)
        check(trace.returncode == 0 and "raw_page_id=" in trace.stdout
              and "entered_clean" in trace.stdout,
              "Q14: trace follows source page, rules and transitions",
              trace.stdout)
    finally:
        landing.close()


def scenario_from_collector_output():
    print("== Q15 quality layer consumes real collector landing output")
    from server.mock_server import make_server
    import threading
    from collector.collector import Collector

    landing = Landing()
    httpd, state = make_server("127.0.0.1", 0)
    thread = threading.Thread(
        target=httpd.serve_forever, kwargs={"poll_interval": 0.02},
        daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    try:
        state.add_version("ok-1", payload={
            "title": "Collected", "publication_year": 2020,
            "doi": "https://doi.org/10.4242/Ok"})
        state.add_version("bad-1", payload={
            "title": "Collected bad", "publication_year": 1500})
        collector = Collector(base, landing.path, ["alpha"], page_size=50,
                              retry_scale=0.01)
        res = collector.run(rounds=1)
        check(res["alpha"].status == "ok",
              "Q15: collector fetched the landing rows")

        landing.version("extra-1", 1, {"title": "No doi",
                                       "publication_year": 2019})
        # note: the extra row above is a synthetic late-arrival in the same
        # landing schema; it proves the layer also sees post-fetch rows.
        result = process(landing)
        rows = current_rows(landing)
        clean = by_accession(rows["clean"])
        quar = by_accession(rows["quarantine"])
        check(set(clean) == {"ok-1", "extra-1"},
              "Q15: collected valid result plus late row are clean",
              f"clean={sorted(clean)}")
        check(set(quar) == {"bad-1"}, "Q15: collected invalid result quarantined")
        check(clean["ok-1"]["doi"] == "10.4242/ok",
              "Q15: doi normalized from collector payload")
        trace = subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_quality.py"), "trace",
             "--db", landing.path, "--tenant", "alpha", "--accession", "bad-1"],
            capture_output=True, text=True)
        check(trace.returncode == 0 and "raw_page_id=" in trace.stdout,
              "Q15: trace resolves the collector's raw_page_id",
              trace.stdout)
        raw_ids = {r["raw_page_id"] for r in landing.query(
            "SELECT raw_page_id FROM result_versions")}
        traced = landing.query(
            "SELECT raw_page_id FROM quality_quarantine"
            " WHERE accession='bad-1'")[0]["raw_page_id"]
        check(traced in raw_ids,
              "Q15: quarantine source page exists in the collector raw store")
    finally:
        landing.close()
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=3)


def scenario_tenant_scope_and_noop_per_tenant():
    print("== Q16 tenant-scoped run leaves other tenants untouched")
    landing = Landing()
    try:
        landing.version("a-ok", 1, {"title": "A", "publication_year": 2020},
                        tenant="alpha")
        landing.version("b-bad", 1, {"title": "", "publication_year": 2020},
                        tenant="beta")
        process(landing)
        landing.version("a-new", 2, {"title": "A2", "publication_year": 2021},
                        tenant="alpha", modified_at=2.0)
        result = process(landing, tenants=["alpha"])
        check(result["processed_tenants"] == ["alpha"],
              "Q16: only the requested tenant rebuilt")
        rows = current_rows(landing)
        check("a-new" in by_accession(rows["clean"], "alpha"),
              "Q16: alpha new input published")
        check("b-bad" in by_accession(rows["quarantine"], "beta"),
              "Q16: beta projection preserved in the new generation")
        result = process(landing, tenants=["beta"])
        check(result["status"] == "noop",
              "Q16: tenant with no new rows is a no-op")
        missing = process(landing, tenants=["gamma"])
        check(missing["status"] == "noop"
              and missing["skipped_tenants"] == ["gamma"],
              "Q16: unknown tenant reported, not fabricated")
    finally:
        landing.close()


def main():
    scenarios = [
        scenario_invalid_year_isolated,
        scenario_newer_bad_revision_masks,
        scenario_doi_conflict_and_release,
        scenario_tenant_isolation,
        scenario_late_and_smaller_revision,
        scenario_rule_upgrade_reevaluation,
        scenario_rule_swap_during_run,
        scenario_rule_version_reuse_conflict,
        scenario_crash_stage_resume_publish,
        scenario_verification_blocks_publish,
        scenario_crash_sigkill_old_generation,
        scenario_stage_only_visibility,
        scenario_identity_replay_no_duplicates,
        scenario_landing_preserved,
        scenario_from_collector_output,
        scenario_tenant_scope_and_noop_per_tenant,
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
    print(f"ALL QUALITY ACCEPTANCE CHECKS PASSED in "
          f"{time.time() - started:.1f}s")
    print("\nOffline reproducible commands:")
    print("  python run_quality.py process --db out/harvest.db "
          "--rules quality/rules/v1.json")
    print("  python run_quality.py status --db out/harvest.db")
    print("  python run_quality.py trace --db out/harvest.db "
          "--tenant alpha --accession ACC")
    print("  python tests/quality_acceptance.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
