#!/usr/bin/env python3
"""Acceptance tests for the offline collaboration-network analysis.

Fully offline, stdlib only.  Exit code 0 = all checks passed.

Scenarios
---------
N1   build: duplicate affiliations & aliases collapse, weight counts
     DISTINCT papers (not authors), same-institution authors never create
     self-loops, triangle cooperation edges all present
N2   neighbors: weight DESC / code ASC ordering, pagination without gaps,
     full neighbor total, unknown institution / negative threshold /
     bad paging errors
N3   path: minimum hops inside the min-weight subgraph, lexicographically
     smallest code sequence among equal-length candidates, explicit no-path,
     per-hop real weights and supporting papers
N4   edge provenance: supporting papers, ADOPTED revision, raw
     pre-normalization affiliation strings explain why the edge exists
N5   revision semantics: affiliation change subtracts the old edge and adds
     the new one; retraction removes; a higher revision restores; a late
     lower revision (even with different content) never overrides
N6   tenant isolation: the same accession in another tenant never leaks
N7   incremental catch-up: a caught-up re-run performs no business writes;
     retransmissions are absorbed by the ledger
N8   real SIGKILL mid contribution-replacement: no negative weights, no
     empty-shell edges, no double counting; restart converges and matches an
     independent recomputation (papers, edge weights, routes)
N9   mapping binding: a changed mapping is refused with a hint to create a
     new network file while the old network stays queryable; a name assigned
     to two codes is rejected at load; a new network file works with the new
     mapping
N10  unmapped affiliations: diagnostics listed, no common node, no edges
N11  one query snapshot: a concurrent commit does not change a query's view
N12  independent recomputation from the landing sample matches the module's
     paper set, edge weights and routes; the landing database is byte
     identical before/after (read-only usage)
"""
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from itertools import combinations

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from collector import db as landing_db              # noqa: E402
from collab import store                            # noqa: E402
from collab.builder import Builder                  # noqa: E402
from collab.engine import CollabEngine              # noqa: E402
from collab.mapping import parse_mapping            # noqa: E402

FAILURES = []

MAPPING_DOC = {
    "mapping_version": "sample-v1",
    "institutions": [
        {"code": "INST-A", "name": "Alpha University",
         "aliases": ["Alpha Univ", "AU"]},
        {"code": "INST-B", "name": "Beta Institute",
         "aliases": ["Beta Inst"]},
        {"code": "INST-C", "name": "Gamma College",
         "aliases": ["Gamma Coll"]},
        {"code": "INST-D", "name": "Delta Laboratory",
         "aliases": ["Delta Lab"]},
        {"code": "INST-E", "name": "Epsilon Center",
         "aliases": []},
    ],
}

BUSINESS_TABLES = ("collab_papers", "collab_contrib", "collab_edges",
                   "collab_paper_orgs", "collab_unmapped", "collab_inst")
ALL_TABLES = BUSINESS_TABLES + ("collab_meta", "collab_cursor",
                                "collab_ledger")


def check(cond, label, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f"  -- {detail}" if (detail and not cond) else ""))
    if not cond:
        FAILURES.append(label)


def section(name):
    print(f"\n=== {name} ===")


def auth(*pairs):
    return [{"name": n, "affiliations": a} for n, a in pairs]


class Fixture:
    """Landing db + network file + mapping file in an isolated temp dir."""

    def __init__(self, prefix="rh-collab-", mapping_doc=MAPPING_DOC):
        self.dir = tempfile.mkdtemp(prefix=prefix)
        self.landing = os.path.join(self.dir, "harvest.db")
        self.net = os.path.join(self.dir, "net.db")
        self.mapping_path = os.path.join(self.dir, "mapping.json")
        self.mapping_doc = mapping_doc
        with open(self.mapping_path, "w", encoding="utf-8") as fh:
            json.dump(mapping_doc, fh, ensure_ascii=False, indent=2)
        conn = landing_db.connect(self.landing)
        landing_db.init(conn)
        conn.commit()
        conn.close()

    def add(self, accession, payload, *, tenant="alpha", revision=1,
            retracted=False, modified_at=1.0):
        conn = landing_db.connect(self.landing)
        with conn:
            raw = conn.execute(
                "INSERT INTO raw_pages(tenant, scan_id, url, status, body,"
                " fetched_at) VALUES (?,?,?,200,'{}',?)",
                (tenant, f"s-{accession}-r{revision}", f"mem://{accession}",
                 time.time())).lastrowid
            conn.execute(
                "INSERT OR IGNORE INTO result_versions(tenant, accession,"
                " revision, modified_at, retracted, payload, raw_page_id)"
                " VALUES (?,?,?,?,?,?,?)",
                (tenant, accession, int(revision), float(modified_at),
                 1 if retracted else 0,
                 json.dumps(payload) if payload is not None else None, raw))
        conn.close()

    def builder(self, **kw):
        return Builder(self.net, self.landing,
                       parse_mapping(self.mapping_doc), **kw)

    def sync(self, **kw):
        return self.builder().sync(**kw)

    def engine(self):
        return CollabEngine(store.connect(self.net))

    def net_sql(self, sql, params=()):
        conn = sqlite3.connect(self.net, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def landing_dump(self):
        conn = sqlite3.connect(self.landing, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            out = []
            for table in ("raw_pages", "result_versions", "scan_state",
                          "watermarks"):
                rows = [tuple(sorted(dict(r).items())) for r in conn.execute(
                    f"SELECT * FROM {table}")]
                out.append((table, sorted(rows)))
            return out
        finally:
            conn.close()

    def dump(self, tables=ALL_TABLES):
        out = {}
        for table in tables:
            rows = self.net_sql(f"SELECT * FROM {table}")
            out[table] = sorted(tuple(sorted(r.items())) for r in rows)
        return out

    def cli(self, *args, env=None):
        return subprocess.run(
            [sys.executable, os.path.join(ROOT, "run_collab.py"), *args],
            capture_output=True, text=True, env=env)

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# independent reference implementation (never imports collab.builder/engine)
# ---------------------------------------------------------------------------
def reference_network(landing_path, mapping_doc, tenant):
    """Recompute adopted papers and edge weights straight from the landing."""
    lookup = {}
    for inst in mapping_doc["institutions"]:
        lookup[inst["name"]] = inst["code"]
        for alias in inst.get("aliases", []):
            lookup[alias] = inst["code"]
    conn = sqlite3.connect(landing_path, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            "SELECT accession, revision, retracted, payload"
            " FROM result_versions WHERE tenant=?"
            " ORDER BY accession, revision", (tenant,)).fetchall()
    finally:
        conn.close()
    latest = {}
    for r in rows:
        latest[r["accession"]] = (int(r["revision"]), int(r["retracted"]),
                                  r["payload"])
    adopted = {}
    edges = {}
    for accession, (rev, retracted, payload) in latest.items():
        adopted[accession] = (rev, retracted)
        if retracted:
            continue
        orgs = set()
        if payload:
            doc = json.loads(payload)
            for author in doc.get("authors", []):
                for raw in author.get("affiliations", []):
                    code = lookup.get(raw)
                    if code:
                        orgs.add(code)
        for a, b in combinations(sorted(orgs), 2):
            edges[(a, b)] = edges.get((a, b), 0) + 1
    return adopted, edges


def reference_path(edges, src, dst, min_weight):
    """Independent min-hop route; lexicographically smallest code sequence."""
    adj = {}
    for (a, b), w in edges.items():
        if w < min_weight:
            continue
        adj.setdefault(a, set()).add(b)
        adj.setdefault(b, set()).add(a)
    dist = {src: 0}
    frontier = [src]
    while frontier:
        nxt = []
        for u in frontier:
            for v in adj.get(u, ()):
                if v not in dist:
                    dist[v] = dist[u] + 1
                    nxt.append(v)
        frontier = nxt
    if dst not in dist:
        return None
    depth = dist[dst]
    best = []

    def dfs(u, path):
        nonlocal best
        if len(path) > depth + 1:
            return
        if u == dst and len(path) == depth + 1:
            if not best or tuple(path) < tuple(best):
                best = list(path)
            return
        for v in sorted(adj.get(u, ())):
            if v not in path and dist.get(v) == dist[u] + 1:
                dfs(v, path + [v])

    dfs(src, [src])
    return best or None


def check_invariants(fx, label):
    bad = fx.net_sql("SELECT * FROM collab_edges WHERE weight <= 0")
    check(not bad, f"{label}: no zero/negative-weight shell edges", str(bad))
    mismatch = fx.net_sql(
        "SELECT e.tenant, e.code_a, e.code_b, e.weight,"
        " (SELECT COUNT(*) FROM collab_contrib c WHERE c.tenant=e.tenant"
        "  AND c.code_a=e.code_a AND c.code_b=e.code_b) AS cnt"
        " FROM collab_edges e WHERE weight <> cnt")
    check(not mismatch, f"{label}: every edge weight equals its"
          " contributing-paper count", str(mismatch))
    orphan = fx.net_sql(
        "SELECT c.* FROM collab_contrib c LEFT JOIN collab_edges e"
        " ON e.tenant=c.tenant AND e.code_a=c.code_a AND e.code_b=c.code_b"
        " WHERE e.weight IS NULL")
    check(not orphan, f"{label}: no contribution without an edge row",
          str(orphan))


def edges_of(fx, tenant):
    return {(r["code_a"], r["code_b"]): r["weight"]
            for r in fx.net_sql(
                "SELECT * FROM collab_edges WHERE tenant=?", (tenant,))}


def build_sample(fx):
    """The fixed sample corpus (tenant alpha unless noted)."""
    fx.add("P1", {"title": "t1", "authors": auth(
        ("Ann", ["Alpha University", "Alpha Univ"]),   # duplicate + alias
        ("Bob", ["Beta Institute"]),
        ("Amy", ["AU"]))})                              # same inst again
    fx.add("P2", {"title": "t2", "authors": auth(
        ("Ben", ["Beta Inst"]), ("Cat", ["Gamma College"]))})
    fx.add("P3", {"title": "t3", "authors": auth(
        ("Ann", ["Alpha University"]), ("Cat", ["Gamma Coll"]))})
    fx.add("P4", {"title": "t4", "authors": auth(
        ("Dan", ["Alpha Univ"]), ("Eli", ["Beta Institute"]))})
    fx.add("P5", {"title": "t5", "authors": auth(
        ("Fay", ["Alpha University"]), ("Gil", ["Delta Lab"]))})
    fx.add("P6", {"title": "t6", "authors": auth(
        ("Hal", ["Gamma College"]), ("Ida", ["Delta Lab"]))})
    fx.add("P7", {"title": "t7", "authors": auth(
        ("Jan", ["Alpha University"]), ("Kai", ["Unknown Org X"]))})
    fx.add("P8", {"title": "t8", "authors": auth(
        ("Kim", ["Unknown Org Y"]), ("Lee", ["Unknown Org Z"]))})
    fx.add("P9", {"title": "t9", "authors": auth(
        ("Ann", ["Alpha University"]), ("Amy", ["AU"]),
        ("Abe", ["Alpha Univ"]))})                      # all at INST-A
    # same accession in another tenant, different collaboration
    fx.add("P1", {"title": "tb", "authors": auth(
        ("Zed", ["Delta Lab"]), ("Yan", ["Epsilon Center"]))}, tenant="beta")


# ---------------------------------------------------------------------------
# N1 build semantics
# ---------------------------------------------------------------------------
def test_build_semantics():
    section("N1 build: dedupe, aliases, paper-count weights, no self-loops")
    fx = Fixture()
    build_sample(fx)
    rep = fx.sync()
    check(rep.applied == 10, "all 10 landing revisions applied",
          f"applied={rep.applied}")
    edges = edges_of(fx, "alpha")
    expect = {("INST-A", "INST-B"): 2, ("INST-B", "INST-C"): 1,
              ("INST-A", "INST-C"): 1, ("INST-A", "INST-D"): 1,
              ("INST-C", "INST-D"): 1}
    check(edges == expect, "alpha edges: triangle + weight=2 for two PAPERS"
          " (not authors)", f"{edges} vs {expect}")
    check(all(a != b for a, b in edges), "no self-loop edges")
    check(edges_of(fx, "beta") == {("INST-D", "INST-E"): 1},
          "beta has its own single edge", str(edges_of(fx, "beta")))
    # P1 alone would weigh 3 if authors (not papers) were counted
    check(edges[("INST-A", "INST-B")] == 2,
          "3 authors / 3 raw names at INST-A in P1 count as ONE paper")
    check_invariants(fx, "N1")
    fx.close()


# ---------------------------------------------------------------------------
# N2 neighbors
# ---------------------------------------------------------------------------
def test_neighbors():
    section("N2 neighbors: ordering, pagination, totals, parameter errors")
    fx = Fixture()
    build_sample(fx)
    fx.sync()
    eng = fx.engine()
    r = eng.neighbors("alpha", "INST-A", limit=20)
    check(r.total == 3, "full neighbor total reported", f"total={r.total}")
    check([(n.code, n.weight) for n in r.page]
          == [("INST-B", 2), ("INST-C", 1), ("INST-D", 1)],
          "ordered by weight DESC then code ASC",
          str([(n.code, n.weight) for n in r.page]))
    # pagination: no gaps, no duplicates, union == full set
    seen = []
    for off in (0, 2):
        seen.extend(n.code for n in
                    eng.neighbors("alpha", "INST-A", limit=2,
                                  offset=off).page)
    check(seen == ["INST-B", "INST-C", "INST-D"],
          "limit=2 pages tile the full neighbor set", str(seen))
    check(eng.neighbors("alpha", "INST-A", limit=2, offset=3).page == [],
          "offset past the end returns an empty page")
    r = eng.neighbors("alpha", "INST-A", min_weight=2)
    check([n.code for n in r.page] == ["INST-B"] and r.total == 1,
          "min-weight filter applies to neighbors")
    # CLI + JSON shape
    proc = fx.cli("neighbors", "--net", fx.net, "--tenant", "alpha",
                  "--inst", "INST-A", "--json")
    doc = json.loads(proc.stdout)
    check(proc.returncode == 0 and doc["total"] == 3
          and doc["neighbors"][0]["code"] == "INST-B",
          "CLI neighbors --json carries total and ordered page")
    # parameter errors
    proc = fx.cli("neighbors", "--net", fx.net, "--tenant", "alpha",
                  "--inst", "INST-ZZZ")
    check(proc.returncode == 2 and "未知机构" in proc.stderr,
          "unknown institution code is a clear error", proc.stderr.strip())
    proc = fx.cli("neighbors", "--net", fx.net, "--tenant", "alpha",
                  "--inst", "INST-A", "--min-weight", "-1")
    check(proc.returncode == 2 and "min-weight" in proc.stderr,
          "negative min-weight is rejected", proc.stderr.strip())
    proc = fx.cli("neighbors", "--net", fx.net, "--tenant", "alpha",
                  "--inst", "INST-A", "--limit", "0")
    check(proc.returncode == 2, "limit 0 is rejected")
    proc = fx.cli("neighbors", "--net", fx.net, "--inst", "INST-A")
    check(proc.returncode == 2, "tenant is mandatory on the CLI")
    fx.close()


# ---------------------------------------------------------------------------
# N3 path
# ---------------------------------------------------------------------------
def test_path():
    section("N3 path: min hops, lexicographic tie-break, hop evidence")
    fx = Fixture()
    build_sample(fx)
    fx.sync()
    eng = fx.engine()
    # B->D: B-A-D and B-C-D are both 2 hops; INST-A < INST-C decides
    r = eng.path("alpha", "INST-B", "INST-D")
    check(r.found and r.hops == 2, "B to D is 2 hops", f"hops={r.hops}")
    check(r.codes == ["INST-B", "INST-A", "INST-D"],
          "equal-length candidates: lexicographically smallest sequence",
          str(r.codes))
    check(len(r.legs) == 2 and r.legs[0].weight == 2
          and {p["accession"] for p in r.legs[0].papers} == {"P1", "P4"},
          "first hop carries real weight and supporting papers",
          str(r.legs[0]))
    check(r.legs[1].weight == 1
          and [p["accession"] for p in r.legs[1].papers] == ["P5"],
          "second hop carries its own weight and papers")
    # min-weight subgraph: only A-B has weight 2, so A..D disconnects
    r = eng.path("alpha", "INST-A", "INST-D", min_weight=2)
    check(not r.found and r.reason, "explicit no-path in the min-weight"
          " subgraph", str(r.reason))
    r = eng.path("alpha", "INST-A", "INST-B", min_weight=2)
    check(r.found and r.hops == 1 and r.legs[0].weight == 2,
          "the weight-2 edge itself remains usable")
    # CLI JSON + errors
    proc = fx.cli("path", "--net", fx.net, "--tenant", "alpha",
                  "--from", "INST-B", "--to", "INST-D", "--json")
    doc = json.loads(proc.stdout)
    check(proc.returncode == 0 and doc["found"]
          and doc["codes"] == ["INST-B", "INST-A", "INST-D"]
          and doc["legs"][0]["weight"] == 2
          and doc["legs"][0]["papers"][0]["accession"] == "P1",
          "CLI path --json: codes, per-hop weights and papers")
    proc = fx.cli("path", "--net", fx.net, "--tenant", "alpha",
                  "--from", "INST-A", "--to", "INST-D", "--min-weight", "2")
    check(proc.returncode == 0 and "NO PATH" in proc.stdout,
          "CLI prints an explicit NO PATH line", proc.stdout.strip())
    proc = fx.cli("path", "--net", fx.net, "--tenant", "alpha",
                  "--from", "INST-A", "--to", "NOPE")
    check(proc.returncode == 2 and "未知机构" in proc.stderr,
          "unknown target institution is a clear error")
    proc = fx.cli("path", "--net", fx.net, "--tenant", "alpha",
                  "--from", "INST-A", "--to", "INST-D",
                  "--min-weight", "-2")
    check(proc.returncode == 2, "negative min-weight rejected for path")
    fx.close()


# ---------------------------------------------------------------------------
# N4 edge provenance
# ---------------------------------------------------------------------------
def test_edge_provenance():
    section("N4 edge: supporting papers, adopted revision, raw affiliations")
    fx = Fixture()
    build_sample(fx)
    fx.sync()
    eng = fx.engine()
    r = eng.edge("alpha", "INST-B", "INST-A")   # order normalized
    check(r.found and r.weight == 2 and r.code_a == "INST-A",
          "edge lookup normalizes endpoint order")
    by_acc = {p["accession"]: p for p in r.papers}
    check(set(by_acc) == {"P1", "P4"}, "both supporting papers listed")
    check(by_acc["P1"]["revision"] == 1, "adopted revision reported")
    check(by_acc["P1"]["raw_affiliations"].get("INST-A")
          == ["AU", "Alpha Univ", "Alpha University"],
          "raw pre-normalization affiliations explain the INST-A side",
          str(by_acc["P1"]["raw_affiliations"]))
    check(by_acc["P1"]["raw_affiliations"].get("INST-B")
          == ["Beta Institute"], "raw affiliations for the other endpoint")
    r = eng.edge("alpha", "INST-A", "INST-E")
    check(not r.found and r.weight == 0 and r.papers == [],
          "a non-existent edge reports weight 0, not an error")
    proc = fx.cli("edge", "--net", fx.net, "--tenant", "alpha",
                  "--a", "INST-A", "--b", "INST-B", "--json")
    doc = json.loads(proc.stdout)
    check(proc.returncode == 0 and doc["weight"] == 2
          and doc["papers"][0]["raw_affiliations"],
          "CLI edge --json carries provenance")
    proc = fx.cli("edge", "--net", fx.net, "--tenant", "alpha",
                  "--a", "INST-A", "--b", "INST-QQ")
    check(proc.returncode == 2, "unknown institution rejected for edge")
    fx.close()


# ---------------------------------------------------------------------------
# N5 revision semantics
# ---------------------------------------------------------------------------
def test_revision_semantics():
    section("N5 revisions: change subtracts/adds, retract breaks,"
            " higher revision restores, late lower revision cannot override")
    fx = Fixture()
    build_sample(fx)
    fx.sync()

    # P5 changes affiliations: A-D must be subtracted, B-D added
    fx.add("P5", {"title": "t5v2", "authors": auth(
        ("Fay", ["Beta Institute"]), ("Gil", ["Delta Laboratory"]))},
        revision=2)
    rep = fx.sync()
    check(rep.applied == 1, "only the new revision is applied",
          f"applied={rep.applied}")
    edges = edges_of(fx, "alpha")
    check(("INST-A", "INST-D") not in edges,
          "old edge A-D removed after the affiliation change")
    check(edges.get(("INST-B", "INST-D")) == 1,
          "new edge B-D added by revision 2")
    eng = fx.engine()
    r = eng.edge("alpha", "INST-B", "INST-D")
    check(r.found and r.papers[0]["accession"] == "P5"
          and r.papers[0]["revision"] == 2,
          "edge provenance shows the ADOPTED revision 2")
    check(r.papers[0]["raw_affiliations"].get("INST-B") == ["Beta Institute"],
          "provenance shows the new raw affiliation strings")

    # retransmit of revision 1 (same content): the landing primary key
    # absorbs it exactly like the collector's INSERT OR IGNORE -- no new
    # landing row exists, so the next sync is a zero-write no-op
    before = fx.dump()
    fx.add("P5", {"title": "t5", "authors": auth(
        ("Fay", ["Alpha University"]), ("Gil", ["Delta Lab"]))}, revision=1)
    rep = fx.sync()
    check(rep.scanned == 0 and rep.applied == 0 and rep.ledger_only == 0,
          "identical retransmission absorbed before any network write",
          f"scanned={rep.scanned} applied={rep.applied}")
    check(fx.dump() == before,
          "retransmission leaves every network table byte-identical")

    # retraction of P6 removes C-D
    fx.add("P6", None, revision=2, retracted=True)
    fx.sync()
    check(("INST-C", "INST-D") not in edges_of(fx, "alpha"),
          "retraction removes the edge contribution")
    check(not eng.edge("alpha", "INST-C", "INST-D").found,
          "retracted edge reports not-found")

    # P10 connects E; its retraction must BREAK the E..A path
    fx.add("P10", {"title": "t10", "authors": auth(
        ("Ivy", ["Delta Lab"]), ("Jay", ["Epsilon Center"]))}, revision=1)
    fx.sync()
    r = eng.path("alpha", "INST-E", "INST-A")
    check(r.found and r.codes == ["INST-E", "INST-D", "INST-B", "INST-A"],
          "E reaches A through D-B (lexicographically smallest 3-hop route)",
          str(r.codes))
    fx.add("P10", None, revision=2, retracted=True)
    fx.sync()
    check(("INST-D", "INST-E") not in edges_of(fx, "alpha"),
          "retraction removes D-E")
    r = eng.path("alpha", "INST-E", "INST-A")
    check(not r.found, "retraction breaks the only route: explicit no-path")
    check(eng.neighbors("alpha", "INST-E").total == 0,
          "isolated institution has zero neighbors")

    # a higher non-retracted revision restores connectivity
    fx.add("P10", {"title": "t10v3", "authors": auth(
        ("Ivy", ["Delta Lab"]), ("Jay", ["Epsilon Center"]))}, revision=3)
    fx.sync()
    r = eng.path("alpha", "INST-E", "INST-A")
    check(r.found and r.codes == ["INST-E", "INST-D", "INST-B", "INST-A"],
          "higher revision restores the route", str(r.codes))

    # P6 restored at revision 3
    fx.add("P6", {"title": "t6v3", "authors": auth(
        ("Hal", ["Gamma College"]), ("Ida", ["Delta Lab"]))}, revision=3)
    fx.sync()
    check(edges_of(fx, "alpha").get(("INST-C", "INST-D")) == 1,
          "retracted-then-restored edge weighs 1 again (no double count)")

    # late LOWER revision: P12's revision 2 lands first, revision 1 arrives
    # afterwards (out-of-order backfill); it must never override revision 2
    fx.add("P12", {"title": "t12", "authors": auth(
        ("Kay", ["Alpha University"]), ("Liz", ["Epsilon Center"]))},
        revision=2)
    fx.sync()
    check(edges_of(fx, "alpha").get(("INST-A", "INST-E")) == 1,
          "P12 revision 2 contributes A-E")
    fx.add("P12", {"title": "t12-late", "authors": auth(
        ("Kay", ["Beta Institute"]), ("Liz", ["Epsilon Center"]))},
        revision=1)
    rep = fx.sync()
    check(rep.ledger_only == 1 and rep.applied == 0,
          "late lower revision is ledger-only", f"{rep}")
    edges = edges_of(fx, "alpha")
    check(edges.get(("INST-A", "INST-E")) == 1
          and ("INST-B", "INST-E") not in edges,
          "late lower revision never overrides the adopted contribution")
    paper = fx.net_sql(
        "SELECT revision FROM collab_papers WHERE tenant='alpha'"
        " AND accession='P12'")[0]
    check(paper["revision"] == 2, "adopted revision stays 2")
    check_invariants(fx, "N5")
    fx.close()


# ---------------------------------------------------------------------------
# N6 tenant isolation
# ---------------------------------------------------------------------------
def test_tenant_isolation():
    section("N6 tenant isolation: same accession, separate networks")
    fx = Fixture()
    build_sample(fx)
    fx.sync()
    eng = fx.engine()
    check(eng.neighbors("beta", "INST-D").total == 1,
          "beta sees its own D-E collaboration")
    check(eng.neighbors("alpha", "INST-E").total == 0,
          "alpha has no INST-E collaboration from beta's P1")
    check(not eng.edge("beta", "INST-A", "INST-B").found,
          "beta has no A-B edge from alpha's P1")
    r = eng.path("beta", "INST-D", "INST-E")
    check(r.found and r.hops == 1, "beta path stays inside beta")
    r = eng.path("alpha", "INST-D", "INST-E")
    check(not r.found, "alpha cannot route through beta's edge")
    papers_beta = fx.net_sql(
        "SELECT * FROM collab_papers WHERE tenant='beta'")
    check(len(papers_beta) == 1 and papers_beta[0]["accession"] == "P1",
          "beta's paper set is its own P1 only")
    fx.close()


# ---------------------------------------------------------------------------
# N7 incremental idempotence
# ---------------------------------------------------------------------------
def test_incremental_idempotence():
    section("N7 caught-up re-run performs no business writes")
    fx = Fixture()
    build_sample(fx)
    fx.sync()
    before = fx.dump()
    rep = fx.sync()
    check(rep.scanned == 0 and rep.applied == 0 and rep.ledger_only == 0,
          "second run scans nothing new", f"{rep}")
    check(fx.dump() == before,
          "a caught-up re-run leaves every table byte-identical")
    # one new paper: only incremental work, no full re-scan
    fx.add("P11", {"title": "t11", "authors": auth(
        ("Kay", ["Alpha University"]), ("Liz", ["Epsilon Center"]))})
    rep = fx.sync()
    check(rep.scanned == 1 and rep.applied == 1,
          "a single new row is a single applied revision", f"{rep}")
    check(edges_of(fx, "alpha").get(("INST-A", "INST-E")) == 1,
          "the new edge appears incrementally")
    check_invariants(fx, "N7")
    fx.close()


# ---------------------------------------------------------------------------
# N8 crash consistency with a real SIGKILL
# ---------------------------------------------------------------------------
def test_crash_mid_replacement():
    section("N8 SIGKILL mid contribution-replacement, then resume")
    fx = Fixture()
    fx.add("Q1", {"title": "q1", "authors": auth(
        ("Ann", ["Alpha University"]), ("Bob", ["Beta Institute"]))})
    fx.add("Q2", {"title": "q2", "authors": auth(
        ("Cat", ["Gamma College"]), ("Dan", ["Delta Lab"]))})
    fx.sync()
    baseline = fx.dump()

    # Q1 revision 2 REPLACES its contribution; Q3 is a fresh paper
    fx.add("Q1", {"title": "q1v2", "authors": auth(
        ("Ann", ["Beta Institute"]), ("Bob", ["Gamma College"]))},
        revision=2)
    fx.add("Q3", {"title": "q3", "authors": auth(
        ("Eli", ["Alpha University"]), ("Fay", ["Delta Lab"]))})

    marker = os.path.join(fx.dir, "marker.flag")
    wait_file = os.path.join(fx.dir, "wait.flag")
    with open(wait_file, "w", encoding="utf-8"):
        pass
    env = dict(os.environ,
               RH_COLLAB_CRASH_POINT="replace_contrib_txn",
               RH_COLLAB_CRASH_MARKER=marker,
               RH_COLLAB_CRASH_WAIT_FILE=wait_file)
    proc = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "run_collab.py"), "update",
         "--db", fx.landing, "--net", fx.net, "--mapping", fx.mapping_path,
         "--chunk-size", "1"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
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
    check(reached, "subprocess reached the in-transaction replacement"
          " barrier", out)

    check(fx.dump() == baseline,
          "killed between delete-old and add-new: network is exactly the"
          " pre-chunk state (rollback, no partial replacement)")
    check_invariants(fx, "N8-post-crash")
    check(fx.net_sql("SELECT revision FROM collab_papers"
                     " WHERE tenant='alpha' AND accession='Q1'")
          == [{"revision": 1}],
          "adopted revision of Q1 is still 1 after the kill")

    proc = fx.cli("update", "--db", fx.landing, "--net", fx.net,
                  "--mapping", fx.mapping_path)
    check(proc.returncode == 0, "restart completes the catch-up",
          proc.stderr)
    adopted, edges = reference_network(fx.landing, MAPPING_DOC, "alpha")
    check(edges_of(fx, "alpha") == edges,
          "after restart the edge weights match an independent"
          " recomputation", f"{edges_of(fx, 'alpha')} vs {edges}")
    db_papers = {r["accession"]: (r["revision"], r["retracted"])
                 for r in fx.net_sql(
                     "SELECT * FROM collab_papers WHERE tenant='alpha'")}
    check(db_papers == adopted,
          "adopted paper set matches the independent recomputation",
          f"{db_papers} vs {adopted}")
    # route comparison after recovery: A reaches C only via A-D-C
    proc = fx.cli("path", "--net", fx.net, "--tenant", "alpha",
                  "--from", "INST-A", "--to", "INST-C", "--json")
    doc = json.loads(proc.stdout)
    ref = reference_path(edges, "INST-A", "INST-C", 1)
    check(doc["found"] and doc["codes"] == ref,
          "post-recovery route equals the reference route",
          f"{doc.get('codes')} vs {ref}")
    # the replaced edge proves exactly one contribution (no double count)
    check(edges_of(fx, "alpha").get(("INST-B", "INST-C")) == 1
          and ("INST-A", "INST-B") not in edges_of(fx, "alpha"),
          "Q1's replacement happened exactly once")
    check_invariants(fx, "N8-final")
    fx.close()


# ---------------------------------------------------------------------------
# N9 mapping binding
# ---------------------------------------------------------------------------
def test_mapping_binding():
    section("N9 mapping digest binding and conflict refusal")
    fx = Fixture()
    build_sample(fx)
    fx.sync()

    changed = json.loads(json.dumps(MAPPING_DOC))
    changed["institutions"].append(
        {"code": "INST-F", "name": "Zeta Forum", "aliases": []})
    changed_path = os.path.join(fx.dir, "mapping-v2.json")
    with open(changed_path, "w", encoding="utf-8") as fh:
        json.dump(changed, fh, ensure_ascii=False)

    proc = fx.cli("update", "--db", fx.landing, "--net", fx.net,
                  "--mapping", changed_path)
    check(proc.returncode == 3 and "另建" in proc.stderr,
          "a changed mapping is refused and told to create a new network",
          proc.stderr.strip())
    proc = fx.cli("rebuild", "--db", fx.landing, "--net", fx.net,
                  "--mapping", changed_path)
    check(proc.returncode == 3, "rebuild with a changed mapping is"
          " refused too")
    # old network stays fully queryable
    proc = fx.cli("neighbors", "--net", fx.net, "--tenant", "alpha",
                  "--inst", "INST-A")
    check(proc.returncode == 0 and "total=3" in proc.stdout,
          "the refused network remains queryable", proc.stdout.strip())
    proc = fx.cli("path", "--net", fx.net, "--tenant", "alpha",
                  "--from", "INST-B", "--to", "INST-D")
    check(proc.returncode == 0 and "hops=2" in proc.stdout,
          "path queries still work on the old network")

    # a NEW network file with the new mapping is the supported workflow
    net2 = os.path.join(fx.dir, "net2.db")
    proc = fx.cli("update", "--db", fx.landing, "--net", net2,
                  "--mapping", changed_path)
    check(proc.returncode == 0, "a separate network file accepts the new"
          " mapping", proc.stderr.strip())

    # one name assigned to two codes is rejected at load
    conflict = json.loads(json.dumps(MAPPING_DOC))
    conflict["institutions"][1]["aliases"].append("Alpha Univ")
    conflict_path = os.path.join(fx.dir, "mapping-conflict.json")
    with open(conflict_path, "w", encoding="utf-8") as fh:
        json.dump(conflict, fh, ensure_ascii=False)
    proc = fx.cli("update", "--db", fx.landing, "--net", net2,
                  "--mapping", conflict_path)
    check(proc.returncode == 3 and "两个" in proc.stderr,
          "a name assigned to two codes is rejected", proc.stderr.strip())

    # status exposes the bound digest
    proc = fx.cli("status", "--net", fx.net, "--json")
    doc = json.loads(proc.stdout)
    check(doc["meta"].get("mapping_digest") and
          doc["meta"].get("mapping_version") == "sample-v1",
          "status shows the bound mapping digest and version")

    # missing landing tables are a clean refusal
    empty_db = os.path.join(fx.dir, "empty.db")
    sqlite3.connect(empty_db).close()
    proc = fx.cli("update", "--db", empty_db, "--net",
                  os.path.join(fx.dir, "net3.db"),
                  "--mapping", fx.mapping_path)
    check(proc.returncode == 2, "missing landing tables refused cleanly")
    fx.close()


# ---------------------------------------------------------------------------
# N10 unmapped affiliations
# ---------------------------------------------------------------------------
def test_unmapped():
    section("N10 unmapped affiliations: diagnostics, never a common node")
    fx = Fixture()
    build_sample(fx)
    fx.sync()
    rows = fx.net_sql(
        "SELECT DISTINCT name FROM collab_unmapped WHERE tenant='alpha'"
        " ORDER BY name")
    check([r["name"] for r in rows]
          == ["Unknown Org X", "Unknown Org Y", "Unknown Org Z"],
          "every unmapped name is listed as a diagnostic", str(rows))
    proc = fx.cli("diagnose", "--net", fx.net)
    check(proc.returncode == 0 and "Unknown Org X" in proc.stdout
          and "P7" in proc.stdout and "P8" in proc.stdout,
          "diagnose lists names with their papers", proc.stdout.strip())
    # P7 {INST-A, X} and P8 {Y, Z}: if unmapped names merged into one node,
    # P7 would create A-? and P8 would create ?-? edges
    check(len(edges_of(fx, "alpha")) == 5,
          "unmapped names create no node and no edge (no shared '?')")
    eng = fx.engine()
    check(all(n.code.startswith("INST-")
              for n in eng.neighbors("alpha", "INST-A", limit=50).page),
          "INST-A has no neighbor synthesized from unknown names")
    codes = {r["code"] for r in fx.net_sql("SELECT code FROM collab_inst")}
    check(codes == {"INST-A", "INST-B", "INST-C", "INST-D", "INST-E"},
          "the node set is exactly the mapping's codes")
    fx.close()


# ---------------------------------------------------------------------------
# N11 snapshot consistency
# ---------------------------------------------------------------------------
def test_snapshot_consistency():
    section("N11 one query snapshot vs a concurrent commit")
    fx = Fixture()
    build_sample(fx)
    fx.sync()
    fx.add("P11", {"title": "t11", "authors": auth(
        ("Kay", ["Alpha University"]), ("Liz", ["Epsilon Center"]))})
    conn = store.connect(fx.net)
    eng = CollabEngine(conn)
    with eng.snapshot():
        # a second connection commits the update while the snapshot is held
        Builder(fx.net, fx.landing, parse_mapping(MAPPING_DOC)).sync()
        check(eng.neighbors("alpha", "INST-A").total == 3,
              "in-flight snapshot does not see the committed new edge")
        check(not eng.edge("alpha", "INST-A", "INST-E").found,
              "edge provenance comes from the same old view")
        r = eng.path("alpha", "INST-A", "INST-E")
        check(not r.found, "path query inside the snapshot sees no route")
    conn.close()
    eng2 = fx.engine()
    check(eng2.edge("alpha", "INST-A", "INST-E").found,
          "after the snapshot closes the new generation is visible")
    fx.close()


# ---------------------------------------------------------------------------
# N12 independent recomputation + landing untouched
# ---------------------------------------------------------------------------
def test_independent_recomputation():
    section("N12 independent recomputation and read-only landing usage")
    fx = Fixture()
    build_sample(fx)
    # run the full revision lifecycle before the final comparison
    fx.sync()
    fx.add("P5", {"title": "t5v2", "authors": auth(
        ("Fay", ["Beta Institute"]), ("Gil", ["Delta Laboratory"]))},
        revision=2)
    fx.add("P6", None, revision=2, retracted=True)
    fx.add("P10", {"title": "t10", "authors": auth(
        ("Ivy", ["Delta Lab"]), ("Jay", ["Epsilon Center"]))}, revision=1)
    fx.add("P10", None, revision=2, retracted=True)
    fx.add("P10", {"title": "t10v3", "authors": auth(
        ("Ivy", ["Delta Lab"]), ("Jay", ["Epsilon Center"]))}, revision=3)
    fx.add("P6", {"title": "t6v3", "authors": auth(
        ("Hal", ["Gamma College"]), ("Ida", ["Delta Lab"]))}, revision=3)
    fx.sync()

    for tenant in ("alpha", "beta"):
        adopted, edges = reference_network(fx.landing, MAPPING_DOC, tenant)
        check(edges_of(fx, tenant) == edges,
              f"{tenant}: edge weights match independent recomputation",
              f"{edges_of(fx, tenant)} vs {edges}")
        db_papers = {r["accession"]: (r["revision"], r["retracted"])
                     for r in fx.net_sql(
                         "SELECT * FROM collab_papers WHERE tenant=?",
                         (tenant,))}
        check(db_papers == adopted,
              f"{tenant}: adopted paper set matches recomputation")
    # routes: compare module paths against the reference for several pairs
    for src, dst, mw in (("INST-A", "INST-D", 1),
                         ("INST-E", "INST-B", 1),
                         ("INST-A", "INST-E", 2),
                         ("INST-B", "INST-D", 1)):
        _, edges = reference_network(fx.landing, MAPPING_DOC, "alpha")
        ref = reference_path(edges, src, dst, mw)
        r = fx.engine().path("alpha", src, dst, min_weight=mw)
        got = r.codes if r.found else None
        check(got == ref, f"route {src}->{dst} (min_weight={mw}) matches"
              f" the reference", f"{got} vs {ref}")
        if ref is not None:
            # every hop's weight equals the recomputed edge weight
            for leg in r.legs:
                lo, hi = sorted((leg.code_a, leg.code_b))
                check(leg.weight == edges[(lo, hi)],
                      f"hop {leg.code_a}-{leg.code_b} carries the real"
                      f" recomputed weight")
    fx.close()

    # landing read-only: a fresh fixture compared before/after ALL operations
    fx = Fixture()
    build_sample(fx)
    before = fx.landing_dump()
    fx.sync()
    fx.engine().neighbors("alpha", "INST-A")
    fx.engine().path("alpha", "INST-A", "INST-D")
    fx.engine().edge("alpha", "INST-A", "INST-B")
    check(fx.landing_dump() == before,
          "landing database is byte-identical after builds and queries")
    fx.close()


def main():
    tests = (test_build_semantics, test_neighbors, test_path,
             test_edge_provenance, test_revision_semantics,
             test_tenant_isolation, test_incremental_idempotence,
             test_crash_mid_replacement, test_mapping_binding,
             test_unmapped, test_snapshot_consistency,
             test_independent_recomputation)
    for fn in tests:
        fn()
    print(f"\n{'ALL PASSED' if not FAILURES else 'FAILURES'}:"
          f" {len(tests) - len(FAILURES)}/{len(tests)} scenario groups ok")
    if FAILURES:
        for label in FAILURES:
            print(f"  FAILED: {label}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
