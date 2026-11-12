"""Projection store for the quality layer.

Everything the layer owns is prefixed ``quality_`` and created additively
(``CREATE ... IF NOT EXISTS``); the landing tables owned by the collector
(``raw_pages`` / ``result_versions`` / ``scan_state`` / ``watermarks``) are
never altered or dropped.

Publication model
-----------------
* A *generation* is a complete snapshot of the projection for every tenant
  that has landed data.
* Staging rows live in the same tables but carry a ``generation_id``; the
  ``quality_pointer`` row (exactly one, id=1) names the published generation.
* Publishing is one transaction: transition events are written, input progress
  is promoted, the pointer flips, and the old generation is retired.
* Readers always resolve through a pointer, so they see one complete
  generation.  A half-built generation is invisible by construction.

Reader interface (stable for downstream research statistics):

    quality_current_clean      current visible clean results
    quality_current_quarantine current visible quarantine records
    quality_current_retracted  current retracted (downstream invisible)

Audit tables (retained across generations):
    quality_inputs      every landing version identity consumed, with payload
    quality_progress    per-tenant incremental cursor (rowid + count)
    quality_events      per-accession before/after transitions incl. reasons
    quality_runs        one row per processing attempt
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS quality_generations (
  generation_id  INTEGER PRIMARY KEY AUTOINCREMENT,
  status         TEXT NOT NULL,          -- staging|published|retired|abandoned|failed
  rule_version   TEXT NOT NULL,
  rule_sha256    TEXT NOT NULL,
  rule_content   TEXT NOT NULL,
  base_generation_id INTEGER,            -- published projection this gen derives from
  input_watermarks TEXT,                 -- JSON {tenant: max result_versions.rowid}
  counts         TEXT,                   -- JSON counts at build time
  build_phase    TEXT,                   -- building|events_staged (NULL on legacy rows)
  created_at     REAL NOT NULL,
  published_at   REAL,
  finished_at    REAL,
  message        TEXT
);
CREATE TABLE IF NOT EXISTS quality_pointer (
  id            INTEGER PRIMARY KEY CHECK (id = 1),
  generation_id INTEGER,
  updated_at    REAL
);
CREATE TABLE IF NOT EXISTS quality_clean (
  tenant           TEXT NOT NULL,
  accession        TEXT NOT NULL,
  revision         INTEGER NOT NULL,
  title            TEXT NOT NULL,
  publication_year INTEGER NOT NULL,
  doi              TEXT,
  doi_key          TEXT,
  raw_page_id      INTEGER,
  generation_id    INTEGER NOT NULL,
  rule_version     TEXT NOT NULL,
  PRIMARY KEY (tenant, accession, generation_id)
);
CREATE INDEX IF NOT EXISTS idx_q_clean_gen ON quality_clean(generation_id, tenant);
CREATE INDEX IF NOT EXISTS idx_q_clean_doi ON quality_clean(generation_id, tenant, doi_key);
CREATE TABLE IF NOT EXISTS quality_quarantine (
  tenant        TEXT NOT NULL,
  accession     TEXT NOT NULL,
  revision      INTEGER NOT NULL,
  rule_ids      TEXT NOT NULL,           -- JSON list of stable rule ids
  reasons       TEXT NOT NULL,           -- JSON list {rule_id,field,value,message}
  original      TEXT NOT NULL,           -- JSON original payload field values
  raw_page_id   INTEGER,
  generation_id INTEGER NOT NULL,
  rule_version  TEXT NOT NULL,
  PRIMARY KEY (tenant, accession, generation_id)
);
CREATE INDEX IF NOT EXISTS idx_q_quar_gen ON quality_quarantine(generation_id, tenant);
CREATE TABLE IF NOT EXISTS quality_retracted (
  tenant        TEXT NOT NULL,
  accession     TEXT NOT NULL,
  revision      INTEGER NOT NULL,
  raw_page_id   INTEGER,
  generation_id INTEGER NOT NULL,
  PRIMARY KEY (tenant, accession, generation_id)
);
CREATE TABLE IF NOT EXISTS quality_inputs (
  tenant          TEXT NOT NULL,
  accession       TEXT NOT NULL,
  revision        INTEGER NOT NULL,
  source_rowid    INTEGER NOT NULL,
  payload         TEXT,
  retracted       INTEGER NOT NULL DEFAULT 0,
  raw_page_id     INTEGER,
  content_hash    TEXT NOT NULL,
  first_seen_generation INTEGER,
  last_generation INTEGER,
  PRIMARY KEY (tenant, accession, revision)
);
CREATE TABLE IF NOT EXISTS quality_pending_inputs (
  generation_id INTEGER NOT NULL,
  tenant        TEXT NOT NULL,
  accession     TEXT NOT NULL,
  revision      INTEGER NOT NULL,
  source_rowid  INTEGER NOT NULL,
  payload       TEXT,
  retracted     INTEGER NOT NULL DEFAULT 0,
  raw_page_id   INTEGER,
  content_hash  TEXT NOT NULL,
  PRIMARY KEY (generation_id, tenant, accession, revision)
);
CREATE TABLE IF NOT EXISTS quality_progress (
  tenant      TEXT PRIMARY KEY,
  last_rowid  INTEGER,
  inputs_seen INTEGER NOT NULL DEFAULT 0,
  last_run_id INTEGER,
  updated_at  REAL
);
CREATE TABLE IF NOT EXISTS quality_pending_events (
  generation_id  INTEGER NOT NULL,
  seq            INTEGER NOT NULL,
  tenant         TEXT NOT NULL,
  accession      TEXT NOT NULL,
  revision       INTEGER,
  event_type     TEXT NOT NULL,
  from_state     TEXT,
  to_state       TEXT,
  rule_version   TEXT,
  rule_version_before TEXT,
  details        TEXT NOT NULL,
  PRIMARY KEY (generation_id, seq)
);
CREATE TABLE IF NOT EXISTS quality_runs (
  run_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  generation_id INTEGER,
  rule_version TEXT,
  status       TEXT NOT NULL,            -- running|ok|noop|staged|failed
  started_at   REAL NOT NULL,
  finished_at  REAL,
  new_inputs   INTEGER,
  dirty_tenants TEXT,
  counts       TEXT,
  message      TEXT
);
CREATE TABLE IF NOT EXISTS quality_events (
  event_id      INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant        TEXT NOT NULL,
  accession     TEXT NOT NULL,
  revision      INTEGER,
  generation_id INTEGER NOT NULL,
  event_type    TEXT NOT NULL,
  from_state    TEXT,
  to_state      TEXT,
  rule_version       TEXT,
  rule_version_before TEXT,
  details       TEXT NOT NULL,
  created_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_q_events_acc ON quality_events(tenant, accession, event_id);
CREATE VIEW IF NOT EXISTS quality_current_clean AS
  SELECT c.tenant, c.accession, c.revision, c.title, c.publication_year, c.doi,
         c.doi_key, c.raw_page_id, c.generation_id, c.rule_version
  FROM quality_clean c JOIN quality_pointer p
    ON p.generation_id = c.generation_id;
CREATE VIEW IF NOT EXISTS quality_current_quarantine AS
  SELECT q.tenant, q.accession, q.revision, q.rule_ids, q.reasons, q.original,
         q.raw_page_id, q.generation_id, q.rule_version
  FROM quality_quarantine q JOIN quality_pointer p
    ON p.generation_id = q.generation_id;
CREATE VIEW IF NOT EXISTS quality_current_retracted AS
  SELECT r.tenant, r.accession, r.revision, r.raw_page_id, r.generation_id
  FROM quality_retracted r JOIN quality_pointer p
    ON p.generation_id = r.generation_id;
"""


class LandingMissing(Exception):
    """The landing database has no collector tables."""


class VerificationError(Exception):
    """Staged generation is internally inconsistent; publish refused."""


class IncompleteBuild(VerificationError):
    """Staged generation did not finish construction; resume or discard it."""


# Build-phase markers stored on quality_generations.  ``counts`` alone is not a
# publishability proof: the counts commit and the transition-history write are
# separate transactions, so a crash between them leaves a projection whose
# summary was committed but whose audit events were never made durable.  The
# marker flips to EVENTS_STAGED in the *same* transaction that writes the
# pending events (even when the event set is legitimately empty).
PHASE_BUILDING = "building"
PHASE_EVENTS_STAGED = "events_staged"
_VALID_PHASES = (PHASE_BUILDING, PHASE_EVENTS_STAGED)


def connect(path):
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


def init(conn):
    with conn:
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO quality_pointer(id, generation_id, updated_at)"
            " VALUES (1, NULL, NULL) ON CONFLICT(id) DO NOTHING")
        _migrate_build_phase(conn)


def _migrate_build_phase(conn):
    """Add the build-phase column to databases created by an older version.

    Legacy rows keep a NULL marker on purpose: we cannot prove whether their
    transition history is complete, so they must never be auto-stamped ready.
    New builds own their marker explicitly from creation onward.
    """
    columns = {r["name"] for r in conn.execute(
        "PRAGMA table_info(quality_generations)")}
    if "build_phase" not in columns:
        conn.execute(
            "ALTER TABLE quality_generations ADD COLUMN build_phase TEXT")


def require_landing(conn):
    names = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
    if "result_versions" not in names:
        raise LandingMissing(
            "landing table result_versions not found; run the harvester first "
            "(this layer never re-fetches or invents history)")
    return names


def content_hash(tenant, accession, revision, retracted, raw_page_id, payload):
    blob = json.dumps([tenant, accession, int(revision), int(bool(retracted)),
                       raw_page_id, payload], sort_keys=True,
                      ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def dumps(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)


def active_generation(conn):
    row = conn.execute(
        "SELECT p.generation_id, g.status, g.rule_version, g.rule_sha256,"
        " g.published_at, g.counts FROM quality_pointer p"
        " LEFT JOIN quality_generations g ON g.generation_id = p.generation_id"
        " WHERE p.id=1").fetchone()
    if not row or row["generation_id"] is None:
        return None
    return dict(row)


def get_generation(conn, generation_id):
    row = conn.execute(
        "SELECT * FROM quality_generations WHERE generation_id=?",
        (generation_id,)).fetchone()
    return dict(row) if row else None


def staging_generation(conn):
    row = conn.execute(
        "SELECT * FROM quality_generations WHERE status='staging'"
        " ORDER BY generation_id DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def copy_generation(conn, src, dst):
    """Copy the complete projection of src into dst (same-table staging)."""
    for table, columns in (
            ("quality_clean",
             "tenant, accession, revision, title, publication_year, doi,"
             " doi_key, raw_page_id, rule_version"),
            ("quality_quarantine",
             "tenant, accession, revision, rule_ids, reasons, original,"
             " raw_page_id, rule_version"),
            ("quality_retracted",
             "tenant, accession, revision, raw_page_id")):
        conn.execute(
            f"INSERT INTO {table}({columns}, generation_id)"
            f" SELECT {columns}, ? FROM {table} WHERE generation_id=?",
            [dst, src])


_copy_generation = copy_generation


def rebase_generation(conn, generation_id):
    """Make an interrupted staging generation start from the published base.

    Called (outside a long write transaction) when a previous attempt applied
    some tenants and then died.  Idempotent.
    """
    gen = get_generation(conn, generation_id)
    if gen is None:
        raise ValueError(f"unknown generation {generation_id}")
    if gen["status"] != "staging":
        return False
    base = gen["base_generation_id"]
    with conn:
        clear_generation_rows(conn, generation_id)
        if base is not None:
            copy_generation(conn, base, generation_id)
    return True


def discard_staging(conn, generation_id, reason="discarded"):
    gen = get_generation(conn, generation_id)
    if gen is None:
        raise ValueError(f"unknown generation {generation_id}")
    if gen["status"] != "staging":
        raise ValueError(
            f"generation {generation_id} is {gen['status']}, not staging")
    with conn:
        clear_generation_rows(conn, generation_id)
        update_generation(conn, generation_id, status="abandoned",
                          finished_at=time.time(), message=reason)
    return True


def _normalize_counts(counts):
    """Drop empty per-tenant buckets so equal projections compare equal."""
    if counts is None:
        return None
    normalized = dict(counts)
    normalized["tenants"] = {t: b for t, b in (counts.get("tenants") or {}).items()
                             if b.get("total")}
    return normalized


def verify_generation(conn, generation_id, expected_counts=None):
    """Refuse to publish an internally inconsistent generation."""
    problems = []
    dup = conn.execute(
        "SELECT tenant, accession, COUNT(*) c FROM quality_clean"
        " WHERE generation_id=? GROUP BY tenant, accession"
        " HAVING c > 1 LIMIT 5", (generation_id,)).fetchall()
    if dup:
        problems.append("duplicate clean rows: " + ", ".join(
            f"{r['tenant']}/{r['accession']}={r['c']}" for r in dup))
    both = conn.execute(
        "SELECT tenant, accession FROM quality_clean WHERE generation_id=?"
        " INTERSECT SELECT tenant, accession FROM quality_quarantine"
        " WHERE generation_id=? LIMIT 5",
        (generation_id, generation_id)).fetchall()
    if both:
        problems.append("accession both clean and quarantined: " + ", ".join(
            f"{r['tenant']}/{r['accession']}" for r in both))
    retr = conn.execute(
        "SELECT tenant, accession FROM quality_retracted WHERE generation_id=?"
        " INTERSECT SELECT tenant, accession FROM quality_clean"
        " WHERE generation_id=?", (generation_id, generation_id)).fetchall()
    if retr:
        problems.append("retracted accession visible as clean: " + ", ".join(
            f"{r['tenant']}/{r['accession']}" for r in retr))
    clean_without_key = conn.execute(
        "SELECT COUNT(*) c FROM quality_clean WHERE generation_id=?"
        " AND doi_key IS NOT NULL AND doi IS NULL", (generation_id,
                                                     )).fetchone()["c"]
    if clean_without_key:
        problems.append("clean rows with doi_key but no doi")
    dup_doi = conn.execute(
        "SELECT tenant, doi_key, COUNT(*) c FROM quality_clean"
        " WHERE generation_id=? AND doi_key IS NOT NULL"
        " GROUP BY tenant, doi_key HAVING c > 1 LIMIT 5",
        (generation_id,)).fetchall()
    if dup_doi:
        problems.append("duplicate visible doi: " + ", ".join(
            f"{r['tenant']}/{r['doi_key']}={r['c']}" for r in dup_doi))
    counts = generation_counts(conn, generation_id)
    if expected_counts is not None:
        if _normalize_counts(counts) != _normalize_counts(expected_counts):
            problems.append(
                f"staged counts {counts} != verified counts {expected_counts}")
    if problems:
        raise VerificationError("; ".join(problems))
    return counts


def stage_events(conn, generation_id, events, crash_barrier=None):
    """Persist the transition history and the completeness marker together.

    The marker flip and the event rows share one transaction, so after commit
    the history is either fully durable with an ``events_staged`` marker or
    absent entirely with the generation still in ``building``.  A batch with
    zero events is marked ready explicitly -- event count is never used as the
    completeness signal, so an empty legitimate batch is not blocked.

    ``crash_barrier`` (test-only) is invoked after the event rows are written
    but before the transaction commits, letting a SIGKILL test prove that the
    uncommitted marker leaves the generation unpublishable.
    """
    with conn:
        conn.execute("DELETE FROM quality_pending_events WHERE generation_id=?",
                     (generation_id,))
        for seq, ev in enumerate(events):
            conn.execute(
                "INSERT INTO quality_pending_events(generation_id, seq, tenant,"
                " accession, revision, event_type, from_state, to_state,"
                " rule_version, rule_version_before, details)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (generation_id, seq, ev["tenant"], ev["accession"],
                 ev.get("revision"), ev["event_type"], ev.get("from_state"),
                 ev.get("to_state"), ev.get("rule_version"),
                 ev.get("rule_version_before"),
                 dumps(ev.get("details") or {})))
        if crash_barrier is not None:
            crash_barrier("stage_events_txn")
        conn.execute(
            "UPDATE quality_generations SET build_phase=? WHERE generation_id=?",
            (PHASE_EVENTS_STAGED, generation_id))


def staged_event_count(conn, generation_id):
    return conn.execute(
        "SELECT COUNT(*) c FROM quality_pending_events WHERE generation_id=?",
        (generation_id,)).fetchone()["c"]


def staging_readiness(conn, generation_id):
    """Classify the durable state of a staging generation.

    Returns a dict describing whether the batch can be delivered as-is.  The
    decision is based on the build-phase marker, not on the presence of rows:
    a zero-event batch is complete, while a legacy/half-written batch is not.

    publishable      marker proves both projection and history are durable
    rebuild_required marker missing/incomplete (crash gap or legacy staging):
                     the events transaction never committed; resume rebuilds
    reason           machine-readable explanation for operators
    """
    gen = get_generation(conn, generation_id)
    if gen is None:
        raise ValueError(f"unknown generation {generation_id}")
    phase = gen.get("build_phase") if gen is not None else None
    if gen["status"] != "staging":
        return {"publishable": False, "rebuild_required": False,
                "phase": phase,
                "reason": f"generation is {gen['status']}, not staging"}
    if phase == PHASE_EVENTS_STAGED:
        return {"publishable": True, "rebuild_required": False,
                "phase": phase,
                "reason": "projection and transition history are durable"}
    if phase == PHASE_BUILDING:
        return {"publishable": False, "rebuild_required": True,
                "phase": phase,
                "reason": "build was interrupted before the transition "
                          "history committed; resume rebuilds this batch"}
    # NULL or unknown value: a legacy database (older schema) or tampered row.
    # Do not stamp it complete -- its history completeness cannot be proven.
    return {"publishable": False, "rebuild_required": True,
            "phase": phase,
            "reason": "no durable completeness marker (legacy or partial "
                      "staging); resume rebuilds this batch from its stored "
                      "rule snapshot"}


def publish_generation(conn, generation_id, run_id=None, expected_counts=None,
                       crash_barrier=None):
    """Atomically make a staging generation visible. Idempotent.

    One transaction: verify the staging rows one final time, absorb each
    tenant's pending inputs into the input ledger, advance per-tenant progress,
    promote staged transition events, flip the single pointer, retire the
    previous generation.

    Publishability is decided by the build-phase marker (see
    :func:`staging_readiness`), never by ``counts`` being non-null or by the
    number of staged events.  ``crash_barrier`` (test-only) fires inside the
    publish transaction so a SIGKILL there leaves the pointer untouched and a
    retry stays idempotent.
    """
    gen = get_generation(conn, generation_id)
    if gen is None:
        raise ValueError(f"unknown generation {generation_id}")
    if gen["status"] in ("published", "retired"):
        return {"published": False, "reason": "already published"}
    if gen["status"] != "staging":
        raise ValueError(
            f"generation {generation_id} is {gen['status']}, cannot publish")
    readiness = staging_readiness(conn, generation_id)
    if not readiness["publishable"]:
        raise IncompleteBuild(
            f"generation {generation_id} is not publishable: "
            f"{readiness['reason']}; resume it or discard it")
    now = time.time()
    with conn:
        # never flip the pointer for an inconsistent projection
        verify_generation(conn, generation_id, expected_counts)
        # pending landing inputs -> durable input ledger
        rows = conn.execute(
            "SELECT * FROM quality_pending_inputs WHERE generation_id=?",
            (generation_id,)).fetchall()
        for row in rows:
            conn.execute(
                "INSERT INTO quality_inputs(tenant, accession, revision,"
                " source_rowid, payload, retracted, raw_page_id, content_hash,"
                " first_seen_generation, last_generation)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(tenant, accession, revision) DO UPDATE SET"
                " source_rowid=excluded.source_rowid, payload=excluded.payload,"
                " retracted=excluded.retracted,"
                " raw_page_id=excluded.raw_page_id,"
                " content_hash=excluded.content_hash,"
                " last_generation=excluded.last_generation",
                (row["tenant"], row["accession"], row["revision"],
                 row["source_rowid"], row["payload"], row["retracted"],
                 row["raw_page_id"], row["content_hash"], generation_id,
                 generation_id))
        watermarks = json.loads(gen["input_watermarks"] or "{}")
        for tenant, rowid in watermarks.items():
            pending = conn.execute(
                "SELECT COUNT(*) c FROM quality_pending_inputs"
                " WHERE generation_id=? AND tenant=?",
                (generation_id, tenant)).fetchone()["c"]
            conn.execute(
                "INSERT INTO quality_progress(tenant, last_rowid, inputs_seen,"
                " last_run_id, updated_at) VALUES (?,?,?,?,?)"
                " ON CONFLICT(tenant) DO UPDATE SET"
                " last_rowid=MAX(COALESCE(quality_progress.last_rowid, 0),"
                "                excluded.last_rowid),"
                " inputs_seen=quality_progress.inputs_seen+excluded.inputs_seen,"
                " last_run_id=excluded.last_run_id,"
                " updated_at=excluded.updated_at",
                (tenant, rowid, pending, run_id, now))
        conn.execute("DELETE FROM quality_pending_inputs WHERE generation_id=?",
                     (generation_id,))
        conn.execute(
            "INSERT INTO quality_events(tenant, accession, revision,"
            " generation_id, event_type, from_state, to_state,"
            " rule_version, rule_version_before, details, created_at)"
            " SELECT tenant, accession, revision, ?, event_type, from_state,"
            " to_state, rule_version, rule_version_before, details, ?"
            " FROM quality_pending_events WHERE generation_id=? ORDER BY seq",
            (generation_id, now, generation_id))
        conn.execute("DELETE FROM quality_pending_events WHERE generation_id=?",
                     (generation_id,))
        if crash_barrier is not None:
            crash_barrier("publish_txn")
        # atomic visibility switch
        previous = conn.execute(
            "SELECT generation_id FROM quality_pointer WHERE id=1"
        ).fetchone()
        prev_id = previous["generation_id"] if previous else None
        conn.execute(
            "UPDATE quality_pointer SET generation_id=?, updated_at=? WHERE id=1",
            (generation_id, now))
        update_generation(conn, generation_id, status="published",
                          published_at=now, finished_at=now)
        if prev_id is not None and prev_id != generation_id:
            previous_gen = get_generation(conn, prev_id)
            if previous_gen and previous_gen["status"] in ("published",
                                                          "retired"):
                update_generation(conn, prev_id, status="retired")
                clear_generation_rows(conn, prev_id)
    return {"published": True, "generation_id": generation_id}


def verify_published(conn, generation_id, expected_counts=None):
    return verify_generation(conn, generation_id, expected_counts)



def create_generation(conn, rules, watermarks, message=None,
                      base_generation_id=None):
    now = time.time()
    cur = conn.execute(
        "INSERT INTO quality_generations(status, rule_version, rule_sha256,"
        " rule_content, base_generation_id, input_watermarks, counts,"
        " build_phase, created_at, message)"
        " VALUES ('staging',?,?,?,?,?,NULL,?, ?,?)",
        (rules.version, rules.sha256, rules.content, base_generation_id,
         dumps(watermarks), PHASE_BUILDING, now, message))
    return cur.lastrowid


def update_generation(conn, generation_id, **fields):
    allowed = {"status", "input_watermarks", "counts", "build_phase",
               "published_at", "finished_at", "message", "rule_version",
               "rule_sha256", "rule_content", "base_generation_id"}
    sets = []
    params = []
    for key, value in fields.items():
        if key not in allowed:
            raise ValueError(f"cannot update generation field {key!r}")
        sets.append(f"{key}=?")
        params.append(value)
    if not sets:
        return
    params.append(generation_id)
    conn.execute(f"UPDATE quality_generations SET {', '.join(sets)}"
                 " WHERE generation_id=?", params)


def clear_generation_rows(conn, generation_id):
    for table in ("quality_clean", "quality_quarantine", "quality_retracted",
                  "quality_pending_inputs", "quality_pending_events"):
        conn.execute(f"DELETE FROM {table} WHERE generation_id=?",
                     (generation_id,))


def generation_counts(conn, generation_id):
    result = {"clean": 0, "quarantine": 0, "retracted": 0, "tenants": {}}
    for table, key in (("quality_clean", "clean"),
                       ("quality_quarantine", "quarantine"),
                       ("quality_retracted", "retracted")):
        total = 0
        for row in conn.execute(
                f"SELECT tenant, COUNT(*) c FROM {table}"
                " WHERE generation_id=? GROUP BY tenant", (generation_id,)):
            total += row["c"]
            bucket = result["tenants"].setdefault(
                row["tenant"], {"clean": 0, "quarantine": 0, "retracted": 0})
            bucket[key] = row["c"]
        result[key] = total
    for bucket in result["tenants"].values():
        bucket["total"] = sum(bucket[k] for k in
                              ("clean", "quarantine", "retracted"))
    return result


def current_counts(conn):
    active = active_generation(conn)
    if not active:
        return {"clean": 0, "quarantine": 0, "retracted": 0, "tenants": {}}
    return generation_counts(conn, active["generation_id"])


def status(conn):
    active = active_generation(conn)
    staging = staging_generation(conn)
    result = {
        "published_generation": active,
        "staging_generation": {
            "generation_id": staging["generation_id"],
            "rule_version": staging["rule_version"],
            "status": staging["status"],
            "build_phase": staging["build_phase"],
            "created_at": staging["created_at"],
        } if staging else None,
        "counts": current_counts(conn),
    }
    if active:
        gen = get_generation(conn, active["generation_id"])
        result["rule_version"] = gen["rule_version"]
        result["rule_sha256"] = gen["rule_sha256"]
        result["published_at"] = gen["published_at"]
    else:
        result["rule_version"] = None
        result["rule_sha256"] = None
        result["published_at"] = None
    return result
