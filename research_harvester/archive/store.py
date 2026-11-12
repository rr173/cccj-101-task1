"""Archive bookkeeping in SQLite.

All objects this layer owns are prefixed ``archive_`` and are created
additively; the collector's tables are never altered structurally.  The one
deliberate mutation of a collector table is clearing an expired
``raw_pages.body`` (set to NULL) after a shard is registered -- the row, its
status/URL/fetch time/id and every ``result_versions.raw_page_id`` stay intact.

Batch lifecycle (``archive_batches.phase``)
-------------------------------------------
    planned     snapshot frozen (cutoff + max page id), nothing written yet
    shard_written  one or more shards exist on disk but registration is not
                   complete for this batch (crash window #1)
    registered  every shard is registered in ``archive_shards``; bodies may or
                may not have been cleared yet (crash window #2)
    cleared     bodies removed from ``raw_pages`` for every registered page
    failed      a shard failed verification; nothing for that tenant is
                registered, its SQLite bodies are preserved, and the batch
                stops there.  Re-running with the same snapshot picks the
                batch up again.

Every step is safe to run twice: registration skips shards whose file digest
was already recorded for that exact path and snapshot, and clearing skips
pages that are not registered or already have a NULL body.

Cutoff rule: ``fetched_at <= cutoff_at`` is expired (a page fetched exactly at
the cutoff belongs to the batch).  The page-id upper bound is frozen at batch
start, so rows written after the snapshot are simply not visible to it, even
when they backdate ``fetched_at``.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS archive_batches (
  batch_id     TEXT PRIMARY KEY,
  status       TEXT NOT NULL,            -- planned|shard_written|registered|cleared|failed
  phase        TEXT NOT NULL,            -- machine stage for operators
  cutoff_at    REAL NOT NULL,            -- inclusive UTC cutoff (fetched_at <=)
  max_page_id  INTEGER NOT NULL,
  tenants      TEXT NOT NULL,            -- JSON list requested for this batch
  filters      TEXT NOT NULL,            -- JSON {tenants, fetch_date}
  snapshot_sig TEXT NOT NULL UNIQUE,     -- digest of (cutoff, max_page_id, filters)
  signer       TEXT,
  created_at   REAL NOT NULL,
  updated_at   REAL NOT NULL,
  message      TEXT
);
CREATE TABLE IF NOT EXISTS archive_shards (
  shard_id       INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id       TEXT NOT NULL,
  tenant         TEXT NOT NULL,
  fetch_date     TEXT NOT NULL,
  shard_no       INTEGER NOT NULL,
  path           TEXT NOT NULL,
  file_sha256    TEXT NOT NULL,
  compressed_bytes INTEGER NOT NULL,
  page_count     INTEGER NOT NULL,
  first_page_id  INTEGER,
  last_page_id   INTEGER,
  cutoff_at      REAL NOT NULL,
  max_page_id    INTEGER NOT NULL,
  page_ids       TEXT NOT NULL,          -- JSON list of archived page ids
  content_sha256 TEXT NOT NULL,
  manifest_sha256 TEXT NOT NULL,         -- JSON {page_id: body digest}
  registered_at  REAL NOT NULL,
  cleared_at     REAL,
  UNIQUE(batch_id, tenant, fetch_date, shard_no, path)
);
CREATE INDEX IF NOT EXISTS idx_archive_shards_tenant_date
  ON archive_shards(tenant, fetch_date);
CREATE INDEX IF NOT EXISTS idx_archive_shards_path ON archive_shards(path);
CREATE TABLE IF NOT EXISTS archive_pages (
  page_id     INTEGER PRIMARY KEY,
  shard_id    INTEGER NOT NULL,
  batch_id    TEXT NOT NULL,
  tenant      TEXT NOT NULL,
  body_sha256 TEXT NOT NULL,
  cleared_at  REAL
);
CREATE INDEX IF NOT EXISTS idx_archive_pages_batch ON archive_pages(batch_id);
"""

PHASE_PLANNED = "planned"
PHASE_SHARD_WRITTEN = "shard_written"
PHASE_REGISTERED = "registered"
PHASE_CLEARED = "cleared"
PHASE_FAILED = "failed"

TERMINAL_OK = (PHASE_CLEARED,)


class ArchiveError(Exception):
    """Base class for archive failures."""


class BatchBusy(ArchiveError):
    """A batch is in a state that must not be resumed by this invocation."""


class IntegrityFailure(ArchiveError):
    """A shard does not match its expected digests; refuse and localise."""

    def __init__(self, tenant, path, detail, *, page_id=None, shard_no=None):
        self.tenant = tenant
        self.path = path
        self.page_id = page_id
        self.shard_no = shard_no
        where = f"tenant={tenant} path={path}"
        if shard_no is not None:
            where += f" shard_no={shard_no}"
        if page_id is not None:
            where += f" page_id={page_id}"
        super().__init__(f"integrity failure at {where}: {detail}")


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


def has_tables(conn):
    """True when the archive_* tables exist; used by read-only commands."""
    return archive_tables_present(conn)


def require_landing(conn):
    names = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
    if "raw_pages" not in names:
        raise ArchiveError(
            "landing table raw_pages not found; run the harvester first")
    return names


def utc_date(fetched_at):
    """UTC calendar date bucket for a timestamp."""
    return time.strftime("%Y-%m-%d", time.gmtime(float(fetched_at)))


def normalize_tenants(tenants):
    if not tenants:
        return None
    values = [t for t in tenants if t]
    return sorted(set(values)) if values else None


def _filters_json(tenants, fetch_date):
    return json.dumps({"tenants": normalize_tenants(tenants),
                       "fetch_date": fetch_date}, sort_keys=True)


def snapshot_signature(cutoff_at, max_page_id, tenants, fetch_date):
    import hashlib
    blob = json.dumps({"cutoff_at": float(cutoff_at),
                       "max_page_id": int(max_page_id),
                       "tenants": normalize_tenants(tenants),
                       "fetch_date": fetch_date}, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def current_max_page_id(conn):
    row = conn.execute(
        "SELECT COALESCE(MAX(id), 0) m FROM raw_pages").fetchone()
    return int(row["m"] or 0)


def find_batch(conn, snapshot_sig):
    row = conn.execute(
        "SELECT * FROM archive_batches WHERE snapshot_sig=?",
        (snapshot_sig,)).fetchone()
    return dict(row) if row else None


RESUMABLE_PHASES = (PHASE_PLANNED, PHASE_SHARD_WRITTEN, PHASE_REGISTERED,
                    PHASE_FAILED)


def find_resumable_batch(conn, *, cutoff_at, tenants, fetch_date):
    """An unfinished batch for this cutoff/filter set, if one exists.

    Rerunning the same command after a crash must reuse the frozen snapshot
    (cutoff *and* max page id), never silently re-freeze around newer rows.
    Terminal batches (``cleared``) are not resumable: the next invocation is a
    new snapshot and may pick up rows written since.
    """
    filters = _filters_json(tenants, fetch_date)
    placeholders = ",".join("?" for _ in RESUMABLE_PHASES)
    row = conn.execute(
        f"SELECT * FROM archive_batches WHERE cutoff_at=? AND filters=?"
        f" AND phase IN ({placeholders})"
        f" ORDER BY created_at ASC, batch_id ASC LIMIT 1",
        [float(cutoff_at), filters, *RESUMABLE_PHASES]).fetchone()
    return dict(row) if row else None


def create_batch(conn, *, cutoff_at, max_page_id, tenants, fetch_date,
                 signer=None):
    """Create the batch for this exact snapshot (or reuse the durable one)."""
    sig = snapshot_signature(cutoff_at, max_page_id, tenants, fetch_date)
    existing = find_batch(conn, sig)
    if existing:
        return existing, False
    now = time.time()
    batch_id = f"b{int(float(cutoff_at))}-{max_page_id}-{uuid.uuid4().hex[:10]}"
    with conn:
        conn.execute(
            "INSERT INTO archive_batches(batch_id, status, phase, cutoff_at,"
            " max_page_id, tenants, filters, snapshot_sig, signer, created_at,"
            " updated_at, message)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (batch_id, PHASE_PLANNED, PHASE_PLANNED, float(cutoff_at),
             int(max_page_id), json.dumps(normalize_tenants(tenants) or []),
             _filters_json(tenants, fetch_date), sig, signer, now, now,
             "batch planned"))
    return find_batch(conn, sig), False


def get_batch(conn, batch_id):
    row = conn.execute(
        "SELECT * FROM archive_batches WHERE batch_id=?",
        (batch_id,)).fetchone()
    return dict(row) if row else None


def latest_batch(conn):
    row = conn.execute(
        "SELECT * FROM archive_batches ORDER BY created_at DESC, batch_id DESC"
        " LIMIT 1").fetchone()
    return dict(row) if row else None


def update_batch(conn, batch_id, *, status=None, phase=None, message=None,
                 signer=None):
    fields = {"updated_at": time.time()}
    if status is not None:
        fields["status"] = status
    if phase is not None:
        fields["phase"] = phase
    if message is not None:
        fields["message"] = message
    if signer is not None:
        fields["signer"] = signer
    sets = ", ".join(f"{key}=?" for key in fields)
    with conn:
        conn.execute(f"UPDATE archive_batches SET {sets} WHERE batch_id=?",
                     [*fields.values(), batch_id])


def expired_pages(conn, *, cutoff_at, max_page_id, tenants=None,
                  fetch_date=None):
    """Landing rows that belong to this snapshot and still have a body.

    The page-id upper bound is what keeps rows written after the batch started
    out of the batch, including backdated ones.
    """
    sql = ("SELECT id, tenant, scan_id, url, status, scan_until, body,"
           " fetched_at FROM raw_pages"
           " WHERE fetched_at <= ? AND id <= ? AND body IS NOT NULL")
    params = [float(cutoff_at), int(max_page_id)]
    normalized = normalize_tenants(tenants)
    if normalized:
        sql += " AND tenant IN (%s)" % ",".join("?" for _ in normalized)
        params.extend(normalized)
    sql += " ORDER BY tenant, fetched_at, id"
    rows = [dict(r) for r in conn.execute(sql, params)]
    if fetch_date:
        rows = [r for r in rows if utc_date(r["fetched_at"]) == fetch_date]
    return rows


def pending_pages(conn, *, cutoff_at, max_page_id, tenants=None,
                  fetch_date=None):
    """Expired rows that are not yet registered to a shard (with a body)."""
    rows = expired_pages(conn, cutoff_at=cutoff_at, max_page_id=max_page_id,
                        tenants=tenants, fetch_date=fetch_date)
    if not archive_tables_present(conn):
        return rows
    registered = {int(r["page_id"]) for r in conn.execute(
        "SELECT page_id FROM archive_pages")}
    return [r for r in rows if int(r["id"]) not in registered]


def archive_tables_present(conn):
    names = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    return {"archive_batches", "archive_shards", "archive_pages"} <= names


def registered_pages_for_batch(conn, batch_id):
    return [dict(r) for r in conn.execute(
        "SELECT * FROM archive_pages WHERE batch_id=? ORDER BY page_id",
        (batch_id,))]


def register_shard(conn, batch_id, *, tenant, fetch_date, shard_no, path,
                   file_sha256, compressed_bytes, manifest, page_ids,
                   cutoff_at, max_page_id):
    """Record one verified shard and its page ownership in one transaction.

    Idempotent: a shard already recorded for the same (batch, tenant, date,
    shard_no, path, file_sha256) is accepted as a replay and its pages are not
    inserted twice.
    """
    existing = conn.execute(
        "SELECT * FROM archive_shards WHERE batch_id=? AND tenant=?"
        " AND fetch_date=? AND shard_no=? AND path=?",
        (batch_id, tenant, fetch_date, int(shard_no), path)).fetchone()
    if existing is not None:
        if existing["file_sha256"] != file_sha256:
            raise IntegrityFailure(
                tenant, path,
                f"registered shard digest {existing['file_sha256']} != "
                f"recomputed {file_sha256}", shard_no=shard_no)
        return dict(existing), False
    now = time.time()
    with conn:
        for page_id in page_ids:
            owner = conn.execute(
                "SELECT shard_id, batch_id FROM archive_pages WHERE page_id=?",
                (int(page_id),)).fetchone()
            if owner is not None:
                raise ArchiveError(
                    f"page {page_id} already archived by batch "
                    f"{owner['batch_id']}")
        cur = conn.execute(
            "INSERT INTO archive_shards(batch_id, tenant, fetch_date,"
            " shard_no, path, file_sha256, compressed_bytes, page_count,"
            " first_page_id, last_page_id, cutoff_at, max_page_id, page_ids,"
            " content_sha256, manifest_sha256, registered_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (batch_id, tenant, fetch_date, int(shard_no), path, file_sha256,
             int(compressed_bytes), int(manifest["page_count"]),
             manifest.get("first_page_id"), manifest.get("last_page_id"),
             float(cutoff_at), int(max_page_id), json.dumps(sorted(page_ids)),
             manifest["content_sha256"],
             json.dumps(manifest["manifest_sha256"], sort_keys=True), now))
        shard_id = cur.lastrowid
        for page_id in sorted(page_ids):
            digest = manifest["manifest_sha256"][str(int(page_id))]
            conn.execute(
                "INSERT INTO archive_pages(page_id, shard_id, batch_id, tenant,"
                " body_sha256, cleared_at) VALUES (?,?,?,?,?,NULL)",
                (int(page_id), shard_id, batch_id, tenant, digest))
    return dict(conn.execute(
        "SELECT * FROM archive_shards WHERE shard_id=?",
        (shard_id,)).fetchone()), True


def shard_for_page(conn, page_id):
    return conn.execute(
        "SELECT s.*, p.body_sha256, p.cleared_at"
        " FROM archive_pages p JOIN archive_shards s ON s.shard_id = p.shard_id"
        " WHERE p.page_id=?", (int(page_id),)).fetchone()


def clear_batch_bodies(conn, batch_id):
    """NULL every registered body of a batch.  Idempotent and convergent."""
    rows = conn.execute(
        "SELECT page_id FROM archive_pages WHERE batch_id=?",
        (batch_id,)).fetchall()
    now = time.time()
    cleared = 0
    with conn:
        for row in rows:
            page_id = int(row["page_id"])
            updated = conn.execute(
                "UPDATE raw_pages SET body=NULL WHERE id=? AND body IS NOT NULL",
                (page_id,)).rowcount
            if updated:
                cleared += 1
            conn.execute(
                "UPDATE archive_pages SET cleared_at=COALESCE(cleared_at, ?)"
                " WHERE page_id=?", (now, page_id))
    # only the batch itself is finalised once every registered page own body
    # is gone; a concurrently written newer row (not part of this batch) is
    # never touched because it has no archive_pages ownership row.
    with conn:
        conn.execute(
            "UPDATE archive_shards SET cleared_at=COALESCE(cleared_at, ?)"
            " WHERE batch_id=?", (now, batch_id))
        if all_batch_pages_cleared(conn, batch_id):
            conn.execute(
                "UPDATE archive_batches SET status=?, phase=?, updated_at=?,"
                " message=? WHERE batch_id=?",
                (PHASE_CLEARED, PHASE_CLEARED, now,
                 f"cleared {cleared} bodies from the main database", batch_id))
    return cleared


def all_batch_pages_cleared(conn, batch_id):
    row = conn.execute(
        "SELECT COUNT(*) c FROM archive_pages WHERE batch_id=?"
        " AND cleared_at IS NULL", (batch_id,)).fetchone()
    return int(row["c"]) == 0


def finalize_partial_clear(conn, batch_id):
    """Mark a batch cleared if every registered page is already body-less."""
    if all_batch_pages_cleared(conn, batch_id):
        now = time.time()
        with conn:
            conn.execute(
                "UPDATE archive_batches SET status=?, phase=?, updated_at=?"
                " WHERE batch_id=?",
                (PHASE_CLEARED, PHASE_CLEARED, now, batch_id))


def batch_summary(conn, batch_id):
    batch = get_batch(conn, batch_id)
    if not batch:
        return None
    shards = [dict(r) for r in conn.execute(
        "SELECT * FROM archive_shards WHERE batch_id=? ORDER BY tenant,"
        " fetch_date, shard_no", (batch_id,))]
    pages = registered_pages_for_batch(conn, batch_id)
    return {
        "batch": batch,
        "shards": shards,
        "page_count": len(pages),
        "cleared_pages": sum(1 for p in pages if p["cleared_at"] is not None),
        "compressed_bytes": sum(s["compressed_bytes"] for s in shards),
    }


def shard_rows(conn, *, tenant=None, tenants=None, fetch_date=None):
    if not archive_tables_present(conn):
        return []
    sql = "SELECT * FROM archive_shards WHERE 1=1"
    params = []
    if tenant:
        sql += " AND tenant=?"
        params.append(tenant)
    if tenants:
        sql += " AND tenant IN (%s)" % ",".join("?" for _ in tenants)
        params.extend(list(tenants))
    if fetch_date:
        sql += " AND fetch_date=?"
        params.append(fetch_date)
    sql += " ORDER BY tenant, fetch_date, shard_no, path"
    return [dict(r) for r in conn.execute(sql, params)]


def archived_page_ids(conn, tenant, page_ids):
    """For a tenant, which of these page ids are registered in a shard."""
    wanted = {int(p) for p in page_ids}
    if not wanted:
        return set()
    found = set()
    rows = conn.execute(
        "SELECT page_id FROM archive_pages WHERE tenant=?", (tenant,))
    for row in rows:
        pid = int(row["page_id"])
        if pid in wanted:
            found.add(pid)
    return found


def page_owner(conn, page_id):
    return conn.execute(
        "SELECT * FROM archive_pages WHERE page_id=?",
        (int(page_id),)).fetchone()
