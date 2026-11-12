"""SQLite storage for the collaboration network (separate database file).

The network lives in its OWN database; the landing database is opened
read-only and never modified.  Writes happen only when an explicit
update/rebuild command runs -- never from a fetch thread.

Tables (all prefixed ``collab_``)
---------------------------------
collab_meta        key/value metadata: schema version, the bound mapping
                   content digest and mapping version, last sync time
collab_inst        snapshot of the mapping's institutions (code, official
                   name, aliases) so query commands are self-contained and
                   never need the mapping file
collab_cursor      per-tenant durable progress: the largest *contiguous
                   consumed prefix* of landing rowids
collab_ledger      every consumed landing identity (tenant, accession,
                   revision) with a content hash: retransmissions are
                   absorbed and the same revision can never contribute twice
collab_papers      one row per (tenant, accession): the adopted (highest
                   applied) revision and its retraction flag
collab_paper_orgs  per adopted paper: institution code -> the raw,
                   pre-normalization affiliation strings that resolved to it
collab_unmapped    per adopted paper: raw affiliation strings with no
                   mapping entry (diagnostics; they form NO node)
collab_contrib     per-paper edge contributions: one row per (tenant,
                   accession, code_a, code_b) with the contributing revision
collab_edges       aggregate undirected edges: weight = number of DISTINCT
                   contributing papers; an edge row is deleted the moment its
                   weight would reach zero (no empty-shell edges)

Atomicity
---------
For every sync chunk, the deletion of replaced contributions, the insertion
of new ones, the edge-weight updates, the ledger entries and the cursor
advance commit in ONE transaction.  A hard kill mid-replacement therefore
rolls back wholesale: no negative weights, no empty-shell edges, no
double-counted papers.  Queries take a WAL snapshot so a neighbor list, a
path and its provenance always come from one consistent view.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager

from . import SCHEMA_VERSION


@contextmanager
def transaction(conn):
    """Explicit BEGIN/COMMIT (connections run in driver autocommit mode)."""
    conn.execute("BEGIN")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


SCHEMA = """
CREATE TABLE IF NOT EXISTS collab_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS collab_inst (
  code    TEXT PRIMARY KEY,
  name    TEXT NOT NULL,
  aliases TEXT NOT NULL              -- JSON array, snapshot of the mapping
);
CREATE TABLE IF NOT EXISTS collab_cursor (
  tenant      TEXT PRIMARY KEY,
  last_rowid  INTEGER NOT NULL DEFAULT 0,
  updated_at  REAL
);
CREATE TABLE IF NOT EXISTS collab_ledger (
  tenant       TEXT NOT NULL,
  accession    TEXT NOT NULL,
  revision     INTEGER NOT NULL,
  source_rowid INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  retracted    INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant, accession, revision)
);
CREATE TABLE IF NOT EXISTS collab_papers (
  tenant     TEXT NOT NULL,
  accession  TEXT NOT NULL,
  revision   INTEGER NOT NULL,
  retracted  INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant, accession)
);
CREATE TABLE IF NOT EXISTS collab_paper_orgs (
  tenant     TEXT NOT NULL,
  accession  TEXT NOT NULL,
  code       TEXT NOT NULL,
  raws       TEXT NOT NULL,          -- JSON array of raw affiliation strings
  PRIMARY KEY (tenant, accession, code)
);
CREATE TABLE IF NOT EXISTS collab_unmapped (
  tenant     TEXT NOT NULL,
  accession  TEXT NOT NULL,
  revision   INTEGER NOT NULL,
  name       TEXT NOT NULL,
  PRIMARY KEY (tenant, accession, name)
);
CREATE TABLE IF NOT EXISTS collab_contrib (
  tenant     TEXT NOT NULL,
  accession  TEXT NOT NULL,
  code_a     TEXT NOT NULL,
  code_b     TEXT NOT NULL,
  revision   INTEGER NOT NULL,
  PRIMARY KEY (tenant, accession, code_a, code_b)
);
CREATE INDEX IF NOT EXISTS idx_contrib_edge
  ON collab_contrib(tenant, code_a, code_b);
CREATE TABLE IF NOT EXISTS collab_edges (
  tenant     TEXT NOT NULL,
  code_a     TEXT NOT NULL,
  code_b     TEXT NOT NULL,
  weight     INTEGER NOT NULL,
  PRIMARY KEY (tenant, code_a, code_b)
);
"""


def connect(path):
    # isolation_level=None: we own every BEGIN/COMMIT explicitly, which is
    # what makes a reader's WAL snapshot hold for a whole query.
    conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=OFF")
    return conn


def connect_landing(path):
    """Open the landing database strictly read-only (no table creation)."""
    uri = "file:" + path + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=30.0,
                           isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA query_only=ON")
    return conn


def init(conn):
    conn.executescript(SCHEMA)
    with transaction(conn):
        conn.execute(
            "INSERT INTO collab_meta(key, value)"
            " VALUES ('schema_version', ?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (SCHEMA_VERSION,))


def get_meta(conn, key, default=None):
    row = conn.execute("SELECT value FROM collab_meta WHERE key=?",
                       (key,)).fetchone()
    return row["value"] if row else default


def get_cursor(conn, tenant):
    row = conn.execute(
        "SELECT last_rowid FROM collab_cursor WHERE tenant=?",
        (tenant,)).fetchone()
    return int(row["last_rowid"]) if row else 0
