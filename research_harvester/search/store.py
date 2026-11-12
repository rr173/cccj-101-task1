"""SQLite storage for the local retrieval index (separate database file).

The index lives in its OWN database; the landing database is opened read-only
and never modified.  Nothing here is coupled to a fetch thread: writes happen
only when the explicit index/update command runs.

Tables (all prefixed ``search_``)
---------------------------------
search_meta        key/value metadata incl. schema version
search_cursor      per-tenant landing rowid watermark (resume position)
search_ledger      every consumed landing identity with a content hash, so
                   late rows / smaller revisions / rowid gaps are detected
                   like the quality layer's input ledger
search_docs        one row per (tenant, accession): the CURRENT revision's
                   original title/abstract, retraction flag, skip reasons
search_terms       dictionary of normalized term strings
search_postings    term -> document -> field, with token positions and the
                   half-open ORIGINAL-character spans of every occurrence
search_skips       audit rows for non-text / non-object fields (with reasons,
                   never stringified objects)

Atomicity
---------
Each sync chunk commits in one transaction containing postings, document text,
current revision and the advanced watermark together.  A hard exit leaves the
previous complete state intact; the watermark only moves inside the same
transaction as the index changes.  Query readers take a snapshot transaction so
total, ordering and excerpts always come from one consistent view.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager

from . import SCHEMA_VERSION


@contextmanager
def transaction(conn):
    """Explicit BEGIN/COMMIT (connections run in driver autocommit mode).

    Commits on success, rolls back on any exception.  Nested usage is not
    needed: each logical write owns one transaction, which is the property
    the crash-consistency guarantees rely on.
    """
    conn.execute("BEGIN")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")

SCHEMA = """
CREATE TABLE IF NOT EXISTS search_meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS search_cursor (
  tenant      TEXT PRIMARY KEY,
  last_rowid  INTEGER NOT NULL DEFAULT 0,
  updated_at  REAL
);
CREATE TABLE IF NOT EXISTS search_ledger (
  tenant       TEXT NOT NULL,
  accession    TEXT NOT NULL,
  revision     INTEGER NOT NULL,
  source_rowid INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  retracted    INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (tenant, accession, revision)
);
CREATE TABLE IF NOT EXISTS search_docs (
  doc_id        INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant        TEXT NOT NULL,
  accession     TEXT NOT NULL,
  revision      INTEGER NOT NULL,
  retracted     INTEGER NOT NULL DEFAULT 0,
  title         TEXT NOT NULL DEFAULT '',
  abstract      TEXT NOT NULL DEFAULT '',
  title_tokens  INTEGER NOT NULL DEFAULT 0,
  abs_tokens    INTEGER NOT NULL DEFAULT 0,
  title_skip    TEXT,
  abstract_skip TEXT,
  indexed_at    REAL NOT NULL,
  UNIQUE (tenant, accession)
);
CREATE TABLE IF NOT EXISTS search_skips (
  skip_id    INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant     TEXT NOT NULL,
  accession  TEXT NOT NULL,
  revision   INTEGER NOT NULL,
  field      TEXT NOT NULL,
  value_type TEXT NOT NULL,
  reason     TEXT NOT NULL,
  noted_at   REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS search_terms (
  term_id INTEGER PRIMARY KEY AUTOINCREMENT,
  term    TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS search_postings (
  term_id   INTEGER NOT NULL,
  doc_id    INTEGER NOT NULL,
  field     TEXT NOT NULL,                 -- 'title' | 'abstract'
  positions TEXT NOT NULL,                 -- JSON array of token positions
  spans     TEXT NOT NULL,                 -- JSON [start,end,...] original chars
  tf        INTEGER NOT NULL,
  PRIMARY KEY (term_id, doc_id, field)
);
CREATE INDEX IF NOT EXISTS idx_sp_doc ON search_postings(doc_id);
CREATE INDEX IF NOT EXISTS idx_sp_term_field ON search_postings(term_id, field);
CREATE INDEX IF NOT EXISTS idx_docs_tenant_acc ON search_docs(tenant, accession);
"""


def connect(path):
    # isolation_level=None puts the driver in autocommit mode so *we* own
    # every BEGIN/COMMIT explicitly; this is what makes a reader's WAL
    # snapshot hold for the whole query instead of being silently restarted.
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
    # executescript issues its own COMMIT, so run the DDL outside an explicit
    # transaction, then make the metadata upsert a real durable transaction.
    conn.executescript(SCHEMA)
    with transaction(conn):
        conn.execute(
            "INSERT INTO search_meta(key, value) VALUES ('schema_version', ?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (SCHEMA_VERSION,))


def get_meta(conn, key, default=None):
    row = conn.execute("SELECT value FROM search_meta WHERE key=?",
                       (key,)).fetchone()
    return row["value"] if row else default


def set_meta(conn, key, value):
    with transaction(conn):
        conn.execute(
            "INSERT INTO search_meta(key, value) VALUES (?, ?)"
            " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value))


def get_cursor(conn, tenant):
    row = conn.execute(
        "SELECT last_rowid FROM search_cursor WHERE tenant=?",
        (tenant,)).fetchone()
    return int(row["last_rowid"]) if row else 0


# -- posting helpers ------------------------------------------------------

def encode_positions(positions):
    return json.dumps(list(positions), separators=(",", ":"))


def encode_spans(spans):
    flat = []
    for start, end in spans:
        flat.extend((start, end))
    return json.dumps(flat, separators=(",", ":"))


def decode_spans(blob):
    flat = json.loads(blob)
    return [(flat[i], flat[i + 1]) for i in range(0, len(flat), 2)]


def decode_positions(blob):
    return json.loads(blob)
