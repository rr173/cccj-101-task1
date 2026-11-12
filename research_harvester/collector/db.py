"""SQLite persistence for the harvester.

Tables
------
raw_pages        every HTTP response body (200 pages and error responses)
result_versions  normalized record versions; PRIMARY KEY (tenant, accession,
                 revision) is the *version identity* used for dedup, so
                 retransmitted confirmed pages are absorbed by INSERT OR IGNORE
scan_state       per-tenant pagination progress of the in-flight round:
                 the fixed scan upper bound plus the last confirmed composite
                 boundary (modified_at, accession)
watermarks       per-tenant high watermark, advanced only when a whole round
                 has been confirmed

Atomicity rule: a page's raw body, its normalized versions and the pagination
progress are written in ONE transaction (commit_page). The round watermark is
written in the same transaction as the final page.
"""
from __future__ import annotations

import json
import sqlite3
import time

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_pages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  tenant TEXT NOT NULL,
  scan_id TEXT,
  url TEXT,
  status INTEGER NOT NULL,
  scan_until REAL,
  body TEXT,
  fetched_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS result_versions (
  tenant TEXT NOT NULL,
  accession TEXT NOT NULL,
  revision INTEGER NOT NULL,
  modified_at REAL NOT NULL,
  retracted INTEGER NOT NULL DEFAULT 0,
  payload TEXT,
  raw_page_id INTEGER REFERENCES raw_pages(id),
  PRIMARY KEY (tenant, accession, revision)
);
CREATE INDEX IF NOT EXISTS idx_versions_tenant_modified
  ON result_versions(tenant, modified_at);
CREATE TABLE IF NOT EXISTS scan_state (
  tenant TEXT PRIMARY KEY,
  scan_id TEXT,
  scan_until REAL,
  confirmed_modified_at REAL,
  confirmed_accession TEXT,
  pages_confirmed INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  round_lower REAL,
  updated_at REAL
);
CREATE TABLE IF NOT EXISTS watermarks (
  tenant TEXT PRIMARY KEY,
  high_modified_at REAL,
  origin_modified_at REAL,
  rounds_completed INTEGER NOT NULL DEFAULT 0
);
"""


def connect(path):
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init(conn):
    with conn:
        conn.executescript(SCHEMA)


def begin_round(conn, tenant, round_lower):
    """Mark a fresh round as in_progress (round_lower = lower bound used)."""
    with conn:
        conn.execute(
            "INSERT INTO scan_state(tenant, scan_id, scan_until,"
            " confirmed_modified_at, confirmed_accession, pages_confirmed,"
            " status, round_lower, updated_at)"
            " VALUES (?, NULL, NULL, NULL, NULL, 0, 'in_progress', ?, ?)"
            " ON CONFLICT(tenant) DO UPDATE SET scan_id=NULL, scan_until=NULL,"
            " confirmed_modified_at=NULL, confirmed_accession=NULL,"
            " pages_confirmed=0, status='in_progress',"
            " round_lower=excluded.round_lower, updated_at=excluded.updated_at",
            (tenant, round_lower, time.time()))


def commit_page(conn, *, tenant, scan_id, url, status, body, items,
                scan_until, round_done, round_lower):
    """Atomically store one page: raw body + versions + pagination progress.

    When round_done is true the round high watermark is advanced in the same
    transaction. Returns the raw_pages row id.
    """
    now = time.time()
    with conn:
        cur = conn.execute(
            "INSERT INTO raw_pages(tenant, scan_id, url, status, scan_until,"
            " body, fetched_at) VALUES (?,?,?,?,?,?,?)",
            (tenant, scan_id, url, status, scan_until, body, now))
        raw_id = cur.lastrowid
        for it in items:
            conn.execute(
                "INSERT OR IGNORE INTO result_versions"
                " (tenant, accession, revision, modified_at, retracted,"
                "  payload, raw_page_id) VALUES (?,?,?,?,?,?,?)",
                (tenant, it["accession"], int(it["revision"]),
                 float(it["modified_at"]), 1 if it.get("retracted") else 0,
                 json.dumps(it.get("payload"), sort_keys=True)
                 if it.get("payload") is not None else None,
                 raw_id))
        if items:
            last = items[-1]
            conn.execute(
                "INSERT INTO scan_state(tenant, scan_id, scan_until,"
                " confirmed_modified_at, confirmed_accession, pages_confirmed,"
                " status, round_lower, updated_at)"
                " VALUES (?,?,?,?,?,1,'in_progress',?,?)"
                " ON CONFLICT(tenant) DO UPDATE SET scan_id=excluded.scan_id,"
                " scan_until=excluded.scan_until,"
                " confirmed_modified_at=excluded.confirmed_modified_at,"
                " confirmed_accession=excluded.confirmed_accession,"
                " pages_confirmed=scan_state.pages_confirmed+1,"
                " status='in_progress', updated_at=excluded.updated_at",
                (tenant, scan_id, scan_until, float(last["modified_at"]),
                 last["accession"], round_lower, now))
        else:
            conn.execute(
                "INSERT INTO scan_state(tenant, scan_id, scan_until,"
                " pages_confirmed, status, round_lower, updated_at)"
                " VALUES (?,?,?,1,'in_progress',?,?)"
                " ON CONFLICT(tenant) DO UPDATE SET scan_id=excluded.scan_id,"
                " scan_until=COALESCE(excluded.scan_until, scan_state.scan_until),"
                " pages_confirmed=scan_state.pages_confirmed+1,"
                " updated_at=excluded.updated_at",
                (tenant, scan_id, scan_until, round_lower, now))
        if round_done:
            conn.execute(
                "UPDATE scan_state SET status='done', updated_at=?"
                " WHERE tenant=?", (now, tenant))
            conn.execute(
                "INSERT INTO watermarks(tenant, high_modified_at,"
                " origin_modified_at, rounds_completed) VALUES (?,?,?,1)"
                " ON CONFLICT(tenant) DO UPDATE SET"
                " high_modified_at=excluded.high_modified_at,"
                " rounds_completed=watermarks.rounds_completed+1",
                (tenant, scan_until, round_lower))
    return raw_id


def record_error(conn, tenant, url, status, body):
    """Store a non-200 response for audit; never touches pagination progress."""
    with conn:
        conn.execute(
            "INSERT INTO raw_pages(tenant, scan_id, url, status, scan_until,"
            " body, fetched_at) VALUES (?,NULL,?,?,NULL,?,?)",
            (tenant, url, status, (body or "")[:4000], time.time()))


def load_scan_state(conn, tenant):
    return conn.execute(
        "SELECT * FROM scan_state WHERE tenant=?", (tenant,)).fetchone()


def load_watermark(conn, tenant):
    return conn.execute(
        "SELECT * FROM watermarks WHERE tenant=?", (tenant,)).fetchone()


def report(conn, tenant):
    wm = load_watermark(conn, tenant)
    agg = conn.execute(
        "SELECT COUNT(*) AS versions, COUNT(DISTINCT accession) AS accessions,"
        " COALESCE(SUM(retracted),0) AS tombstones"
        " FROM result_versions WHERE tenant=?", (tenant,)).fetchone()
    pages = conn.execute(
        "SELECT COUNT(*) AS c FROM raw_pages WHERE tenant=? AND status=200",
        (tenant,)).fetchone()["c"]
    return {
        "tenant": tenant,
        "confirmed_interval":
            [wm["origin_modified_at"], wm["high_modified_at"]] if wm else None,
        "rounds_completed": wm["rounds_completed"] if wm else 0,
        "versions": agg["versions"],
        "accessions": agg["accessions"],
        "tombstones": agg["tombstones"],
        "pages": pages,
    }
