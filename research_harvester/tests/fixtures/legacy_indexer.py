"""FROZEN copy of the pre-fix indexer, used only to produce a legacy-broken
index in acceptance tests.  Do not import from production code.

It reproduces the old progress rule: group the batch by accession, commit
chunks, and advance the cursor to the max rowid touched by that chunk.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from search import store
from search.tokenizer import tokenize

CHUNK = 500


def make_crash_barrier():
    """Test-only hard-exit barrier, inert unless RH_SEARCH_CRASH_POINT is set.

    Production code never pays a cost.  When the environment variable names a
    barrier point, the process writes the marker file and blocks (then the
    driver sends a real SIGKILL); the points sit inside the write transaction
    before its commit, proving the uncommitted index changes roll back and the
    watermark never advances without the postings.
    """
    point = os.environ.get("RH_SEARCH_CRASH_POINT")
    if not point:
        return None
    marker = os.environ.get("RH_SEARCH_CRASH_MARKER")
    wait_file = os.environ.get("RH_SEARCH_CRASH_WAIT_FILE")

    def barrier(name):
        if name != point:
            return
        if marker:
            with open(marker, "w", encoding="utf-8") as fh:
                fh.write(name)
                fh.flush()
                os.fsync(fh.fileno())
        if wait_file:
            deadline = time.time() + 120
            while time.time() < deadline and os.path.exists(wait_file):
                time.sleep(0.01)
        os._exit(137)

    return barrier


class LandingMissing(Exception):
    """The landing database has no collector tables."""


@dataclass
class DocText:
    title: str = ""
    abstract: str = ""
    title_skip: Optional[str] = None
    abstract_skip: Optional[str] = None


@dataclass
class IndexReport:
    scanned: int = 0
    new_identities: int = 0
    changed_identities: int = 0
    documents_upserted: int = 0
    documents_removed: int = 0
    skips: List[dict] = field(default_factory=list)
    tenants: List[str] = field(default_factory=list)
    watermarks: Dict[str, int] = field(default_factory=dict)


def _content_hash(row) -> str:
    blob = json.dumps(
        [row["tenant"], row["accession"], int(row["revision"]),
         int(row["retracted"]), row["payload"]],
        sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _field_text(payload, name) -> tuple:
    """Return (text_or_None, skip_reason_or_None) for one payload field.

    Objects and other non-text values are NEVER coerced to strings.
    A missing/None field is simply empty (searchable via the other field).
    """
    if not isinstance(payload, dict):
        return None, f"payload 不是 JSON 对象，{name} 不可索引"
    if name not in payload or payload[name] is None:
        return None, None
    value = payload[name]
    if not isinstance(value, str):
        kind = type(value).__name__
        return None, (f"{name} 不是文本（实际类型 {kind}），按规则跳过，"
                      "不将对象/数值转成字符串混入索引")
    return value, None


def extract_text(payload_text: Optional[str]) -> DocText:
    """Extract searchable title/abstract with explicit non-text reasons."""
    out = DocText()
    if payload_text is None:
        # retracted versions carry no payload; callers retract the document
        out.title_skip = "payload 缺失（撤回版本无正文）"
        return out
    try:
        payload = json.loads(payload_text)
    except (json.JSONDecodeError, TypeError) as exc:
        out.title_skip = f"payload 不是合法 JSON：{exc}"
        out.abstract_skip = out.title_skip
        return out
    for name in ("title", "abstract"):
        text, reason = _field_text(payload, name)
        if reason is not None:
            if name == "title":
                out.title_skip = reason
            else:
                out.abstract_skip = reason
        elif text is not None:
            if name == "title":
                out.title = text
            else:
                out.abstract = text
    return out


class Indexer:
    def __init__(self, index_path, landing_path, *, now=time.time,
                 crash_barrier=None, chunk_size=CHUNK):
        self.index_path = index_path
        self.landing_path = landing_path
        self._now = now
        # test-only barrier; defaults to the env-driven one, which is inert
        # unless RH_SEARCH_CRASH_POINT is set
        self.crash_barrier = (crash_barrier if crash_barrier is not None
                              else make_crash_barrier())
        # production keeps the module default; tests shrink it to force
        # multiple commit chunks on a small corpus
        self.chunk_size = int(chunk_size)

    def _open(self):
        landing = store.connect_landing(self.landing_path)
        names = {r["name"] for r in landing.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "result_versions" not in names:
            landing.close()
            raise LandingMissing(
                "landing table result_versions not found; run the harvester "
                "first (the search layer never fetches or invents data)")
        conn = store.connect(self.index_path)
        store.init(conn)
        return conn, landing

    # -- public commands -------------------------------------------------
    def sync(self, *, tenants=None, full=False) -> IndexReport:
        """Catch the index up with every landing row written so far."""
        conn, landing = self._open()
        report = IndexReport()
        try:
            available = [r["tenant"] for r in landing.execute(
                "SELECT DISTINCT tenant FROM result_versions ORDER BY tenant")]
            selected = tenants or available
            report.tenants = selected
            for tenant in selected:
                self._sync_tenant(conn, landing, tenant, full, report)
            store.set_meta(conn, "last_sync_at", str(self._now()))
        finally:
            conn.close()
            landing.close()
        return report

    def rebuild(self, *, tenants=None) -> IndexReport:
        """Drop all index contents and build again from the landing rows."""
        conn, landing = self._open()
        report = IndexReport()
        try:
            with store.transaction(conn):
                for table in ("search_postings", "search_terms", "search_docs",
                              "search_skips", "search_ledger",
                              "search_cursor"):
                    conn.execute(f"DELETE FROM {table}")
            available = [r["tenant"] for r in landing.execute(
                "SELECT DISTINCT tenant FROM result_versions ORDER BY tenant")]
            selected = tenants or available
            report.tenants = selected
            for tenant in selected:
                self._sync_tenant(conn, landing, tenant, True, report)
            store.set_meta(conn, "last_sync_at", str(self._now()))
        finally:
            conn.close()
            landing.close()
        return report

    # -- per-tenant processing -------------------------------------------
    def _sync_tenant(self, conn, landing, tenant, full, report):
        if full:
            with store.transaction(conn):
                conn.execute(
                    "DELETE FROM search_postings WHERE doc_id IN"
                    " (SELECT doc_id FROM search_docs WHERE tenant=?)",
                    (tenant,))
                for table in ("search_docs", "search_skips",
                              "search_ledger", "search_cursor"):
                    conn.execute(
                        f"DELETE FROM {table} WHERE tenant=?", (tenant,))
            watermark = 0
        else:
            watermark = store.get_cursor(conn, tenant)

        max_row = landing.execute(
            "SELECT COALESCE(MAX(rowid), 0) AS m FROM result_versions"
            " WHERE tenant=?", (tenant,)).fetchone()["m"]

        # Strictly append-only landing (the collector only INSERTs): every new
        # or late row has a rowid greater than the durable watermark, so the
        # incremental scan is bounded by the watermark and never re-reads
        # history.  The ledger still absorbs retransmitted identical rows and
        # records each identity's content hash; an out-of-band rewrite of an
        # old row is reconciled by the explicit ``rebuild`` command.
        known = {
            (r["accession"], int(r["revision"])): r["content_hash"]
            for r in conn.execute(
                "SELECT accession, revision, content_hash FROM search_ledger"
                " WHERE tenant=?", (tenant,))}

        rows = landing.execute(
            "SELECT rowid AS source_rowid, tenant, accession, revision,"
            " retracted, payload FROM result_versions WHERE tenant=?"
            " AND rowid > ? ORDER BY rowid",
            (tenant, watermark)).fetchall()

        pending = []
        for row in rows:
            digest = _content_hash(row)
            key = (row["accession"], int(row["revision"]))
            if known.get(key) == digest:
                continue
            if key in known:
                report.changed_identities += 1
            else:
                report.new_identities += 1
            pending.append(row)

        report.scanned += len(rows)
        self._apply(conn, landing, tenant, pending, max_row, report)
        report.watermarks[tenant] = store.get_cursor(conn, tenant)

    def _apply(self, conn, landing, tenant, rows, max_row, report):
        # Group rows by accession; within one chunk, determine the newest
        # revision actually present, then combine with the stored current.
        by_accession: Dict[str, List] = {}
        for row in rows:
            by_accession.setdefault(row["accession"], []).append(row)

        items = list(by_accession.items())
        for start in range(0, len(items), self.chunk_size):
            group = items[start:start + self.chunk_size]
            self._apply_chunk(conn, landing, tenant, group, max_row, report)
        if not by_accession:
            # nothing new: still record the cursor so later runs start here
            with store.transaction(conn):
                conn.execute(
                    "INSERT INTO search_cursor(tenant, last_rowid, updated_at)"
                    " VALUES (?,?,?) ON CONFLICT(tenant) DO UPDATE SET"
                    " last_rowid=excluded.last_rowid, updated_at=excluded.updated_at",
                    (tenant, int(max_row), self._now()))

    def _apply_chunk(self, conn, landing, tenant, group, max_row, report):
        now = self._now()
        # The watermark advances only to the max rowid actually processed in
        # THIS chunk.  Advancing it to the global max inside an early chunk
        # would let a crash between chunks permanently skip the remaining
        # rows: any row below a committed watermark is never re-scanned.
        # Re-scanning a committed chunk after a crash is harmless because the
        # content-hash ledger makes identical identities idempotent.
        chunk_max_row = max(
            int(r["source_rowid"]) for _, versions in group
            for r in versions)
        with store.transaction(conn):
            for accession, versions in group:
                versions = sorted(versions,
                                  key=lambda r: (int(r["revision"]),
                                                 r["source_rowid"]))
                # durable ledger updates for every new/changed identity
                for row in versions:
                    digest = _content_hash(row)
                    conn.execute(
                        "INSERT INTO search_ledger(tenant, accession, revision,"
                        " source_rowid, content_hash, retracted)"
                        " VALUES (?,?,?,?,?,?)"
                        " ON CONFLICT(tenant, accession, revision) DO UPDATE SET"
                        " source_rowid=excluded.source_rowid,"
                        " content_hash=excluded.content_hash,"
                        " retracted=excluded.retracted",
                        (tenant, accession, int(row["revision"]),
                         row["source_rowid"], digest,
                         int(row["retracted"])))

                # The newest revision of the whole landing table decides the
                # current document -- a late smaller revision can never win,
                # and the payload must come from landing (the ledger stores
                # only hashes).
                newest = landing.execute(
                    "SELECT revision, retracted, payload FROM result_versions"
                    " WHERE tenant=? AND accession=?"
                    " ORDER BY revision DESC, rowid DESC LIMIT 1",
                    (tenant, accession)).fetchone()
                stored = conn.execute(
                    "SELECT doc_id, revision FROM search_docs"
                    " WHERE tenant=? AND accession=?",
                    (tenant, accession)).fetchone()

                if int(newest["retracted"]):
                    if stored is not None:
                        self._remove_doc(conn, stored["doc_id"])
                        report.documents_removed += 1
                    continue

                incoming_rev = int(newest["revision"])
                if stored is not None and int(stored["revision"]) > incoming_rev:
                    # late older revision: current text stays untouched
                    continue

                text = extract_text(newest["payload"])
                if stored is not None:
                    self._remove_doc(conn, stored["doc_id"])
                self._insert_doc(conn, tenant, accession, incoming_rev,
                                 text, now, report)
                report.documents_upserted += 1

            conn.execute(
                "INSERT INTO search_cursor(tenant, last_rowid, updated_at)"
                " VALUES (?,?,?) ON CONFLICT(tenant) DO UPDATE SET"
                " last_rowid=excluded.last_rowid, updated_at=excluded.updated_at",
                (tenant, int(chunk_max_row), now))
            # Real commit boundary: postings, document text, current revision
            # and this chunk's watermark are all written, not yet committed.
            # A SIGKILL here rolls the whole transaction back, so the reader
            # can never match old terms against a newly displayed abstract;
            # the watermark stays at the previous chunk and a resumed run
            # re-reads this chunk (ledger makes the re-read idempotent).
            if self.crash_barrier is not None:
                self.crash_barrier("apply_chunk_txn")

    # -- document / posting maintenance ----------------------------------
    def _remove_doc(self, conn, doc_id):
        conn.execute("DELETE FROM search_postings WHERE doc_id=?", (doc_id,))
        conn.execute("DELETE FROM search_docs WHERE doc_id=?", (doc_id,))
        # dictionary terms are retained; harmless and keeps updates cheap

    def _insert_doc(self, conn, tenant, accession, revision, text: DocText,
                    now, report):
        cur = conn.execute(
            "INSERT INTO search_docs(tenant, accession, revision, retracted,"
            " title, abstract, title_tokens, abs_tokens, title_skip,"
            " abstract_skip, indexed_at) VALUES (?,?,?,0,?,?,?,?,?,?,?)",
            (tenant, accession, revision, text.title, text.abstract, 0, 0,
             text.title_skip, text.abstract_skip, now))
        doc_id = cur.lastrowid
        total_title = self._index_field(conn, doc_id, "title", text.title)
        total_abs = self._index_field(conn, doc_id, "abstract",
                                      text.abstract)
        conn.execute(
            "UPDATE search_docs SET title_tokens=?, abs_tokens=? WHERE doc_id=?",
            (total_title, total_abs, doc_id))
        for field_name, reason in (("title", text.title_skip),
                                   ("abstract", text.abstract_skip)):
            if reason is not None:
                conn.execute(
                    "INSERT INTO search_skips(tenant, accession, revision,"
                    " field, value_type, reason, noted_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (tenant, accession, revision, field_name,
                     "non-text-or-missing", reason, now))
                report.skips.append({"tenant": tenant, "accession": accession,
                                     "field": field_name, "reason": reason})

    def _index_field(self, conn, doc_id, field_name, text):
        tokens = tokenize(text)
        # group occurrences by term
        grouped: Dict[str, List] = {}
        for tok in tokens:
            grouped.setdefault(tok.term, []).append(tok)
        for term, occ in grouped.items():
            term_id = self._term_id(conn, term)
            positions = [t.position for t in occ]
            spans = [(t.start, t.end) for t in occ]
            conn.execute(
                "INSERT INTO search_postings(term_id, doc_id, field,"
                " positions, spans, tf) VALUES (?,?,?,?,?,?)",
                (term_id, doc_id, field_name,
                 store.encode_positions(positions),
                 store.encode_spans(spans), len(occ)))
        return len(tokens)

    @staticmethod
    def _term_id(conn, term):
        conn.execute(
            "INSERT INTO search_terms(term) VALUES (?) ON CONFLICT(term)"
            " DO NOTHING", (term,))
        row = conn.execute(
            "SELECT term_id FROM search_terms WHERE term=?", (term,)).fetchone()
        return row["term_id"]
