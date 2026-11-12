"""Incremental indexer: landing database -> local retrieval index.

Design mirrors the proven patterns of the quality layer but stays completely
independent (separate DB file, separate tables, no network, never invoked from
a fetch thread):

* Identity is ``(tenant, accession, revision)`` from ``result_versions``.
* Progress rule: the durable cursor is the **largest contiguous consumed
  prefix** of landing rowids for one tenant.  A rowid R is consumed when
  every landing row with rowid <= R has been applied (or matched the durable
  content-hash ledger of an already-applied identical identity) and those
  effects were committed.  "The largest rowid this chunk happened to touch"
  is NOT a valid progress value: another accession's older row may still be
  unprocessed below it.  Rows above the cursor may have been applied out of
  order, but they are never *claimed* as progress and are re-read safely.
* A per-tenant rowid watermark (``search_cursor``) makes the first build a
  full scan and every later run an incremental read of ``rowid > watermark``;
  there is NO repeated full scan of the source table.  The ledger makes such
  re-reads idempotent and also detects identities consumed in earlier runs.
* Only the newest revision present in the landing snapshot decides the
  searchable text.  The snapshot is bounded by the max rowid observed at the
  start of the run, so data appended while the run is in flight is left for
  the next update and is never falsely marked as consumed.  A late smaller
  revision never overwrites the current text; a retraction removes the
  document; an even newer non-retracted revision restores it.
* Each chunk commits postings, original text, current revision, the ledger
  entries and the new contiguous-prefix cursor in ONE transaction.  A hard
  exit mid-transaction rolls back wholesale, so a reader can never match old
  terms against a newly displayed abstract and progress never moves ahead of
  the postings.
* Non-text title/abstract values are never stringified: they are skipped with
  an explicit reason recorded in ``search_skips`` and on the document row.

Legacy repair
-------------
An older progress rule advanced the cursor to the max rowid of the group of
accessions processed by a chunk, which could jump over rows of accessions
grouped into a later, uncommitted chunk.  ``Indexer.repair`` performs one
explicit audit of the already-consumed rowid range, finds identities whose
landing row is absent from (or disagrees with) the ledger, rewinds the cursor
to just before the first of them and resumes normal incremental catch-up.
The repair journal is durable and resumable; a repeated run which finds no
gap adds no records and keeps every other tenant's index untouched.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import store
from .tokenizer import tokenize

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


@dataclass
class RepairItem:
    accession: str
    revision: int
    source_rowid: int


@dataclass
class RepairTenantReport:
    tenant: str
    audited_from: int = 0
    audited_to: int = 0
    rows_checked: int = 0
    missing_identities: int = 0
    rewind_to: Optional[int] = None
    status: str = "no_gap"  # no_gap | repaired | already_done | failed
    missing: List[RepairItem] = field(default_factory=list)


@dataclass
class RepairReport:
    tenants: List[RepairTenantReport] = field(default_factory=list)


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
        """Catch the index up with landing rows committed before this run."""
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
        """Drop ALL index contents and build again from the landing rows.

        This is the explicit full-reconciliation command for out-of-band
        rewrites of already-consumed rows; it is never the normal catch-up.
        """
        conn, landing = self._open()
        report = IndexReport()
        try:
            with store.transaction(conn):
                for table in ("search_postings", "search_terms", "search_docs",
                              "search_skips", "search_ledger",
                              "search_cursor", "search_repair",
                              "search_repair_missing"):
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

    # -- progress rule ----------------------------------------------------
    @staticmethod
    def _set_cursor(conn, tenant, rowid, now):
        """Durably record a *contiguous consumed prefix* end (caller owns txn)."""
        conn.execute(
            "INSERT INTO search_cursor(tenant, last_rowid, updated_at)"
            " VALUES (?,?,?) ON CONFLICT(tenant) DO UPDATE SET"
            " last_rowid=excluded.last_rowid, updated_at=excluded.updated_at",
            (tenant, int(rowid), now))

    def _snapshot_max(self, landing, tenant):
        return int(landing.execute(
            "SELECT COALESCE(MAX(rowid), 0) AS m FROM result_versions"
            " WHERE tenant=?", (tenant,)).fetchone()["m"])

    # -- per-tenant processing -------------------------------------------
    def _sync_tenant(self, conn, landing, tenant, full, report,
                     snapshot_max=None):
        if full:
            with store.transaction(conn):
                conn.execute(
                    "DELETE FROM search_postings WHERE doc_id IN"
                    " (SELECT doc_id FROM search_docs WHERE tenant=?)",
                    (tenant,))
                for table in ("search_docs", "search_skips",
                              "search_ledger", "search_cursor",
                              "search_repair", "search_repair_missing"):
                    conn.execute(
                        f"DELETE FROM {table} WHERE tenant=?", (tenant,))
            watermark = 0
        else:
            watermark = store.get_cursor(conn, tenant)

        # Freeze the landing view for this run.  Rows appended while the run
        # is in flight get rowids above this bound: they are left for a later
        # update and can never be claimed by the cursor this run commits.
        # Repair passes the bound recorded at audit time, so revisions
        # appended during the repair cannot be marked consumed.
        if snapshot_max is None:
            snapshot_max = self._snapshot_max(landing, tenant)
        if snapshot_max <= watermark:
            report.watermarks[tenant] = watermark
            return

        # Strictly append-only landing (the collector only INSERTs): every new
        # or late row has a rowid greater than the durable watermark, so the
        # incremental scan is bounded by the watermark and never re-reads
        # history.  The ledger still absorbs retransmitted identical rows and
        # records each identity's content hash; an out-of-band rewrite of an
        # old row is reconciled by the explicit ``repair`` audit.
        known = {
            (r["accession"], int(r["revision"])): r["content_hash"]
            for r in conn.execute(
                "SELECT accession, revision, content_hash FROM search_ledger"
                " WHERE tenant=?", (tenant,))}

        rows = landing.execute(
            "SELECT rowid AS source_rowid, tenant, accession, revision,"
            " retracted, payload FROM result_versions WHERE tenant=?"
            " AND rowid > ? AND rowid <= ? ORDER BY rowid",
            (tenant, watermark, snapshot_max)).fetchall()

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
        self._apply(conn, landing, tenant, pending, watermark, snapshot_max,
                    report)
        report.watermarks[tenant] = store.get_cursor(conn, tenant)

    def _apply(self, conn, landing, tenant, pending, watermark, snapshot_max,
               report):
        if not pending:
            # Every row in the window matches the durable ledger, i.e. its
            # identity was already fully applied.  They form a contiguous
            # consumed prefix, so the cursor may advance to the snapshot bound.
            with store.transaction(conn):
                self._set_cursor(conn, tenant, snapshot_max, self._now())
            return

        # Commit in landing-rowid order.  Each chunk is a consecutive slice of
        # the pending rows; the cursor may therefore advance to the last rowid
        # of the slice: every tenant row at or below it is either in an
        # already-committed slice or an identical ledger-known identity.
        # Grouping by accession before slicing (the legacy rule) breaks that
        # property: a group's later row can be far above an unprocessed row
        # belonging to a group placed in a later, uncommitted chunk.
        for start in range(0, len(pending), self.chunk_size):
            chunk = pending[start:start + self.chunk_size]
            self._apply_chunk(conn, landing, tenant, chunk, report)
        # Every pending row is now applied.  Any rows still between the last
        # committed chunk and the snapshot bound matched the ledger already,
        # so they too belong to the contiguous consumed prefix.
        self._advance_over_known(conn, landing, tenant, snapshot_max)

    def _advance_over_known(self, conn, landing, tenant, snapshot_max):
        """Extend the cursor across trailing rows whose identity is consumed."""
        cursor = store.get_cursor(conn, tenant)
        if cursor >= snapshot_max:
            return
        known = {
            (r["accession"], int(r["revision"])): r["content_hash"]
            for r in conn.execute(
                "SELECT accession, revision, content_hash FROM search_ledger"
                " WHERE tenant=?", (tenant,))}
        contiguous = cursor
        for row in landing.execute(
                "SELECT rowid AS source_rowid, tenant, accession, revision,"
                " retracted, payload FROM result_versions"
                " WHERE tenant=? AND rowid > ? AND rowid <= ? ORDER BY rowid",
                (tenant, cursor, snapshot_max)):
            if known.get((row["accession"], int(row["revision"])))\
                    != _content_hash(row):
                break
            contiguous = int(row["source_rowid"])
        if contiguous > cursor:
            with store.transaction(conn):
                self._set_cursor(conn, tenant, contiguous, self._now())

    def _apply_chunk(self, conn, landing, tenant, chunk, report):
        now = self._now()
        chunk_max_rowid = int(chunk[-1]["source_rowid"])
        by_accession: Dict[str, List] = {}
        for row in chunk:
            by_accession.setdefault(row["accession"], []).append(row)
        with store.transaction(conn):
            for accession, versions in by_accession.items():
                versions = sorted(versions,
                                  key=lambda r: (int(r["revision"]),
                                                 int(r["source_rowid"])))
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
                         int(row["retracted"])))  # noqa: E501

                # The newest revision *within this chunk's consumed prefix*
                # decides the text.  Bounding by the chunk end (not the whole
                # landing) keeps document content and the cursor in step: a
                # newer revision from a later, not-yet-committed chunk is
                # applied by that chunk, never silently pre-indexed.
                newest = landing.execute(
                    "SELECT revision, retracted, payload FROM result_versions"
                    " WHERE tenant=? AND accession=? AND rowid <= ?"
                    " ORDER BY revision DESC, rowid DESC LIMIT 1",
                    (tenant, accession, chunk_max_rowid)).fetchone()
                stored = conn.execute(
                    "SELECT doc_id, revision FROM search_docs"
                    " WHERE tenant=? AND accession=?",
                    (tenant, accession)).fetchone()

                if newest is None:
                    # Possible only if the identity now disappeared; the
                    # landing is append-only, so leave the current document.
                    continue
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

            # Contiguous-prefix progress: text, postings, ledger entries and
            # this cursor all commit together.
            self._set_cursor(conn, tenant, chunk_max_rowid, now)
            # Real commit boundary: a SIGKILL here rolls the whole transaction
            # back, so the reader can never match old terms against a newly
            # displayed abstract and the cursor stays at the previous chunk.
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

    # -- legacy-progress repair ------------------------------------------
    def repair(self, *, tenants=None) -> RepairReport:
        """One explicit audit of already-consumed rowids, then catch-up.

        An older indexer could commit a cursor past rows of accessions whose
        chunk never committed.  This method compares every landing row inside
        the *consumed* range with the durable ledger, records the audit scope
        and the missing identities, rewinds the cursor just before the earliest
        unresolved one, and runs ordinary incremental catch-up from there.
        The journal makes the operation resumable and idempotent; tenants
        outside the selection are never touched, and a repeated run that finds
        nothing unresolved adds no records.
        """
        conn, landing = self._open()
        report = RepairReport()
        try:
            available = [r["tenant"] for r in landing.execute(
                "SELECT DISTINCT tenant FROM result_versions ORDER BY tenant")]
            selected = tenants or available
            for tenant in selected:
                report.tenants.append(
                    self._repair_tenant(conn, landing, tenant))
            store.set_meta(conn, "last_repair_at", str(self._now()))
        finally:
            conn.close()
            landing.close()
        return report

    def _unresolved(self, conn, landing, tenant):
        """Missing/differing identities of a recorded audit still not applied."""
        items = []
        rows = conn.execute(
            "SELECT accession, revision, source_rowid FROM search_repair_missing"
            " WHERE tenant=? ORDER BY source_rowid", (tenant,)).fetchall()
        trace = {
            (r["accession"], int(r["revision"])): (
                r["content_hash"], int(r["source_rowid"]))
            for r in conn.execute(
                "SELECT accession, revision, source_rowid, content_hash"
                " FROM search_ledger WHERE tenant=?", (tenant,))}
        for item in rows:
            landing_row = landing.execute(
                "SELECT rowid AS source_rowid, tenant, accession, revision,"
                " retracted, payload FROM result_versions"
                " WHERE tenant=? AND accession=? AND revision=?",
                (tenant, item["accession"], int(item["revision"])),
            ).fetchone()
            key = (item["accession"], int(item["revision"]))
            if landing_row is None:
                # landing is append-only; treat a vanished row as resolved so
                # the journal can finish (there is nothing left to re-read).
                continue
            consumed = trace.get(key)
            if (consumed is not None
                    and consumed[1] == int(landing_row["source_rowid"])
                    and consumed[0] == _content_hash(landing_row)):
                continue
            items.append(RepairItem(item["accession"], int(item["revision"]),
                                    int(landing_row["source_rowid"])))
        return items

    def _repair_tenant(self, conn, landing, tenant):
        now = self._now()
        journal = conn.execute(
            "SELECT * FROM search_repair WHERE tenant=?", (tenant,)).fetchone()
        cursor = store.get_cursor(conn, tenant)

        if journal is not None and journal["status"] == "done":
            # A completed repair never adds records; later revisions belong
            # to a normal ``update``.
            return RepairTenantReport(
                tenant=tenant,
                audited_from=int(journal["audit_from"]),
                audited_to=int(journal["audit_to"]),
                rows_checked=0,
                missing_identities=int(journal["missing_count"]),
                rewind_to=(int(journal["rewind_to"])
                           if journal["rewind_to"] is not None else None),
                status="already_done")

        if journal is None:
            # Exactly one historical audit of the consumed range: every landing
            # row at or below the cursor is claimed consumed, so each must
            # agree with the durable ledger.
            audit_from, audit_to = 0, cursor
            rows = landing.execute(
                "SELECT rowid AS source_rowid, tenant, accession, revision,"
                " retracted, payload FROM result_versions"
                " WHERE tenant=? AND rowid <= ? ORDER BY rowid",
                (tenant, audit_to))
            trace = {
                (r["accession"], int(r["revision"])): (
                    r["content_hash"], int(r["source_rowid"]))
                for r in conn.execute(
                    "SELECT accession, revision, source_rowid, content_hash"
                    " FROM search_ledger WHERE tenant=?", (tenant,))}
            missing: List[RepairItem] = []
            seen = set()
            checked = 0
            for row in rows:
                checked += 1
                key = (row["accession"], int(row["revision"]))
                consumed = trace.get(key)
                if (consumed is not None
                        and consumed[1] == int(row["source_rowid"])
                        and consumed[0] == _content_hash(row)):
                    continue
                if key in seen:
                    continue
                seen.add(key)
                missing.append(RepairItem(row["accession"],
                                          int(row["revision"]),
                                          int(row["source_rowid"])))
            rewind = min(item.source_rowid for item in missing) - 1 \
                if missing else None
            with store.transaction(conn):
                for item in missing:
                    conn.execute(
                        "INSERT INTO search_repair_missing(tenant, accession,"
                        " revision, source_rowid) VALUES (?,?,?,?)"
                        " ON CONFLICT(tenant, accession, revision) DO NOTHING",
                        (tenant, item.accession, item.revision,
                         item.source_rowid))
                conn.execute(
                    "INSERT INTO search_repair(tenant, audit_from, audit_to,"
                    " cursor_before, rewind_to, missing_count, status,"
                    " noted_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(tenant) DO UPDATE SET"
                    " audit_from=excluded.audit_from,"
                    " audit_to=excluded.audit_to,"
                    " cursor_before=excluded.cursor_before,"
                    " rewind_to=excluded.rewind_to,"
                    " missing_count=excluded.missing_count,"
                    " status=excluded.status, updated_at=excluded.updated_at",
                    (tenant, audit_from, audit_to, cursor, rewind,
                     len(missing), "audited" if missing else "done",
                     now, now))
                if rewind is not None:
                    # Rewind in the same transaction as the journal: a hard
                    # exit records both or neither.  Never move forward.
                    self._set_cursor(conn, tenant, min(rewind, cursor), now)
                if self.crash_barrier is not None:
                    self.crash_barrier("repair_audited_txn")
            if not missing:
                return RepairTenantReport(
                    tenant=tenant, audited_from=audit_from, audited_to=audit_to,
                    rows_checked=checked, missing_identities=0,
                    rewind_to=None, status="no_gap")
            audit_from0, audit_to0, rewind0, count0 = (
                audit_from, audit_to, rewind, len(missing))
            bound = audit_to
        else:
            audit_from0 = int(journal["audit_from"])
            audit_to0 = int(journal["audit_to"])
            rewind0 = (int(journal["rewind_to"])
                       if journal["rewind_to"] is not None else None)
            count0 = int(journal["missing_count"])
            bound = audit_to0

        unresolved = self._unresolved(conn, landing, tenant)
        if not unresolved:
            with store.transaction(conn):
                conn.execute(
                    "UPDATE search_repair SET status='done', updated_at=?"
                    " WHERE tenant=?", (now, tenant))
                if self.crash_barrier is not None:
                    self.crash_barrier("repair_done_txn")
            return RepairTenantReport(
                tenant=tenant, audited_from=audit_from0, audited_to=audit_to0,
                rows_checked=0, missing_identities=count0, rewind_to=rewind0,
                status="repaired")

        target = min(item.source_rowid for item in unresolved) - 1
        if cursor > target:
            # Resume: a normal catch-up may have advanced past the gap after
            # an interrupted repair, so rewind again to the durable target.
            with store.transaction(conn):
                self._set_cursor(conn, tenant, min(target, cursor), now)

        # Ordinary incremental catch-up from the rewound cursor.  It reads only
        # rows above the cursor; the frozen snapshot leaves concurrently
        # appended revisions to the next explicit update.
        index_report = IndexReport()
        self._sync_tenant(conn, landing, tenant, False, index_report,
                          snapshot_max=bound)

        still = self._unresolved(conn, landing, tenant)
        if still:
            # The last chunk could have been interrupted; leave the journal
            # open and the missing rows durable for the next repair run.
            return RepairTenantReport(
                tenant=tenant, audited_from=audit_from0, audited_to=audit_to0,
                rows_checked=index_report.scanned,
                missing_identities=len(still),
                rewind_to=target, status="failed", missing=still)
        with store.transaction(conn):
            conn.execute(
                "UPDATE search_repair SET status='done', updated_at=?"
                " WHERE tenant=?", (now, tenant))
            if self.crash_barrier is not None:
                self.crash_barrier("repair_done_txn")
        return RepairTenantReport(
            tenant=tenant, audited_from=audit_from0, audited_to=audit_to0,
            rows_checked=index_report.scanned,
            missing_identities=count0, rewind_to=target,
            status="repaired", missing=unresolved)
