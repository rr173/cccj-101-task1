"""Incremental builder: landing database -> collaboration network.

Mirrors the proven patterns of the search indexer while staying completely
independent (own DB file, own tables, no network, never invoked from a fetch
thread):

* Identity is ``(tenant, accession, revision)`` from ``result_versions``;
  the paper key is ``(tenant, accession)`` and every tenant computes its own
  network.
* Progress rule: the durable cursor is the largest contiguous consumed
  prefix of landing rowids.  The first run is a full scan; later runs read
  only ``rowid > watermark`` -- never a re-scan of all papers.  The ledger
  absorbs retransmitted identical identities, so the same revision of the
  same paper can never contribute twice, and a fully caught-up run performs
  no business writes at all.
* Contribution replacement: a paper's contribution is the set of undirected
  edges between the DISTINCT institution codes its authors' affiliations
  resolve to (exact match against the bound mapping; duplicates and aliases
  collapse, same-institution authors never create self-loops or inflate
  weights).  Applying a higher revision deletes the old contribution and
  inserts the new one IN THE SAME TRANSACTION, together with the edge-weight
  updates, the ledger entry and the cursor advance.  A process killed
  between "delete old" and "add new" is rolled back wholesale: no negative
  weights, no empty-shell edges, no double-counted papers.
* Revision semantics: only a revision HIGHER than the adopted one replaces
  the contribution.  A retraction removes it; a later higher non-retracted
  revision restores it; a late lower revision is recorded in the ledger (so
  progress can advance) but never overrides the adopted contribution.
* Unmapped affiliation strings are recorded as diagnostics per adopted
  revision.  They form no node and no edge, and different unrecognized
  names are never merged into a shared node.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import store
from .mapping import Mapping

CHUNK = 500


def make_crash_barrier():
    """Test-only hard-exit barrier, inert unless RH_COLLAB_CRASH_POINT is set.

    The named points sit inside the chunk write transaction before its
    commit, proving that an interrupted contribution replacement rolls back
    atomically.  Production code never pays a cost.
    """
    point = os.environ.get("RH_COLLAB_CRASH_POINT")
    if not point:
        return None
    marker = os.environ.get("RH_COLLAB_CRASH_MARKER")
    wait_file = os.environ.get("RH_COLLAB_CRASH_WAIT_FILE")

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


class MappingMismatch(Exception):
    """The network file is bound to a different mapping content digest."""


@dataclass
class BuildReport:
    scanned: int = 0
    new_identities: int = 0
    changed_identities: int = 0
    applied: int = 0                # revisions that replaced a contribution
    ledger_only: int = 0            # late lower revisions, recorded only
    unmapped_names: int = 0         # raw strings with no mapping entry
    tenants: List[str] = field(default_factory=list)
    watermarks: Dict[str, int] = field(default_factory=dict)


def _content_hash(row) -> str:
    blob = json.dumps(
        [row["tenant"], row["accession"], int(row["revision"]),
         int(row["retracted"]), row["payload"]],
        sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def extract_affiliations(payload_text: Optional[str], mapping: Mapping):
    """Resolve one revision's affiliations against the mapping.

    Returns ``(org_map, unmapped)`` where ``org_map`` maps an institution
    code to the sorted list of DISTINCT raw affiliation strings that
    resolved to it, and ``unmapped`` is the sorted list of raw strings with
    no mapping entry.  Malformed structures (non-list authors, non-object
    entries, non-string affiliations) are ignored defensively -- they are
    landing-data issues, not collaboration evidence, and are never coerced.
    """
    org_map: Dict[str, set] = {}
    unmapped = set()
    if payload_text is None:
        return org_map, []
    try:
        payload = json.loads(payload_text)
    except (json.JSONDecodeError, TypeError):
        return org_map, []
    if not isinstance(payload, dict):
        return org_map, []
    authors = payload.get("authors")
    if not isinstance(authors, list):
        return org_map, []
    for author in authors:
        if not isinstance(author, dict):
            continue
        affiliations = author.get("affiliations")
        if not isinstance(affiliations, list):
            continue
        for raw in affiliations:
            if not isinstance(raw, str) or not raw:
                continue
            code = mapping.resolve(raw)
            if code is None:
                unmapped.add(raw)
            else:
                org_map.setdefault(code, set()).add(raw)
    return ({code: sorted(raws) for code, raws in org_map.items()},
            sorted(unmapped))


class Builder:
    def __init__(self, net_path, landing_path, mapping: Mapping, *,
                 now=time.time, crash_barrier=None, chunk_size=CHUNK):
        self.net_path = net_path
        self.landing_path = landing_path
        self.mapping = mapping
        self._now = now
        self.crash_barrier = (crash_barrier if crash_barrier is not None
                              else make_crash_barrier())
        self.chunk_size = int(chunk_size)

    # -- setup -------------------------------------------------------------
    def _open(self):
        landing = store.connect_landing(self.landing_path)
        names = {r["name"] for r in landing.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "result_versions" not in names:
            landing.close()
            raise LandingMissing(
                "landing table result_versions not found; run the harvester "
                "first (the collab layer never fetches or invents data)")
        conn = store.connect(self.net_path)
        store.init(conn)
        self._bind_mapping(conn)
        return conn, landing

    def _bind_mapping(self, conn):
        """Bind the network file to the mapping content digest on first use.

        Every later run must present a mapping with the SAME digest; a
        mismatch refuses the operation so two affiliation standards are
        never silently mixed in one network file.
        """
        bound = store.get_meta(conn, "mapping_digest")
        if bound is None:
            with store.transaction(conn):
                conn.execute(
                    "INSERT INTO collab_meta(key, value)"
                    " VALUES ('mapping_digest', ?)", (self.mapping.digest,))
                conn.execute(
                    "INSERT INTO collab_meta(key, value)"
                    " VALUES ('mapping_version', ?)", (self.mapping.version,))
                conn.execute("DELETE FROM collab_inst")
                for inst in self.mapping.institutions:
                    conn.execute(
                        "INSERT INTO collab_inst(code, name, aliases)"
                        " VALUES (?,?,?)",
                        (inst.code, inst.name,
                         json.dumps(sorted(set(inst.aliases)),
                                    ensure_ascii=False)))
            return
        if bound != self.mapping.digest:
            raise MappingMismatch(
                "对照表内容摘要不匹配：本网络文件绑定的是另一份机构对照表"
                f"（绑定摘要 {bound[:12]}…，当前提供 "
                f"{self.mapping.digest[:12]}…）。为避免混用两套单位口径，"
                "已拒绝本次操作；请为新的对照表另建一个网络文件。")

    # -- public commands ---------------------------------------------------
    def sync(self, *, tenants=None, full=False) -> BuildReport:
        """Catch the network up with landing rows committed before this run."""
        conn, landing = self._open()
        report = BuildReport()
        try:
            available = [r["tenant"] for r in landing.execute(
                "SELECT DISTINCT tenant FROM result_versions ORDER BY tenant")]
            selected = tenants or available
            report.tenants = selected
            for tenant in selected:
                self._sync_tenant(conn, landing, tenant, full, report)
            if report.applied or report.ledger_only:
                self._set_meta(conn, "last_sync_at", str(self._now()))
        finally:
            conn.close()
            landing.close()
        return report

    def rebuild(self, *, tenants=None) -> BuildReport:
        """Drop ALL network contents and build again from the landing rows."""
        conn, landing = self._open()
        report = BuildReport()
        try:
            with store.transaction(conn):
                for table in ("collab_contrib", "collab_edges",
                              "collab_paper_orgs", "collab_unmapped",
                              "collab_papers", "collab_ledger",
                              "collab_cursor"):
                    conn.execute(f"DELETE FROM {table}")
            available = [r["tenant"] for r in landing.execute(
                "SELECT DISTINCT tenant FROM result_versions ORDER BY tenant")]
            selected = tenants or available
            report.tenants = selected
            for tenant in selected:
                self._sync_tenant(conn, landing, tenant, True, report)
            if report.applied or report.ledger_only:
                self._set_meta(conn, "last_sync_at", str(self._now()))
        finally:
            conn.close()
            landing.close()
        return report

    @staticmethod
    def _set_meta(conn, key, value):
        with store.transaction(conn):
            conn.execute(
                "INSERT INTO collab_meta(key, value) VALUES (?, ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value))

    # -- progress rule -------------------------------------------------------
    def _set_cursor(self, conn, tenant, rowid):
        conn.execute(
            "INSERT INTO collab_cursor(tenant, last_rowid, updated_at)"
            " VALUES (?,?,?) ON CONFLICT(tenant) DO UPDATE SET"
            " last_rowid=excluded.last_rowid, updated_at=excluded.updated_at",
            (tenant, int(rowid), self._now()))

    @staticmethod
    def _snapshot_max(landing, tenant):
        return int(landing.execute(
            "SELECT COALESCE(MAX(rowid), 0) AS m FROM result_versions"
            " WHERE tenant=?", (tenant,)).fetchone()["m"])

    # -- per-tenant processing ----------------------------------------------
    def _sync_tenant(self, conn, landing, tenant, full, report):
        if full:
            with store.transaction(conn):
                for table in ("collab_contrib", "collab_edges",
                              "collab_paper_orgs", "collab_unmapped",
                              "collab_papers", "collab_ledger",
                              "collab_cursor"):
                    conn.execute(f"DELETE FROM {table} WHERE tenant=?",
                                 (tenant,))
            watermark = 0
        else:
            watermark = store.get_cursor(conn, tenant)

        # Freeze the landing view for this run; rows appended while it runs
        # get rowids above the bound and are left for the next update.
        snapshot_max = self._snapshot_max(landing, tenant)
        if snapshot_max <= watermark:
            report.watermarks[tenant] = watermark
            return

        known = {
            (r["accession"], int(r["revision"])): r["content_hash"]
            for r in conn.execute(
                "SELECT accession, revision, content_hash FROM collab_ledger"
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
        if not pending:
            # Every row in the window matched the durable ledger: a fully
            # caught-up tenant.  Advance the cursor only -- no other writes.
            with store.transaction(conn):
                self._set_cursor(conn, tenant, snapshot_max)
            report.watermarks[tenant] = store.get_cursor(conn, tenant)
            return

        for start in range(0, len(pending), self.chunk_size):
            self._apply_chunk(conn, tenant, pending[start:start + self.chunk_size],
                              report)
        report.watermarks[tenant] = store.get_cursor(conn, tenant)

    # -- chunk application (ONE transaction per chunk) -----------------------
    def _apply_chunk(self, conn, tenant, chunk, report):
        chunk_max_rowid = int(chunk[-1]["source_rowid"])
        by_accession: Dict[str, List] = {}
        for row in chunk:
            by_accession.setdefault(row["accession"], []).append(row)
        with store.transaction(conn):
            replaced = False
            for accession, versions in by_accession.items():
                versions = sorted(versions,
                                  key=lambda r: (int(r["revision"]),
                                                 int(r["source_rowid"])))
                for row in versions:
                    digest = _content_hash(row)
                    conn.execute(
                        "INSERT INTO collab_ledger(tenant, accession, revision,"
                        " source_rowid, content_hash, retracted)"
                        " VALUES (?,?,?,?,?,?)"
                        " ON CONFLICT(tenant, accession, revision) DO UPDATE SET"
                        " source_rowid=excluded.source_rowid,"
                        " content_hash=excluded.content_hash,"
                        " retracted=excluded.retracted",
                        (tenant, accession, int(row["revision"]),
                         row["source_rowid"], digest, int(row["retracted"])))

                    adopted = conn.execute(
                        "SELECT revision FROM collab_papers"
                        " WHERE tenant=? AND accession=?",
                        (tenant, accession)).fetchone()
                    incoming = int(row["revision"])
                    if adopted is not None and incoming < int(adopted["revision"]):
                        # Late lower revision: ledger records it as consumed,
                        # the adopted contribution is never overridden.
                        report.ledger_only += 1
                        continue
                    # Note: identical retransmissions never reach this point
                    # (absorbed by the ledger scan); an incoming revision
                    # equal to the adopted one means an out-of-band content
                    # change, which the replacement below reconciles.
                    had_old = self._replace_contribution(
                        conn, tenant, accession, incoming,
                        int(row["retracted"]), row["payload"], report)
                    replaced = replaced or had_old
                    report.applied += 1

            # Contiguous-prefix progress commits together with the data.
            self._set_cursor(conn, tenant, chunk_max_rowid)
            # Real commit boundary: a SIGKILL here rolls the whole
            # transaction back -- no partial contribution replacement.
            if self.crash_barrier is not None:
                self.crash_barrier("apply_chunk_txn")
                if replaced:
                    self.crash_barrier("replace_contrib_txn")

    # -- contribution replacement (caller owns the transaction) --------------
    def _replace_contribution(self, conn, tenant, accession, revision,
                              retracted, payload_text, report) -> bool:
        """Subtract the paper's old edge contributions, add the new ones.

        Returns True when an old contribution existed (a real replacement).
        Everything runs inside the caller's transaction, so the network can
        never be observed (or left, after a crash) between the subtraction
        and the addition.
        """
        old = conn.execute(
            "SELECT code_a, code_b FROM collab_contrib"
            " WHERE tenant=? AND accession=?", (tenant, accession)).fetchall()
        had_old = bool(old)
        for edge in old:
            self._decrement_edge(conn, tenant, edge["code_a"], edge["code_b"])
        conn.execute(
            "DELETE FROM collab_contrib WHERE tenant=? AND accession=?",
            (tenant, accession))
        conn.execute(
            "DELETE FROM collab_paper_orgs WHERE tenant=? AND accession=?",
            (tenant, accession))
        conn.execute(
            "DELETE FROM collab_unmapped WHERE tenant=? AND accession=?",
            (tenant, accession))

        if not retracted:
            org_map, unmapped = extract_affiliations(payload_text,
                                                     self.mapping)
            codes = sorted(org_map)
            for i in range(len(codes)):
                for j in range(i + 1, len(codes)):
                    a, b = codes[i], codes[j]
                    conn.execute(
                        "INSERT INTO collab_contrib(tenant, accession, code_a,"
                        " code_b, revision) VALUES (?,?,?,?,?)",
                        (tenant, accession, a, b, revision))
                    self._increment_edge(conn, tenant, a, b)
            for code in codes:
                conn.execute(
                    "INSERT INTO collab_paper_orgs(tenant, accession, code,"
                    " raws) VALUES (?,?,?,?)",
                    (tenant, accession, code,
                     json.dumps(org_map[code], ensure_ascii=False)))
            for name in unmapped:
                conn.execute(
                    "INSERT INTO collab_unmapped(tenant, accession, revision,"
                    " name) VALUES (?,?,?,?)",
                    (tenant, accession, revision, name))
            report.unmapped_names += len(unmapped)

        conn.execute(
            "INSERT INTO collab_papers(tenant, accession, revision, retracted)"
            " VALUES (?,?,?,?)"
            " ON CONFLICT(tenant, accession) DO UPDATE SET"
            " revision=excluded.revision, retracted=excluded.retracted",
            (tenant, accession, revision, retracted))
        return had_old

    @staticmethod
    def _increment_edge(conn, tenant, a, b):
        conn.execute(
            "INSERT INTO collab_edges(tenant, code_a, code_b, weight)"
            " VALUES (?,?,?,1)"
            " ON CONFLICT(tenant, code_a, code_b) DO UPDATE SET"
            " weight = weight + 1", (tenant, a, b))

    @staticmethod
    def _decrement_edge(conn, tenant, a, b):
        conn.execute(
            "UPDATE collab_edges SET weight = weight - 1"
            " WHERE tenant=? AND code_a=? AND code_b=?", (tenant, a, b))
        # An edge whose last supporting paper leaves is deleted outright:
        # the network never carries zero-weight empty-shell edges.
        conn.execute(
            "DELETE FROM collab_edges WHERE tenant=? AND code_a=?"
            " AND code_b=? AND weight <= 0", (tenant, a, b))
