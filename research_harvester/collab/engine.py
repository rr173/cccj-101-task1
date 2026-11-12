"""Read-only collaboration queries over one consistent WAL snapshot.

Every command pins a single read snapshot at entry, so a neighbor list, a
path and its per-hop provenance always come from the same network
generation even if a builder commits concurrently.

Queries
-------
neighbors   direct collaborators of one institution, ordered by cooperation
            weight DESC then institution code ASC, paginated, with the FULL
            neighbor total reported alongside the page
path        minimum-hop route between two institutions inside the subgraph
            of edges with ``weight >= min_weight``; among equally short
            routes the lexicographically smallest code sequence wins; every
            hop carries its real weight and supporting papers; an explicit
            "no path" is returned when the target is unreachable
edge        provenance of one collaboration edge: the supporting papers
            with their adopted revisions and the raw, pre-normalization
            affiliation strings that resolved to each endpoint

All queries require an explicit tenant and never cross tenants.  Unknown
institution codes and invalid parameters (negative thresholds, bad paging)
raise :class:`QueryError` with a clear message.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from . import store


class QueryError(Exception):
    """Invalid query parameters (unknown institution, bad threshold...)."""


@dataclass
class Neighbor:
    code: str
    name: str
    weight: int


@dataclass
class NeighborResult:
    tenant: str
    code: str
    total: int
    offset: int
    limit: int
    min_weight: int
    page: List[Neighbor] = field(default_factory=list)


@dataclass
class Hop:
    code_a: str
    code_b: str
    weight: int
    papers: List[dict] = field(default_factory=list)  # accession + revision


@dataclass
class PathResult:
    tenant: str
    src: str
    dst: str
    min_weight: int
    found: bool
    hops: int = 0
    codes: List[str] = field(default_factory=list)
    legs: List[Hop] = field(default_factory=list)
    reason: Optional[str] = None


@dataclass
class EdgeResult:
    tenant: str
    code_a: str
    code_b: str
    found: bool
    weight: int = 0
    papers: List[dict] = field(default_factory=list)


class CollabEngine:
    def __init__(self, conn):
        self.conn = conn

    # -- snapshot handling ---------------------------------------------------
    @staticmethod
    def _pin_snapshot(conn):
        """Force the deferred read transaction to acquire its WAL snapshot."""
        conn.execute(
            "SELECT value FROM collab_meta WHERE key='schema_version'")

    def snapshot(self):
        """Context manager holding one WAL snapshot across several queries."""
        conn = self.conn
        outer = conn.in_transaction

        class _Snapshot:
            def __enter__(self_inner):
                if not outer:
                    conn.execute("BEGIN")
                    CollabEngine._pin_snapshot(conn)
                return self

            def __exit__(self_inner, exc_type, exc, tb):
                if not outer:
                    conn.execute("COMMIT")
                return False

        return _Snapshot()

    def _run_consistent(self, fn):
        conn = self.conn
        own = not conn.in_transaction
        if own:
            conn.execute("BEGIN")
            self._pin_snapshot(conn)
        try:
            return fn()
        finally:
            if own:
                conn.execute("COMMIT")

    # -- validation ------------------------------------------------------------
    def _require_tenant(self, tenant):
        if not isinstance(tenant, str) or not tenant:
            raise QueryError("tenant 是所有查询的必填入口参数")

    def _require_inst(self, code, label="inst"):
        if not isinstance(code, str) or not code:
            raise QueryError(f"{label} 必须是非空的机构代码字符串")
        row = self.conn.execute(
            "SELECT name FROM collab_inst WHERE code=?", (code,)).fetchone()
        if row is None:
            raise QueryError(
                f"未知机构代码 {code!r}：不在本网络文件绑定的对照表快照中")
        return row["name"]

    @staticmethod
    def _require_min_weight(min_weight):
        if not isinstance(min_weight, int) or isinstance(min_weight, bool):
            raise QueryError("min-weight 必须是整数")
        if min_weight < 0:
            raise QueryError("min-weight 不能为负数")

    # -- neighbors ---------------------------------------------------------------
    def neighbors(self, tenant, code, *, min_weight=1, limit=20,
                  offset=0) -> NeighborResult:
        self._require_tenant(tenant)
        if limit < 1 or offset < 0:
            raise QueryError("limit 必须 >= 1 且 offset 必须 >= 0")
        self._require_min_weight(min_weight)

        def work():
            self._require_inst(code)
            rows = self.conn.execute(
                "SELECT code_a, code_b, weight FROM collab_edges"
                " WHERE tenant=? AND weight>=? AND (code_a=? OR code_b=?)",
                (tenant, min_weight, code, code)).fetchall()
            peers: List[Neighbor] = []
            names = {r["code"]: r["name"] for r in
                     self.conn.execute("SELECT code, name FROM collab_inst")}
            for row in rows:
                peer = row["code_b"] if row["code_a"] == code else row["code_a"]
                peers.append(Neighbor(peer, names.get(peer, peer),
                                      int(row["weight"])))
            peers.sort(key=lambda n: (-n.weight, n.code))
            page = peers[offset:offset + limit]
            return NeighborResult(tenant=tenant, code=code, total=len(peers),
                                  offset=offset, limit=limit,
                                  min_weight=min_weight, page=page)

        return self._run_consistent(work)

    # -- path -------------------------------------------------------------------
    def _subgraph(self, tenant, min_weight) -> Dict[str, Dict[str, int]]:
        adj: Dict[str, Dict[str, int]] = {}
        for row in self.conn.execute(
                "SELECT code_a, code_b, weight FROM collab_edges"
                " WHERE tenant=? AND weight>=?",
                (tenant, min_weight)):
            adj.setdefault(row["code_a"], {})[row["code_b"]] = int(row["weight"])
            adj.setdefault(row["code_b"], {})[row["code_a"]] = int(row["weight"])
        return adj

    @staticmethod
    def _bfs_dist(adj, src) -> Dict[str, int]:
        dist = {src: 0}
        frontier = [src]
        depth = 0
        while frontier:
            nxt = []
            for u in frontier:
                for v in adj.get(u, ()):
                    if v not in dist:
                        dist[v] = depth + 1
                        nxt.append(v)
            frontier = nxt
            depth += 1
        return dist

    def _hop(self, tenant, a, b) -> Hop:
        lo, hi = (a, b) if a < b else (b, a)
        row = self.conn.execute(
            "SELECT weight FROM collab_edges WHERE tenant=? AND code_a=?"
            " AND code_b=?", (tenant, lo, hi)).fetchone()
        papers = [dict(accession=r["accession"], revision=int(r["revision"]))
                  for r in self.conn.execute(
                      "SELECT accession, revision FROM collab_contrib"
                      " WHERE tenant=? AND code_a=? AND code_b=?"
                      " ORDER BY accession", (tenant, lo, hi))]
        return Hop(code_a=a, code_b=b,
                   weight=int(row["weight"]) if row else 0, papers=papers)

    def path(self, tenant, src, dst, *, min_weight=1) -> PathResult:
        self._require_tenant(tenant)
        self._require_min_weight(min_weight)

        def work():
            self._require_inst(src, "from")
            self._require_inst(dst, "to")
            if src == dst:
                return PathResult(tenant=tenant, src=src, dst=dst,
                                  min_weight=min_weight, found=True,
                                  hops=0, codes=[src])
            adj = self._subgraph(tenant, min_weight)
            dist_s = self._bfs_dist(adj, src)
            if dst not in dist_s:
                return PathResult(
                    tenant=tenant, src=src, dst=dst, min_weight=min_weight,
                    found=False,
                    reason=(f"在边权 >= {min_weight} 的合作子图中，"
                            f"{src} 与 {dst} 之间不存在通路"))
            dist_t = self._bfs_dist(adj, dst)
            total = dist_s[dst]
            # Greedy lexicographically-smallest shortest path: at each step
            # take the smallest code that stays on a shortest route.
            codes = [src]
            while codes[-1] != dst:
                u = codes[-1]
                nxt = min(
                    v for v in adj.get(u, ())
                    if dist_s.get(v) == dist_s[u] + 1
                    and dist_s[v] + dist_t.get(v, 1 << 30) == total)
                codes.append(nxt)
            legs = [self._hop(tenant, codes[i], codes[i + 1])
                    for i in range(len(codes) - 1)]
            return PathResult(tenant=tenant, src=src, dst=dst,
                              min_weight=min_weight, found=True,
                              hops=total, codes=codes, legs=legs)

        return self._run_consistent(work)

    # -- edge provenance ---------------------------------------------------------
    def edge(self, tenant, code_a, code_b) -> EdgeResult:
        self._require_tenant(tenant)

        def work():
            self._require_inst(code_a, "a")
            self._require_inst(code_b, "b")
            lo, hi = sorted((code_a, code_b))
            row = self.conn.execute(
                "SELECT weight FROM collab_edges WHERE tenant=? AND code_a=?"
                " AND code_b=?", (tenant, lo, hi)).fetchone()
            if row is None:
                return EdgeResult(tenant=tenant, code_a=lo, code_b=hi,
                                  found=False, weight=0, papers=[])
            papers = []
            for r in self.conn.execute(
                    "SELECT accession, revision FROM collab_contrib"
                    " WHERE tenant=? AND code_a=? AND code_b=?"
                    " ORDER BY accession", (tenant, lo, hi)):
                raws: Dict[str, List[str]] = {}
                for o in self.conn.execute(
                        "SELECT code, raws FROM collab_paper_orgs"
                        " WHERE tenant=? AND accession=? AND code IN (?,?)"
                        " ORDER BY code", (tenant, r["accession"], lo, hi)):
                    raws[o["code"]] = json.loads(o["raws"])
                papers.append({
                    "accession": r["accession"],
                    "revision": int(r["revision"]),
                    "raw_affiliations": raws,
                })
            return EdgeResult(tenant=tenant, code_a=lo, code_b=hi,
                              found=True, weight=int(row["weight"]),
                              papers=papers)

        return self._run_consistent(work)
