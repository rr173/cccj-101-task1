#!/usr/bin/env python3
"""Command line entry for the offline collaboration-network analysis.

Commands
--------
update      catch the network up with the landing database (first run binds
            the mapping content digest; later runs must present the SAME
            mapping content)
rebuild     drop all network contents and build again (same mapping only)
neighbors   direct collaborators of one institution, weight DESC / code ASC,
            paginated, with the full neighbor total
path        minimum-hop route inside the min-weight subgraph; ties broken by
            the lexicographically smallest code sequence; explicit no-path
edge        provenance of one edge: supporting papers, adopted revisions and
            the raw pre-normalization affiliation strings
diagnose    unmapped affiliation names for ONE explicit tenant (they form no
            node, never merge); a missing or blank --tenant is refused and
            never degrades into a cross-tenant global listing
status      network metadata: bound mapping digest, cursors, counts

The network lives in its own SQLite file; the landing database is opened
read-only and never modified, and no command touches the network or the
collector's fetch threads.

Exit codes: 0 success (including "no path" / empty results); 2 parameter
errors (unknown institution, negative threshold, missing tenant, landing
missing); 3 mapping file invalid or mapping digest mismatch.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collab import store                                   # noqa: E402
from collab.builder import Builder, LandingMissing, MappingMismatch  # noqa: E402
from collab.engine import CollabEngine, QueryError         # noqa: E402
from collab.mapping import MappingError, load_mapping      # noqa: E402


def _tenants(arg):
    if not arg:
        return None
    return [t.strip() for t in arg.split(",") if t.strip()]


def _load_mapping_or_exit(path):
    try:
        return load_mapping(path)
    except MappingError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        raise SystemExit(3)


def _run_builder(args, full):
    mapping = _load_mapping_or_exit(args.mapping)
    builder = Builder(args.net, args.db, mapping,
                      chunk_size=args.chunk_size)
    try:
        if full:
            report = builder.rebuild(tenants=_tenants(args.tenants))
        else:
            report = builder.sync(tenants=_tenants(args.tenants))
    except MappingMismatch as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 3
    except LandingMissing as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    print(f"scanned_rows={report.scanned}"
          f" new_identities={report.new_identities}"
          f" changed_identities={report.changed_identities}"
          f" applied_revisions={report.applied}"
          f" ledger_only={report.ledger_only}"
          f" unmapped_names={report.unmapped_names}")
    for tenant in report.tenants:
        print(f"  {tenant}: watermark={report.watermarks.get(tenant)}")
    return 0


def cmd_update(args):
    return _run_builder(args, full=False)


def cmd_rebuild(args):
    return _run_builder(args, full=True)


def _open_engine(args):
    conn = store.connect(args.net)
    store.init(conn)
    return conn, CollabEngine(conn)


def cmd_neighbors(args):
    conn, engine = _open_engine(args)
    try:
        try:
            result = engine.neighbors(
                args.tenant, args.inst, min_weight=args.min_weight,
                limit=args.limit, offset=args.offset)
        except QueryError as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 2
        if args.json:
            print(json.dumps({
                "tenant": result.tenant, "inst": result.code,
                "total": result.total, "offset": result.offset,
                "limit": result.limit, "min_weight": result.min_weight,
                "neighbors": [dict(code=n.code, name=n.name, weight=n.weight)
                              for n in result.page]},
                ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        print(f"tenant={result.tenant} inst={result.code}"
              f" total={result.total} offset={result.offset}"
              f" limit={result.limit} min_weight={result.min_weight}")
        for i, n in enumerate(result.page, start=result.offset + 1):
            print(f"  [{i}] {n.code} weight={n.weight} name={n.name!r}")
        return 0
    finally:
        conn.close()


def cmd_path(args):
    conn, engine = _open_engine(args)
    try:
        try:
            result = engine.path(args.tenant, args.src, args.dst,
                                 min_weight=args.min_weight)
        except QueryError as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 2
        if args.json:
            print(json.dumps({
                "tenant": result.tenant, "from": result.src,
                "to": result.dst, "min_weight": result.min_weight,
                "found": result.found, "hops": result.hops,
                "codes": result.codes,
                "legs": [dict(code_a=h.code_a, code_b=h.code_b,
                              weight=h.weight, papers=h.papers)
                         for h in result.legs],
                "reason": result.reason},
                ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        print(f"tenant={result.tenant} from={result.src} to={result.dst}"
              f" min_weight={result.min_weight}")
        if not result.found:
            print(f"NO PATH: {result.reason}")
            return 0
        print(f"found=true hops={result.hops}")
        print("  path: " + " -> ".join(result.codes))
        for i, hop in enumerate(result.legs, start=1):
            papers = ", ".join(f"{p['accession']}(r{p['revision']})"
                               for p in hop.papers)
            print(f"  hop {i}: {hop.code_a} - {hop.code_b}"
                  f" weight={hop.weight} papers: {papers}")
        return 0
    finally:
        conn.close()


def cmd_edge(args):
    conn, engine = _open_engine(args)
    try:
        try:
            result = engine.edge(args.tenant, args.a, args.b)
        except QueryError as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 2
        if args.json:
            print(json.dumps({
                "tenant": result.tenant, "code_a": result.code_a,
                "code_b": result.code_b, "found": result.found,
                "weight": result.weight, "papers": result.papers},
                ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        print(f"tenant={result.tenant} edge={result.code_a}-{result.code_b}")
        if not result.found:
            print("no such edge (weight 0)")
            return 0
        print(f"weight={result.weight}")
        for p in result.papers:
            print(f"  {p['accession']} revision={p['revision']}")
            for code, raws in p["raw_affiliations"].items():
                print(f"    {code} raw: {json.dumps(raws, ensure_ascii=False)}")
        return 0
    finally:
        conn.close()


def cmd_diagnose(args):
    # diagnose 与 neighbors/path/edge 一样是租户入口：必须显式指定非空白
    # 租户，缺参由 argparse 拦截（退出码 2），空白值在这里拦截——任何形式
    # 的缺失都只能报错，绝不允许回退成跨租户的全局汇总。
    tenant = args.tenant
    if not isinstance(tenant, str) or not tenant.strip():
        print("refused: diagnose 需要非空的 --tenant；不会执行跨租户汇总",
              file=sys.stderr)
        return 2
    conn = store.connect(args.net)
    store.init(conn)
    try:
        rows = conn.execute(
            "SELECT tenant, name, COUNT(DISTINCT accession) AS papers"
            " FROM collab_unmapped WHERE tenant=?"
            " GROUP BY tenant, name"
            " ORDER BY papers DESC, name", (tenant,)).fetchall()
        if args.json:
            print(json.dumps({
                "tenant": tenant,
                "unmapped": [dict(tenant=r["tenant"], name=r["name"],
                                  papers=r["papers"],
                                  accessions=[x["accession"] for x in
                                              conn.execute(
                                                  "SELECT DISTINCT accession"
                                                  " FROM collab_unmapped"
                                                  " WHERE tenant=? AND name=?"
                                                  " ORDER BY accession",
                                                  (tenant, r["name"]))])
                             for r in rows]},
                ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        if not rows:
            print(f"tenant={tenant}")
            print("no unmapped affiliation names")
            return 0
        print(f"tenant={tenant}")
        for r in rows:
            print(f"  {r['name']!r} papers={r['papers']}")
            for x in conn.execute(
                    "SELECT DISTINCT accession FROM collab_unmapped"
                    " WHERE tenant=? AND name=? ORDER BY accession",
                    (tenant, r["name"])):
                print(f"    {x['accession']}")
        return 0
    finally:
        conn.close()


def cmd_status(args):
    conn = store.connect(args.net)
    store.init(conn)
    try:
        meta = {r["key"]: r["value"]
                for r in conn.execute("SELECT key, value FROM collab_meta")}
        insts = conn.execute(
            "SELECT COUNT(*) c FROM collab_inst").fetchone()["c"]
        edges = conn.execute(
            "SELECT tenant, COUNT(*) c, COALESCE(SUM(weight),0) w"
            " FROM collab_edges GROUP BY tenant ORDER BY tenant").fetchall()
        papers = conn.execute(
            "SELECT tenant, COUNT(*) c, SUM(retracted) r FROM collab_papers"
            " GROUP BY tenant ORDER BY tenant").fetchall()
        unmapped = conn.execute(
            "SELECT tenant, COUNT(DISTINCT name) c FROM collab_unmapped"
            " GROUP BY tenant ORDER BY tenant").fetchall()
        if args.json:
            print(json.dumps({
                "meta": meta,
                "institutions": insts,
                "cursors": [dict(r) for r in conn.execute(
                    "SELECT * FROM collab_cursor ORDER BY tenant")],
                "papers": [dict(r) for r in papers],
                "edges": [dict(r) for r in edges],
                "unmapped_names": [dict(r) for r in unmapped]},
                ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        digest = meta.get("mapping_digest", "")
        print(f"schema_version={meta.get('schema_version')}"
              f" mapping_version={meta.get('mapping_version')}"
              f" mapping_digest={digest[:16]}…"
              f" last_sync_at={meta.get('last_sync_at')}")
        print(f"institutions={insts}")
        for r in conn.execute(
                "SELECT * FROM collab_cursor ORDER BY tenant"):
            print(f"cursor {r['tenant']}: last_rowid={r['last_rowid']}")
        for r in papers:
            print(f"papers {r['tenant']}: {r['c']}"
                  f" (retracted: {r['r'] or 0})")
        for r in edges:
            print(f"edges {r['tenant']}: {r['c']} (total weight: {r['w']})")
        for r in unmapped:
            print(f"unmapped_names {r['tenant']}: {r['c']}")
        return 0
    finally:
        conn.close()


def main(argv=None):
    p = argparse.ArgumentParser(
        description="Offline institutional collaboration-network analysis")
    sub = p.add_subparsers(dest="command")

    def add_build(sp):
        sp.add_argument("--db", required=True,
                        help="landing SQLite database written by the harvester")
        sp.add_argument("--net", required=True,
                        help="collaboration network SQLite file"
                             " (created if absent)")
        sp.add_argument("--mapping", required=True,
                        help="local JSON affiliation mapping table")
        sp.add_argument("--tenants",
                        help="comma-separated tenant ids (default all)")
        sp.add_argument("--chunk-size", type=int, default=500,
                        help="landing rows committed per transaction"
                             " (testing/tuning)")

    for name, func in (("update", cmd_update), ("rebuild", cmd_rebuild)):
        sp = sub.add_parser(name)
        add_build(sp)
        sp.set_defaults(func=func)

    nb = sub.add_parser("neighbors")
    nb.add_argument("--net", required=True)
    nb.add_argument("--tenant", required=True,
                    help="mandatory tenant scope; networks never cross tenants")
    nb.add_argument("--inst", required=True, help="institution code")
    nb.add_argument("--min-weight", type=int, default=1)
    nb.add_argument("--limit", type=int, default=20)
    nb.add_argument("--offset", type=int, default=0)
    nb.add_argument("--json", action="store_true")
    nb.set_defaults(func=cmd_neighbors)

    pa = sub.add_parser("path")
    pa.add_argument("--net", required=True)
    pa.add_argument("--tenant", required=True)
    pa.add_argument("--from", dest="src", required=True,
                    help="source institution code")
    pa.add_argument("--to", dest="dst", required=True,
                    help="target institution code")
    pa.add_argument("--min-weight", type=int, default=1)
    pa.add_argument("--json", action="store_true")
    pa.set_defaults(func=cmd_path)

    ed = sub.add_parser("edge")
    ed.add_argument("--net", required=True)
    ed.add_argument("--tenant", required=True)
    ed.add_argument("--a", required=True, help="first institution code")
    ed.add_argument("--b", required=True, help="second institution code")
    ed.add_argument("--json", action="store_true")
    ed.set_defaults(func=cmd_edge)

    dg = sub.add_parser("diagnose")
    dg.add_argument("--net", required=True)
    dg.add_argument("--tenant", required=True,
                    help="mandatory tenant scope; diagnostics never mix"
                         " tenants -- a blank value is refused instead of"
                         " falling back to a cross-tenant global listing")
    dg.add_argument("--json", action="store_true")
    dg.set_defaults(func=cmd_diagnose)

    st = sub.add_parser("status")
    st.add_argument("--net", required=True)
    st.add_argument("--json", action="store_true")
    st.set_defaults(func=cmd_status)

    args = p.parse_args(argv)
    if not getattr(args, "command", None):
        p.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
