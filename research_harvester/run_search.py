#!/usr/bin/env python3
"""Command line entry for the local offline retrieval module (stdlib only).

Commands
--------
index      build the index from the landing database (first full build)
update     catch up with landing rows written since the last index/update
rebuild    drop and rebuild the whole index (explicit full re-scan)
repair     audit legacy progress in the consumed rowid range, recover rows the
           old rule skipped, then resume incremental catch-up (idempotent)
query      run one boolean expression against a consistent snapshot
status     index metadata, per-tenant cursors, document and term counts
explain    show the parsed syntax tree and normalized leaf terms

The index lives in its own SQLite file; the landing database is opened
read-only and never modified, and none of these commands touch the network or
the collector's fetch threads.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from search import store                                   # noqa: E402
from search.engine import SearchEngine                     # noqa: E402
from search.indexer import Indexer, LandingMissing         # noqa: E402
from search.parser import QuerySyntaxError, leaves, parse  # noqa: E402
from search.tokenizer import make_excerpt                  # noqa: E402


def _open_index(path):
    conn = store.connect(path)
    store.init(conn)
    return conn


def _tenants(arg):
    if not arg:
        return None
    return [t.strip() for t in arg.split(",") if t.strip()]


def cmd_index(args):
    indexer = Indexer(args.index, args.db)
    try:
        report = indexer.sync(tenants=_tenants(args.tenants), full=True)
    except LandingMissing as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    _print_report(report)
    return 0


def cmd_update(args):
    indexer = Indexer(args.index, args.db)
    try:
        report = indexer.sync(tenants=_tenants(args.tenants), full=False)
    except LandingMissing as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    _print_report(report)
    return 0


def cmd_rebuild(args):
    indexer = Indexer(args.index, args.db)
    try:
        report = indexer.rebuild(tenants=_tenants(args.tenants))
    except LandingMissing as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    _print_report(report)
    return 0


def cmd_repair(args):
    indexer = Indexer(args.index, args.db)
    try:
        report = indexer.repair(tenants=_tenants(args.tenants))
    except LandingMissing as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2
    for tr in report.tenants:
        scope = (f"audited_rowids={tr.audited_from}..{tr.audited_to}"
                 f" missing_identities={tr.missing_identities}"
                 f" rewind_to={tr.rewind_to}")
        print(f"repair {tr.tenant}: {tr.status} {scope}")
        for item in tr.missing:
            print(f"  recovered {item.accession}"
                  f" revision={item.revision}"
                  f" source_rowid={item.source_rowid}")
    return 0


def _print_report(report):
    print(f"scanned_rows={report.scanned}"
          f" new_identities={report.new_identities}"
          f" changed_identities={report.changed_identities}"
          f" docs_upserted={report.documents_upserted}"
          f" docs_removed={report.documents_removed}"
          f" skip_notes={len(report.skips)}")
    for tenant in report.tenants:
        print(f"  {tenant}: watermark={report.watermarks.get(tenant)}")
    for note in report.skips:
        print(f"  skip {note['tenant']}/{note['accession']}"
              f" {note['field']}: {note['reason']}")


def cmd_query(args):
    conn = _open_index(args.index)
    try:
        engine = SearchEngine(conn)
        try:
            result = engine.search(
                args.expression, tenant=args.tenant,
                limit=args.limit, offset=args.offset, order=args.order)
        except QuerySyntaxError as exc:
            print(str(exc), file=sys.stderr)
            return 3
        except ValueError as exc:
            print(f"refused: {exc}", file=sys.stderr)
            return 2
        if args.json:
            payload = {
                "total": result.total, "offset": result.offset,
                "limit": result.limit, "order": result.order,
                "results": [_hit_dict(h) for h in result.page]}
            print(json.dumps(payload, ensure_ascii=False, indent=2,
                             sort_keys=True))
            return 0
        print(f"total={result.total} offset={result.offset}"
              f" limit={result.limit} order={result.order}")
        for i, hit in enumerate(result.page, start=result.offset + 1):
            print(f"\n[{i}] {hit.tenant}/{hit.accession}"
                  f" revision={hit.revision} score={hit.score}")
            print("  title: " + make_excerpt(hit.title, hit.title_spans))
            if hit.abstract:
                print("  abstract: "
                      + make_excerpt(hit.abstract, hit.abstract_spans))
            else:
                note = hit.abstract_skip or "空摘要（可按标题检索）"
                print(f"  abstract: <{note}>")
            if hit.title_skip:
                print(f"  note(title): {hit.title_skip}")
        return 0
    finally:
        conn.close()


def _hit_dict(hit):
    return {
        "tenant": hit.tenant, "accession": hit.accession,
        "revision": hit.revision, "score": hit.score,
        "title": hit.title, "abstract": hit.abstract,
        "title_spans": hit.title_spans,
        "abstract_spans": hit.abstract_spans,
        "title_excerpt": make_excerpt(hit.title, hit.title_spans),
        "abstract_excerpt": make_excerpt(hit.abstract, hit.abstract_spans),
        "title_skip": hit.title_skip, "abstract_skip": hit.abstract_skip,
    }


def cmd_status(args):
    conn = _open_index(args.index)
    try:
        meta = {r["key"]: r["value"]
                for r in conn.execute("SELECT key, value FROM search_meta")}
        docs = conn.execute(
            "SELECT tenant, COUNT(*) c, SUM(retracted) r FROM search_docs"
            " GROUP BY tenant ORDER BY tenant").fetchall()
        terms = conn.execute(
            "SELECT COUNT(*) c FROM search_terms").fetchone()["c"]
        postings = conn.execute(
            "SELECT COUNT(*) c FROM search_postings").fetchone()["c"]
        skips = conn.execute(
            "SELECT tenant, field, COUNT(*) c FROM search_skips"
            " GROUP BY tenant, field ORDER BY tenant, field").fetchall()
        repairs = [dict(r) for r in conn.execute(
            "SELECT * FROM search_repair ORDER BY tenant")]
        repair_missing = conn.execute(
            "SELECT COUNT(*) c FROM search_repair_missing m"
            " JOIN search_repair r ON r.tenant=m.tenant"
            " WHERE r.status <> 'done'").fetchone()["c"]
        if args.json:
            print(json.dumps({
                "meta": meta,
                "cursors": [dict(r) for r in conn.execute(
                    "SELECT * FROM search_cursor ORDER BY tenant")],
                "repair": repairs,
                "repair_missing": repair_missing,
                "documents": [dict(r) for r in docs],
                "terms": terms, "postings": postings,
                "skips": [dict(r) for r in skips]},
                ensure_ascii=False, indent=2, sort_keys=True))
            return 0
        print(f"schema_version={meta.get('schema_version')}"
              f" last_sync_at={meta.get('last_sync_at')}")
        print(f"terms={terms} postings={postings}")
        for r in conn.execute(
                "SELECT * FROM search_cursor ORDER BY tenant"):
            print(f"cursor {r['tenant']}: last_rowid={r['last_rowid']}")
        for r in docs:
            print(f"docs {r['tenant']}: {r['c']} (retracted visible: "
                  f"{r['r'] or 0})")
        for r in skips:
            print(f"skips {r['tenant']} {r['field']}: {r['c']}")
        for r in repairs:
            print(f"repair {r['tenant']}: status={r['status']}"
                  f" audit={r['audit_from']}..{r['audit_to']}"
                  f" missing={r['missing_count']}"
                  f" rewind_to={r['rewind_to']}")
        if repair_missing:
            print(f"repair_outstanding={repair_missing}")
        return 0
    finally:
        conn.close()


def cmd_explain(args):
    try:
        ast = parse(args.expression)
    except QuerySyntaxError as exc:
        print(str(exc), file=sys.stderr)
        return 3

    def render(node, depth=0):
        indent = "  " * depth
        from search.parser import AndNode, OrNode, DiffNode, TermNode
        if isinstance(node, TermNode):
            scope = node.field or "title|abstract"
            kind = "phrase" if node.phrase else "term"
            print(f"{indent}- {kind} field={scope} terms={node.terms}"
                  f" raw={node.raw!r}")
            return
        if isinstance(node, AndNode):
            print(f"{indent}- AND")
        elif isinstance(node, OrNode):
            print(f"{indent}- OR")
        elif isinstance(node, DiffNode):
            print(f"{indent}- NOT(difference)")
            render(node.left, depth + 1)
            render(node.right, depth + 1)
            return
        for child in node.children:
            render(child, depth + 1)

    render(ast)
    print("leaves:")
    for leaf in leaves(ast):
        scope = leaf.field or "title|abstract"
        print(f"  field={scope} phrase={leaf.phrase} terms={leaf.terms}")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(
        description="Local offline boolean retrieval for harvested results")
    sub = p.add_subparsers(dest="command")

    def add_db(sp):
        sp.add_argument("--db", required=True,
                        help="landing SQLite database written by the harvester")
        sp.add_argument("--index", required=True,
                        help="search index SQLite file (created if absent)")
        sp.add_argument("--tenants",
                        help="comma-separated tenant ids (default all)")

    for name, func in (("index", cmd_index), ("update", cmd_update),
                       ("rebuild", cmd_rebuild), ("repair", cmd_repair)):
        sp = sub.add_parser(name)
        add_db(sp)
        sp.set_defaults(func=func)

    q = sub.add_parser("query")
    q.add_argument("--index", required=True)
    q.add_argument("--tenant", required=True,
                   help="mandatory tenant scope; results never cross tenants")
    q.add_argument("expression",
                   help='e.g. title:光谱 AND (abstract:"mass spectrometry"'
                        ' OR abstract:蛋白)')
    q.add_argument("--limit", type=int, default=10)
    q.add_argument("--offset", type=int, default=0)
    q.add_argument("--order", choices=("relevance", "accession"),
                   default="relevance")
    q.add_argument("--json", action="store_true")
    q.set_defaults(func=cmd_query)

    st = sub.add_parser("status")
    st.add_argument("--index", required=True)
    st.add_argument("--json", action="store_true")
    st.set_defaults(func=cmd_status)

    ex = sub.add_parser("explain")
    ex.add_argument("expression")
    ex.set_defaults(func=cmd_explain)

    args = p.parse_args(argv)
    if not getattr(args, "command", None):
        p.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
