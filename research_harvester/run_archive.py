#!/usr/bin/env python3
"""Retention / archive command for harvested raw HTTP responses (stdlib only).

This is a separate entry point from ``run_fetch.py`` and ``run_quality.py``.
It moves expired response bodies out of the main SQLite database into
deterministic gzip-compressed JSONL shards, keeps every ``raw_pages`` row and
``result_versions.raw_page_id`` reference queryable, and never modifies the
quality projection, rule generations or processing progress.

Commands
--------
    archive    freeze a cutoff + max page id, write/verify/register/clear shards
    preview    read-only estimate of releasable body bytes and shard counts
    list       find registered shards by tenant and UTC fetch date
    verify     recompute whole-file and per-page digests (read-only)
    batches    batch ids, phases and progress for on-call re-runs

Exit codes: 0 success, 1 integrity problem, 3 landing database missing,
2 usage error.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from archive import archiver, inspect, shard as shard_mod, store  # noqa: E402


DEFAULT_RETENTION_DAYS = 30.0


def _connect(db_path, *, create=False):
    """Open the landing DB.

    ``create=False`` (preview/list/verify/batches) never creates a table or
    writes a row: the read-side helpers tolerate the archive tables being
    absent.  ``create=True`` (archive) additively ensures ``archive_*``.
    """
    if not os.path.exists(db_path):
        raise SystemExit(f"landing database not found: {db_path}")
    conn = store.connect(db_path)
    store.require_landing(conn)
    if create:
        store.init(conn)
    return conn


def _resolve_cutoff(args):
    if args.cutoff_at is not None:
        return float(args.cutoff_at)
    days = args.retention_days
    if days is None:
        days = DEFAULT_RETENTION_DAYS
    return time.time() - float(days) * 86400.0


def _tenants(value):
    if not value:
        return None
    items = [t.strip() for t in value.split(",") if t.strip()]
    return items or None


def _print_preview(report):
    print(f"retention preview (cutoff_at={report['cutoff_at']},"
          " read-only: no files written, no bodies cleared)")
    for tenant, bucket in sorted(report["tenants"].items()):
        print(f"{tenant}: releasable_pages={bucket['releasable_pages']}"
              f" releasable_bytes={bucket['releasable_bytes']}"
              f" shards={bucket['shards']}"
              f" page_range={bucket['first_page_id']}..{bucket['last_page_id']}")
        for group, info in sorted(bucket["groups"].items()):
            print(f"  {group}: pages={info['pages']} bytes={info['bytes']}"
                  f" shards={info['shards']}")
    totals = report["totals"]
    print(f"totals: tenants={totals['tenant_count']}"
          f" pages={totals['releasable_pages']}"
          f" bytes={totals['releasable_bytes']}"
          f" shards={totals['shards']}")


def _print_archive(report):
    print(f"batch {report['batch_id']}: status={report['status']}"
          f" phase={report['phase']}"
          f" cutoff_at={report['cutoff_at']}"
          f" max_page_id={report['max_page_id']}")
    for shard in report["shards"]:
        print(f"  {shard['tenant']} {shard['fetch_date']}"
              f" shard={shard['shard_no']} pages={shard['page_count']}"
              f" range={shard['first_page_id']}..{shard['last_page_id']}"
              f" bytes={shard['compressed_bytes']}"
              f" sha256={shard['file_sha256']} path={shard['path']}")
    print(f"registered_pages={report['registered_pages']}"
          f" cleared_pages={report['cleared_pages']}"
          f" cleared_now={report['cleared_now']}")
    for failure in report["failures"]:
        print(f"INTEGRITY FAILURE tenant={failure['tenant']}"
              f" path={failure['path']}"
              f" page_id={failure['page_id']}: {failure['detail']}",
              file=sys.stderr)


def cmd_archive(args):
    cutoff = _resolve_cutoff(args)
    conn = _connect(args.db, create=True)
    try:
        if args.resume_batch:
            batch = store.get_batch(conn, args.resume_batch)
            if batch is None:
                print(f"unknown batch: {args.resume_batch}", file=sys.stderr)
                return 2
            engine = archiver.Archiver(
                args.db, args.archive_dir,
                page_group_limit=args.page_group_limit)
            report = engine._run_batch(conn, batch, clear=not args.no_clear)
        else:
            engine = archiver.Archiver(
                args.db, args.archive_dir,
                page_group_limit=args.page_group_limit)
            batch = engine.freeze_snapshot(
                conn, cutoff_at=cutoff, tenants=_tenants(args.tenants),
                fetch_date=args.fetch_date)
            report = engine._run_batch(conn, batch, clear=not args.no_clear)
    except shard_mod.ShardIntegrityError as exc:
        print(f"integrity failure: {exc}", file=sys.stderr)
        return 1
    finally:
        conn.close()

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        _print_archive(report)
    return 0 if report.get("ok") else 1


def cmd_preview(args):
    cutoff = _resolve_cutoff(args)
    conn = _connect(args.db)
    try:
        report = inspect.preview(
            conn, cutoff_at=cutoff, tenants=_tenants(args.tenants),
            fetch_date=args.fetch_date, page_group_limit=args.page_group_limit)
    finally:
        conn.close()
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        _print_preview(report)
    return 0


def cmd_list(args):
    conn = _connect(args.db)
    try:
        shards = inspect.list_shards(
            conn, tenant=args.tenant, fetch_date=args.fetch_date)
    finally:
        conn.close()
    for shard in shards:
        shard["file_exists"] = os.path.exists(shard["path"])
        shard["pages"] = json.loads(shard["page_ids"] or "[]")
        shard.pop("page_ids", None)
    if args.json:
        print(json.dumps(shards, indent=2, sort_keys=True, default=str))
        return 0
    for shard in shards:
        print(f"{shard['tenant']} {shard['fetch_date']}"
              f" shard={shard['shard_no']} pages={shard['page_count']}"
              f" range={shard['first_page_id']}..{shard['last_page_id']}"
              f" bytes={shard['compressed_bytes']}"
              f" exists={shard['file_exists']} path={shard['path']}")
    if not shards:
        print("no archived shards for this filter")
    return 0


def cmd_verify(args):
    conn = _connect(args.db)
    try:
        report = inspect.verify_shards(
            conn, tenant=args.tenant, fetch_date=args.fetch_date)
    finally:
        conn.close()
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True, default=str))
    else:
        print(f"shards={report['shard_count']}"
              f" online_pages={report['online_pages']}"
              f" moved_out_pages={report['moved_out_pages']}"
              f" problem_shards={report['problem_shards']}"
              f" problem_pages={report['problem_pages']}")
        for problem in report["problems"]:
            print(f"INTEGRITY PROBLEM tenant={problem['tenant']}"
                  f" path={problem['path']}: {problem['problem']}",
                  file=sys.stderr)
    return 0 if report["ok"] else 1


def cmd_batches(args):
    conn = _connect(args.db)
    try:
        if not store.archive_tables_present(conn):
            rows = []
        elif args.batch_id:
            rows = [store.get_batch(conn, args.batch_id)]
            rows = [r for r in rows if r]
        else:
            rows = [dict(r) for r in conn.execute(
                "SELECT * FROM archive_batches"
                " ORDER BY created_at DESC, batch_id DESC LIMIT 50")]
    finally:
        conn.close()
    if args.json:
        print(json.dumps(rows, indent=2, sort_keys=True, default=str))
        return 0
    for row in rows:
        shards = row.get("_shards")
        print(f"batch {row['batch_id']}: status={row['status']}"
              f" phase={row['phase']}"
              f" cutoff_at={row['cutoff_at']}"
              f" max_page_id={row['max_page_id']}"
              f" tenants={row['tenants']}"
              f" filters={row['filters']}")
        print(f"  message={row['message']}")
    if not rows:
        print("no archive batches")
        return 0
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(
        description="Retention/archive of harvested raw responses")
    sub = p.add_subparsers(dest="command")

    def add_common(sp, *, create=False):
        sp.add_argument("--db", required=True, help="landing SQLite database")

    def add_filters(sp):
        sp.add_argument("--tenant", help="single tenant filter")
        sp.add_argument("--fetch-date", help="UTC date filter YYYY-MM-DD")

    arc = sub.add_parser("archive", help="archive expired response bodies")
    add_common(arc)
    arc.add_argument("--archive-dir", required=True,
                     help="directory for compressed JSONL shards")
    arc.add_argument("--retention-days", type=float, default=None,
                     help=f"body retention in days (default "
                          f"{DEFAULT_RETENTION_DAYS})")
    arc.add_argument("--cutoff-at", type=float, default=None,
                     help="explicit inclusive UTC cutoff timestamp")
    arc.add_argument("--tenants", help="comma-separated tenant ids")
    arc.add_argument("--fetch-date", help="only archive this UTC date")
    arc.add_argument("--page-group-limit", type=int, default=200,
                     help="pages per shard file")
    arc.add_argument("--no-clear", action="store_true",
                     help="register shards but keep bodies in SQLite")
    arc.add_argument("--resume-batch", help="resume this exact batch id")
    arc.add_argument("--json", action="store_true")
    arc.set_defaults(func=cmd_archive)

    pre = sub.add_parser("preview", help="read-only releasable capacity")
    add_common(pre)
    pre.add_argument("--retention-days", type=float, default=None)
    pre.add_argument("--cutoff-at", type=float, default=None)
    pre.add_argument("--tenants")
    pre.add_argument("--fetch-date")
    pre.add_argument("--page-group-limit", type=int, default=200)
    pre.add_argument("--json", action="store_true")
    pre.set_defaults(func=cmd_preview)

    lst = sub.add_parser("list", help="find archived shards by tenant/date")
    add_common(lst)
    add_filters(lst)
    lst.add_argument("--json", action="store_true")
    lst.set_defaults(func=cmd_list)

    ver = sub.add_parser("verify", help="recompute shard and page digests")
    add_common(ver)
    add_filters(ver)
    ver.add_argument("--json", action="store_true")
    ver.set_defaults(func=cmd_verify)

    bat = sub.add_parser("batches", help="batch ids, phases and progress")
    add_common(bat)
    bat.add_argument("--batch-id")
    bat.add_argument("--json", action="store_true")
    bat.set_defaults(func=cmd_batches)

    args = p.parse_args(argv)
    if not getattr(args, "command", None):
        p.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
