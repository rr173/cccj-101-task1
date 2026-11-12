#!/usr/bin/env python3
"""Independent quality-isolation / projection command (stdlib only).

This command reads the *landing* database written by run_fetch.py and prepares
a quality-isolated projection for downstream research statistics.  It never
contacts the network and never mutates the collector's tables.

Usage:
    # evaluate new landing rows against the rules and publish atomically
    python run_quality.py process --db out/harvest.db --rules quality/rules/v1.json

    # only build a staged generation and verify it (nothing becomes visible)
    python run_quality.py process --db out/harvest.db \
        --rules quality/rules/v1.json --stage-only

    # resume an interrupted staging generation (same rule snapshot as before)
    python run_quality.py process --db out/harvest.db \
        --rules quality/rules/v1.json --resume

    # explicitly re-evaluate already-read inputs with a new rule version
    python run_quality.py process --db out/harvest.db \
        --rules quality/rules/v2.json --reevaluate

    # inspect current published state / counts
    python run_quality.py status --db out/harvest.db

    # trace one result: its landing versions, rules applied, transitions
    python run_quality.py trace --db out/harvest.db --tenant alpha \
        --accession a0001

    # publish a verified staged generation, or safely drop it
    python run_quality.py publish --db out/harvest.db
    python run_quality.py discard-staging --db out/harvest.db
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from quality import rules as rules_mod          # noqa: E402
from quality import store                       # noqa: E402
from quality.processor import Processor, ProcessingRefused  # noqa: E402


def _connect(db_path):
    if not os.path.exists(db_path):
        raise SystemExit(f"landing database not found: {db_path}")
    conn = store.connect(db_path)
    # additive migration only: ensures build_phase exists on legacy databases
    store.init(conn)
    return conn


def cmd_process(args):
    snapshot = rules_mod.load_rules(args.rules)
    tenants = [t.strip() for t in args.tenants.split(",") if t.strip()] \
        if args.tenants else None
    processor = Processor(args.db, snapshot)
    try:
        result = processor.process(tenants=tenants,
                                   reevaluate=args.reevaluate,
                                   resume=args.resume,
                                   stage_only=args.stage_only)
    except ProcessingRefused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 3
    except store.VerificationError as exc:
        print(f"verification failed, nothing published: {exc}", file=sys.stderr)
        return 4
    conn = _connect(args.db)
    result["status_report"] = store.status(conn)
    conn.close()
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_result(result)
    return 0


def cmd_status(args):
    conn = _connect(args.db)
    store.require_landing(conn)
    report = store.status(conn)
    runs = [dict(r) for r in conn.execute(
        "SELECT * FROM quality_runs ORDER BY run_id DESC LIMIT 5")]
    conn.close()
    report["recent_runs"] = runs
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    print(_format_status(report))
    return 0


def cmd_trace(args):
    conn = _connect(args.db)
    store.require_landing(conn)
    report = _trace(conn, args.tenant, args.accession)
    conn.close()
    if not report["inputs"] and not report["events"] and not report["current"]:
        print(f"no trace found for {args.tenant}/{args.accession}",
              file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    print(_format_trace(report))
    return 0


def cmd_publish(args):
    conn = _connect(args.db)
    store.require_landing(conn)
    staging = store.staging_generation(conn)
    if not staging:
        print("no staging generation to publish")
        conn.close()
        return 0
    gen_id = staging["generation_id"]
    readiness = store.staging_readiness(conn, gen_id)
    if not readiness["publishable"]:
        print(f"generation {gen_id} is not publishable: {readiness['reason']}",
              file=sys.stderr)
        print("use 'process --resume' to finish/rebuild it or "
              "'discard-staging' to drop it", file=sys.stderr)
        conn.close()
        return 4
    try:
        counts = store.verify_generation(conn, gen_id)
        result = store.publish_generation(conn, gen_id, expected_counts=counts)
    except store.IncompleteBuild as exc:
        print(f"generation {gen_id} is incomplete: {exc}", file=sys.stderr)
        print("use 'process --resume' to finish it or 'discard-staging' to drop it",
              file=sys.stderr)
        conn.close()
        return 4
    except store.VerificationError as exc:
        print(f"refusing to publish generation {gen_id}: {exc}",
              file=sys.stderr)
        conn.close()
        return 4
    print(f"generation {gen_id}: {result.get('reason', 'published')}")
    print(_format_status(store.status(conn)))
    conn.close()
    return 0


def cmd_discard_staging(args):
    conn = _connect(args.db)
    store.require_landing(conn)
    staging = store.staging_generation(conn)
    if not staging:
        print("no staging generation to discard")
        return 0
    store.discard_staging(conn, staging["generation_id"],
                          "discarded by explicit command")
    active = store.active_generation(conn)
    print(f"discarded generation {staging['generation_id']}; "
          f"published generation is "
          f"{active['generation_id'] if active else None}")
    conn.close()
    return 0


def cmd_rules_check(args):
    try:
        snapshot = rules_mod.load_rules(args.rules)
    except rules_mod.RuleError as exc:
        print(f"invalid rules: {exc}", file=sys.stderr)
        return 1
    print(f"rule_version={snapshot.version} sha256={snapshot.sha256}")
    return 0


def cmd_generations(args):
    conn = _connect(args.db)
    store.require_landing(conn)
    rows = [dict(r) for r in conn.execute(
        "SELECT generation_id, status, rule_version, rule_sha256,"
        " base_generation_id, created_at, published_at, counts, build_phase,"
        " message FROM quality_generations ORDER BY generation_id DESC")]
    conn.close()
    if args.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
        return 0
    for row in rows:
        counts = row["counts"] or "-"
        phase = row["build_phase"] or "legacy"
        print(f"generation {row['generation_id']}: {row['status']}"
              f" phase={phase}"
              f" rule={row['rule_version']}"
              f" base={row['base_generation_id']}"
              f" published_at={row['published_at']} counts={counts}")
    return 0


def _print_result(result):
    print(f"status={result['status']} rule_version="
          f"{result.get('status_report', {}).get('rule_version')}"
          f" generation={result.get('generation_id')}"
          f" published_generation={result.get('active_generation')}"
          f" new_inputs={result.get('new_inputs', 0)}"
          f" events={result.get('events', 0)}")
    if result.get("processed_tenants"):
        print("processed tenants: " + ", ".join(result["processed_tenants"]))
    if result.get("skipped_tenants"):
        print("tenants with no landing rows: "
              + ", ".join(result["skipped_tenants"]))
    counts = result.get("counts")
    if counts:
        print(f"clean={counts['clean']} quarantine={counts['quarantine']}"
              f" retracted={counts['retracted']}")
        for tenant, bucket in sorted(counts.get("tenants", {}).items()):
            print(f"  {tenant}: clean={bucket['clean']}"
                  f" quarantine={bucket['quarantine']}"
                  f" retracted={bucket['retracted']}")


def _format_status(report):
    active = report.get("published_generation")
    lines = []
    if not active:
        lines.append("published generation: none")
    else:
        lines.append(
            f"published generation: {active['generation_id']}"
            f"  rule_version={active['rule_version']}"
            f"  rule_sha256={active['rule_sha256']}"
            f"  published_at={active['published_at']}")
    staging = report.get("staging_generation")
    lines.append("staging generation: "
                 + (f"{staging['generation_id']}"
                    f" (rule {staging['rule_version']})" if staging else "none"))
    counts = report.get("counts") or {}
    lines.append(f"clean={counts.get('clean', 0)}"
                 f" quarantine={counts.get('quarantine', 0)}"
                 f" retracted={counts.get('retracted', 0)}")
    for tenant, bucket in sorted((counts.get("tenants") or {}).items()):
        lines.append(f"  {tenant}: clean={bucket['clean']}"
                     f" quarantine={bucket['quarantine']}"
                     f" retracted={bucket['retracted']}")
    for run in report.get("recent_runs", []):
        lines.append(
            f"  run {run['run_id']}: {run['status']}"
            f" rule={run['rule_version']} gen={run['generation_id']}"
            f" new_inputs={run['new_inputs']} at {run['started_at']}")
    return "\n".join(lines)


def _trace(conn, tenant, accession):
    inputs = [dict(r) for r in conn.execute(
        "SELECT * FROM quality_inputs WHERE tenant=? AND accession=?"
        " ORDER BY revision", (tenant, accession))]
    events = [dict(r) for r in conn.execute(
        "SELECT * FROM quality_events WHERE tenant=? AND accession=?"
        " ORDER BY event_id", (tenant, accession))]
    current = None
    for view, state in (("quality_current_clean", "clean"),
                        ("quality_current_quarantine", "quarantine"),
                        ("quality_current_retracted", "retracted")):
        row = conn.execute(
            f"SELECT * FROM {view} WHERE tenant=? AND accession=?",
            (tenant, accession)).fetchone()
        if row:
            current = dict(row)
            current["state"] = state
            current["view"] = view
            break
    if current is not None and current["state"] == "retracted":
        # retraction rows carry no rule version; report the published one
        gen = store.active_generation(conn)
        current["rule_version"] = gen["rule_version"] if gen else None
    for entry in events:
        entry["details"] = _json(entry.get("details"))
    for entry in inputs:
        entry.pop("payload", None)  # full payload available via --json
        entry["payload"] = _payload(conn, tenant, accession, entry["revision"])
    raw_ids = sorted({r["raw_page_id"] for r in inputs
                      if r.get("raw_page_id") is not None})
    raw_pages = []
    for raw_id in raw_ids:
        row = conn.execute("SELECT id, tenant, status, url, fetched_at,"
                           " scan_until FROM raw_pages WHERE id=?",
                           (raw_id,)).fetchone()
        if row:
            raw_pages.append(dict(row))
    return {"tenant": tenant, "accession": accession, "inputs": inputs,
            "events": events, "current": current, "raw_pages": raw_pages}


def _payload(conn, tenant, accession, revision):
    row = conn.execute(
        "SELECT payload FROM quality_inputs WHERE tenant=? AND accession=?"
        " AND revision=?", (tenant, accession, revision)).fetchone()
    return _json(row["payload"]) if row else None


def _json(text):
    if text is None:
        return None
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return text


def _format_trace(report):
    lines = [f"trace {report['tenant']}/{report['accession']}"]
    current = report.get("current")
    if current:
        lines.append(f"current: {current['state']}"
                     f" revision={current.get('revision')}"
                     f" generation={current.get('generation_id')}"
                     f" rule_version={current.get('rule_version')}")
        if current["state"] == "quarantine":
            lines.append(f"  rule_ids={_json(current['rule_ids'])}")
            for reason in _json(current["reasons"]) or []:
                lines.append(f"  - {reason.get('rule_id')} "
                             f"{reason.get('field')}={reason.get('value')!r}:"
                             f" {reason.get('message')}")
    else:
        lines.append("current: not visible in the published generation")
    lines.append("landing versions:")
    for entry in report["inputs"]:
        lines.append(f"  revision {entry['revision']}:"
                     f" retracted={entry['retracted']}"
                     f" raw_page_id={entry['raw_page_id']}"
                     f" source_rowid={entry['source_rowid']}")
        lines.append(f"    payload={json.dumps(entry['payload'], sort_keys=True)}")
    lines.append("transitions:")
    for entry in report["events"] or []:
        lines.append(f"  gen {entry['generation_id']} {entry['event_type']}:"
                     f" {entry['from_state']} -> {entry['to_state']}"
                     f" revision={entry['revision']}"
                     f" rule={entry['rule_version']}"
                     f" (was {entry['rule_version_before']})")
        after = (entry["details"] or {}).get("after") or {}
        if after.get("rule_ids"):
            lines.append(f"    rule_ids={after['rule_ids']}")
            for reason in after.get("reasons") or []:
                lines.append(f"    - {reason.get('rule_id')}:"
                             f" {reason.get('message')}")
    if report["raw_pages"]:
        lines.append("source raw pages:")
        for page in report["raw_pages"]:
            lines.append(f"  raw_page_id={page['id']} status={page['status']}"
                         f" url={page['url']}")
    return "\n".join(lines)


def main(argv=None):
    p = argparse.ArgumentParser(
        description="Quality isolation and projection for harvested results")
    sub = p.add_subparsers(dest="command")

    def add_common(sp):
        sp.add_argument("--db", required=True, help="landing SQLite database")

    proc = sub.add_parser("process", help="evaluate inputs and publish")
    add_common(proc)
    proc.add_argument("--rules", default=os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "quality/rules/v1.json"))
    proc.add_argument("--tenants", help="comma-separated tenants (default all)")
    proc.add_argument("--reevaluate", action="store_true",
                      help="re-judge all already-read inputs with this rules "
                           "version (rule upgrade)")
    proc.add_argument("--resume", action="store_true",
                      help="resume an interrupted staging generation")
    proc.add_argument("--stage-only", action="store_true",
                      help="prepare and verify a generation without publishing")
    proc.add_argument("--json", action="store_true")
    proc.set_defaults(func=cmd_process)

    st = sub.add_parser("status", help="show generation, rules and counts")
    add_common(st)
    st.add_argument("--json", action="store_true")
    st.set_defaults(func=cmd_status)

    tr = sub.add_parser("trace", help="trace one result end to end")
    add_common(tr)
    tr.add_argument("--tenant", required=True)
    tr.add_argument("--accession", required=True)
    tr.add_argument("--json", action="store_true")
    tr.set_defaults(func=cmd_trace)

    pub = sub.add_parser("publish", help="publish a staged generation")
    add_common(pub)
    pub.set_defaults(func=cmd_publish)

    dis = sub.add_parser("discard-staging",
                         help="safely drop an unpublished generation")
    add_common(dis)
    dis.set_defaults(func=cmd_discard_staging)

    chk = sub.add_parser("rules-check", help="validate a rules file")
    chk.add_argument("--rules", required=True)
    chk.set_defaults(func=cmd_rules_check)

    gens = sub.add_parser("generations",
                          help="list generation history and statuses")
    add_common(gens)
    gens.add_argument("--json", action="store_true")
    gens.set_defaults(func=cmd_generations)

    args = p.parse_args(argv)
    if not getattr(args, "command", None):
        p.print_help()
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
