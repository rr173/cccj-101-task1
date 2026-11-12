#!/usr/bin/env python3
"""Configurable fetch entry point for the research-results harvester.

Example:
    python run_fetch.py --server http://127.0.0.1:8734 --db out/harvest.db \
        --tenants alpha,beta --rounds 2 --page-size 50 --retry-scale 0.05

Prints each tenant's confirmed interval and version counts; exit code is 0
only if every tenant finished its rounds successfully.
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collector import db
from collector.collector import Collector


def main(argv=None):
    p = argparse.ArgumentParser(description="Incremental research-results harvester")
    p.add_argument("--server", required=True, help="base URL of the scan API")
    p.add_argument("--db", required=True, help="SQLite database path")
    p.add_argument("--tenants", required=True, help="comma-separated tenant ids")
    p.add_argument("--rounds", type=int, default=1,
                   help="scan rounds per tenant (each round resumes or re-opens)")
    p.add_argument("--poll-interval", type=float, default=0.0,
                   help="virtual seconds to wait between rounds")
    p.add_argument("--page-size", type=int, default=100)
    p.add_argument("--in-flight", type=int, default=4,
                   help="max concurrent in-flight HTTP requests across tenants")
    p.add_argument("--retry-scale", type=float, default=1.0,
                   help="scale factor applied to Retry-After/backoff waits")
    p.add_argument("--max-transient", type=int, default=8,
                   help="max consecutive 503/network retries per request")
    p.add_argument("--start", type=float, default=0.0,
                   help="initial modified_at lower bound for the first round")
    p.add_argument("--request-timeout", type=float, default=10.0)
    args = p.parse_args(argv)

    tenants = [t.strip() for t in args.tenants.split(",") if t.strip()]
    collector = Collector(args.server, args.db, tenants,
                          page_size=args.page_size, in_flight=args.in_flight,
                          retry_scale=args.retry_scale,
                          max_transient=args.max_transient, start=args.start,
                          request_timeout=args.request_timeout)
    try:
        results = collector.run(rounds=args.rounds,
                                poll_interval=args.poll_interval)
    except KeyboardInterrupt:
        collector.cancel()
        collector.wait(5)
        results = collector.results

    conn = db.connect(args.db)
    reports = [db.report(conn, t) for t in tenants]
    conn.close()

    header = (f"{'tenant':<16} {'status':<12} {'confirmed_interval':<28}"
              f" {'rounds':>6} {'versions':>8} {'accessions':>10}"
              f" {'tombstones':>10} {'pages':>5}")
    print(header)
    print("-" * len(header))
    for t, r in zip(tenants, reports):
        st = results.get(t)
        status = st.status if st else "unknown"
        iv = r["confirmed_interval"]
        ivs = f"[{iv[0]}, {iv[1]}]" if iv else "-"
        print(f"{t:<16} {status:<12} {ivs:<28} {r['rounds_completed']:>6}"
              f" {r['versions']:>8} {r['accessions']:>10}"
              f" {r['tombstones']:>10} {r['pages']:>5}")
    print(json.dumps({"tenants": reports,
                      "status": {t: results[t].status for t in tenants
                                 if t in results}}, indent=2, sort_keys=True))

    ok = all(results.get(t) and results[t].status == "ok" for t in tenants)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
