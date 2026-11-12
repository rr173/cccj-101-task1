"""Read-side operations: retention preview, shard listing and verification.

None of these functions write to ``archive_*``, ``raw_pages``,
``scan_state``, ``watermarks`` or any ``quality_*`` table.  The preview only
computes; listing and verification only read files and tables.
"""
from __future__ import annotations

import json
import os

from . import shard as shard_mod
from . import store


def preview(conn, *, cutoff_at, tenants=None, fetch_date=None,
            page_group_limit=200):
    """Compute releasable capacity and shard counts without touching anything.

    Returns per-tenant buckets plus totals.  Only pages that are expired by
    the inclusive cutoff, still hold a body and are not already registered to
    a shard are counted as releasable.
    """
    pages = store.pending_pages(
        conn, cutoff_at=cutoff_at,
        max_page_id=store.current_max_page_id(conn),
        tenants=tenants, fetch_date=fetch_date)
    buckets = {}
    for page in pages:
        tenant = page["tenant"]
        bucket = buckets.setdefault(tenant, {
            "tenant": tenant,
            "releasable_pages": 0,
            "releasable_bytes": 0,
            "shards": 0,
            "groups": {},
            "first_page_id": None,
            "last_page_id": None,
        })
        size = len((page["body"] or "").encode("utf-8"))
        bucket["releasable_pages"] += 1
        bucket["releasable_bytes"] += size
        group = store.utc_date(page["fetched_at"])
        group_bucket = bucket["groups"].setdefault(
            group, {"fetch_date": group, "pages": 0, "bytes": 0, "shards": 0})
        group_bucket["pages"] += 1
        group_bucket["bytes"] += size
        group_bucket["shards"] = _ceil_div(group_bucket["pages"],
                                           page_group_limit)
        bucket["first_page_id"] = (int(page["id"])
                                   if bucket["first_page_id"] is None
                                   else min(bucket["first_page_id"],
                                            int(page["id"])))
        bucket["last_page_id"] = (int(page["id"])
                                  if bucket["last_page_id"] is None
                                  else max(bucket["last_page_id"],
                                           int(page["id"])))
    for bucket in buckets.values():
        bucket["shards"] = sum(g["shards"] for g in bucket["groups"].values())
    return {
        "cutoff_at": float(cutoff_at),
        "tenants": buckets,
        "totals": {
            "tenant_count": len(buckets),
            "releasable_pages": sum(b["releasable_pages"]
                                    for b in buckets.values()),
            "releasable_bytes": sum(b["releasable_bytes"]
                                    for b in buckets.values()),
            "shards": sum(b["shards"] for b in buckets.values()),
        },
        "wrote_files": False,
        "cleared_bodies": 0,
    }


def _ceil_div(value, divisor):
    return (int(value) + int(divisor) - 1) // int(divisor)


def list_shards(conn, *, tenant=None, fetch_date=None):
    """Shard rows registered in SQLite, filtered by tenant and UTC date."""
    return store.shard_rows(conn, tenant=tenant, fetch_date=fetch_date)


def verify_shards(conn, *, tenant=None, fetch_date=None, repair=False):
    """Recompute whole-file and per-page digests for matching shards.

    Returns counts of pages that are still online (body present in SQLite),
    moved out (body cleared and registered) and pages whose shard has an
    integrity problem.  ``repair`` is intentionally unsupported: a corrupt
    shard must never be overwritten, and that body must never be cleared.
    """
    shards = list_shards(conn, tenant=tenant, fetch_date=fetch_date)
    problems = []
    moved_out = 0
    shard_reports = []
    for shard in shards:
        path = shard["path"]
        entry = {"tenant": shard["tenant"], "fetch_date": shard["fetch_date"],
                 "shard_no": shard["shard_no"], "path": path,
                 "page_count": shard["page_count"],
                 "ok": True, "problem": None}
        if not os.path.exists(path):
            entry["ok"] = False
            entry["problem"] = "missing file"
            entry["problem_pages"] = shard["page_count"]
            problems.append(entry)
            shard_reports.append(entry)
            continue
        try:
            verified = shard_mod.read_shard(path, verify=True)
        except (shard_mod.ShardFormatError,
                shard_mod.ShardIntegrityError) as exc:
            entry["ok"] = False
            entry["problem"] = str(exc)
            entry["problem_pages"] = shard["page_count"]
            problems.append(entry)
            shard_reports.append(entry)
            continue
        sidecar = shard_mod.read_sidecar(path)
        if sidecar is not None and \
                sidecar.get("file_sha256") != verified["file_sha256"]:
            entry["ok"] = False
            entry["problem"] = (
                f"file digest {verified['file_sha256']} != sidecar "
                f"{sidecar.get('file_sha256')}")
            entry["problem_pages"] = shard["page_count"]
            problems.append(entry)
            shard_reports.append(entry)
            continue
        registered = set(json.loads(shard["page_ids"] or "[]"))
        if int(verified["manifest"].get("page_count", -1)) != len(registered):
            entry["ok"] = False
            entry["problem"] = ("file page count disagrees with registration")
            entry["problem_pages"] = shard["page_count"]
            problems.append(entry)
            shard_reports.append(entry)
            continue
        if not registered.issubset(
                {int(p["page_id"]) for p in verified["pages"]}) or \
                {int(p["page_id"]) for p in verified["pages"]} != registered:
            entry["ok"] = False
            entry["problem"] = "file pages disagree with registered page ids"
            entry["problem_pages"] = shard["page_count"]
            problems.append(entry)
            shard_reports.append(entry)
            continue
        if verified["file_sha256"] != shard["file_sha256"]:
            entry["ok"] = False
            entry["problem"] = (
                f"file digest {verified['file_sha256']} != registered "
                f"{shard['file_sha256']}")
            entry["problem_pages"] = shard["page_count"]
            problems.append(entry)
            shard_reports.append(entry)
            continue
        for record in verified["pages"]:
            page_id = int(record["page_id"])
            cleared = store.page_owner(conn, page_id)
            if cleared is not None and cleared["cleared_at"] is not None:
                moved_out += 1
        entry["file_sha256"] = verified["file_sha256"]
        shard_reports.append(entry)

    online = 0
    sql = "SELECT COUNT(*) c FROM raw_pages WHERE body IS NOT NULL"
    params = []
    if tenant:
        sql += " AND tenant=?"
        params.append(tenant)
    if fetch_date:
        rows = conn.execute(sql.replace("COUNT(*) c", "fetched_at"),
                            params).fetchall()
        online = sum(1 for r in rows
                     if store.utc_date(r["fetched_at"]) == fetch_date)
    else:
        online = conn.execute(sql, params).fetchone()["c"]

    return {
        "tenant": tenant,
        "fetch_date": fetch_date,
        "shards": shard_reports,
        "shard_count": len(shard_reports),
        "online_pages": online,
        "moved_out_pages": moved_out,
        "problem_shards": len(problems),
        "problem_pages": sum(p["problem_pages"] for p in problems),
        "problems": problems,
        "ok": not problems,
    }
