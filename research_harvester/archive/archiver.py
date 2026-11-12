"""Orchestration for raw-response retention (write -> verify -> register -> clear).

The publish step spans two resources -- files and SQLite -- so it is split into
idempotent stages instead of one atomic action:

    1. render the shard bytes for a tenant/fetch-date group from the frozen
       snapshot,
    2. write them to a deterministic, snapshot-addressed path (temp + fsync +
       link, never overwriting),
    3. re-read the file *from disk* and recompute the whole-file and per-page
       digests,
    4. register the verified shard and its page ids in SQLite (one transaction),
    5. clear the bodies in raw_pages (one transaction; every page that still has
       a body is counted).

Crash windows and recovery
--------------------------
* after step 2, before step 4: the batch is ``shard_written``.  Rerunning with
  the same frozen snapshot rebuilds the identical bytes at the identical path,
  sees the file already present, verifies it, and adopts it -- no data loss, no
  duplicate registration.
* after step 4, before step 5: the batch is ``registered``.  The rerun skips
  writing (the shard is durable and verified) and simply clears the bodies.

A shard whose on-disk content does not match the expected digest is never
overwritten, never registered, and its raw bodies are never cleared.  The error
names the tenant, path and (when known) page id so an operator can locate it.
Tenant isolation: verification failures are collected per tenant and the run
continues with other tenants that have not started yet.
"""
from __future__ import annotations

import os
import time

from . import shard as shard_mod
from . import store

PAGE_GROUP_LIMIT = 200


def make_crash_barrier():
    """Test-only fault barrier (inert unless RH_ARCHIVE_CRASH_POINT is set)."""
    point = os.environ.get("RH_ARCHIVE_CRASH_POINT")
    if not point:
        return None
    marker = os.environ.get("RH_ARCHIVE_CRASH_MARKER")
    wait_file = os.environ.get("RH_ARCHIVE_CRASH_WAIT_FILE")

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


def shard_path(archive_dir, tenant, fetch_date, shard_no, snapshot_sig,
               first_page_id, last_page_id):
    """Deterministic snapshot-addressed path.

    The path embeds the batch snapshot signature and the page range, so the
    same input snapshot always resolves to the same file while a different
    snapshot can never collide with (or overwrite) an old one.
    """
    safe_tenant = "".join(c if c.isalnum() or c in "._-" else "_"
                          for c in str(tenant)) or "_"
    return os.path.join(
        archive_dir, safe_tenant, fetch_date,
        f"part-{int(shard_no):04d}-{snapshot_sig[:16]}"
        f"-{first_page_id}-{last_page_id}.jsonl.gz")


class Archiver:
    def __init__(self, db_path, archive_dir, *, page_group_limit=200,
                 crash_barrier=None, signer=None):
        self.db_path = db_path
        self.archive_dir = os.path.abspath(archive_dir)
        self.page_group_limit = max(1, int(page_group_limit))
        self.crash_barrier = (crash_barrier if crash_barrier is not None
                              else make_crash_barrier())
        self.signer = signer or os.environ.get("RH_ARCHIVE_SIGNER")

    # -- connections -----------------------------------------------------
    def _open(self):
        conn = store.connect(self.db_path)
        store.require_landing(conn)
        store.init(conn)
        return conn

    def _barrier(self, name):
        if self.crash_barrier is not None:
            self.crash_barrier(name)

    # -- planning --------------------------------------------------------
    def freeze_snapshot(self, conn, *, cutoff_at, tenants=None, fetch_date=None):
        """Freeze the cutoff and max page id *once* at batch start."""
        if not store.archive_tables_present(conn):
            store.init(conn)
        resumable = store.find_resumable_batch(
            conn, cutoff_at=cutoff_at, tenants=tenants, fetch_date=fetch_date)
        if resumable is not None:
            # an interrupted attempt owns this snapshot: keep its frozen
            # cutoff AND max page id, do not re-freeze around newer rows
            return resumable
        max_page_id = store.current_max_page_id(conn)
        batch, _existing = store.create_batch(
            conn, cutoff_at=cutoff_at, max_page_id=max_page_id,
            tenants=tenants, fetch_date=fetch_date, signer=self.signer)
        return batch

    def _groups(self, conn, batch, *, pending_only=True):
        """Group snapshot pages by tenant and UTC fetch date, page order."""
        pages = (store.pending_pages if pending_only else store.expired_pages)(
            conn, cutoff_at=batch["cutoff_at"],
            max_page_id=batch["max_page_id"],
            tenants=(batch["filters"] and _load_filters(batch)["tenants"]),
            fetch_date=_load_filters(batch)["fetch_date"])
        grouped = {}
        for page in pages:
            grouped.setdefault(
                (page["tenant"], store.utc_date(page["fetched_at"])),
                []).append(page)
        for key in grouped:
            grouped[key].sort(key=lambda p: (float(p["fetched_at"]), int(p["id"])))
        return grouped

    def _already_registered(self, conn, batch):
        return [dict(r) for r in conn.execute(
            "SELECT * FROM archive_shards WHERE batch_id=?"
            " ORDER BY tenant, fetch_date, shard_no", (batch["batch_id"],))]

    # -- per-shard write/verify/register ---------------------------------
    def _verify_registered(self, conn, filters):
        """Re-verify registered shards in scope; returns any failures.

        A corrupt/corruptable registered shard is returned as a failure for
        its tenant (not raised) so that other tenants can still converge; the
        corrupt path is never rewritten and its bodies are never cleared.
        """
        failures = []
        for row in store.shard_rows(conn, tenants=filters.get("tenants"),
                                    fetch_date=filters.get("fetch_date")):
            try:
                verified = shard_mod.read_shard(row["path"], verify=True)
                if verified["file_sha256"] != row["file_sha256"]:
                    raise shard_mod.ShardIntegrityError(
                        "registered file digest changed", path=row["path"])
                for record in verified["pages"]:
                    owner = conn.execute(
                        "SELECT body_sha256 FROM archive_pages WHERE page_id=?",
                        (int(record["page_id"]),)).fetchone()
                    if owner is not None and owner["body_sha256"] != \
                            record.get("body_sha256"):
                        raise shard_mod.ShardIntegrityError(
                            "page body digest disagrees with registration",
                            path=row["path"],
                            page_id=int(record["page_id"]))
            except (shard_mod.ShardIntegrityError,
                    shard_mod.ShardFormatError) as exc:
                failures.append({
                    "tenant": row["tenant"], "path": row["path"],
                    "page_id": getattr(exc, "page_id", None),
                    "detail": str(exc),
                })
        return failures

    def _ensure_shard(self, conn, batch, tenant, fetch_date, shard_no, pages):
        """Render -> write (or adopt an existing identical file) -> verify."""
        first, last = int(pages[0]["id"]), int(pages[-1]["id"])
        path = shard_path(self.archive_dir, tenant, fetch_date, shard_no,
                          batch["snapshot_sig"], first, last)
        meta = {"tenant": tenant, "fetch_date": fetch_date,
                "shard_no": shard_no, "cutoff_at": batch["cutoff_at"],
                "max_page_id": batch["max_page_id"]}
        compressed, manifest = shard_mod.build_shard(pages, **meta)
        expected_ids = sorted(int(p["id"]) for p in pages)

        if os.path.exists(path):
            # same snapshot -> same deterministic path.  Adopt it only if it
            # verifies AND carries exactly the pages we intended to write.
            sidecar = shard_mod.read_sidecar(path)
            try:
                verified = shard_mod.read_shard(path, verify=True)
            except shard_mod.ShardFormatError as exc:
                raise shard_mod.ShardIntegrityError(
                    f"existing shard {path} cannot be read: {exc}", path=path)
            except shard_mod.ShardIntegrityError:
                raise
            if sidecar is not None and \
                    sidecar.get("file_sha256") != verified["file_sha256"]:
                raise shard_mod.ShardIntegrityError(
                    f"existing shard {path} no longer matches its sidecar "
                    f"digest {sidecar.get('file_sha256')}", path=path)
            if sorted(int(p["page_id"]) for p in verified["pages"]) != \
                    expected_ids:
                raise shard_mod.ShardIntegrityError(
                    f"existing shard {path} covers pages "
                    f"{verified['manifest'].get('first_page_id')}.."
                    f"{verified['manifest'].get('last_page_id')} instead of "
                    f"the expected {first}..{last}; refusing to overwrite",
                    path=path)
            if verified["manifest"].get("content_sha256") != \
                    manifest.get("content_sha256"):
                raise shard_mod.ShardIntegrityError(
                    f"existing shard {path} content differs from the frozen "
                    f"snapshot; refusing to overwrite", path=path)
            if sidecar is None:
                # crash between data file and sidecar: complete the pair
                shard_mod.write_sidecar(
                    path, shard_mod.manifest_from_file(
                        path, verified["file_sha256"],
                        os.path.getsize(path)))
            return {"path": path, "manifest": verified["manifest"],
                    "file_sha256": verified["file_sha256"],
                    "compressed_bytes": os.path.getsize(path),
                    "adopted": True}

        manifest_written, file_digest, size = shard_mod.write_shard(
            path, pages, **meta)
        # verify strictly from disk before anything is registered
        verified = shard_mod.read_shard(path, verify=True)
        if verified["file_sha256"] != file_digest:
            raise shard_mod.ShardIntegrityError(
                f"shard {path} changed immediately after writing", path=path)
        return {"path": path, "manifest": verified["manifest"],
                "file_sha256": verified["file_sha256"],
                "compressed_bytes": size, "adopted": False}

    def _register(self, conn, batch, tenant, fetch_date, shard_no, artifact,
                  pages):
        row, created = store.register_shard(
            conn, batch["batch_id"], tenant=tenant, fetch_date=fetch_date,
            shard_no=shard_no, path=artifact["path"],
            file_sha256=artifact["file_sha256"],
            compressed_bytes=artifact["compressed_bytes"],
            manifest=artifact["manifest"],
            page_ids=[int(p["id"]) for p in pages],
            cutoff_at=batch["cutoff_at"], max_page_id=batch["max_page_id"])
        return row, created

    # -- main entry ------------------------------------------------------
    def run(self, *, cutoff_at, tenants=None, fetch_date=None,
            clear=True):
        """Archive expired bodies for this frozen snapshot.

        Returns a report dict; ``ok`` is false when any tenant hit an integrity
        failure (those tenants are left untouched and localised).
        """
        conn = self._open()
        try:
            batch = self.freeze_snapshot(
                conn, cutoff_at=cutoff_at, tenants=tenants,
                fetch_date=fetch_date)
            result = self._run_batch(conn, batch, clear=clear)
            return result
        finally:
            conn.close()

    def _run_batch(self, conn, batch, *, clear):
        filters = _load_filters(batch)
        # Never act on a snapshot whose already-registered shards no longer
        # verify: a corrupt shard must stop the run and be reported, not be
        # silently ignored or overwritten.
        registered_shards = self._already_registered(conn, batch)
        verify_failures = self._verify_registered(conn, filters)
        if verify_failures:
            store.update_batch(conn, batch["batch_id"],
                               status=store.PHASE_FAILED,
                               phase=store.PHASE_FAILED,
                               message="; ".join(f["detail"]
                                                 for f in verify_failures))

        if batch["phase"] in (store.PHASE_REGISTERED, store.PHASE_CLEARED):
            # crash window #2 or a plain replay: never rewrite or re-register
            if batch["phase"] == store.PHASE_REGISTERED and clear \
                    and not verify_failures:
                self._barrier("before_clear")
                cleared = store.clear_batch_bodies(conn, batch["batch_id"])
            else:
                cleared = 0
            return self._report(conn, batch, ok=not verify_failures,
                                cleared=cleared, failures=verify_failures,
                                adopted=0, written=0,
                                resumed_phase=batch["phase"])

        failures = []
        written = 0
        adopted = 0
        # A previous attempt may have written shards and even registered some
        # before dying; resume from what is durable rather than trusting the
        # phase label (planned/shard_written/failed are all resumable).
        if batch["phase"] in (store.PHASE_SHARD_WRITTEN, store.PHASE_FAILED):
            store.update_batch(conn, batch["batch_id"],
                               status=store.PHASE_PLANNED,
                               phase=store.PHASE_PLANNED,
                               message="resuming an interrupted batch")
        groups = self._groups(conn, batch, pending_only=True)
        requested = filters["tenants"] or sorted({k[0] for k in groups})
        # shard ordinals continue after shards a previous attempt registered
        next_shard = {}
        for shard_row in registered_shards:
            key = (shard_row["tenant"], shard_row["fetch_date"])
            next_shard[key] = max(next_shard.get(key, 0),
                                  int(shard_row["shard_no"]) + 1)
        bad_tenants = {f["tenant"] for f in verify_failures}
        failures.extend(verify_failures)
        complete = not verify_failures
        for tenant in requested:
            if tenant in bad_tenants:
                continue  # never clear or re-write around a corrupt shard
            try:
                for (group_tenant, group_date), pages in sorted(
                        groups.items()):
                    if group_tenant != tenant:
                        continue
                    base_no = next_shard.get((tenant, group_date), 0)
                    for offset in range(0, len(pages),
                                        self.page_group_limit):
                        chunk = pages[offset:offset + self.page_group_limit]
                        shard_no = base_no + offset // self.page_group_limit
                        artifact = self._ensure_shard(
                            conn, batch, tenant, group_date, shard_no, chunk)
                        if artifact["adopted"]:
                            adopted += 1
                        else:
                            written += 1
                            store.update_batch(
                                conn, batch["batch_id"], status=store.PHASE_SHARD_WRITTEN,
                                phase=store.PHASE_SHARD_WRITTEN,
                                message=f"shard written for {tenant} {group_date}")
                        # crash window #1: file is durable, registration is not
                        self._barrier("after_shard_write")
                        self._register(conn, batch, tenant, group_date,
                                       shard_no, artifact, chunk)
            except shard_mod.ShardIntegrityError as exc:
                complete = False
                failures.append({
                    "tenant": tenant, "path": exc.path,
                    "page_id": exc.page_id,
                    "detail": str(exc),
                })
                # this tenant stops here; its bodies are preserved.  Other
                # tenants that have not started yet keep going.
                continue

        registered = self._already_registered(conn, batch)
        complete = complete and not failures
        if not verify_failures:
            store.update_batch(
                conn, batch["batch_id"],
                status=(store.PHASE_FAILED if failures
                        else store.PHASE_REGISTERED),
                phase=(store.PHASE_FAILED if failures
                       else store.PHASE_REGISTERED),
                message=("registered %d shards; %d tenant(s) failed"
                         % (len(registered), len(failures))))
        cleared = 0
        if clear and complete:
            # crash window #2: registration durable, bodies still in SQLite
            self._barrier("before_clear")
            cleared = store.clear_batch_bodies(conn, batch["batch_id"])
        elif clear and registered:
            # converge the successfully archived tenants only
            cleared = self._clear_registered_tenants(conn, batch, failures)
        if failures:
            store.update_batch(conn, batch["batch_id"], status=store.PHASE_FAILED,
                               phase=store.PHASE_FAILED,
                               message="; ".join(f["detail"] for f in failures))
        return self._report(conn, batch, ok=not failures, cleared=cleared,
                            failures=failures, adopted=adopted, written=written)

    def _clear_registered_tenants(self, conn, batch, failures):
        """Clear only tenants with no integrity failure (partial progress)."""
        bad_tenants = {f["tenant"] for f in failures}
        page_ids = [int(r["page_id"]) for r in conn.execute(
            "SELECT page_id, tenant FROM archive_pages WHERE batch_id=?",
            (batch["batch_id"],))
            if r["tenant"] not in bad_tenants]
        if not page_ids:
            return 0
        now = time.time()
        cleared = 0
        with conn:
            for page_id in page_ids:
                updated = conn.execute(
                    "UPDATE raw_pages SET body=NULL WHERE id=? AND body IS NOT NULL",
                    (page_id,)).rowcount
                cleared += 1 if updated else 0
                conn.execute(
                    "UPDATE archive_pages SET cleared_at=COALESCE(cleared_at, ?)"
                    " WHERE page_id=?", (now, page_id))
            conn.execute(
                "UPDATE archive_shards SET cleared_at=COALESCE(cleared_at, ?)"
                " WHERE batch_id=?", (now, batch["batch_id"]))
            store.finalize_partial_clear(conn, batch["batch_id"])
        return cleared

    def _report(self, conn, batch, *, ok, cleared, failures, **extra):
        refreshed = store.get_batch(conn, batch["batch_id"])
        shards = self._already_registered(conn, refreshed)
        pages = store.registered_pages_for_batch(conn, refreshed["batch_id"])
        report = {
            "batch_id": refreshed["batch_id"],
            "status": refreshed["status"],
            "phase": refreshed["phase"],
            "cutoff_at": refreshed["cutoff_at"],
            "max_page_id": refreshed["max_page_id"],
            "tenants": _load_filters(refreshed)["tenants"],
            "fetch_date": _load_filters(refreshed)["fetch_date"],
            "shards": [
                {key: s[key] for key in (
                    "tenant", "fetch_date", "shard_no", "path", "file_sha256",
                    "compressed_bytes", "page_count", "first_page_id",
                    "last_page_id", "content_sha256", "cleared_at")}
                for s in shards],
            "registered_pages": len(pages),
            "cleared_pages": sum(1 for p in pages if p["cleared_at"]),
            "cleared_now": cleared,
            "failures": failures,
            "ok": ok,
        }
        report.update(extra)
        return report


def _load_filters(batch):
    import json
    try:
        value = json.loads(batch.get("filters") or "{}")
    except (ValueError, TypeError):
        value = {}
    return {"tenants": value.get("tenants"),
            "fetch_date": value.get("fetch_date")}


# ---------------------------------------------------------------------------
# read-side helpers (used by trace and by the verify/list commands)
# ---------------------------------------------------------------------------
def load_page_body(conn, page_id):
    """Return the original response body for a page, archived or online.

    Returns (body, source) where source is ``sqlite`` or ``archive``.  A
    registered page whose shard fails verification raises
    ShardIntegrityError with the path/page id -- the caller must refuse rather
    than return a partial answer.
    """
    row = conn.execute(
        "SELECT id, body FROM raw_pages WHERE id=?", (int(page_id),)).fetchone()
    if row is None:
        return None, "missing"
    if row["body"] is not None:
        return row["body"], "sqlite"
    owner = store.page_owner(conn, page_id)
    if owner is None:
        # body was cleared outside this layer; nothing can restore it
        return None, "missing"
    shard = conn.execute(
        "SELECT path, file_sha256, manifest_sha256 FROM archive_shards"
        " WHERE shard_id=?", (owner["shard_id"],)).fetchone()
    if shard is None:
        raise shard_mod.ShardIntegrityError(
            f"page {page_id} references a missing shard registration",
            path=None, page_id=int(page_id))
    verified = shard_mod.read_shard(shard["path"], verify=True)
    for record in verified["pages"]:
        if int(record["page_id"]) == int(page_id):
            if record.get("body_sha256") != owner["body_sha256"]:
                raise shard_mod.ShardIntegrityError(
                    f"page {page_id} body digest disagrees with registration",
                    path=shard["path"], page_id=int(page_id))
            return record.get("body", ""), "archive"
    raise shard_mod.ShardIntegrityError(
        f"page {page_id} is registered to shard {shard['path']} but absent "
        f"from the file", path=shard["path"], page_id=int(page_id))
