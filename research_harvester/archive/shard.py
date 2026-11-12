"""Shard file format for archived raw responses.

A shard is a gzip-compressed text file containing one JSON object per line.
The format has a fixed shape so integrity can be re-derived offline with
``gunzip | sha256sum`` and the per-page digests:

    line 0            manifest header (``kind: "rh-archive-manifest"``)
    line 1..n         one page record per line
    last line         footer with the whole-file digest

Header fields
    kind              "rh-archive-manifest"
    version           1
    tenant            owning tenant (a shard never mixes tenants)
    fetch_date        UTC date bucket, "YYYY-MM-DD"
    shard_no          per-batch shard ordinal starting at 0
    cutoff_at         inclusive UTC cutoff timestamp frozen at batch start
    max_page_id       largest raw_pages.id considered by the batch
    page_count        number of page records
    first_page_id / last_page_id
    content_sha256    digest of the canonical body of (page lines + footer)
    manifest_sha256   per-page digest map {page_id: sha256(body)}

Page record fields (one per line, in ascending page id order)
    kind              "page"
    page_id           raw_pages.id (the number result_versions references)
    tenant, scan_id, url, status, scan_until, fetched_at
    body_sha256       sha256 of the exact body text
    body              the original response body, byte-for-byte

Footer
    kind              "footer"
    content_sha256    digest of the canonical body of all page lines
    page_count        repeated page count (guards truncation)
    last_page_id      repeated last page id

The canonical digest input is the UTF-8 text of the page lines joined with
``\\n`` plus a trailing newline, no header/footer included.  It is stable
across gzip implementations, so the file can be verified without trusting the
metadata written next to it.
"""
from __future__ import annotations

import hashlib
import json
import os
import tempfile

FORMAT_VERSION = 1
KIND_MANIFEST = "rh-archive-manifest"
KIND_PAGE = "page"
KIND_FOOTER = "footer"


class ShardFormatError(Exception):
    """The shard cannot be parsed or is structurally inconsistent."""


class ShardIntegrityError(Exception):
    """The shard's content does not match its recorded digests."""

    def __init__(self, message, *, path=None, page_id=None):
        super().__init__(message)
        self.path = path
        self.page_id = page_id


def body_sha256(body):
    """Digest of one exact response body (newline-safe, UTF-8)."""
    return hashlib.sha256((body if body is not None else "").encode(
        "utf-8")).hexdigest()


def canonical_page_line(record):
    """Serialize one page record deterministically (canonical JSON line)."""
    return json.dumps(record, sort_keys=True, ensure_ascii=False,
                      separators=(",", ":"))


def content_digest(page_lines):
    """Whole-shard digest over the canonical page lines."""
    blob = "\n".join(page_lines)
    if page_lines:
        blob += "\n"
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _open_gzip(path, mode):
    import gzip
    return gzip.open(path, mode, encoding="utf-8", newline="\n")


def build_shard(records, *, tenant, fetch_date, shard_no, cutoff_at,
                max_page_id):
    """Render shard bytes and metadata for a list of page rows.

    ``records`` are landing rows (dicts with id/tenant/scan_id/url/status/
    scan_until/fetched_at/body).  Returns (payload_bytes, manifest_dict).
    """
    rows = sorted(records, key=lambda r: int(r["id"]))
    for row in rows:
        if row["tenant"] != tenant:
            raise ShardFormatError(
                f"shard tenant {tenant!r} got row from {row['tenant']!r}")
    page_lines = []
    per_page = {}
    for row in rows:
        digest = body_sha256(row["body"])
        record = {
            "kind": KIND_PAGE,
            "page_id": int(row["id"]),
            "tenant": row["tenant"],
            "scan_id": row["scan_id"],
            "url": row["url"],
            "status": int(row["status"]),
            "scan_until": row["scan_until"],
            "fetched_at": float(row["fetched_at"]),
            "body_sha256": digest,
            "body": row["body"] if row["body"] is not None else "",
        }
        page_lines.append(canonical_page_line(record))
        per_page[str(int(row["id"]))] = digest
    content = content_digest(page_lines)
    manifest = {
        "kind": KIND_MANIFEST,
        "version": FORMAT_VERSION,
        "tenant": tenant,
        "fetch_date": fetch_date,
        "shard_no": int(shard_no),
        "cutoff_at": float(cutoff_at),
        "max_page_id": int(max_page_id),
        "page_count": len(rows),
        "first_page_id": int(rows[0]["id"]) if rows else None,
        "last_page_id": int(rows[-1]["id"]) if rows else None,
        "content_sha256": content,
        "manifest_sha256": per_page,
    }
    footer = {
        "kind": KIND_FOOTER,
        "content_sha256": content,
        "page_count": len(rows),
        "last_page_id": int(rows[-1]["id"]) if rows else None,
    }
    text_lines = [canonical_page_line(manifest), *page_lines,
                  canonical_page_line(footer)]
    payload = ("\n".join(text_lines) + "\n").encode("utf-8")
    import gzip
    compressed = gzip.compress(payload, mtime=0)
    return compressed, manifest


def write_shard(path, records, **meta):
    """Atomically write a shard file; never overwrites an existing path.

    Returns (manifest, file_sha256, compressed_bytes).  The temporary file is
    renamed into place only after it has been fully flushed, so a crash leaves
    either no file or a complete one.  The compressed bytes are recorded in a
    sidecar manifest written after the data file, so the *pages* of a shard
    can be recovered from disk alone.
    """
    compressed, manifest = build_shard(records, **meta)
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    if os.path.exists(path):
        raise FileExistsError(f"refusing to overwrite existing shard: {path}")
    if os.path.exists(sidecar_path(path)):
        raise FileExistsError(
            f"refusing to write a shard beside an existing manifest: {path}")
    fd, tmp = tempfile.mkstemp(prefix=".shard-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(compressed)
            fh.flush()
            os.fsync(fh.fileno())
        # link() fails if the target exists, closing the check-then-write race
        try:
            os.link(tmp, path)
        except FileExistsError:
            raise FileExistsError(
                f"refusing to overwrite existing shard: {path}")
        finally:
            os.unlink(tmp)
        try:
            fopen = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fopen)
            finally:
                os.close(fopen)
        except OSError:
            pass
    except BaseException:
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass
        raise
    digest = file_sha256(path)
    sidecar = manifest_from_file(path, digest, len(compressed))
    write_sidecar(path, sidecar)
    return manifest, digest, len(compressed)


def sidecar_path(path):
    return path + ".manifest.json"


def manifest_from_file(path, digest, compressed_bytes):
    """Build the external manifest record (importable without SQLite)."""
    return {
        "path": path,
        "file_sha256": digest,
        "compressed_bytes": int(compressed_bytes),
    }


def write_sidecar(path, record):
    """Write the sidecar manifest atomically; never overwrite a different one."""
    target = sidecar_path(path)
    directory = os.path.dirname(os.path.abspath(target))
    fd, tmp = tempfile.mkstemp(prefix=".manifest-", dir=directory)
    payload = json.dumps(record, sort_keys=True,
                         ensure_ascii=False).encode("utf-8") + b"\n"
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        try:
            os.link(tmp, target)
        except FileExistsError:
            raise FileExistsError(
                f"refusing to overwrite existing manifest: {target}")
        finally:
            os.unlink(tmp)
    except BaseException:
        if os.path.exists(tmp):
            try:
                os.unlink(tmp)
            except OSError:
                pass
        raise
    return target


def read_sidecar(path):
    target = sidecar_path(path)
    if not os.path.exists(target):
        return None
    with open(target, "r", encoding="utf-8") as fh:
        return json.load(fh)


def read_shard(path, *, verify=True):
    """Parse a shard and re-derive every digest from the file itself.

    Returns a dict with ``manifest``, ``pages`` (list of page records) and
    ``file_sha256``.  Structural problems raise ShardFormatError; digest
    mismatches raise ShardIntegrityError carrying the path/page id.
    """
    try:
        with _open_gzip(path, "rt") as fh:
            text = fh.read()
    except (OSError, EOFError, Exception) as exc:  # gzip.BadGzipFile etc.
        if isinstance(exc, ShardIntegrityError):
            raise
        raise ShardFormatError(f"cannot read shard {path}: {exc}") from exc
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    if len(lines) < 2:
        raise ShardFormatError(f"shard {path} has no manifest/page lines")
    try:
        manifest = json.loads(lines[0])
        footer = json.loads(lines[-1])
    except ValueError as exc:
        raise ShardFormatError(f"shard {path} has malformed JSON lines: {exc}")
    if manifest.get("kind") != KIND_MANIFEST:
        raise ShardFormatError(f"shard {path} missing manifest header")
    if footer.get("kind") != KIND_FOOTER:
        raise ShardFormatError(f"shard {path} missing footer")
    page_lines = lines[1:-1]
    pages = []
    for line in page_lines:
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise ShardFormatError(
                f"shard {path} has a malformed page line: {exc}")
        if record.get("kind") != KIND_PAGE:
            raise ShardFormatError(
                f"shard {path} has an unexpected line kind")
        pages.append(record)
    if verify:
        content = content_digest(page_lines)
        if manifest.get("content_sha256") != content:
            raise ShardIntegrityError(
                f"shard {path}: manifest content digest does not match file",
                path=path)
        if footer.get("content_sha256") != content:
            raise ShardIntegrityError(
                f"shard {path}: footer content digest does not match file",
                path=path)
        if int(manifest.get("page_count", -1)) != len(pages):
            raise ShardIntegrityError(
                f"shard {path}: page count {manifest.get('page_count')}"
                f" != {len(pages)}", path=path)
        if footer.get("page_count") != len(pages):
            raise ShardIntegrityError(
                f"shard {path}: footer page count disagrees with file",
                path=path)
        expected = manifest.get("manifest_sha256") or {}
        if set(expected) != {str(int(p["page_id"])) for p in pages}:
            raise ShardIntegrityError(
                f"shard {path}: manifest page id map disagrees with file",
                path=path)
        for record in pages:
            page_id = int(record["page_id"])
            actual = body_sha256(record.get("body"))
            recorded = record.get("body_sha256")
            if actual != recorded:
                raise ShardIntegrityError(
                    f"shard {path}: page {page_id} digest mismatch",
                    path=path, page_id=page_id)
            if expected.get(str(page_id)) != actual:
                raise ShardIntegrityError(
                    f"shard {path}: page {page_id} disagrees with manifest",
                    path=path, page_id=page_id)
        first = int(pages[0]["page_id"]) if pages else None
        last = int(pages[-1]["page_id"]) if pages else None
        if manifest.get("first_page_id") != first or \
                manifest.get("last_page_id") != last:
            raise ShardIntegrityError(
                f"shard {path}: page id range disagrees with manifest",
                path=path)
    return {"manifest": manifest, "pages": pages,
            "file_sha256": file_sha256(path)}
