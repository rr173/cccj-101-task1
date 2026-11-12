"""Retention and archive layer for harvested raw HTTP responses (stdlib only).

The collector keeps every response body in ``raw_pages``.  This layer moves
*expired* bodies out of the main database into deterministic compressed JSONL
shards, while leaving the ``raw_pages`` row (status, URL, fetch time and page
number) and every ``result_versions.raw_page_id`` reference intact.

Design guarantees:

* a batch freezes its UTC cutoff and maximum page id when it starts; pages
  written later can never enter that batch,
* the cutoff is an **inclusive** upper bound: ``fetched_at <= cutoff_at``,
* shard paths are content-addressed, so the same input snapshot reuses a
  verified file and a different snapshot can never overwrite an old path,
* a file is only registered after its whole-file digest and every per-page
  digest have been recomputed from disk,
* registration and body cleanup are separate, idempotent, resumable steps, so
  a crash between them converges on retry,
* tenant failures are isolated: a corrupt shard for one tenant stops that
  tenant and is reported with its path, while other tenants continue.

Only the raw-response retention concern belongs here.  Quality projections,
rule generations and processing progress are never written by this layer.
"""
