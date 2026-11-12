"""Offline institutional collaboration-network analysis.

Pieces
------
mapping   local JSON affiliation-mapping table: stable institution codes,
          official names and aliases; strict validation (one name may never
          be assigned to two codes) and a canonical content digest that every
          network file binds to at creation time
store     SQLite schema for the network's OWN database file (all tables
          prefixed ``collab_``); the landing database is opened read-only
builder   incremental landing -> network sync: per-paper contribution
          replacement (subtract the old revision's edges, add the new one's)
          committed in ONE transaction per chunk, a consumption ledger and a
          contiguous-prefix cursor so a caught-up run performs no writes
engine    read-only queries over one WAL snapshot: neighbor lists ordered by
          weight, minimum-hop paths inside a minimum-weight subgraph with
          real per-hop weights and supporting papers, and edge provenance
          with the pre-normalization affiliation strings

The module never fetches, never guesses affiliations by similarity, and never
merges unrecognized names into a shared node.  Samples keep arriving through
the ordinary landing tables; the harvester needs no new interface.
"""

SCHEMA_VERSION = "1"

__all__ = ["SCHEMA_VERSION"]
