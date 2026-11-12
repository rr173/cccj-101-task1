"""Local offline retrieval module for the research harvester.

Pieces
------
tokenizer  deterministic NFKC + casefold tokenization with offsets mapped back
           to the *unnormalized* original text
parser     boolean query grammar (fields, parentheses, phrases, AND/OR/NOT with
           default AND and binary-only NOT); syntax errors carry positions and
           never fall back to plain text search
store      SQLite index schema (dictionary, postings with positions/spans,
           documents, watermarks, consumption ledger, skip reasons)
indexer    incremental catch-up from the landing database, one atomic
           transaction per sync, retraction/revision aware
engine     AST evaluation over complete matching sets, highlighting with
           half-open Unicode character intervals, stable scoring/pagination
           and one-snapshot reads
naive      a brute-force reference interpreter used by the acceptance suite to
           cross-check the inverted-index engine
"""

FIELDS = ("title", "abstract")
SCHEMA_VERSION = "1"

__all__ = ["FIELDS", "SCHEMA_VERSION"]
