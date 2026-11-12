"""Query engine: AST evaluation over complete matching sets.

The engine never turns the parsed expression into SQL text.  SQL is used only
for exact dictionary lookups and posting retrieval; matching sets are combined
in Python according to the syntax tree, so every operand contributes its
*whole* candidate set -- nothing truncates a term to its first few hits.

Consistency
-----------
A query opens one deferred read transaction (a WAL snapshot).  Total count,
ordering, excerpts and page contents therefore all come from the same index
generation even if an updater commits concurrently.  Tenant scoping is applied
to every leaf lookup, so field-restricted and negated sub-expressions can never
pull another tenant's documents into the set.

Scoring
-------
Relevance is the number of query *leaves* matched in the document, where each
distinct TermNode counts once (a term repeated in the text does not inflate the
score); NOT-right operands and unmatched OR branches contribute neither a
match nor a highlight.  Ties break deterministically on (tenant, accession).
The alternate ``order=accession`` ordering sorts by (tenant, accession) alone.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from . import store
from .parser import AndNode, DiffNode, OrNode, TermNode, parse
from .tokenizer import merge_spans


@dataclass
class Hit:
    tenant: str
    accession: str
    revision: int
    score: int
    title: str
    abstract: str
    # field -> merged half-open intervals over the ORIGINAL text
    title_spans: List[Tuple[int, int]] = field(default_factory=list)
    abstract_spans: List[Tuple[int, int]] = field(default_factory=list)
    title_skip: Optional[str] = None
    abstract_skip: Optional[str] = None


@dataclass
class SearchResult:
    total: int
    page: List[Hit]
    offset: int
    limit: int
    order: str


class SearchEngine:
    def __init__(self, conn):
        self.conn = conn

    def _pin_snapshot(self, conn):
        """Force the deferred read transaction to acquire its WAL snapshot.

        A plain BEGIN is DEFERRED: the snapshot exists only from the first
        read statement.  Without this read, a writer committing before our
        first SELECT would change what the query sees, breaking the
        one-generation view.  Reading a stable metadata row pins the snapshot
        at transaction entry.
        """
        conn.execute("SELECT value FROM search_meta WHERE key='schema_version'")

    # -- entry point -----------------------------------------------------
    def search(self, expression: str, *, tenant: str, limit: int = 10,
               offset: int = 0, order: str = "relevance") -> SearchResult:
        if not isinstance(tenant, str) or not tenant:
            raise ValueError("tenant 是查询入口的必填项")
        if order not in ("relevance", "accession"):
            raise ValueError("order 只能是 relevance 或 accession")
        if limit < 1 or offset < 0:
            raise ValueError("limit 必须 >= 1 且 offset 必须 >= 0")
        ast = parse(expression)          # syntax errors propagate with positions
        conn = self.conn
        own_transaction = not conn.in_transaction
        if own_transaction:
            # one consistent snapshot for total / ordering / excerpts
            conn.execute("BEGIN")
            self._pin_snapshot(conn)
        try:
            return self._run(ast, tenant, limit, offset, order)
        finally:
            if own_transaction:
                conn.execute("COMMIT")

    def snapshot(self):
        """Context manager holding one WAL snapshot across several searches.

        Every search performed inside the block resolves total, ordering and
        excerpts against the same index generation, even if an updater
        commits meanwhile.  The snapshot is pinned by a read at block entry,
        before any external writer can move the read point.  A single
        ``search`` call already wraps itself in a snapshot; this is for
        multi-call consistency checks.
        """
        conn = self.conn
        outer = conn.in_transaction

        class _Snapshot:
            def __enter__(self_inner):
                if not outer:
                    conn.execute("BEGIN")
                    self._pin_snapshot(conn)
                return self

            def __exit__(self_inner, exc_type, exc, tb):
                if not outer:
                    conn.execute("COMMIT")
                return False

        return _Snapshot()

    def _run(self, ast, tenant, limit, offset, order):
        conn = self.conn
        doc_ids, span_map, score_map = self._evaluate(ast, tenant)
        rows = self._load_docs(doc_ids)
        hits = []
        for row in rows:
            doc_id = row["doc_id"]
            spans = span_map.get(doc_id, {})
            hits.append(Hit(
                tenant=row["tenant"], accession=row["accession"],
                revision=int(row["revision"]),
                score=score_map.get(doc_id, 0),
                title=row["title"], abstract=row["abstract"],
                title_spans=merge_spans(spans.get("title", [])),
                abstract_spans=merge_spans(spans.get("abstract", [])),
                title_skip=row["title_skip"],
                abstract_skip=row["abstract_skip"]))
        if order == "relevance":
            hits.sort(key=lambda h: (-h.score, h.tenant, h.accession))
        else:
            hits.sort(key=lambda h: (h.tenant, h.accession))
        total = len(hits)
        page = hits[offset:offset + limit]
        return SearchResult(total=total, page=page, offset=offset,
                            limit=limit, order=order)

    # -- AST evaluation --------------------------------------------------
    def _evaluate(self, node, tenant: str):
        """Return (doc_id set, doc_id -> field -> spans, doc_id -> score)."""
        if isinstance(node, TermNode):
            return self._eval_term(node, tenant)
        if isinstance(node, AndNode):
            sets, span_map, score_map = [], {}, {}
            for child in node.children:
                ids, sm, sc = self._evaluate(child, tenant)
                sets.append(ids)
                _merge_maps(span_map, sm)
                _merge_scores(score_map, sc)
            result = set.intersection(*sets) if sets else set()
            return result, _filter_map(span_map, result), {
                k: v for k, v in score_map.items() if k in result}
        if isinstance(node, OrNode):
            union: Set[int] = set()
            span_map, score_map = {}, {}
            for child in node.children:
                ids, sm, sc = self._evaluate(child, tenant)
                union |= ids
                _merge_maps(span_map, sm)     # unmatched branch adds nothing
                _merge_scores(score_map, sc)
            return union, _filter_map(span_map, union), {
                k: v for k, v in score_map.items() if k in union}
        if isinstance(node, DiffNode):
            left_ids, left_spans, left_score = self._evaluate(node.left, tenant)
            right_ids, _, _ = self._evaluate(node.right, tenant)
            result = left_ids - right_ids
            # the negated branch contributes no spans and no score
            return result, _filter_map(left_spans, result), {
                k: v for k, v in left_score.items() if k in result}
        raise TypeError(f"unknown AST node {type(node)!r}")

    def _eval_term(self, node: TermNode, tenant: str):
        fields = (node.field,) if node.field else ("title", "abstract")
        matched: Set[int] = set()
        span_map: Dict[int, Dict[str, List]] = {}
        for field_name in fields:
            doc_hits = self._field_lookup(node, field_name, tenant)
            for doc_id, spans in doc_hits.items():
                matched.add(doc_id)
                span_map.setdefault(doc_id, {}).setdefault(
                    field_name, []).extend(spans)
        span_map = {doc_id: {f: merge_spans(spans)
                             for f, spans in by_field.items()}
                    for doc_id, by_field in span_map.items()}
        score_map = {doc_id: 1 for doc_id in matched}
        return matched, span_map, score_map

    def _field_lookup(self, node: TermNode, field_name: str, tenant: str):
        """Return {doc_id: [original-text spans]} for one leaf/field."""
        conn = self.conn
        terms = node.terms
        term_ids = []
        for term in terms:
            row = conn.execute(
                "SELECT term_id FROM search_terms WHERE term=?",
                (term,)).fetchone()
            if row is None:
                return {}                    # one missing term -> no match
            term_ids.append(row["term_id"])
        if node.phrase or len(terms) > 1:
            return self._phrase_lookup(term_ids, node, field_name, tenant)
        return self._single_lookup(term_ids[0], field_name, tenant)

    def _single_lookup(self, term_id, field_name, tenant):
        out: Dict[int, List] = {}
        rows = self.conn.execute(
            "SELECT p.doc_id, p.spans FROM search_postings p"
            " JOIN search_docs d ON d.doc_id = p.doc_id"
            " WHERE p.term_id=? AND p.field=? AND d.tenant=?",
            (term_id, field_name, tenant)).fetchall()
        for row in rows:
            out[row["doc_id"]] = store.decode_spans(row["spans"])
        return out

    def _phrase_lookup(self, term_ids, node, field_name, tenant):
        """Consecutive token positions inside ONE field; spans stay original.

        Term slots are kept as an ordered list rather than a term_id-keyed
        dict, so a phrase containing a repeated term (e.g. ``"long long"``)
        does not collapse onto a single posting row.
        """
        conn = self.conn
        per_doc: Dict[int, List[Optional[dict]]] = {}
        for slot_index, term_id in enumerate(term_ids):
            rows = conn.execute(
                "SELECT p.doc_id, p.positions, p.spans FROM search_postings p"
                " JOIN search_docs d ON d.doc_id=p.doc_id"
                " WHERE p.term_id=? AND p.field=? AND d.tenant=?",
                (term_id, field_name, tenant)).fetchall()
            for row in rows:
                slots = per_doc.setdefault(row["doc_id"], [])
                while len(slots) < len(term_ids):
                    slots.append(None)
                slots[slot_index] = {
                    "positions": store.decode_positions(row["positions"]),
                    "spans": store.decode_spans(row["spans"])}
        out: Dict[int, List] = {}
        width = len(term_ids)
        for doc_id, slots in per_doc.items():
            if len(slots) < width or any(s is None for s in slots):
                continue
            first = slots[0]
            hits = []
            for idx, pos in enumerate(first["positions"]):
                chain_spans = [first["spans"][idx]]
                ok = True
                for k in range(1, width):
                    entry = slots[k]
                    try:
                        j = entry["positions"].index(pos + k)
                    except ValueError:
                        ok = False
                        break
                    chain_spans.append(entry["spans"][j])
                if ok:
                    # phrase span = from first token start to last token end
                    starts = [s for s, _ in chain_spans]
                    ends = [e for _, e in chain_spans]
                    hits.append((min(starts), max(ends)))
            if hits:
                out[doc_id] = hits
        return out

    # -- data loading ----------------------------------------------------
    def _load_docs(self, doc_ids: Set[int]):
        if not doc_ids:
            return []
        placeholders = ",".join("?" for _ in doc_ids)
        return self.conn.execute(
            f"SELECT doc_id, tenant, accession, revision, title, abstract,"
            f" title_skip, abstract_skip FROM search_docs"
            f" WHERE doc_id IN ({placeholders})",
            tuple(doc_ids)).fetchall()


def _merge_maps(target, source):
    for doc_id, by_field in source.items():
        bucket = target.setdefault(doc_id, {})
        for field_name, spans in by_field.items():
            bucket.setdefault(field_name, []).extend(spans)


def _merge_scores(target, source):
    for doc_id, score in source.items():
        target[doc_id] = target.get(doc_id, 0) + score


def _filter_map(span_map, keep):
    return {doc_id: by_field for doc_id, by_field in span_map.items()
            if doc_id in keep}
