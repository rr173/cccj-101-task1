"""Brute-force reference interpreter for acceptance cross-checks.

This deliberately ignores the inverted index and re-derives every result from
the original texts, so the acceptance suite does not trust the full-text engine
blindly: the SQLite-backed engine and this independent interpreter must agree,
document by document, including highlight intervals.

Matching model (intentionally simple and explicit)
--------------------------------------------------
* Documents are the current, non-retracted revisions within one tenant.
* A single-term leaf matches a field when the normalized term occurs there;
  its spans are the token's original half-open character intervals.
* A phrase leaf matches when its normalized terms occupy consecutive token
  positions within that one field; title positions never continue into the
  abstract.  Its span runs from the first token's start to the last token's
  end in the ORIGINAL text.
* AND intersects, OR unions, NOT is a binary set difference; negated branches
  never contribute spans, and unmatched OR branches create nothing.
* Score = number of distinct matched leaves, tie order (tenant, accession).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .parser import AndNode, DiffNode, OrNode, TermNode, parse
from .tokenizer import merge_spans, tokenize


@dataclass
class NaiveDoc:
    tenant: str
    accession: str
    revision: int
    title: str
    abstract: str
    title_skip: Optional[str] = None
    abstract_skip: Optional[str] = None

    def tokens(self, field_name):
        return tokenize(self.title if field_name == "title"
                        else self.abstract)


@dataclass
class NaiveHit:
    doc: NaiveDoc
    score: int
    spans: Dict[str, List[Tuple[int, int]]]


class NaiveEngine:
    def __init__(self, docs: List[NaiveDoc]):
        self.docs = docs

    def search(self, expression: str, *, tenant: str, order="relevance"):
        ast = parse(expression)
        scope = [d for d in self.docs
                 if d.tenant == tenant]      # tenant is the mandatory scope
        result: Dict[str, NaiveHit] = {}
        ids, spans, scores = self._eval(ast, scope)
        for doc in scope:
            key = (doc.tenant, doc.accession)
            if key in ids:
                result[key] = NaiveHit(
                    doc, scores.get(key, 0),
                    {f: merge_spans(s) for f, s in spans.get(key, {}).items()})
        hits = list(result.values())
        if order == "accession":
            hits.sort(key=lambda h: (h.doc.tenant, h.doc.accession))
        else:
            hits.sort(key=lambda h: (-h.score, h.doc.tenant,
                                     h.doc.accession))
        return hits

    # -- AST -------------------------------------------------------------
    def _eval(self, node, scope):
        if isinstance(node, TermNode):
            return self._eval_term(node, scope)
        if isinstance(node, AndNode):
            parts = [self._eval(c, scope) for c in node.children]
            ids = set.intersection(*(p[0] for p in parts)) if parts else set()
            spans, scores = {}, {}
            for p_ids, p_spans, p_scores in parts:
                self._add(spans, p_spans, ids)
                for k, v in p_scores.items():
                    if k in ids:
                        scores[k] = scores.get(k, 0) + v
            return ids, spans, scores
        if isinstance(node, OrNode):
            ids, spans, scores = set(), {}, {}
            for child in node.children:
                c_ids, c_spans, c_scores = self._eval(child, scope)
                ids |= c_ids
                self._add(spans, c_spans, c_ids)
                for k, v in c_scores.items():
                    if k in c_ids:
                        scores[k] = scores.get(k, 0) + v
            return ids, spans, scores
        if isinstance(node, DiffNode):
            l_ids, l_spans, l_scores = self._eval(node.left, scope)
            r_ids, _, _ = self._eval(node.right, scope)
            ids = l_ids - r_ids
            spans = {}
            self._add(spans, l_spans, ids)
            scores = {k: v for k, v in l_scores.items() if k in ids}
            return ids, spans, scores
        raise TypeError(type(node))

    @staticmethod
    def _add(target, source, keep):
        for key, by_field in source.items():
            if key not in keep:
                continue
            bucket = target.setdefault(key, {})
            for field_name, sp in by_field.items():
                bucket.setdefault(field_name, []).extend(sp)

    def _eval_term(self, node: TermNode, scope):
        fields = (node.field,) if node.field else ("title", "abstract")
        ids, spans, scores = set(), {}, {}
        for doc in scope:
            key = (doc.tenant, doc.accession)
            doc_spans = {}
            for field_name in fields:
                found = self._match_field(node, doc, field_name)
                if found:
                    doc_spans[field_name] = found
            if doc_spans:
                ids.add(key)
                spans[key] = doc_spans
                scores[key] = 1
        return ids, spans, scores

    @staticmethod
    def _match_field(node: TermNode, doc: NaiveDoc, field_name):
        tokens = doc.tokens(field_name)
        terms = node.terms
        if not node.phrase and len(terms) == 1:
            term = terms[0]
            return [(t.start, t.end) for t in tokens if t.term == term]
        # phrase (or quoted single term): consecutive token positions
        width = len(terms)
        hits = []
        for i in range(0, len(tokens) - width + 1):
            if [tokens[i + k].term for k in range(width)] == terms:
                hits.append((tokens[i].start, tokens[i + width - 1].end))
        return hits
