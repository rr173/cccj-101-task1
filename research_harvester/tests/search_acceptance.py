#!/usr/bin/env python3
"""Acceptance assertions for the local offline retrieval module.

Fully offline, stdlib only.  Landing data is built directly in the
collector's schema; the search index is its own database.  The SQLite-backed
engine is cross-checked document by document against the brute-force
reference interpreter in ``search.naive`` -- the inverted index's own output
is never taken as ground truth.

Scenarios
---------
S1  parser: valid grammar, and every required syntax error carries a position
S2  tokenization offsets survive full-width / ligature / casefold / combining
S3  complex expression real hits with highlights (title + abstract spans)
S4  cross-field fake phrase cannot bridge title end -> abstract start
S5  OR one-side-only hits; unmatched branch creates no span/score;
    NOT-right terms are not highlighted
S6  non-text fields get explicit reasons; objects are never stringified;
    empty abstract stays searchable by title
S7  naive interpreter agrees with the engine on every query (sets, scores,
    spans, ordering, total, pagination)
S8  incremental: abstract revision removes old terms; late smaller revision
    never overwrites; retraction hides; higher revision restores; no full
    re-scan after the first build
S9  hard SIGKILL inside the index write transaction: nothing partial is
    visible and the resumed run converges; no old-term/new-text mix
S10 tenant scope is mandatory and never leaks; field and NOT expressions stay
    inside the requested tenant
S11 consistent snapshot: a query sees one generation while an update commits

Usage:  python tests/search_acceptance.py
Exit code 0 = all checks passed.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from collector import db as landing_db              # noqa: E402
from search import store                            # noqa: E402
from search.engine import SearchEngine              # noqa: E402
from search.indexer import Indexer                  # noqa: E402
from search.naive import NaiveDoc, NaiveEngine      # noqa: E402
from search.parser import QuerySyntaxError, parse   # noqa: E402
from search.tokenizer import make_excerpt, tokenize  # noqa: E402

FAILURES = []


def check(cond, label, detail=""):
    print(f"  [{'PASS' if cond else 'FAIL'}] {label}"
          + (f"  -- {detail}" if (detail and not cond) else ""))
    if not cond:
        FAILURES.append(label)


def section(name):
    print(f"\n=== {name} ===")


class Fixture:
    """Landing db + search index in an isolated temp directory."""

    def __init__(self, prefix="rh-search-"):
        self.dir = tempfile.mkdtemp(prefix=prefix)
        self.landing = os.path.join(self.dir, "harvest.db")
        self.index = os.path.join(self.dir, "search.db")
        conn = landing_db.connect(self.landing)
        landing_db.init(conn)
        conn.commit()
        conn.close()

    def add(self, accession, payload, *, tenant="alpha", revision=1,
            retracted=False, modified_at=1.0):
        conn = landing_db.connect(self.landing)
        with conn:
            raw = conn.execute(
                "INSERT INTO raw_pages(tenant, scan_id, url, status, body,"
                " fetched_at) VALUES (?,?,?,200,'{}',?)",
                (tenant, f"s-{accession}-r{revision}", f"mem://{accession}",
                 time.time())).lastrowid
            conn.execute(
                "INSERT INTO result_versions(tenant, accession, revision,"
                " modified_at, retracted, payload, raw_page_id)"
                " VALUES (?,?,?,?,?,?,?)",
                (tenant, accession, int(revision), float(modified_at),
                 1 if retracted else 0,
                 json.dumps(payload) if payload is not None else None, raw))
        conn.close()

    def raw_sql(self, sql, params=()):
        import sqlite3
        conn = sqlite3.connect(self.landing, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def index_sql(self, sql, params=()):
        import sqlite3
        conn = sqlite3.connect(self.index, timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in conn.execute(sql, params)]
        finally:
            conn.close()

    def sync(self, **kw):
        return Indexer(self.index, self.landing).sync(**kw)

    def engine(self):
        return SearchEngine(store.connect(self.index))

    def close(self):
        shutil.rmtree(self.dir, ignore_errors=True)


# A fixed corpus exercising every normalization and matching edge.
CORPUS = [
    # d1: full-width, ligature, casefold length change, CJK+latin mix
    ("d1", "alpha", {
        "title": "ＡＢＣ ﬁle 光谱 mass spectrometry study",
        "abstract": "We use mass spectrometry and 蛋白 analysis in Straße lab.",
        "publication_year": 2024}),
    # d2: cross-field fake phrase: title ends with spectrometry,
    # abstract starts with protein
    ("d2", "alpha", {
        "title": "New spectrometry methods",
        "abstract": "protein folding via 质谱 spectrometry arrays",
        "publication_year": 2023}),
    # d3: OR one-side-only (mass yes, spectrometry no)
    ("d3", "alpha", {
        "title": "Mass balance review",
        "abstract": "mass transport with no spectrum device, balance only",
        "publication_year": 2022}),
    # d4: empty abstract; title-only CJK search
    ("d4", "alpha", {
        "title": "蛋白 结构 总论",
        "publication_year": 2021}),
    # d5: non-text abstract and a numeric non-text title-ish extra field
    ("d5", "alpha", {
        "title": "Sensor network report",
        "abstract": {"nested": ["object", "not", "text"]},
        "publication_year": 2020}),
    # d6: combining character composition (e + combining acute)
    ("d6", "alpha", {
        "title": "Café resumé notes",
        "abstract": "resumé data on 蛋白 binding",
        "publication_year": 2019}),
    # other tenant: same words, must never leak
    ("x1", "beta", {
        "title": "mass spectrometry in beta tenant",
        "abstract": "蛋白 isolated tenant",
        "publication_year": 2024}),
    # d7: repeated term phrase and punctuation between phrase tokens
    ("d7", "alpha", {
        "title": "A long long walk",
        "abstract": "mass, spectrometry with a comma",
        "publication_year": 2018}),
]


def build_corpus(fx):
    for accession, tenant, payload in CORPUS:
        fx.add(accession, payload, tenant=tenant)


# --------------------------------------------------------------------------
# S1 parser
# --------------------------------------------------------------------------
def test_parser():
    section("S1 query parser: grammar and positioned syntax errors")
    valid = [
        'title:光谱 AND (abstract:"mass spectrometry" OR abstract:蛋白)',
        "a b", "a AND b", "a OR b", "a NOT b", "a AND NOT b",
        "(a OR b) AND (c OR d)", 'abstract:"蛋白 分析"',
        "Straße", "title:ＡＢＣ", "NOT-free-word",
    ]
    for expr in valid:
        try:
            parse(expr)
            check(True, f"accepted: {expr!r}")
        except QuerySyntaxError as exc:
            check(False, f"accepted: {expr!r}", str(exc))

    invalid = [
        ("title:x AND", "dangling AND"),
        ("AND a", "leading AND"),
        ("a OR", "dangling OR"),
        ("NOT a", "leading NOT"),
        ("a OR NOT b", "OR NOT without left"),
        ("foo:bar", "unknown field"),
        ("()", "empty parentheses"),
        ("(a AND b", "unclosed parenthesis"),
        ('abstract:"unclosed', "unclosed quote"),
        ("a AND ()", "empty group after operator"),
        ("title:", "colon without value"),
        (":", "lone colon"),
        ("a b )", "extra right paren"),
        ('""', "empty phrase"),
        ("a NOT", "dangling NOT"),
        ("a AND OR b", "two operators"),
        ("a (( b", "nested empty-ish parens"),
    ]
    for expr, label in invalid:
        try:
            parse(expr)
            check(False, f"rejected ({label}): {expr!r}", "was accepted")
        except QuerySyntaxError as exc:
            check(0 <= exc.position < len(expr)
                  or (not expr and exc.position == 0),
                  f"rejected ({label}) with position: {expr!r}",
                  f"pos={exc.position}")
            check("^" in str(exc), f"error shows pointer ({label})")

    # syntax failure must not be a plain-text fallback: the engine raises
    fx = Fixture()
    build_corpus(fx)
    fx.sync()
    eng = fx.engine()
    try:
        eng.search("title:", tenant="alpha")
        check(False, "engine refuses malformed query instead of text search")
    except QuerySyntaxError:
        check(True, "engine refuses malformed query (no text-search fallback)")
    finally:
        fx.close()


# --------------------------------------------------------------------------
# S2 tokenizer offsets over the ORIGINAL text
# --------------------------------------------------------------------------
def test_tokenizer_offsets():
    section("S2 normalization preserves original half-open character spans")
    # explicit, checked one by one
    t = tokenize("ＡＢＣ")
    check(t[0].term == "abc" and (t[0].start, t[0].end) == (0, 3),
          "full-width maps to original span", str(t))
    t = tokenize("ﬁle")
    check(t[0].term == "file" and (t[0].start, t[0].end) == (0, 3)
          and "ﬁle"[t[0].start:t[0].end] == "ﬁle",
          "ligature expansion keeps original span")
    t = tokenize("Straße")
    check(t[0].term == "strasse" and (t[0].start, t[0].end) == (0, 6)
          and "Straße"[0:6] == "Straße",
          "casefold length change keeps original span")
    # decomposed form: 'e' + combining acute (5 source chars) composes to
    # the single token 'café' while the span still covers all source chars
    decomposed = "Cafe\u0301"
    t = tokenize(decomposed)
    check(len(t) == 1 and t[0].term == "café"
          and (t[0].start, t[0].end) == (0, 5)
          and decomposed[t[0].start:t[0].end] == decomposed,
          "combining mark composes, span counts source chars", str(t))
    t = tokenize("光谱 mass")
    check([x.term for x in t] == ["光", "谱", "mass"]
          and [x.position for x in t] == [0, 1, 2]
          and t[0].start == 0 and t[2].end == 7,
          "Han chars are single positions; latin run is one word")
    # punctuation occupies no position
    t = tokenize("a, b！c")
    check([x.term for x in t] == ["a", "b", "c"]
          and [x.position for x in t] == [0, 1, 2],
          "punctuation separates without taking a position", str(t))
    # every span is a valid half-open interval slicing real source characters
    for text in ("ＡＢＣ ﬁle 光谱", "Straße Café", "蛋白-mass！１２３"):
        toks = tokenize(text)
        ok = all(0 <= tk.start < tk.end <= len(text)
                 and text[tk.start:tk.end] != "" for tk in toks)
        check(ok, f"all spans valid inside {text!r}", str(toks))


# --------------------------------------------------------------------------
# S3-S7 engine behavior + naive cross-check
# --------------------------------------------------------------------------
EXPRESSIONS = [
    'title:光谱 AND (abstract:"mass spectrometry" OR abstract:蛋白)',
    'abstract:mass OR abstract:spectrometry',
    'abstract:mass NOT abstract:spectrometry',
    '"spectrometry protein"',
    'title:spectrometry AND abstract:protein',
    'abstract:"mass spectrometry"',
    'title:蛋白',
    'abstract:蛋白',
    "title:abc",
    "title:file",
    "abstract:strasse",
    "title:resume",
    "mass OR 蛋白 OR 质谱 OR protein OR balance OR 结构",
    "(title:mass OR title:sensor) NOT abstract:object",
    'abstract:"质谱 spectrometry"',
    "title:光谱 abstract:蛋白",                 # default AND
    "a000unknownterm",
    'title:"long long"',                        # repeated term inside a phrase
    'abstract:"mass, spectrometry"',            # punctuation spans no position
]


def _naive_docs(fx):
    rows = fx.index_sql(
        "SELECT tenant, accession, revision, title, abstract,"
        " title_skip, abstract_skip FROM search_docs ORDER BY accession")
    return [NaiveDoc(r["tenant"], r["accession"], r["revision"],
                     r["title"], r["abstract"], r["title_skip"],
                     r["abstract_skip"]) for r in rows]


def test_engine_and_naive():
    section("S3-S7 engine results, highlights, and naive cross-check")
    fx = Fixture()
    build_corpus(fx)
    fx.sync()
    eng = fx.engine()
    naive = NaiveEngine(_naive_docs(fx))

    # S3 real complex hit + highlight intervals correspond to original text
    r = eng.search(
        'title:光谱 AND (abstract:"mass spectrometry" OR abstract:蛋白)',
        tenant="alpha")
    check(r.total == 1 and r.page[0].accession == "d1",
          "complex expression hits exactly d1", f"total={r.total}")
    h = r.page[0]
    title_slices = [h.title[s:e] for s, e in h.title_spans]
    check(title_slices == ["光谱"],
          "title highlight spans slice the original title", str(title_slices))
    check(any(h.abstract[s:e] == "mass spectrometry"
              for s, e in h.abstract_spans),
          "phrase highlight slices original abstract text")
    check(any(h.abstract[s:e] == "蛋白" for s, e in h.abstract_spans),
          "OR-second-branch CJK term is highlighted")
    print("    excerpt:", make_excerpt(h.abstract, h.abstract_spans))

    # S4 cross-field fake phrase
    r = eng.search('"spectrometry protein"', tenant="alpha")
    check(r.total == 0, "cross-field fake phrase never matches")
    r = eng.search("title:spectrometry AND abstract:protein",
                   tenant="alpha")
    check([x.accession for x in r.page] == ["d2"],
          "field-scoped AND still matches d2 independently")

    # S5 OR one side only; unmatched branch contributes nothing
    r = eng.search("abstract:mass OR abstract:spectrometry",
                   tenant="alpha")
    ids = {x.accession: x for x in r.page}
    check(set(ids) == {"d1", "d2", "d3", "d7"}, "OR union is complete",
          str(sorted(ids)))
    d3 = ids["d3"]
    check(d3.score == 1 and all(s != e for s, e in d3.abstract_spans),
          "d3 matched only the mass side (score 1, real spans only)")
    hit_slices = [d3.abstract[s:e].casefold() for s, e in d3.abstract_spans]
    check(hit_slices == ["mass"],
          "no highlight manufactured for the absent spectrometry branch",
          str(hit_slices))
    # NOT right operand must not be highlighted; difference is set subtraction
    r = eng.search("abstract:mass NOT abstract:balance", tenant="alpha")
    check({x.accession for x in r.page} == {"d1", "d7"}
          and all(not x.abstract_spans == [] for x in r.page),
          "difference keeps mass-without-balance docs and removes d3")
    r = eng.search("title:mass NOT abstract:zzzz", tenant="alpha")
    check({x.accession for x in r.page} == {"d1", "d3"}
          and all(not x.abstract_spans for x in r.page),
          "NOT branch adds no highlight")

    # S6 non-text handling and empty abstract
    skips = fx.index_sql(
        "SELECT field, reason FROM search_skips WHERE accession='d5'")
    check(any(s["field"] == "abstract" and "dict" in s["reason"]
              for s in skips),
          "non-text abstract recorded with reason, not stringified")
    check(fx.index_sql(
        "SELECT COUNT(*) c FROM search_postings p JOIN search_docs d"
        " ON d.doc_id=p.doc_id WHERE d.accession='d5' AND p.field='abstract'"
    )[0]["c"] == 0, "no postings created from the object abstract")
    r = eng.search("title:蛋白", tenant="alpha")
    check("d4" in {x.accession for x in r.page},
          "empty-abstract document is searchable by title")
    r = eng.search("abstract:蛋白", tenant="alpha")
    check("d4" not in {x.accession for x in r.page},
          "empty abstract produces no abstract hits")

    # S7 naive cross-check over every expression, both orders and pages
    for expr in EXPRESSIONS:
        for order in ("relevance", "accession"):
            er = eng.search(expr, tenant="alpha", limit=3, offset=0,
                            order=order)
            nr = naive.search(expr, tenant="alpha", order=order)
            eng_ids = [x.accession for x in er.page]
            naive_ids = [x.doc.accession for x in nr[:3]]
            check(eng_ids == naive_ids,
                  f"engine==naive ordering {order}: {expr!r}",
                  f"{eng_ids} != {naive_ids}")
            check(er.total == len(nr),
                  f"total matches naive: {expr!r}",
                  f"{er.total} != {len(nr)}")
            # full unfiltered agreement on sets and spans
            full = eng.search(expr, tenant="alpha", limit=1000, order=order)
            naive_map = {(h.doc.tenant, h.doc.accession): h for h in nr}
            check(len(full.page) == len(nr),
                  f"full set size agrees: {expr!r}")
            for hit in full.page:
                nh = naive_map[(hit.tenant, hit.accession)]
                check(hit.score == nh.score,
                      f"score agrees {hit.accession}: {expr!r}",
                      f"{hit.score} != {nh.score}")
                check(hit.title_spans == nh.spans.get("title", [])
                      and hit.abstract_spans == nh.spans.get("abstract", []),
                      f"spans agree {hit.accession}: {expr!r}",
                      f"{hit.title_spans}/{hit.abstract_spans} vs "
                      f"{nh.spans}")
    # repeated-term phrase does not collapse; punctuation takes no position
    r = eng.search('title:"long long"', tenant="alpha")
    check([h.accession for h in r.page] == ["d7"]
          and r.page[0].title_spans == [(2, 11)],
          "repeated-term phrase matches with the full original span",
          str([(h.accession, h.title_spans) for h in r.page]))
    r = eng.search('abstract:"mass, spectrometry"', tenant="alpha")
    check({h.accession for h in r.page} == {"d1", "d7"},
          "phrase matches across a punctuation separator (no token position)")
    # empty result still reports a stable total of zero
    r = eng.search("abstract:nosuchword_zzz", tenant="alpha")
    check(r.total == 0 and r.page == [], "empty match set has total 0")

    # pagination semantics: total is the full set; pages partition it
    expr = "mass OR 蛋白 OR 质谱 OR protein OR balance OR 结构"
    full = eng.search(expr, tenant="alpha", limit=100)
    p1 = eng.search(expr, tenant="alpha", limit=2, offset=0)
    p2 = eng.search(expr, tenant="alpha", limit=2, offset=2)
    p3 = eng.search(expr, tenant="alpha", limit=2, offset=4)
    check(p1.total == p2.total == p3.total == full.total,
          "total is stable across pages")
    seen = [h.accession for h in p1.page] + [h.accession for h in p2.page] \
        + [h.accession for h in p3.page]
    check(seen == [h.accession for h in full.page],
          "pages partition the full ordered set without gaps or duplicates",
          str(seen))
    fx.close()


# --------------------------------------------------------------------------
# S8 incremental lifecycle
# --------------------------------------------------------------------------
def test_incremental():
    section("S8 revisions, late older revision, retract, restore, no rescan")
    fx = Fixture()
    fx.add("w1", {"title": "Balance methods",
                  "abstract": "balance equations and mass terms",
                  "publication_year": 2020}, revision=1)
    fx.sync()
    eng = fx.engine()
    check({h.accession for h in eng.search(
        "abstract:balance", tenant="alpha").page} == {"w1"},
          "initial term present")

    # newer revision removes old terms
    fx.add("w1", {"title": "Balance methods",
                  "abstract": "quantum optics rewrite of the whole study",
                  "publication_year": 2020}, revision=2)
    rep = fx.sync()
    check(rep.scanned == 1, "incremental scan reads only the new row",
          f"scanned={rep.scanned}")
    check(not {h.accession for h in eng.search(
        "abstract:balance", tenant="alpha").page},
          "new revision removes the old term")
    check({h.accession for h in eng.search(
        "abstract:quantum", tenant="alpha").page} == {"w1"},
          "new revision adds the new term")
    # no old-term/new-text mix: doc text and postings agree
    doc = fx.index_sql("SELECT abstract FROM search_docs WHERE accession='w1'")[0]
    check("quantum" in doc["abstract"] and "balance equations" not in
          doc["abstract"], "stored text is the new revision")

    # late smaller revision (higher rowid, lower revision number) cannot win:
    # a fresh accession whose rev3 arrives before its rev1
    fx.add("w2", {"title": "Late arrival",
                  "abstract": "photon detector current",
                  "publication_year": 2020}, revision=3)
    fx.sync()
    fx.add("w2", {"title": "Late arrival",
                  "abstract": "old balance legacy",
                  "publication_year": 2020}, revision=1, modified_at=0.4)
    fx.sync()
    row = fx.index_sql(
        "SELECT revision, abstract FROM search_docs WHERE accession='w2'")[0]
    check(row["revision"] == 3 and "photon" in row["abstract"],
          "late smaller revision never overwrites current text", str(row))
    check(not {h.accession for h in eng.search(
        "abstract:legacy", tenant="alpha").page},
          "late older text is not indexed as current")

    # retraction hides; higher revision restores
    fx.add("w2", None, revision=4, retracted=True)
    fx.sync()
    check(not {h.accession for h in eng.search(
        "abstract:photon", tenant="alpha").page},
          "retracted document disappears from results")
    fx.add("w2", {"title": "Late arrival restored",
                  "abstract": "photon detector confirmed again",
                  "publication_year": 2020}, revision=5)
    fx.sync()
    check({h.accession for h in eng.search(
        "abstract:photon", tenant="alpha").page} == {"w2"},
          "higher non-retracted revision restores searchability")

    # idempotent no-op sync does not rescan
    rep = fx.sync()
    check(rep.scanned == 0 and rep.documents_upserted == 0,
          "a caught-up sync scans zero rows", str(rep))
    fx.close()


# --------------------------------------------------------------------------
# S9 hard SIGKILL inside the write transaction
# --------------------------------------------------------------------------
def test_crash_consistency():
    section("S9 SIGKILL in the index write transaction, then resume")
    fx = Fixture()
    fx.add("k1", {"title": "Crash title alpha",
                  "abstract": "survivable terms photon",
                  "publication_year": 2020}, revision=1)
    fx.sync()
    before = fx.index_sql(
        "SELECT last_rowid FROM search_cursor WHERE tenant='alpha'")[0]

    # a second revision whose sync will be killed mid-transaction
    fx.add("k1", {"title": "Crash title alpha revised",
                  "abstract": "survivable terms quantum update",
                  "publication_year": 2020}, revision=2)

    marker = os.path.join(fx.dir, "marker.flag")
    wait_file = os.path.join(fx.dir, "wait.flag")
    with open(wait_file, "w", encoding="utf-8"):
        pass
    env = dict(os.environ,
               RH_SEARCH_CRASH_POINT="apply_chunk_txn",
               RH_SEARCH_CRASH_MARKER=marker,
               RH_SEARCH_CRASH_WAIT_FILE=wait_file)
    cmd = [sys.executable, os.path.join(ROOT, "run_search.py"), "update",
           "--db", fx.landing, "--index", fx.index]
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)
    deadline = time.time() + 60
    while time.time() < deadline and not os.path.exists(marker):
        if proc.poll() is not None:
            break
        time.sleep(0.01)
    reached = os.path.exists(marker)
    if proc.poll() is None:
        proc.kill()
    out, _ = proc.communicate(timeout=15)
    if os.path.exists(wait_file):
        os.remove(wait_file)
    check(reached, "subprocess reached the in-transaction barrier", out)

    # the committed state must be the old complete generation
    after_crash = fx.index_sql(
        "SELECT last_rowid FROM search_cursor WHERE tenant='alpha'")[0]
    check(after_crash["last_rowid"] == before["last_rowid"],
          "watermark did not advance under a killed transaction",
          f"{after_crash} vs {before}")
    eng = fx.engine()
    check(not {h.accession for h in eng.search(
        "abstract:quantum", tenant="alpha").page},
          "uncommitted new term is not searchable")
    check({h.accession for h in eng.search(
        "abstract:photon", tenant="alpha").page} == {"k1"},
          "old term still matches the old (visible) text")
    docs = fx.index_sql(
        "SELECT revision, abstract FROM search_docs WHERE accession='k1'")
    check(len(docs) == 1 and docs[0]["revision"] == 1,
          "no partial duplicate document row", str(docs))

    # resume converges to the new revision
    rep = fx.sync()
    check(rep.documents_upserted == 1, "resumed run applies the pending row",
          str(rep))
    check({h.accession for h in eng.search(
        "abstract:quantum", tenant="alpha").page} == {"k1"},
          "after resume the new term is searchable")
    check(not {h.accession for h in eng.search(
        "abstract:photon", tenant="alpha").page},
          "after resume the old term is gone")
    doc = fx.index_sql(
        "SELECT revision, abstract FROM search_docs WHERE accession='k1'")[0]
    check(doc["revision"] == 2 and "quantum" in doc["abstract"],
          "resumed index stores exactly the new revision", str(doc))
    fx.close()


# --------------------------------------------------------------------------
# S10 tenant isolation
# --------------------------------------------------------------------------
def test_tenant_isolation():
    section("S10 mandatory tenant scope never leaks across tenants")
    fx = Fixture()
    build_corpus(fx)
    fx.sync()
    eng = fx.engine()
    try:
        eng.search("mass", tenant="")
        check(False, "empty tenant is rejected")
    except ValueError:
        check(True, "tenant is a mandatory query entry point")
    for expr in ('abstract:蛋白',
                 'title:mass NOT abstract:zzzz',
                 '(abstract:mass OR abstract:protein)'):
        for tenant in ("alpha", "beta"):
            r = eng.search(expr, tenant=tenant, limit=100)
            check(all(h.tenant == tenant for h in r.page),
                  f"all hits stay in tenant {tenant}: {expr!r}")
    r = eng.search("abstract:蛋白", tenant="beta")
    check([h.accession for h in r.page] == ["x1"] and r.total == 1,
          "beta sees only its own document")
    r = eng.search("abstract:蛋白", tenant="alpha")
    check("x1" not in {h.accession for h in r.page},
          "alpha cannot see beta's document")
    fx.close()


# --------------------------------------------------------------------------
# S11 consistent snapshot under concurrent update
# --------------------------------------------------------------------------
def test_snapshot_consistency():
    section("S11 one query reads a single consistent generation")
    fx = Fixture()
    build_corpus(fx)
    fx.sync()
    baseline = fx.engine().search("abstract:mass", tenant="alpha",
                                   limit=100).total
    # a new landing row that a second indexer connection will commit while a
    # query holds a snapshot
    fx.add("zzz", {"title": "Concurrent insert",
                   "abstract": "snapshot term witness",
                   "publication_year": 2024})
    conn = store.connect(fx.index)
    eng = SearchEngine(conn)
    with eng.snapshot():
        # a separate connection runs the incremental update to completion
        other_indexer = Indexer(fx.index, fx.landing)
        other_indexer.sync()
        r = eng.search("abstract:witness", tenant="alpha")
        check(r.total == 0,
              "in-flight query does not see a concurrently committed update")
        r2 = eng.search("abstract:mass", tenant="alpha", limit=100)
        check(r2.total == baseline
              and all(h.accession != "zzz" for h in r2.page),
              "the whole query stays on the pre-update snapshot")
    # the snapshot connection keeps its read snapshot; a brand new connection
    # opened after the snapshot ends must observe the committed update
    conn.close()
    r3 = SearchEngine(store.connect(fx.index)).search(
        "abstract:witness", tenant="alpha")
    check(any(h.accession == "zzz" for h in r3.page),
          "a new query on a fresh connection after the snapshot sees the update")
    fx.close()


# --------------------------------------------------------------------------
# S12 multi-chunk commit: a crash after an early chunk must not skip rows
# --------------------------------------------------------------------------
def test_multi_chunk_crash():
    section("S12 chunked commit keeps every row across a hard exit")
    fx = Fixture()
    # enough distinct accessions to span several chunks of size 2
    n = 7
    for i in range(n):
        fx.add(f"m{i:02d}",
               {"title": f"Chunk paper {i}",
                "abstract": f"chunkterm marker{i} photon",
                "publication_year": 2024})

    # Run the chunked build in a real subprocess and SIGKILL-equivalent it
    # (os._exit) while the SECOND chunk's transaction is open, so the crash
    # cannot be mistaken for a catchable in-process exception.
    driver = os.path.join(fx.dir, "chunk_driver.py")
    with open(driver, "w", encoding="utf-8") as fh:
        fh.write(
            "import os, sys\n"
            f"sys.path.insert(0, {ROOT!r})\n"
            "from search.indexer import Indexer\n"
            "calls = {'n': 0}\n"
            "def barrier(name):\n"
            "    if name != 'apply_chunk_txn':\n"
            "        return\n"
            "    calls['n'] += 1\n"
            "    if calls['n'] == 2:\n"
            "        os._exit(137)\n"
            f"Indexer({fx.index!r}, {fx.landing!r}, crash_barrier=barrier,"
            " chunk_size=2).sync()\n")
    proc = subprocess.run([sys.executable, driver], capture_output=True,
                          text=True)
    check(proc.returncode == 137,
          "subprocess hard-exited inside the second chunk transaction",
          f"rc={proc.returncode} {proc.stderr[-300:]}")

    # first chunk committed (2 docs); watermark must NOT have jumped past the
    # unprocessed rows
    visible = fx.index_sql("SELECT COUNT(*) c FROM search_docs")[0]["c"]
    check(visible == 2, "only the first committed chunk is visible",
          f"visible={visible}")
    eng = fx.engine()
    check(eng.search("abstract:photon", tenant="alpha",
                     limit=100).total == 2,
          "visible docs are exactly the first committed chunk")

    # resume with normal chunking: every remaining row is eventually indexed
    fx.sync()
    final = fx.engine().search("abstract:photon", tenant="alpha",
                               limit=100)
    check(final.total == n, "resume indexes all rows, none skipped",
          f"total={final.total} vs {n}")
    check(len({h.accession for h in final.page}) == n,
          "no duplicate or missing accessions after chunked resume")
    # watermark reaches the true max rowid
    max_row = fx.raw_sql(
        "SELECT COALESCE(MAX(rowid),0) m FROM result_versions")[0]["m"]
    check(fx.index_sql("SELECT last_rowid FROM search_cursor"
                       " WHERE tenant='alpha'")[0]["last_rowid"] == max_row,
          "final watermark equals the landing max rowid")
    fx.close()


# --------------------------------------------------------------------------
# S13 the reported interleave: per-accession grouping vs. commit boundaries
# --------------------------------------------------------------------------
def _add_batch(fx, accessions, revision, marker, tenant="alpha"):
    for accession in accessions:
        fx.add(accession,
               {"title": f"paper {accession}",
                "abstract": f"{marker} abstract {accession}",
                "publication_year": 2024},
               tenant=tenant, revision=revision, modified_at=float(revision))


def _has(engine, term, accession, tenant="alpha"):
    r = engine.search(f"abstract:{term}", tenant=tenant, limit=100000)
    return any(h.accession == accession for h in r.page), r.total


def test_interleaved_accessions_across_commit_boundaries():
    section("S13 interleaved revisions: cursor is a contiguous consumed prefix")
    fx = Fixture()
    n = 501
    names = [f"A{i:03d}" for i in range(n)]
    _add_batch(fx, names, 1, "oldmarker")
    fx.sync()
    # second generation for every accession, then A000's third revision last
    _add_batch(fx, names, 2, "newmarker")
    fx.add("A000", {"title": "paper A000 third",
                    "abstract": "thirdmarker abstract A000",
                    "publication_year": 2024},
           revision=3, modified_at=3.0)

    marker = os.path.join(fx.dir, "marker.flag")
    wait_file = os.path.join(fx.dir, "wait.flag")
    with open(wait_file, "w", encoding="utf-8"):
        pass
    driver = os.path.join(fx.dir, "kill_second_chunk.py")
    with open(driver, "w", encoding="utf-8") as fh:
        fh.write(
            "import os, sys, time\n"
            f"sys.path.insert(0, {ROOT!r})\n"
            "from search.indexer import Indexer\n"
            f"marker = {marker!r}\n"
            f"wait = {wait_file!r}\n"
            "calls = {'n': 0}\n"
            "def barrier(name):\n"
            "    if name != 'apply_chunk_txn':\n"
            "        return\n"
            "    calls['n'] += 1\n"
            "    if calls['n'] == 2:\n"
            "        with open(marker, 'w') as fh:\n"
            "            fh.write('second')\n"
            "        while True:\n"
            "            time.sleep(0.01)\n"
            f"Indexer({fx.index!r}, {fx.landing!r}, crash_barrier=barrier,"
            " chunk_size=500).sync()\n")
    proc = subprocess.Popen([sys.executable, driver],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    deadline = time.time() + 60
    while time.time() < deadline and not os.path.exists(marker):
        if proc.poll() is not None:
            break
        time.sleep(0.01)
    reached = os.path.exists(marker)
    if proc.poll() is None:
        proc.kill()
    out, _ = proc.communicate(timeout=15)
    if os.path.exists(wait_file):
        os.remove(wait_file)
    check(reached, "subprocess was killed inside the second chunk", out)
    check(proc.returncode in (137, -9),
          "hard exit code, not a caught exception", str(proc.returncode))

    # durable progress must stop at the contiguous committed prefix, well
    # below A500's second-revision rowid, even though A000's revision 3 was
    # handled by the first chunk.
    cursor = fx.index_sql("SELECT last_rowid FROM search_cursor"
                          " WHERE tenant='alpha'")[0]["last_rowid"]
    a500_r2_rowid = fx.raw_sql(
        "SELECT rowid AS r FROM result_versions"
        " WHERE tenant='alpha' AND accession='A500' AND revision=2")[0]["r"]
    seen, _ = _has(fx.engine(), "newmarker", "A000")
    check(seen, "rows inside the committed first chunk are visible")
    row_a000 = fx.index_sql("SELECT revision FROM search_docs"
                            " WHERE accession='A000'")[0]["revision"]
    check(row_a000 == 2,
          "A000 shows the newest revision inside the committed prefix", 
          str(row_a000))
    check(cursor < a500_r2_rowid,
          "cursor did not leap over the uncommitted accession",
          f"cursor={cursor} < A500 r2 rowid={a500_r2_rowid}")
    staged = fx.index_sql("SELECT revision FROM search_docs"
                          " WHERE accession='A500'")[0]["revision"]
    check(staged == 1, "A500 still shows its first revision", str(staged))

    # a normal update must rescan exactly the un-consumed rows (no full scan)
    rep = fx.sync()
    check(rep.scanned < n,
          "catch-up re-reads only rows above the durable cursor",
          f"scanned={rep.scanned}")
    seen, total = _has(fx.engine(), "newmarker", "A500")
    check(seen, "A500 second revision became searchable after resume")
    check(total == n - 1,
          "all second-generation documents except A000 are on newmarker",
          f"total={total}")
    seen_old, _ = _has(fx.engine(), "oldmarker", "A500")
    check(not seen_old, "A500 no longer matches the old marker")
    seen_third, _ = _has(fx.engine(), "thirdmarker", "A000")
    check(seen_third, "A000's third revision is current")
    row = fx.index_sql("SELECT revision, abstract FROM search_docs"
                       " WHERE accession='A000'")[0]
    check(row["revision"] == 3 and "thirdmarker" in row["abstract"],
          "A000 stores exactly its newest revision", str(row))
    # cursor now equals the landing max: the whole prefix is consumed
    max_row = fx.raw_sql(
        "SELECT COALESCE(MAX(rowid),0) m FROM result_versions")[0]["m"]
    check(fx.index_sql("SELECT last_rowid FROM search_cursor"
                       " WHERE tenant='alpha'")[0]["last_rowid"] == max_row,
          "final cursor covers the complete landing prefix")
    # no duplicate document rows anywhere
    dup = fx.index_sql(
        "SELECT COUNT(*) c FROM (SELECT tenant, accession FROM search_docs"
        " GROUP BY tenant, accession HAVING COUNT(*) > 1)")[0]["c"]
    check(dup == 0, "no duplicate document rows after resume")
    fx.close()


# --------------------------------------------------------------------------
# S14 independent expected text for every consumed identity
# --------------------------------------------------------------------------
def test_document_text_matches_landing_truth():
    section("S14 document text derived from landing, not from the index")
    fx = Fixture()
    n = 40
    names = [f"B{i:02d}" for i in range(n)]
    _add_batch(fx, names, 1, "firstmarker")
    fx.sync()
    _add_batch(fx, names, 2, "secondmarker")
    fx.add("B05", None, revision=3, retracted=True)
    fx.add("B07", {"title": "B07 late", "abstract": "latearrival text",
                   "publication_year": 2024}, revision=4, modified_at=0.5)
    fx.sync()
    # independent truth: recompute the expected current revision per accession
    # directly from result_versions, relying on no index table.
    truth = {}
    for row in fx.raw_sql(
            "SELECT accession, revision, retracted, payload FROM"
            " result_versions WHERE tenant='alpha'"):
        cur = truth.get(row["accession"])
        if cur is None or row["revision"] > cur["revision"]:
            truth[row["accession"]] = row
    eng = fx.engine()
    ok_all = True
    for accession, row in truth.items():
        docs = fx.index_sql(
            "SELECT revision, retracted, title, abstract FROM search_docs"
            " WHERE tenant='alpha' AND accession=?", (accession,))
        if row["retracted"]:
            if docs:
                ok_all = False
                check(False, f"retracted {accession} must not stay indexed",
                      str(docs))
            continue
        payload = json.loads(row["payload"])
        expected_title = payload.get("title", "")
        expected_abs = payload.get("abstract", "")
        if (len(docs) != 1 or docs[0]["revision"] != row["revision"]
                or docs[0]["title"] != expected_title
                or docs[0]["abstract"] != expected_abs):
            ok_all = False
            check(False, f"{accession} text/revision matches landing truth",
                  f"{docs} vs rev {row['revision']}")
    check(ok_all, "every indexed document matches landing-derived truth")
    # naive engine cross-check on the new state, not trusting postings
    docs_naive = _naive_docs(fx)
    naive = NaiveEngine(docs_naive)
    for expr in ("abstract:secondmarker", "abstract:latearrival",
                 "abstract:firstmarker", "title:paper AND abstract:marker"):
        full = eng.search(expr, tenant="alpha", limit=1000)
        ref = naive.search(expr, tenant="alpha", limit=0) \
            if False else naive.search(expr, tenant="alpha")
        check({h.accession for h in full.page}
              == {h.doc.accession for h in ref},
              f"engine matches naive on landing-derived state: {expr!r}")
    fx.close()


# --------------------------------------------------------------------------
# S15 repair of an index broken by the legacy progress rule
# --------------------------------------------------------------------------
def _legacy_index(fx, till_barrier):
    """Run the frozen pre-fix indexer; optionally kill in chunk 2."""
    from tests.fixtures import legacy_indexer
    marker = os.path.join(fx.dir, "legacy.marker")
    wait_file = os.path.join(fx.dir, "legacy.wait")
    with open(wait_file, "w", encoding="utf-8"):
        pass
    driver = os.path.join(fx.dir, "legacy_driver.py")
    with open(driver, "w", encoding="utf-8") as fh:
        fh.write(
            "import os, sys, time\n"
            f"sys.path.insert(0, {ROOT!r})\n"
            "from tests.fixtures import legacy_indexer\n"
            f"marker = {marker!r}\n"
            f"wait = {wait_file!r}\n"
            f"kill = {bool(till_barrier)!r}\n"
            "calls = {'n': 0}\n"
            "def barrier(name):\n"
            "    if name != 'apply_chunk_txn':\n"
            "        return\n"
            "    calls['n'] += 1\n"
            "    if kill and calls['n'] == 2:\n"
            "        with open(marker, 'w') as fh:\n"
            "            fh.write('second')\n"
            "        while True:\n"
            "            time.sleep(0.01)\n"
            f"legacy_indexer.Indexer({fx.index!r}, {fx.landing!r},"
            " crash_barrier=barrier, chunk_size=500).sync()\n")
    proc = subprocess.Popen([sys.executable, driver],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    if till_barrier:
        deadline = time.time() + 60
        while time.time() < deadline and not os.path.exists(marker):
            if proc.poll() is not None:
                break
            time.sleep(0.01)
        reached = os.path.exists(marker)
        if proc.poll() is None:
            proc.kill()
        out, _ = proc.communicate(timeout=15)
        if os.path.exists(wait_file):
            os.remove(wait_file)
        return reached, out
    out, _ = proc.communicate(timeout=120)
    return proc.returncode == 0, out


def _build_legacy_broken(fx, count=501):
    n = count
    names = [f"A{i:03d}" for i in range(n)]
    _add_batch(fx, names, 1, "oldmarker")
    from tests.fixtures import legacy_indexer
    legacy_indexer.Indexer(fx.index, fx.landing).sync()
    _add_batch(fx, names, 2, "newmarker")
    fx.add("A000", {"title": "paper A000 third",
                    "abstract": "thirdmarker abstract A000",
                    "publication_year": 2024},
           revision=3, modified_at=3.0)
    reached, out = _legacy_index(fx, till_barrier=True)
    return reached, out, names


def test_repair_legacy_broken_index():
    section("S15 repair a legacy-broken index without rebuild or manual rowids")
    fx = Fixture()
    reached, out, names = _build_legacy_broken(fx)
    check(reached, "legacy indexer was killed in its second chunk", out)
    # reproduce the reported residue: new marker misses exactly A500
    eng = fx.engine()
    seen_new, total_new = _has(eng, "newmarker", "A500")
    check(not seen_new and total_new == len(names) - 2,
          "legacy residue: A500 second revision is missing from newmarker",
          f"total={total_new}")
    seen_old, _ = _has(eng, "oldmarker", "A500")
    check(seen_old, "A500 is still searchable by its first revision's term")
    row = fx.index_sql("SELECT revision FROM search_docs"
                       " WHERE accession='A500'")[0]
    check(row["revision"] == 1, "A500 document still holds revision 1")
    # the a normal update repeats the silent success
    rep = fx.sync()
    check(rep.scanned == 0, "plain update has nothing to read (silent gap)",
          str(rep))

    other_doc = fx.index_sql(
        "SELECT doc_id, revision, abstract FROM search_docs"
        " WHERE accession='A001'")[0]
    # repair: one audit of the consumed range, then catch-up from the gap
    rep = Indexer(fx.index, fx.landing).repair()
    tr = rep.tenants[0]
    check(tr.status == "repaired", "repair reports its work", str(tr))
    check(tr.audited_from == 0 and tr.audited_to > 0,
          "audit scope is stated as the consumed rowid range",
          f"{tr.audited_from}..{tr.audited_to}")
    check(tr.rewind_to is not None and tr.rewind_to >= 0,
          "repair states the rewind target", str(tr.rewind_to))
    check(any(item.accession == "A500" and item.revision == 2
              for item in tr.missing),
          "audit identified A500 revision 2 as the missing input",
          str([(i.accession, i.revision) for i in tr.missing]))
    seen_new, total_new = _has(fx.engine(), "newmarker", "A500")
    check(seen_new, "A500 second revision is searchable after repair")
    check(total_new == len(names) - 1,
          "all other accessions keep exactly one current document",
          f"total={total_new}")
    seen_old, _ = _has(fx.engine(), "oldmarker", "A500")
    check(not seen_old, "old term vanished with the repaired document")
    row = fx.index_sql("SELECT revision, abstract FROM search_docs"
                       " WHERE accession='A500'")[0]
    check(row["revision"] == 2 and "newmarker" in row["abstract"],
          "A500 stores exactly its second revision", str(row))
    # documents unrelated to the gap were not rewritten
    keep = fx.index_sql(
        "SELECT doc_id, revision, abstract FROM search_docs"
        " WHERE accession='A001'")[0]
    check(keep == other_doc, "other documents keep their doc_id and text",
          str(keep))
    # the repair journal is finished and the cursor covers the full prefix
    jr = fx.index_sql("SELECT * FROM search_repair WHERE tenant='alpha'")[0]
    check(jr["status"] == "done" and jr["missing_count"] >= 1,
          "repair journal records audited scope and completion", str(jr))
    max_row = fx.raw_sql(
        "SELECT COALESCE(MAX(rowid),0) m FROM result_versions")[0]["m"]
    check(fx.index_sql("SELECT last_rowid FROM search_cursor"
                       " WHERE tenant='alpha'")[0]["last_rowid"] == max_row,
          "cursor covers the whole landing prefix after repair")
    # idempotent repeat: no new rows, no duplicate documents, same answer
    before_docs = fx.index_sql("SELECT COUNT(*) c FROM search_docs")[0]["c"]
    rep2 = Indexer(fx.index, fx.landing).repair()
    check(rep2.tenants[0].status == "already_done",
          "a repeated repair performs no second audit", str(rep2.tenants[0]))
    check(rep2.tenants[0].rows_checked == 0,
          "repeat repair reads zero rows", str(rep2.tenants[0]))
    after_docs = fx.index_sql("SELECT COUNT(*) c FROM search_docs")[0]["c"]
    check(after_docs == before_docs, "repeat repair adds no records",
          f"{after_docs} vs {before_docs}")
    check(len(fx.index_sql(
        "SELECT * FROM search_repair WHERE tenant='alpha'")) == 1,
        "repeat repair does not append journal rows")
    seen_new, total_new = _has(fx.engine(), "newmarker", "A500")
    check(seen_new and total_new == len(names) - 1,
          "answers stay identical after the repeated repair")
    fx.close()


def test_repair_other_tenant_untouched():
    section("S15b repair keeps another institution's index intact")
    fx = Fixture()
    reached, out, names = _build_legacy_broken(fx)
    check(reached, "legacy indexer was killed in its second chunk", out)
    # beta's own healthy index, built by an old run before alpha was broken
    for i in range(5):
        fx.add(f"Z{i:02d}", {"title": f"beta {i}",
                             "abstract": f"betaterm{i} stable",
                             "publication_year": 2024}, tenant="beta")
    fx.sync()
    before = fx.index_sql(
        "SELECT doc_id, accession, revision, title, abstract FROM search_docs"
        " WHERE tenant='beta' ORDER BY accession")
    beta_cursor = fx.index_sql(
        "SELECT last_rowid FROM search_cursor WHERE tenant='beta'")[0]
    rep = Indexer(fx.index, fx.landing).repair(tenants=["alpha"])
    check(rep.tenants[0].tenant == "alpha"
          and rep.tenants[0].status == "repaired",
          "scoped repair fixes alpha", str(rep.tenants[0]))
    after = fx.index_sql(
        "SELECT doc_id, accession, revision, title, abstract FROM search_docs"
        " WHERE tenant='beta' ORDER BY accession")
    check(before == after, "beta documents are byte-identical after repair",
          f"{before} vs {after}")
    check(fx.index_sql("SELECT last_rowid FROM search_cursor"
                       " WHERE tenant='beta'")[0] == beta_cursor,
          "beta cursor is untouched by an alpha-scoped repair")
    check(fx.index_sql(
        "SELECT COUNT(*) c FROM search_repair WHERE tenant='beta'")[0]["c"]
        == 0, "repair creates no beta journal row")
    r = SearchEngine(store.connect(fx.index)).search(
        "betaterm3", tenant="beta")
    check([h.accession for h in r.page] == ["Z03"],
          "beta still answers correctly")
    check(Indexer(fx.index, fx.landing).repair(
        tenants=["alpha"]).tenants[0].status == "already_done",
        "a repeated scoped repair is a no-op")
    fx.close()


# --------------------------------------------------------------------------
# S16 repair interrupted, then new revisions appended mid-repair
# --------------------------------------------------------------------------
def test_repair_resume_and_new_rows():
    section("S16 repair resumes after a hard exit; new rows stay for update")
    fx = Fixture()
    # 1001 accessions: the legacy second chunk leaves 501 identities missing,
    # so the repaired catch-up spans several commit chunks.
    reached, out, names = _build_legacy_broken(fx, count=1001)
    check(reached, "legacy indexer was killed in its second chunk", out)

    marker = os.path.join(fx.dir, "repair.marker")
    wait_file = os.path.join(fx.dir, "repair.wait")
    with open(wait_file, "w", encoding="utf-8"):
        pass
    driver = os.path.join(fx.dir, "repair_driver.py")
    with open(driver, "w", encoding="utf-8") as fh:
        fh.write(
            "import os, sys, time\n"
            f"sys.path.insert(0, {ROOT!r})\n"
            "from search.indexer import Indexer\n"
            f"marker = {marker!r}\n"
            f"wait = {wait_file!r}\n"
            "calls = {'n': 0}\n"
            "def barrier(name):\n"
            "    if name != 'apply_chunk_txn':\n"
            "        return\n"
            "    calls['n'] += 1\n"
            "    if calls['n'] == 2:\n"
            "        with open(marker, 'w') as fh:\n"
            "            fh.write('second')\n"
            "        while True:\n"
            "            time.sleep(0.01)\n"
            f"Indexer({fx.index!r}, {fx.landing!r}, crash_barrier=barrier,"
            " chunk_size=2).repair()\n")
    proc = subprocess.Popen([sys.executable, driver],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    deadline = time.time() + 60
    while time.time() < deadline and not os.path.exists(marker):
        if proc.poll() is not None:
            break
        time.sleep(0.01)
    reached_mid = os.path.exists(marker)
    if proc.poll() is None:
        proc.kill()
    out, _ = proc.communicate(timeout=15)
    if os.path.exists(wait_file):
        os.remove(wait_file)
    check(reached_mid, "repair was hard-exited inside its second chunk", out)
    jr = fx.index_sql("SELECT status, rewind_to, missing_count FROM"
                      " search_repair WHERE tenant='alpha'")[0]
    check(jr["status"] == "audited", "journal survives: audit is durable",
          str(jr))
    pending = fx.index_sql(
        "SELECT COUNT(*) c FROM search_repair_missing")[0]["c"]
    check(pending >= 1, "missing identities survive the exit", str(pending))
    # chunk 2 rolled back: the tail is still missing and the cursor stopped
    # at the first committed repair chunk
    cursor_mid = fx.index_sql("SELECT last_rowid FROM search_cursor"
                              " WHERE tenant='alpha'")[0]["last_rowid"]
    gap_low = fx.raw_sql(
        "SELECT MIN(rowid) m FROM result_versions"
        " WHERE tenant='alpha' AND revision=2")[0]["m"]
    check(gap_low > 1001,
          "second-generation rows start after the first-generation prefix",
          str(gap_low))
    seen_tail, _ = _has(fx.engine(), "newmarker", names[-1])
    check(not seen_tail and cursor_mid < fx.raw_sql(
        "SELECT MAX(rowid) m FROM result_versions"
        " WHERE tenant='alpha' AND revision=2")[0]["m"],
          "interrupted repair left the tail un-consumed",
          f"cursor={cursor_mid}")

    # new revisions arrive while a repair is in progress
    _add_batch(fx, names, 4, "latermarker")
    new_rowids = fx.raw_sql(
        "SELECT MIN(rowid) m FROM result_versions WHERE revision=4"
        " AND tenant='alpha'")[0]["m"]
    resumed = Indexer(fx.index, fx.landing).repair()
    tr = resumed.tenants[0]
    check(tr.status == "repaired", "resumed repair completes", str(tr))
    seen_new, total_new = _has(fx.engine(), "newmarker", "A500")
    check(seen_new, "A500 second revision is recovered on resume")
    # newly appended third generation must NOT be marked consumed by repair
    cursor = fx.index_sql("SELECT last_rowid FROM search_cursor"
                          " WHERE tenant='alpha'")[0]["last_rowid"]
    check(cursor < new_rowids,
          "repair cursor stops below revisions appended during repair",
          f"cursor={cursor} < new rows from {new_rowids}")
    seen_later, _ = _has(fx.engine(), "latermarker", "A001")
    check(not seen_later, "mid-repair arrivals are not indexed yet")
    check(fx.index_sql(
        "SELECT COUNT(*) c FROM search_ledger WHERE revision=4 AND"
        " tenant='alpha'")[0]["c"] == 0,
        "mid-repair arrivals are not in the consumption ledger")
    # the next normal update consumes exactly those new rows
    rep = fx.sync()
    check(rep.scanned == len(names),
          "the later update reads only the newly appended rows",
          f"scanned={rep.scanned}")
    seen_later, total_later = _has(fx.engine(), "latermarker", "A001")
    check(seen_later and total_later == len(names),
          "newly appended generation becomes current after the explicit"
          " update", f"total={total_later}")
    max_row = fx.raw_sql(
        "SELECT COALESCE(MAX(rowid),0) m FROM result_versions")[0]["m"]
    check(fx.index_sql("SELECT last_rowid FROM search_cursor"
                       " WHERE tenant='alpha'")[0]["last_rowid"] == max_row,
          "cursor finally covers the whole appended prefix")
    # repair repeated after completion is a no-op
    rep3 = Indexer(fx.index, fx.landing).repair()
    check(rep3.tenants[0].status == "already_done"
          and rep3.tenants[0].rows_checked == 0,
          "completed repair adds nothing on a repeat", str(rep3.tenants[0]))
    fx.close()


# --------------------------------------------------------------------------
# S13b one accession spanning chunks: content and progress stay in step
# --------------------------------------------------------------------------
def test_same_accession_across_chunks():
    section("S13b identical accession across commit chunks stays consistent")
    fx = Fixture()
    n = 12
    names = [f"C{i:02d}" for i in range(n)]
    for name in names:
        fx.add(name, {"title": f"c {name}", "abstract": "gen1 marker",
                      "publication_year": 2024}, revision=1)
    fx.sync()
    # revision 2 of C00..C04, revision 3 of C01 and C04, then revision 2 of
    # C05..C11: with chunk_size 3 the revisions of C04 span two chunks.
    for name in names[:5]:
        fx.add(name, {"title": f"c {name}", "abstract": "gen2 marker",
                      "publication_year": 2024}, revision=2)
    for name in ("C01", "C04"):
        fx.add(name, {"title": f"c {name}", "abstract": "gen3 marker",
                      "publication_year": 2024}, revision=3)
    for name in names[5:]:
        fx.add(name, {"title": f"c {name}", "abstract": "gen2 marker",
                      "publication_year": 2024}, revision=2)
    rep = Indexer(fx.index, fx.landing, chunk_size=3).sync()
    check(rep.documents_upserted > 0, "chunked run applied revisions", str(rep))
    eng = fx.engine()
    r3 = eng.search("abstract:gen3", tenant="alpha", limit=100)
    check({h.accession for h in r3.page} == {"C01", "C04"} and r3.total == 2,
          "newest revision wins within an accession spanning chunks",
          str([(h.accession, h.revision) for h in r3.page]))
    r2 = eng.search("abstract:gen2", tenant="alpha", limit=100)
    check({h.accession for h in r2.page}
          == {n for n in names if n not in ("C01", "C04")},
          "older revisions of the split accession are gone",
          str(sorted(h.accession for h in r2.page)))
    r1 = eng.search("abstract:gen1", tenant="alpha", limit=100)
    check(r1.total == 0, "no stale first-generation text remains")
    # independent truth check: postings must match the stored document text
    for row in fx.index_sql(
            "SELECT doc_id, title, abstract, title_tokens, abs_tokens"
            " FROM search_docs"):
        doc = fx.index_sql(
            "SELECT field, SUM(tf) s FROM search_postings"
            " WHERE doc_id=?", (row["doc_id"],)) if False else None
        ok = True
        for field_name, text, count_col in (
                ("title", row["title"], row["title_tokens"]),
                ("abstract", row["abstract"], row["abs_tokens"])):
            ntok = len(tokenize(text))
            if ntok != count_col:
                ok = False
            rows = fx.index_sql(
                "SELECT positions, tf FROM search_postings p JOIN search_terms"
                " t ON t.term_id=p.term_id WHERE p.doc_id=? AND p.field=?",
                (row["doc_id"], field_name))
            if sum(r["tf"] for r in rows) != count_col:
                ok = False
        check(ok, f"postings and token counts agree with text {row['doc_id']}",
              str(row))
    # and the engine agrees with the naive interpreter on this state
    naive = NaiveEngine(_naive_docs(fx))
    for expr in ("abstract:gen3", "abstract:gen2", "title:c"):
        full = eng.search(expr, tenant="alpha", limit=1000)
        ref = naive.search(expr, tenant="alpha")
        check({h.accession for h in full.page}
              == {h.doc.accession for h in ref},
              f"engine matches naive across chunks: {expr!r}")
    # cursor is the landing max after the run
    max_row = fx.raw_sql(
        "SELECT COALESCE(MAX(rowid),0) m FROM result_versions")[0]["m"]
    check(fx.index_sql("SELECT last_rowid FROM search_cursor"
                       " WHERE tenant='alpha'")[0]["last_rowid"] == max_row,
          "chunked cursor reaches the landing max")
    fx.close()


# --------------------------------------------------------------------------
# S17 the full operator recovery flow over the reported scene
# --------------------------------------------------------------------------
def test_operator_recovery_flow():
    section("S17 operator recovery: reproduce, repair, verify reproducible")
    fx = Fixture()
    reached, out, names = _build_legacy_broken(fx)
    check(reached, "legacy indexer was killed in its second chunk", out)
    eng = fx.engine()
    seen, _ = _has(eng, "newmarker", "A500")
    check(not seen, "reported symptom reproduced: A500 missing from newmarker")
    # status reports the repaired journal state before and after
    status_before = fx.index_sql(
        "SELECT COUNT(*) c FROM search_repair")[0]["c"]
    check(status_before == 0, "a legacy index has no repair journal yet")
    tr = Indexer(fx.index, fx.landing).repair().tenants[0]
    check(tr.status == "repaired" and tr.missing,
          "repair names the recovered identities", str(tr))
    check(tr.audited_from == 0 and tr.audited_to > 0,
          "audit scope covers the consumed prefix", str(tr))
    row = fx.index_sql("SELECT * FROM search_repair"
                       " WHERE tenant='alpha'")[0]
    check(row["status"] == "done" and row["missing_count"] == 1,
          "journal shows one recovered input and completion", str(row))
    check(fx.index_sql(
        "SELECT COUNT(*) c FROM search_repair_missing")[0]["c"] == 1,
        "outstanding-gap table keeps the audited identity for audit")
    seen, total = _has(fx.engine(), "newmarker", "A500")
    check(seen and total == len(names) - 1,
          "recovered index answers the query correctly")
    # a normal update afterwards reads nothing and adds no records
    rep = fx.sync()
    check(rep.scanned == 0 and rep.new_identities == 0,
          "after repair update is a clean no-op", str(rep))
    # rebuild remains available but is not what recovered the scene: the
    # journal is evidence that repair did it, no rebuild was needed.
    check(fx.index_sql("SELECT COUNT(*) c FROM search_docs")[0]["c"]
          == len(names), "one document per accession, no duplicates")
    fx.close()


def main():
    tests = [test_parser, test_tokenizer_offsets, test_engine_and_naive,
             test_incremental, test_crash_consistency,
             test_tenant_isolation, test_snapshot_consistency,
             test_multi_chunk_crash,
             test_interleaved_accessions_across_commit_boundaries,
             test_same_accession_across_chunks,
             test_document_text_matches_landing_truth,
             test_repair_legacy_broken_index,
             test_repair_other_tenant_untouched,
             test_repair_resume_and_new_rows,
             test_operator_recovery_flow]
    for t in tests:
        t()
    print("\n" + "=" * 60)
    if FAILURES:
        print(f"FAILED {len(FAILURES)} check(s):")
        for label in FAILURES:
            print(" -", label)
        return 1
    print("ALL SEARCH ACCEPTANCE CHECKS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
