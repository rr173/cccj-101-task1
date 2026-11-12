"""Deterministic tokenization with offsets mapped to the ORIGINAL text.

Contract (the project's fixed segmentation convention)
------------------------------------------------------
1. Normalize the text with Unicode NFKC, then ``str.casefold``.
2. A run of consecutive Latin letters or decimal digits is one word token;
   each Han (CJK ideograph) character is its own token at its own position.
   Punctuation and whitespace only separate tokens and occupy no token
   position.  Every other character (other scripts, symbols, control chars)
   likewise separates and occupies no position.
3. Phrases therefore mean "consecutive token positions inside one field";
   a phrase can never run from the end of a title into the beginning of an
   abstract because positions are numbered per field.

The hard part is requirement 3 of the acceptance brief: returned half-open
character intervals count *Unicode characters of the unnormalized original*,
even when normalization changes the number of characters.  Examples that must
stay exact:

  full-width   ＡＢ        (NFKC -> AB, same length)
  ligature     ﬁle        (NFKC -> fi + le, one source char becomes two)
  casefold     Straße     (casefold -> strasse, the ß becomes two chars)
  combining    é    (NFKC may compose two source chars into one)

Implementation
--------------
We build an aligned stream of normalized pieces, each tagged with the span
[orig_start, orig_end) of source characters that produced it:

* stage 1: apply NFKC *per source character* (most compatibility mappings are
  per-character, so a piece never crosses a source-character boundary);
* stage 2: canonical composition of a base piece followed by combining marks,
  which is the common way NFKC merges two source characters into one;
* stage 3: casefold each piece; casefold never composes, so pieces stay
  aligned to the same source span.

A piece may contain several normalized characters; adjacent pieces are then
classified into tokens.  A token's span is the union of the spans of its
pieces, which is exactly the half-open interval of original Unicode code
points whose normalized forms contributed to the token.
"""
from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class Token:
    """One indexed token.

    term        normalized surface form as stored in the dictionary
    position    zero-based token position *within the field*
    start       half-open start interval in ORIGINAL Unicode characters
    end         half-open end interval in ORIGINAL Unicode characters
    """
    term: str
    position: int
    start: int
    end: int


def is_latin(ch: str) -> bool:
    """True for Latin-script letters (including Latin-1 supplement etc.)."""
    if not ch.isalpha():
        return False
    # \N LATIN ... LETTER ... also covers e.g. Latin Extended-[A-Z].
    name = unicodedata.name(ch, "")
    return name.startswith("LATIN ") and "LETTER" in name


def is_han(ch: str) -> bool:
    """True for CJK ideographs (the characters that tokenize one per char)."""
    cp = ord(ch)
    # CJK Unified Ideographs and extensions A..G, Compatibility Ideographs.
    ranges = (
        (0x3400, 0x4DBF),     # Ext A
        (0x4E00, 0x9FFF),     # Unified
        (0xF900, 0xFAFF),     # Compatibility
        (0x20000, 0x2A6DF),   # Ext B
        (0x2A700, 0x2EBEF),   # Ext C/D/E/F
        (0x2F800, 0x2FA1F),   # Compatibility supplement
        (0x30000, 0x3134F),   # Ext G/H
    )
    return any(lo <= cp <= hi for lo, hi in ranges)


def is_decimal(ch: str) -> bool:
    """Decimal digits only; full-width digits arrive here after NFKC."""
    return unicodedata.category(ch) == "Nd"


def _is_combinable(ch: str) -> bool:
    return unicodedata.category(ch) in ("Mn", "Mc", "Me")


@dataclass
class _Piece:
    text: str          # normalized (NFKC + casefold) characters
    start: int         # original span [start, end)
    end: int


def _aligned_pieces(text: str) -> List[_Piece]:
    """Produce normalized pieces while retaining original character spans."""
    # Stage 1: per-character NFKC.  NFC/NFKC of a single character never needs
    # context beyond combining sequences, which we repair in stage 2.
    pieces: List[_Piece] = []
    for i, ch in enumerate(text):
        norm = unicodedata.normalize("NFKC", ch)
        if norm:
            pieces.append(_Piece(norm, i, i + 1))

    # Stage 2: canonical composition across piece boundaries.  NFKC on the
    # whole string may compose a base + combining mark(s) that per-character
    # normalization left separate (e.g. "e" + U+0301 -> U+00E9), including
    # Hangul jamo sequences.  Walk left to right, greedily composing.
    composed: List[_Piece] = []
    for p in pieces:
        merged = False
        if composed and _is_combinable(p.text[0]) and len(p.text) == 1:
            candidate = unicodedata.normalize(
                "NFC", composed[-1].text + p.text)
            # NFC composes when it can and does not reorder other marks;
            # a shorter result means a real composition happened.
            if len(candidate) < len(composed[-1].text) + len(p.text):
                composed[-1].text = candidate
                composed[-1].end = p.end
                merged = True
            else:
                # Reorder only: attach without changing alignment.
                composed[-1].text = candidate
                composed[-1].end = p.end
                merged = True
        if not merged:
            composed.append(_Piece(p.text, p.start, p.end))

    # Stage 3: casefold per piece; casefold maps 1 char to N chars but never
    # composes pieces, so source spans stay valid.
    for p in composed:
        p.text = p.text.casefold()
    return composed


def _is_word_piece(text: str) -> bool:
    """A piece belongs to a Latin/digit word run.

    Combining marks may appear *inside* a piece (e.g. casefold of 'İ' is
    'i' + U+0307); they are allowed as word-internal characters but cannot
    make a piece word-like on their own.
    """
    has_word = False
    for ch in text:
        if is_latin(ch) or is_decimal(ch):
            has_word = True
        elif not _is_combinable(ch):
            return False
    return has_word


def tokenize(text: str) -> List[Token]:
    """Tokenize one field's text into positioned tokens with original spans.

    Returns an empty list for ``None`` / empty / separator-only input.
    """
    if not text:
        return []
    pieces = _aligned_pieces(text)
    tokens: List[Token] = []
    position = 0
    i = 0
    n = len(pieces)
    while i < n:
        p = pieces[i]
        if _is_word_piece(p.text):
            # Greedy run of adjacent word pieces; punctuation/whitespace
            # pieces in between break the run (they carry no position).
            term_chars: List[str] = []
            start = p.start
            end = p.end
            j = i
            while j < n and _is_word_piece(pieces[j].text):
                q = pieces[j]
                term_chars.append(q.text)
                end = q.end
                j += 1
            tokens.append(Token("".join(term_chars), position, start, end))
            position += 1
            i = j
        elif is_han(p.text[0]) and len(p.text) == 1:
            # Each Han character occupies one token position.
            tokens.append(Token(p.text[0], position, p.start, p.end))
            position += 1
            i += 1
        else:
            # Separator: no token position consumed.
            i += 1
    return tokens


def term_terms(text: str) -> List[str]:
    """Normalized term sequence (used for phrase parsing of query input)."""
    return [t.term for t in tokenize(text)]


def merge_spans(spans: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Merge overlapping/adjacent half-open intervals into sorted disjoint ones."""
    if not spans:
        return []
    ordered = sorted(spans)
    merged = [ordered[0]]
    for start, end in ordered[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end:
            if end > last_end:
                merged[-1] = (last_start, end)
        else:
            merged.append((start, end))
    return merged


def highlight(text: str, spans: List[Tuple[int, int]]) -> str:
    """Render text with 【...】 around each merged hit span (CLI display)."""
    if not text:
        return ""
    out = []
    cursor = 0
    for start, end in merge_spans(spans):
        if start < cursor:
            continue
        out.append(text[cursor:start])
        out.append("【")
        out.append(text[start:end])
        out.append("】")
        cursor = end
    out.append(text[cursor:])
    return "".join(out)


def make_excerpt(text: str, spans: List[Tuple[int, int]],
                 radius: int = 40) -> str:
    """A window of original characters around the first hit, with highlights."""
    if not text:
        return ""
    if not spans:
        return text[: 2 * radius] + ("..." if len(text) > 2 * radius else "")
    first = min(spans)
    start = max(0, first[0] - radius)
    end = min(len(text), first[1] + radius)
    window = text[start:end]
    local_spans = [(max(0, s - start), min(len(window), e - start))
                   for s, e in spans if e > start and s < end]
    rendered = highlight(window, local_spans)
    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(text) else ""
    return prefix + rendered + suffix
