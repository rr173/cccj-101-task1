"""Versioned quality contract: loading, validation, normalization, evaluation.

A rules file is read exactly once per processing run (load_rules returns an
immutable snapshot).  Everything downstream -- including the generation that is
later published -- uses that snapshot; a file replaced on disk after loading
cannot change decisions already prepared.

Stable rule identifiers (referenced by quarantine rows and the audit trail):

  R-PAYLOAD-001  payload missing or not a JSON object
  R-PAYLOAD-002  payload is not valid JSON
  R-TITLE-001    title missing
  R-TITLE-002    title is not text
  R-TITLE-003    title is empty / only whitespace
  R-TITLE-004    title longer than max_length
  R-TITLE-005    title contains control characters
  R-YEAR-001     publication_year missing
  R-YEAR-002     publication_year is not an integer (bool and floats reject)
  R-YEAR-003     publication_year outside the allowed range
  R-DOI-001      doi is not text
  R-DOI-002      doi longer than max_length after normalization
  R-DOI-003      doi does not match the configured pattern
  R-DOI-004      normalized doi collision between two otherwise-valid results
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

RULE_IDS = {
    "payload_missing": "R-PAYLOAD-001",
    "payload_json": "R-PAYLOAD-002",
    "title_missing": "R-TITLE-001",
    "title_type": "R-TITLE-002",
    "title_empty": "R-TITLE-003",
    "title_too_long": "R-TITLE-004",
    "title_control": "R-TITLE-005",
    "year_missing": "R-YEAR-001",
    "year_type": "R-YEAR-002",
    "year_range": "R-YEAR-003",
    "doi_type": "R-DOI-001",
    "doi_too_long": "R-DOI-002",
    "doi_format": "R-DOI-003",
    "doi_conflict": "R-DOI-004",
}


class RuleError(Exception):
    """Rules file is missing/invalid."""


class RuleVersionConflict(RuleError):
    """A rule_version already used with different content."""


@dataclass(frozen=True)
class RuleSnapshot:
    version: str
    sha256: str
    content: str
    config: Dict[str, Any]
    path: Optional[str] = None

    def section(self, name: str) -> Dict[str, Any]:
        return self.config.get(name) or {}


@dataclass
class Violation:
    rule_id: str
    field: str
    value: Any
    message: str

    def as_dict(self) -> Dict[str, Any]:
        return {"rule_id": self.rule_id, "field": self.field,
                "value": self.value, "message": self.message}


@dataclass
class Evaluation:
    outcome: str                      # "clean" | "quarantine"
    title: Optional[str] = None
    publication_year: Optional[int] = None
    doi: Optional[str] = None         # normalized form
    violations: List[Violation] = field(default_factory=list)
    original: Dict[str, Any] = field(default_factory=dict)

    @property
    def rule_ids(self) -> List[str]:
        seen = []
        for v in self.violations:
            if v.rule_id not in seen:
                seen.append(v.rule_id)
        return seen


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _check_bool(value, name):
    if not isinstance(value, bool):
        raise RuleError(f"{name} must be a boolean")
    return value


def _check_int(value, name, minimum=None):
    if not _is_int(value):
        raise RuleError(f"{name} must be an integer")
    if minimum is not None and value < minimum:
        raise RuleError(f"{name} must be >= {minimum}")
    return value


def validate_config(config: Dict[str, Any]) -> None:
    """Reject ambiguous contracts up front so a batch never half-applies."""
    if not isinstance(config, dict):
        raise RuleError("rules content must be a JSON object")
    version = config.get("rule_version")
    if not isinstance(version, str) or not version.strip():
        raise RuleError("rule_version must be a non-empty string")

    title = config.get("title")
    if not isinstance(title, dict):
        raise RuleError("title section is required")
    _check_bool(title.get("required", True), "title.required")
    _check_int(title.get("min_length", 1), "title.min_length", 0)
    _check_int(title.get("max_length", 2000), "title.max_length", 1)
    if title["max_length"] < title.get("min_length", 1):
        raise RuleError("title.max_length must be >= title.min_length")
    _check_bool(title.get("forbid_control_chars", True),
               "title.forbid_control_chars")

    year = config.get("publication_year")
    if not isinstance(year, dict):
        raise RuleError("publication_year section is required")
    _check_bool(year.get("required", True), "publication_year.required")
    if year.get("type", "integer") != "integer":
        raise RuleError("publication_year.type must be 'integer'")
    _check_int(year.get("min", 1800), "publication_year.min")
    _check_int(year.get("max", 2026), "publication_year.max")
    if year["max"] < year["min"]:
        raise RuleError("publication_year.max must be >= min")

    doi = config.get("doi")
    if not isinstance(doi, dict):
        raise RuleError("doi section is required")
    _check_bool(doi.get("required", False), "doi.required")
    _check_int(doi.get("max_length", 512), "doi.max_length", 1)
    norm = doi.get("normalize") or {}
    if not isinstance(norm, dict):
        raise RuleError("doi.normalize must be an object")
    if norm.get("case", "lower") not in ("lower", "keep"):
        raise RuleError("doi.normalize.case must be 'lower' or 'keep'")
    prefixes = norm.get("strip_prefixes", [])
    if not isinstance(prefixes, list) or not all(
            isinstance(p, str) for p in prefixes):
        raise RuleError("doi.normalize.strip_prefixes must be a string list")
    _check_bool(norm.get("trim_whitespace", True),
                "doi.normalize.trim_whitespace")
    if "collapse_spaces" in norm:
        _check_bool(norm["collapse_spaces"], "doi.normalize.collapse_spaces")
    pattern = doi.get("pattern")
    if not isinstance(pattern, str) or not pattern:
        raise RuleError("doi.pattern must be a non-empty string")
    try:
        re.compile(pattern)
    except re.error as e:
        raise RuleError(f"doi.pattern is not a valid regex: {e}") from e


def load_rules(path: str) -> RuleSnapshot:
    """Read the file once; the returned object is the batch's contract."""
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except OSError as e:
        raise RuleError(f"cannot read rules file {path!r}: {e}") from e
    try:
        content = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        raise RuleError(f"rules file is not UTF-8: {e}") from e
    try:
        config = json.loads(content)
    except json.JSONDecodeError as e:
        raise RuleError(f"rules file is not valid JSON: {e}") from e
    validate_config(config)
    return RuleSnapshot(version=config["rule_version"].strip(),
                        sha256=hashlib.sha256(raw).hexdigest(),
                        content=content, config=config, path=path)


def normalize_doi(raw: str, config: Dict[str, Any]) -> str:
    """Normalize a DOI candidate exactly according to the snapshot's config.

    Whitespace handling is deliberately conservative: surrounding whitespace is
    trimmed and whitespace around a stripped prefix is discarded, but spaces
    *inside* the identifier are preserved (and then rejected by the pattern),
    because silently gluing tokens together would invent a different DOI.
    """
    norm_cfg = config.get("normalize") or {}
    value = raw
    if norm_cfg.get("trim_whitespace", True):
        value = value.strip()
    prefixes = norm_cfg.get("strip_prefixes", [])
    changed = True
    while changed:
        changed = False
        low = value.lower()
        for prefix in prefixes:
            if prefix and low.startswith(prefix.lower()):
                value = value[len(prefix):]
                if norm_cfg.get("trim_whitespace", True):
                    value = value.strip()
                changed = True
                break
    if norm_cfg.get("case", "lower") == "lower":
        value = value.lower()
    return value


def evaluate_payload(payload_text: Optional[str],
                     rules: RuleSnapshot) -> Evaluation:
    """Evaluate one landing payload against the immutable rule snapshot."""
    cfg = rules.config
    title_cfg = rules.section("title")
    year_cfg = rules.section("publication_year")
    doi_cfg = rules.section("doi")
    original: Dict[str, Any] = {}
    violations: List[Violation] = []

    def quarantine():
        return Evaluation(outcome="quarantine", violations=violations,
                          original=original)

    if payload_text is None:
        violations.append(Violation(
            RULE_IDS["payload_missing"], "payload", None,
            "payload is missing (retracted versions carry no payload)"))
        return quarantine()
    try:
        payload = json.loads(payload_text)
    except (json.JSONDecodeError, TypeError) as e:
        violations.append(Violation(
            RULE_IDS["payload_json"], "payload", payload_text,
            f"payload is not valid JSON: {e}"))
        return quarantine()
    if not isinstance(payload, dict):
        violations.append(Violation(
            RULE_IDS["payload_missing"], "payload", payload,
            "payload must be a JSON object"))
        return quarantine()

    missing = object()
    raw_title = payload.get("title", missing)
    raw_year = payload.get("publication_year", missing)
    raw_doi = payload.get("doi", missing)
    original = {
        "title": None if raw_title is missing else raw_title,
        "publication_year": None if raw_year is missing else raw_year,
        "doi": None if raw_doi is missing else raw_doi,
    }

    # -- title ---------------------------------------------------------
    title = None
    if raw_title is missing or raw_title is None:
        if title_cfg.get("required", True):
            violations.append(Violation(
                RULE_IDS["title_missing"], "title", None,
                "title is required but missing"))
    elif not isinstance(raw_title, str):
        violations.append(Violation(
            RULE_IDS["title_type"], "title", raw_title,
            "title must be text"))
    else:
        stripped = raw_title.strip()
        if len(stripped) < title_cfg.get("min_length", 1):
            violations.append(Violation(
                RULE_IDS["title_empty"], "title", raw_title,
                "title must contain non-whitespace text"))
        elif len(stripped) > title_cfg.get("max_length", 2000):
            violations.append(Violation(
                RULE_IDS["title_too_long"], "title", raw_title,
                f"title exceeds {title_cfg['max_length']} characters"))
        elif title_cfg.get("forbid_control_chars", True) and any(
                ord(ch) < 32 or ord(ch) == 127 for ch in stripped):
            violations.append(Violation(
                RULE_IDS["title_control"], "title", raw_title,
                "title contains control characters"))
        else:
            title = stripped

    # -- publication year ---------------------------------------------
    year = None
    if raw_year is missing or raw_year is None:
        if year_cfg.get("required", True):
            violations.append(Violation(
                RULE_IDS["year_missing"], "publication_year", None,
                "publication_year is required but missing"))
    elif not _is_int(raw_year):
        violations.append(Violation(
            RULE_IDS["year_type"], "publication_year", raw_year,
            "publication_year must be an integer"))
    elif not (year_cfg["min"] <= raw_year <= year_cfg["max"]):
        violations.append(Violation(
            RULE_IDS["year_range"], "publication_year", raw_year,
            f"publication_year must be between {year_cfg['min']} and "
            f"{year_cfg['max']}"))
    else:
        year = raw_year

    # -- doi (optional) ------------------------------------------------
    doi_norm = None
    if raw_doi is missing or raw_doi is None or (
            isinstance(raw_doi, str) and not raw_doi.strip()):
        if doi_cfg.get("required", False):
            violations.append(Violation(
                RULE_IDS["doi_type"], "doi", raw_doi,
                "doi is required but missing"))
    elif not isinstance(raw_doi, str):
        violations.append(Violation(
            RULE_IDS["doi_type"], "doi", raw_doi,
            "doi must be text when present"))
    else:
        normalized = normalize_doi(raw_doi, doi_cfg)
        if len(normalized) > doi_cfg.get("max_length", 512):
            violations.append(Violation(
                RULE_IDS["doi_too_long"], "doi", raw_doi,
                "normalized doi is longer than the allowed maximum"))
        elif not re.fullmatch(doi_cfg["pattern"], normalized):
            violations.append(Violation(
                RULE_IDS["doi_format"], "doi", raw_doi,
                f"normalized doi {normalized!r} does not match the "
                "allowed pattern"))
        else:
            doi_norm = normalized

    return Evaluation(
        outcome="quarantine" if violations else "clean",
        title=title, publication_year=year, doi=doi_norm,
        violations=violations, original=original)
