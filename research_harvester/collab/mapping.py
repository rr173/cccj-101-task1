"""Affiliation mapping table: load, validate, digest, resolve.

The mapping is a local JSON document that assigns every known affiliation
string (official name or alias) to exactly one stable institution code::

    {
      "mapping_version": "v1",
      "institutions": [
        {"code": "INST-A", "name": "Alpha University",
         "aliases": ["Alpha Univ", "AU"]}
      ]
    }

Rules (violations raise :class:`MappingError` and refuse the command):

* ``code`` and ``name`` are mandatory non-empty strings; codes are unique.
* ``aliases`` is an optional list of strings.
* A name -- official or alias -- may never be assigned to two different
  codes.  The check is exact string equality; there is no similarity
  guessing anywhere in this module.
* Resolution is exact string equality against the official names and
  aliases.  A string not present in the table is *unmapped*: it is reported
  as a diagnostic and never merged into a shared "unknown" node.

The content digest binds a network file to the mapping it was created with.
It is computed over a canonical form (institutions sorted by code, aliases
sorted and de-duplicated), so semantically identical files digest equally
while any real change of the code/name assignment is detected.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Dict, List


class MappingError(Exception):
    """The mapping file is invalid or conflicts with a bound network."""


@dataclass
class Institution:
    code: str
    name: str
    aliases: List[str] = field(default_factory=list)


@dataclass
class Mapping:
    version: str
    institutions: List[Institution]
    lookup: Dict[str, str]          # every name/alias -> code (exact match)
    digest: str                     # sha256 over the canonical content

    def resolve(self, raw: str):
        """Return the institution code for an exact affiliation string."""
        return self.lookup.get(raw)

    def name_of(self, code: str) -> str:
        for inst in self.institutions:
            if inst.code == code:
                return inst.name
        return code


def _canonical(institutions, version):
    return {
        "mapping_version": version,
        "institutions": [
            {"code": inst.code, "name": inst.name,
             "aliases": sorted(set(inst.aliases))}
            for inst in sorted(institutions, key=lambda i: i.code)
        ],
    }


def digest_of(institutions, version) -> str:
    blob = json.dumps(_canonical(institutions, version),
                      sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def parse_mapping(doc) -> Mapping:
    """Validate a decoded JSON document and build the lookup table."""
    if not isinstance(doc, dict):
        raise MappingError("对照表顶层必须是 JSON 对象")
    version = doc.get("mapping_version", "")
    if not isinstance(version, str) or not version.strip():
        raise MappingError("对照表缺少非空的 mapping_version 字符串")
    raw_insts = doc.get("institutions")
    if not isinstance(raw_insts, list) or not raw_insts:
        raise MappingError("对照表缺少非空的 institutions 数组")

    institutions: List[Institution] = []
    seen_codes = set()
    lookup: Dict[str, str] = {}
    for idx, entry in enumerate(raw_insts):
        where = f"institutions[{idx}]"
        if not isinstance(entry, dict):
            raise MappingError(f"{where} 必须是对象")
        code = entry.get("code")
        if not isinstance(code, str) or not code.strip():
            raise MappingError(f"{where}.code 必须是非空字符串")
        code = code.strip()
        if code in seen_codes:
            raise MappingError(f"机构代码重复：{code!r}")
        seen_codes.add(code)
        name = entry.get("name")
        if not isinstance(name, str) or not name.strip():
            raise MappingError(f"{where}.name 必须是非空字符串")
        name = name.strip()
        aliases_raw = entry.get("aliases", [])
        if not isinstance(aliases_raw, list) or \
                any(not isinstance(a, str) for a in aliases_raw):
            raise MappingError(f"{where}.aliases 必须是字符串数组")
        aliases = [a.strip() for a in aliases_raw if a.strip()]

        for label in (name, *aliases):
            other = lookup.get(label)
            if other is not None and other != code:
                raise MappingError(
                    f"名称 {label!r} 同时分配给两个代码 "
                    f"{other!r} 与 {code!r}；同一名称不允许归属两个机构")
            lookup[label] = code
        institutions.append(Institution(code=code, name=name,
                                        aliases=aliases))
    return Mapping(version=version.strip(), institutions=institutions,
                   lookup=lookup,
                   digest=digest_of(institutions, version.strip()))


def load_mapping(path: str) -> Mapping:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
    except FileNotFoundError:
        raise MappingError(f"对照表文件不存在：{path}")
    except json.JSONDecodeError as exc:
        raise MappingError(f"对照表不是合法 JSON：{exc}")
    return parse_mapping(doc)
