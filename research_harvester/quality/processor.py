"""Quality isolation processor: landing -> staged generation -> atomic publish.

Guarantees implemented here
---------------------------
* Identity is (tenant, accession, revision); the landing tables are the only
  input (no re-fetching, no invented history).
* Incremental progress is per tenant and rowid-based, so a late row or a row
  with a *smaller* revision than the tenant maximum is still detected.
* The newest revision of an accession is the only one judged.  When the newest
  revision breaks the contract the accession is quarantined -- the older clean
  version is NOT carried forward.  Retraction wins over payload quality.
* Two clean results sharing one normalized DOI are both quarantined with
  R-DOI-004.  The verdict is recomputed from scratch on every run, so a later
  revision that fixes or retracts one side automatically releases the other.
* All judgement uses one immutable rule snapshot per run.
* A generation is prepared off to the side (staging rows carry its
  generation_id), verified, then made visible by flipping one pointer row in a
  single transaction.  Readers only ever resolve through the pointer.
"""
from __future__ import annotations

import json
import time
from typing import Dict, List, Optional

from . import rules as rules_mod
from . import store


class ProcessingRefused(Exception):
    """Contract or precondition prevents this run from changing the projection."""


class _AccState:
    __slots__ = ("accession", "kind", "revision", "rule_ids", "doi",
                 "raw_page_id", "reasons", "original", "rule_version",
                 "title", "publication_year")

    def __init__(self, accession, kind, revision, rule_ids=None, doi=None,
                 raw_page_id=None, reasons=None, original=None,
                 rule_version=None, title=None, publication_year=None):
        self.accession = accession
        self.kind = kind
        self.revision = revision
        self.rule_ids = list(rule_ids or [])
        self.doi = doi
        self.raw_page_id = raw_page_id
        self.reasons = reasons or []
        self.original = original or {}
        self.rule_version = rule_version
        self.title = title
        self.publication_year = publication_year

    def snapshot(self):
        return {
            "state": self.kind,
            "revision": self.revision,
            "rule_ids": list(self.rule_ids),
            "doi": self.doi,
            "raw_page_id": self.raw_page_id,
            "rule_version": self.rule_version,
            "reasons": self.reasons,
            "original": self.original,
        }

    def comparable(self):
        return (self.kind, self.revision, tuple(sorted(self.rule_ids)),
                self.doi, json.dumps(self.reasons, sort_keys=True))


def _json_list(text):
    try:
        value = json.loads(text) if text else []
        return value if isinstance(value, list) else []
    except (ValueError, TypeError):
        return []


def _json_obj(text):
    try:
        value = json.loads(text) if text else {}
        return value if isinstance(value, dict) else {}
    except (ValueError, TypeError):
        return {}


def _landing_tenants(conn) -> List[str]:
    return [r["tenant"] for r in conn.execute(
        "SELECT DISTINCT tenant FROM result_versions ORDER BY tenant")]


def _tenant_rows(conn, tenant) -> List[dict]:
    rows = conn.execute(
        "SELECT rowid AS source_rowid, tenant, accession, revision, retracted,"
        " payload, raw_page_id FROM result_versions WHERE tenant=?"
        " ORDER BY accession, revision, rowid", (tenant,)).fetchall()
    return [dict(r) for r in rows]


def _max_rowid(conn, tenant) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(rowid), 0) m FROM result_versions WHERE tenant=?",
        (tenant,)).fetchone()
    return int(row["m"] or 0)


def _base_states(conn, generation_id, tenants) -> Dict[str, Dict[str, _AccState]]:
    base: Dict[str, Dict[str, _AccState]] = {t: {} for t in tenants}
    if generation_id is None or not tenants:
        return base
    placeholders = ",".join("?" for _ in tenants)
    for row in conn.execute(
            f"SELECT tenant, accession, revision, title, publication_year, doi,"
            f" raw_page_id, rule_version FROM quality_clean"
            f" WHERE generation_id=? AND tenant IN ({placeholders})",
            [generation_id, *tenants]):
        base[row["tenant"]][row["accession"]] = _AccState(
            row["accession"], "clean", row["revision"], doi=row["doi"],
            raw_page_id=row["raw_page_id"], rule_version=row["rule_version"],
            title=row["title"], publication_year=row["publication_year"])
    for row in conn.execute(
            f"SELECT tenant, accession, revision, rule_ids, reasons, original,"
            f" raw_page_id, rule_version FROM quality_quarantine"
            f" WHERE generation_id=? AND tenant IN ({placeholders})",
            [generation_id, *tenants]):
        base[row["tenant"]][row["accession"]] = _AccState(
            row["accession"], "quarantine", row["revision"],
            rule_ids=_json_list(row["rule_ids"]),
            raw_page_id=row["raw_page_id"],
            reasons=_json_list(row["reasons"]),
            original=_json_obj(row["original"]),
            rule_version=row["rule_version"])
    for row in conn.execute(
            f"SELECT tenant, accession, revision, raw_page_id"
            f" FROM quality_retracted WHERE generation_id=? AND tenant IN"
            f" ({placeholders})", [generation_id, *tenants]):
        base[row["tenant"]][row["accession"]] = _AccState(
            row["accession"], "retracted", row["revision"],
            raw_page_id=row["raw_page_id"])
    return base


def _build_tenant(rows, rule_snapshot):
    """Deterministically judge the *current* revision of every accession."""
    by_accession: Dict[str, List[dict]] = {}
    for row in rows:
        by_accession.setdefault(row["accession"], []).append(row)

    states: Dict[str, _AccState] = {}
    evaluations: Dict[str, rules_mod.Evaluation] = {}
    current: Dict[str, dict] = {}

    for accession, versions in by_accession.items():
        latest = max(versions, key=lambda v: int(v["revision"]))
        current[accession] = latest
        if latest["retracted"]:
            states[accession] = _AccState(
                accession, "retracted", int(latest["revision"]),
                raw_page_id=latest["raw_page_id"])
        else:
            evaluations[accession] = rules_mod.evaluate_payload(
                latest["payload"], rule_snapshot)

    # -- DOI uniqueness among otherwise-clean results --------------------
    doi_groups: Dict[str, List[str]] = {}
    for accession, ev in evaluations.items():
        if ev.outcome == "clean" and ev.doi:
            doi_groups.setdefault(ev.doi, []).append(accession)
    conflicted = {acc for group in doi_groups.values() if len(group) > 1
                  for acc in group}

    clean: List[_AccState] = []
    quarantine: List[_AccState] = []
    for accession in sorted(evaluations):
        ev = evaluations[accession]
        latest = current[accession]
        if ev.outcome == "clean" and accession not in conflicted:
            state = _AccState(
                accession, "clean", int(latest["revision"]), doi=ev.doi,
                raw_page_id=latest["raw_page_id"],
                rule_version=rule_snapshot.version, title=ev.title,
                publication_year=ev.publication_year)
            states[accession] = state
            clean.append(state)
            continue

        violations = list(ev.violations)
        original = dict(ev.original)
        if accession in conflicted:
            peers = sorted(a for a in doi_groups[ev.doi] if a != accession)
            original["normalized_doi"] = ev.doi
            violations.append(rules_mod.Violation(
                rules_mod.RULE_IDS["doi_conflict"], "doi", ev.doi,
                f"normalized doi {ev.doi!r} is shared by another valid result"))
        reasons = [v.as_dict() for v in violations]
        for reason in reasons:
            if reason["rule_id"] == rules_mod.RULE_IDS["doi_conflict"]:
                reason["conflicts_with"] = sorted(
                    a for a in doi_groups.get(ev.doi or "", [])
                    if a != accession)
        rule_ids = []
        for reason in reasons:
            if reason["rule_id"] not in rule_ids:
                rule_ids.append(reason["rule_id"])
        state = _AccState(
            accession, "quarantine", int(latest["revision"]),
            rule_ids=rule_ids, raw_page_id=latest["raw_page_id"],
            reasons=reasons, original=original,
            rule_version=rule_snapshot.version)
        states[accession] = state
        quarantine.append(state)

    retracted = sorted((s for s in states.values() if s.kind == "retracted"),
                       key=lambda s: (s.revision, s.accession))
    return states, clean, quarantine, retracted, current


def _classify_transition(before: Optional[_AccState], after: _AccState,
                         rule_snapshot) -> Optional[str]:
    if before is None:
        return {"clean": "entered_clean", "quarantine": "entered_quarantine",
                "retracted": "entered_retracted"}[after.kind]
    if before.kind != after.kind:
        if after.kind == "retracted":
            return "retracted"
        if after.kind == "quarantine":
            if rules_mod.RULE_IDS["doi_conflict"] in after.rule_ids:
                return "conflict_quarantined"
            return "quarantined"
        if before.kind == "quarantine" and after.kind == "clean":
            if rules_mod.RULE_IDS["doi_conflict"] in before.rule_ids:
                return "conflict_released"
            return "recovered"
        return "state_changed"
    if after.kind == "clean" and before.revision != after.revision:
        return "clean_revised"
    if after.kind == "quarantine" and (
            before.revision != after.revision
            or tuple(sorted(before.rule_ids)) != tuple(sorted(after.rule_ids))):
        return "quarantine_changed"
    if before.rule_version and before.rule_version != rule_snapshot.version:
        return "rule_reevaluated"
    return None


def _events_for_tenant(tenant, base, states, rule_snapshot) -> List[dict]:
    events = []
    for accession in sorted(set(base) | set(states)):
        before = base.get(accession)
        after = states.get(accession)
        if after is None:
            if before is None:
                continue
            events.append({
                "tenant": tenant, "accession": accession,
                "revision": before.revision, "event_type": "removed",
                "from_state": before.kind, "to_state": None,
                "rule_version": rule_snapshot.version,
                "rule_version_before": before.rule_version,
                "details": {"before": before.snapshot(), "after": None,
                            "raw_page_id": before.raw_page_id}})
            continue
        if before is None:
            event_type = {"clean": "entered_clean",
                          "quarantine": "entered_quarantine",
                          "retracted": "entered_retracted"}[after.kind]
        else:
            event_type = _classify_transition(before, after, rule_snapshot)
            if event_type is None:
                if before.comparable() == after.comparable():
                    # identical verdict at the same rule version: no trace
                    continue
                event_type = "state_changed"
        events.append({
            "tenant": tenant, "accession": accession,
            "revision": after.revision, "event_type": event_type,
            "from_state": before.kind if before else None,
            "to_state": after.kind,
            "rule_version": rule_snapshot.version,
            "rule_version_before": before.rule_version if before else None,
            "details": {
                "before": before.snapshot() if before else None,
                "after": after.snapshot(),
                "raw_page_id": after.raw_page_id,
                "rule_version_used": rule_snapshot.version,
            }})
    return events


def _pending_rows(conn, tenant, rows, last_rowid):
    """Rows that must be absorbed: new identities and changed content.

    Per-tenant progress is a rowid watermark, but it is never used as "the
    tenant's max revision".  A row may arrive late with a smaller revision,
    occupy a rowid gap below the watermark, or be rewritten in place; each is
    compared against the durable input ledger by identity + content hash.
    """
    known = {r["accession"] + "\x00" + str(r["revision"]): r["content_hash"]
             for r in conn.execute(
                 "SELECT accession, revision, content_hash FROM quality_inputs"
                 " WHERE tenant=?", (tenant,))}
    pending = []
    for row in rows:
        key = row["accession"] + "\x00" + str(int(row["revision"]))
        digest = store.content_hash(tenant, row["accession"], row["revision"],
                                    row["retracted"], row["raw_page_id"],
                                    row["payload"])
        if known.get(key) == digest:
            continue
        if last_rowid is not None and int(row["source_rowid"]) <= int(last_rowid) \
                and key not in known:
            # rowid gap: a newly landed identity below the watermark
            pending.append(row)
        elif known.get(key) is not None and known[key] != digest:
            # same identity, content rewritten in place
            pending.append(row)
        else:
            if last_rowid is None or int(row["source_rowid"]) > int(last_rowid):
                pending.append(row)
    return pending


class Processor:
    def __init__(self, db_path, rule_snapshot, *, on_tenant_applied=None,
                 now=time.time):
        self.db_path = db_path
        self.rules = rule_snapshot
        self.on_tenant_applied = on_tenant_applied
        self._now = now

    # -- helpers ---------------------------------------------------------
    def _open(self):
        conn = store.connect(self.db_path)
        store.require_landing(conn)
        store.init(conn)
        return conn

    def _start_run(self, conn):
        with conn:
            cur = conn.execute(
                "INSERT INTO quality_runs(rule_version, status, started_at)"
                " VALUES (?, 'running', ?)", (self.rules.version, time.time()))
        return cur.lastrowid

    def _finish_run(self, conn, run_id, status, generation_id=None,
                    new_inputs=0, dirty=None, counts=None, message=None):
        with conn:
            conn.execute(
                "UPDATE quality_runs SET generation_id=?, status=?,"
                " finished_at=?, new_inputs=?, dirty_tenants=?, counts=?,"
                " message=? WHERE run_id=?",
                (generation_id, status, time.time(), new_inputs,
                 store.dumps(sorted(dirty or [])),
                 store.dumps(counts) if counts is not None else None,
                 message, run_id))

    def _last_rowid(self, conn, tenant):
        row = conn.execute(
            "SELECT last_rowid FROM quality_progress WHERE tenant=?",
            (tenant,)).fetchone()
        return None if not row or row["last_rowid"] is None else int(
            row["last_rowid"])

    # -- main entry ------------------------------------------------------
    def process(self, *, tenants=None, reevaluate=False, resume=False,
                stage_only=False):
        conn = self._open()
        run_id = self._start_run(conn)
        try:
            return self._process(conn, run_id, tenants=tenants,
                                 reevaluate=reevaluate, resume=resume,
                                 stage_only=stage_only)
        except BaseException as exc:
            try:
                self._finish_run(conn, run_id, "failed",
                                 message=f"{type(exc).__name__}: {exc}")
            except Exception:
                pass
            raise
        finally:
            conn.close()

    def _process(self, conn, run_id, *, tenants, reevaluate, resume, stage_only):
        active = store.active_generation(conn)
        active_id = active["generation_id"] if active else None
        published_version = active["rule_version"] if active else None

        staging = store.staging_generation(conn)
        resumed = bool(staging and resume)

        if not resumed and published_version is not None:
            if self.rules.version != published_version and not reevaluate:
                raise ProcessingRefused(
                    f"rules version {self.rules.version!r} differs from the"
                    f" published rule version {published_version!r};"
                    " re-evaluate explicitly with --reevaluate")
            if (active["rule_sha256"] != self.rules.sha256
                    and not reevaluate):
                raise ProcessingRefused(
                    f"rules version {self.rules.version!r} was already used"
                    " with different content; publish a new version or pass"
                    " --reevaluate deliberately")

        landing_tenants = _landing_tenants(conn)
        base_generation_id = staging["base_generation_id"] if resumed else active_id
        known_tenants = set(landing_tenants) | set(
            _base_tenants(conn, base_generation_id))
        requested = tenants or sorted(known_tenants)
        selected = [t for t in requested if t in known_tenants]
        skipped = [t for t in requested if t not in known_tenants]

        if resumed:
            # a suspended batch keeps the rule snapshot it started with, even
            # if the file on disk changed while the process was down
            if staging["rule_sha256"] != self.rules.sha256:
                self.rules = _snapshot_from_generation(staging)

        base_states = _base_states(conn, active_id, selected) if not resumed \
            else _base_states(conn, staging["base_generation_id"], selected)
        dirty = []
        for tenant in selected:
            last_rowid = self._last_rowid(conn, tenant)
            max_rowid = _max_rowid(conn, tenant)
            pending = _pending_rows(conn, tenant, _tenant_rows(conn, tenant),
                                    last_rowid)
            if (resumed or reevaluate or last_rowid is None
                    or max_rowid > last_rowid or not base_states.get(tenant)
                    or pending):
                dirty.append(tenant)

        if not dirty and not resumed:
            self._finish_run(conn, run_id, "noop", dirty=[], new_inputs=0,
                             message="no new landing inputs")
            return {"status": "noop", "run_id": run_id,
                    "processed_tenants": [], "skipped_tenants": skipped,
                    "published": False, "generation_id": None,
                    "active_generation": active_id, "new_inputs": 0}

        if resumed:
            gen_id = staging["generation_id"]
            store.rebase_generation(conn, gen_id)
            base_states = _base_states(conn, staging["base_generation_id"],
                                       dirty)
        else:
            if staging is not None:
                store.discard_staging(conn, staging["generation_id"],
                                      "superseded by a new processing run")
            gen_id = store.create_generation(
                conn, self.rules, watermarks={},
                base_generation_id=active_id, message="staging begun")
            if active_id is not None:
                # start from the complete published projection; dirty tenants
                # are rebuilt below and clean/verify against this base
                with conn:
                    store.copy_generation(conn, active_id, gen_id)

        events: List[dict] = []
        expected = {"clean": 0, "quarantine": 0, "retracted": 0, "tenants": {}}
        watermarks: Dict[str, int] = {}
        new_inputs = 0
        processed = []

        for tenant in dirty:
            rows = _tenant_rows(conn, tenant)
            last_rowid = self._last_rowid(conn, tenant)
            pending = _pending_rows(conn, tenant, rows, last_rowid)
            states, clean, quarantine, retracted, _current = _build_tenant(
                rows, self.rules)
            watermarks[tenant] = _max_rowid(conn, tenant)

            with conn:
                for table in ("quality_clean", "quality_quarantine",
                              "quality_retracted", "quality_pending_inputs"):
                    conn.execute(
                        f"DELETE FROM {table} WHERE generation_id=?"
                        " AND tenant=?", (gen_id, tenant))
                for state in clean:
                    conn.execute(
                        "INSERT INTO quality_clean(tenant, accession, revision,"
                        " title, publication_year, doi, doi_key, raw_page_id,"
                        " generation_id, rule_version)"
                        " VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (tenant, state.accession, state.revision,
                         state.title, state.publication_year, state.doi,
                         state.doi, state.raw_page_id, gen_id,
                         self.rules.version))
                for state in quarantine:
                    conn.execute(
                        "INSERT INTO quality_quarantine(tenant, accession,"
                        " revision, rule_ids, reasons, original, raw_page_id,"
                        " generation_id, rule_version)"
                        " VALUES (?,?,?,?,?,?,?,?,?)",
                        (tenant, state.accession, state.revision,
                         store.dumps(state.rule_ids),
                         store.dumps(state.reasons),
                         store.dumps(state.original), state.raw_page_id, gen_id,
                         self.rules.version))
                for state in retracted:
                    conn.execute(
                        "INSERT INTO quality_retracted(tenant, accession,"
                        " revision, raw_page_id, generation_id)"
                        " VALUES (?,?,?,?,?)",
                        (tenant, state.accession, state.revision,
                         state.raw_page_id, gen_id))
                for row in pending:
                    conn.execute(
                        "INSERT OR IGNORE INTO quality_pending_inputs("
                        "generation_id, tenant, accession, revision,"
                        " source_rowid, payload, retracted, raw_page_id,"
                        " content_hash) VALUES (?,?,?,?,?,?,?,?,?)",
                        (gen_id, tenant, row["accession"],
                         int(row["revision"]), int(row["source_rowid"]),
                         row["payload"], int(row["retracted"]),
                         row["raw_page_id"],
                         store.content_hash(tenant, row["accession"],
                                            row["revision"], row["retracted"],
                                            row["raw_page_id"], row["payload"])))
                tenant_pending = conn.execute(
                    "SELECT COUNT(*) c FROM quality_pending_inputs"
                    " WHERE generation_id=? AND tenant=?",
                    (gen_id, tenant)).fetchone()["c"]
                new_inputs += tenant_pending

            events.extend(_events_for_tenant(tenant, base_states.get(tenant, {}),
                                             states, self.rules))
            bucket = {"clean": len(clean), "quarantine": len(quarantine),
                      "retracted": len(retracted)}
            bucket["total"] = sum(bucket.values())
            expected["tenants"][tenant] = bucket
            for key in ("clean", "quarantine", "retracted"):
                expected[key] += bucket[key]
            processed.append({"tenant": tenant, "inputs": len(rows),
                              "new_inputs": len(pending), "counts": bucket,
                              "revision_watermark": watermarks[tenant],
                              "pending_input_count": tenant_pending})
            if self.on_tenant_applied is not None:
                self.on_tenant_applied(tenant)

        # untouched tenants keep the projection they were copied with
        base_generation = staging["base_generation_id"] if resumed else active_id
        for tenant in _base_tenants(conn, base_generation):
            if tenant in expected["tenants"]:
                continue
            bucket = _base_bucket(conn, base_generation, tenant)
            expected["tenants"][tenant] = bucket
            for key in ("clean", "quarantine", "retracted"):
                expected[key] += bucket[key]

        verified = store.verify_generation(conn, gen_id, expected)
        if verified != expected:
            raise store.VerificationError("staged counts changed during verify")

        with conn:
            store.update_generation(conn, gen_id,
                                    input_watermarks=store.dumps(watermarks),
                                    counts=store.dumps(verified))
        store.stage_events(conn, gen_id, events)

        if stage_only:
            self._finish_run(conn, run_id, "staged", generation_id=gen_id,
                             new_inputs=new_inputs, dirty=dirty, counts=verified,
                             message="staged, not published")
            return {"status": "staged", "run_id": run_id,
                    "generation_id": gen_id, "resumed": resumed,
                    "processed_tenants": [p["tenant"] for p in processed],
                    "skipped_tenants": skipped, "published": False,
                    "counts": verified, "new_inputs": new_inputs,
                    "events": len(events), "active_generation": active_id,
                    "details": processed}

        result = store.publish_generation(conn, gen_id, run_id=run_id,
                                          expected_counts=verified)
        self._finish_run(conn, run_id, "ok", generation_id=gen_id,
                         new_inputs=new_inputs, dirty=dirty, counts=verified,
                         message=f"published generation {gen_id}")
        return {"status": "published", "run_id": run_id,
                "generation_id": gen_id, "resumed": resumed,
                "processed_tenants": [p["tenant"] for p in processed],
                "skipped_tenants": skipped,
                "published": result.get("published", False),
                "counts": verified, "new_inputs": new_inputs,
                "events": len(events), "active_generation": gen_id,
                "details": processed}


def _snapshot_from_generation(gen):
    config = json.loads(gen["rule_content"])
    rules_mod.validate_config(config)
    return rules_mod.RuleSnapshot(
        version=gen["rule_version"], sha256=gen["rule_sha256"],
        content=gen["rule_content"], config=config, path=None)


def _base_tenants(conn, generation_id):
    if generation_id is None:
        return []
    tenants = set()
    for table in ("quality_clean", "quality_quarantine", "quality_retracted"):
        for row in conn.execute(
                f"SELECT DISTINCT tenant FROM {table} WHERE generation_id=?",
                (generation_id,)):
            tenants.add(row["tenant"])
    return sorted(tenants)


def _base_bucket(conn, generation_id, tenant):
    bucket = {"clean": 0, "quarantine": 0, "retracted": 0}
    if generation_id is not None:
        for table, key in (("quality_clean", "clean"),
                           ("quality_quarantine", "quarantine"),
                           ("quality_retracted", "retracted")):
            bucket[key] = conn.execute(
                f"SELECT COUNT(*) c FROM {table}"
                " WHERE generation_id=? AND tenant=?",
                (generation_id, tenant)).fetchone()["c"]
    bucket["total"] = sum(bucket.values())
    return bucket
