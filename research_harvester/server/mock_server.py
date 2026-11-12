#!/usr/bin/env python3
"""Controllable mock of the research-results incremental scan API (stdlib only).

Data API
--------
GET /v1/results
    Header: X-Tenant-Id
    Open a new scan (no cursor):
        since=<float>            closed lower bound on modified_at
        after_accession=<str>    tie-breaker: rows with modified_at == since are
                                 only returned when accession > after_accession
        until=<float>            optional fixed closed upper bound; defaults to
                                 the server's current max modified_at
        limit=<int>              page size (1..1000)
    Next page:
        cursor=<token>&limit=<int>
    Responses:
        200 {"data": [...], "page": {"cursor": ..., "next": bool,
                                     "until": float, "scan_id": str}}
        410 cursor unknown/expired -> client must re-open from its persisted
             composite boundary (since, after_accession) with the same until
        429 rate limited (Retry-After header, per-tenant limiter)
        503 transient (Retry-After header)
        401 tenant credentials invalid -> client must stop this tenant

    A scan is a *frozen view*: the matching version list is snapshotted when
    the scan is opened. Writes that happen during the scan never leak into it.
    Retracted results are returned as tombstones {"retracted": true}.
    Every record version carries a monotonically increasing revision.

Control plane (POST JSON unless noted)
--------------------------------------
    POST /control/record          {"accession","revision"?,"modified_at"?,"payload"?}
    POST /control/retract         {"accession","modified_at"?}
    POST /control/time            {"set":float} | {"advance":float}
    POST /control/fault           {"tenant","kind":429|503|401|"none",
                                   "times":int|null,"retry_after":float}
    POST /control/trigger         {"tenant","after_requests":int,"action":{...}|[...]}
                                  action fires when the tenant's Nth API request
                                  arrives (before it is handled). Action types:
                                  record / retract / time / expire_cursors /
                                  fault / latency
    POST /control/expire_cursors  {}
    POST /control/latency         {"seconds":float}
    POST /control/reset           {}
    GET  /control/requests?tenant=&since_seq=
    GET  /control/stats
"""
from __future__ import annotations

import argparse
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs


class State:
    """Whole server state; guarded by one re-entrant lock (test-scale data)."""

    def __init__(self):
        self.lock = threading.RLock()
        self.records = {}          # accession -> [version dict, ...] ascending revision
        self.virtual_now = 1_700_000_000.0
        self.cursor_ttl = 300.0    # virtual seconds
        self.scans = {}            # cursor token -> frozen scan view
        self.scan_seq = 0
        self.faults = {}           # tenant -> {"kind","times"(None=forever),"retry_after"}
        self.triggers = {}         # tenant -> [{"after_requests","action","done"}]
        self.req_counts = {}       # tenant -> number of API requests seen
        self.log = []              # {"seq","ts","tenant","path","status"}
        self.latency = 0.0         # artificial per-request latency (real seconds)
        self.inflight = 0
        self.max_inflight = 0

    def add_version(self, accession, revision=None, modified_at=None,
                    payload=None, retracted=False):
        with self.lock:
            versions = self.records.setdefault(accession, [])
            if revision is None:
                revision = (versions[-1]["revision"] + 1) if versions else 1
            if modified_at is None:
                modified_at = self.virtual_now
            v = {"accession": accession, "revision": int(revision),
                 "modified_at": float(modified_at),
                 "retracted": bool(retracted), "payload": payload}
            versions.append(v)
            versions.sort(key=lambda x: x["revision"])
            return v

    def current_versions(self):
        return [vs[-1] for vs in self.records.values() if vs]


def apply_action(state, action, default_tenant=None):
    """Apply one scripted control action (used by triggers)."""
    t = action["type"]
    if t == "record":
        state.add_version(action["accession"], action.get("revision"),
                          action.get("modified_at"), action.get("payload"))
    elif t == "retract":
        with state.lock:
            known = action["accession"] in state.records
        if known:
            state.add_version(action["accession"], None,
                              action.get("modified_at"), None, True)
    elif t == "time":
        with state.lock:
            if "set" in action:
                state.virtual_now = float(action["set"])
            if "advance" in action:
                state.virtual_now += float(action["advance"])
    elif t == "expire_cursors":
        with state.lock:
            state.scans.clear()
    elif t == "fault":
        tenant = action.get("tenant", default_tenant)
        with state.lock:
            state.faults[tenant] = {"kind": int(action["kind"]),
                                    "times": action.get("times"),
                                    "retry_after": float(action.get("retry_after", 1.0))}
    elif t == "latency":
        with state.lock:
            state.latency = float(action["seconds"])
    else:
        raise ValueError(f"unknown action type: {t!r}")


def handle_scan(state, tenant, qs):
    """Open a scan or serve the next page. Returns (status, body, headers)."""
    limit_raw = qs.get("limit", ["100"])[0]
    try:
        limit = max(1, min(1000, int(limit_raw)))
    except ValueError:
        return 400, {"error": "bad limit"}, {}
    cursor = qs.get("cursor", [None])[0]
    with state.lock:
        if cursor:
            scan = state.scans.get(cursor)
            if scan is None or scan["tenant"] != tenant:
                return 410, {"error": "cursor_expired"}, {}
            if state.virtual_now > scan["expires_at"]:
                del state.scans[cursor]
                return 410, {"error": "cursor_expired"}, {}
            token = cursor
        else:
            try:
                since = float(qs.get("since", ["0"])[0])
            except ValueError:
                return 400, {"error": "bad since"}, {}
            after = qs.get("after_accession", [None])[0] or None
            until_q = qs.get("until", [None])[0]
            cur = state.current_versions()
            if until_q is not None:
                try:
                    until = float(until_q)
                except ValueError:
                    return 400, {"error": "bad until"}, {}
            else:
                until = max((v["modified_at"] for v in cur), default=state.virtual_now)
            # frozen view: snapshot the matching versions now
            items = [v for v in cur
                     if v["modified_at"] <= until and
                     (v["modified_at"] > since or
                      (v["modified_at"] == since and
                       (after is None or v["accession"] > after)))]
            items.sort(key=lambda v: (v["modified_at"], v["accession"]))
            state.scan_seq += 1
            token = f"scan-{state.scan_seq}"
            state.scans[token] = {"items": items, "pos": 0, "tenant": tenant,
                                  "until": until,
                                  "expires_at": state.virtual_now + state.cursor_ttl}
        scan = state.scans[token]
        start = scan["pos"]
        end = min(start + limit, len(scan["items"]))
        page_items = scan["items"][start:end]
        scan["pos"] = end
        more = end < len(scan["items"])
        body = {"data": page_items,
                "page": {"cursor": token if more else None, "next": more,
                         "until": scan["until"], "scan_id": token,
                         "returned": len(page_items)}}
        return 200, body, {}


class Handler(BaseHTTPRequestHandler):
    server_version = "MockResearch/1.0"
    protocol_version = "HTTP/1.0"

    def log_message(self, *args):  # keep test output clean
        pass

    # -- helpers ---------------------------------------------------------
    def _respond(self, code, obj, headers=None, tenant=None):
        state = self.server.state
        if tenant is not None:
            with state.lock:
                state.log.append({"seq": len(state.log), "ts": time.time(),
                                  "tenant": tenant, "path": self.path,
                                  "status": code})
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # client went away mid-response (e.g. killed collector)

    def _read_json(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            n = 0
        raw = self.rfile.read(n) if n else b"{}"
        try:
            return json.loads(raw.decode() or "{}")
        except json.JSONDecodeError:
            return {}

    # -- routing ---------------------------------------------------------
    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        if parsed.path == "/v1/results":
            self._api(qs)
        elif parsed.path == "/control/requests":
            self._control_requests(qs)
        elif parsed.path == "/control/stats":
            self._control_stats()
        else:
            self._respond(404, {"error": "unknown path"})

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/control/"):
            self._control(parsed.path)
        else:
            self._respond(404, {"error": "unknown path"})

    # -- data API --------------------------------------------------------
    def _api(self, qs):
        state = self.server.state
        tenant = self.headers.get("X-Tenant-Id") or (qs.get("tenant", [""])[0])
        with state.lock:
            state.inflight += 1
            state.max_inflight = max(state.max_inflight, state.inflight)
            n = state.req_counts.get(tenant, 0) + 1
            state.req_counts[tenant] = n
            due = [tr for tr in state.triggers.get(tenant, [])
                   if not tr["done"] and n >= tr["after_requests"]]
            for tr in due:
                tr["done"] = True
        try:
            if state.latency > 0:
                time.sleep(state.latency)
            for tr in due:
                actions = tr["action"] if isinstance(tr["action"], list) else [tr["action"]]
                for a in actions:
                    apply_action(state, a, default_tenant=tenant)
            with state.lock:
                fault = None
                f = state.faults.get(tenant)
                if f and (f["times"] is None or f["times"] > 0):
                    if f["times"] is not None:
                        f["times"] -= 1
                    fault = dict(f)
            if fault:
                ra = str(fault.get("retry_after", 1.0))
                if fault["kind"] == 429:
                    self._respond(429, {"error": "rate_limited", "tenant": tenant},
                                  {"Retry-After": ra}, tenant)
                elif fault["kind"] == 503:
                    self._respond(503, {"error": "service_unavailable"},
                                  {"Retry-After": ra}, tenant)
                elif fault["kind"] == 401:
                    self._respond(401, {"error": "unauthorized"}, {}, tenant)
                else:
                    self._respond(500, {"error": "bad fault kind"}, {}, tenant)
                return
            code, body, headers = handle_scan(state, tenant, qs)
            self._respond(code, body, headers, tenant)
        finally:
            with state.lock:
                state.inflight -= 1

    # -- control plane ---------------------------------------------------
    def _control(self, path):
        state = self.server.state
        body = self._read_json()
        if path == "/control/record":
            v = state.add_version(body["accession"], body.get("revision"),
                                  body.get("modified_at"), body.get("payload"),
                                  bool(body.get("retracted", False)))
            self._respond(200, {"ok": True, "version": v})
        elif path == "/control/retract":
            with state.lock:
                known = body["accession"] in state.records
            if not known:
                self._respond(404, {"error": "unknown accession"})
            else:
                v = state.add_version(body["accession"], None,
                                      body.get("modified_at"), None, True)
                self._respond(200, {"ok": True, "version": v})
        elif path == "/control/time":
            with state.lock:
                if "set" in body:
                    state.virtual_now = float(body["set"])
                if "advance" in body:
                    state.virtual_now += float(body["advance"])
                now = state.virtual_now
            self._respond(200, {"virtual_now": now})
        elif path == "/control/fault":
            kind = body["kind"]
            with state.lock:
                if kind in ("none", None):
                    state.faults.pop(body["tenant"], None)
                else:
                    state.faults[body["tenant"]] = {
                        "kind": int(kind), "times": body.get("times"),
                        "retry_after": float(body.get("retry_after", 1.0))}
            self._respond(200, {"ok": True})
        elif path == "/control/trigger":
            with state.lock:
                state.triggers.setdefault(body["tenant"], []).append(
                    {"after_requests": int(body["after_requests"]),
                     "action": body["action"], "done": False})
            self._respond(200, {"ok": True})
        elif path == "/control/expire_cursors":
            with state.lock:
                n = len(state.scans)
                state.scans.clear()
            self._respond(200, {"expired": n})
        elif path == "/control/latency":
            with state.lock:
                state.latency = float(body["seconds"])
            self._respond(200, {"ok": True})
        elif path == "/control/reset":
            self.server.state = State()
            self._respond(200, {"ok": True})
        else:
            self._respond(404, {"error": "unknown control path"})

    def _control_requests(self, qs):
        state = self.server.state
        tenant = qs.get("tenant", [None])[0]
        try:
            since_seq = int(qs.get("since_seq", ["-1"])[0])
        except ValueError:
            since_seq = -1
        with state.lock:
            reqs = [r for r in state.log
                    if (tenant is None or r["tenant"] == tenant) and r["seq"] > since_seq]
        self._respond(200, {"requests": reqs})

    def _control_stats(self):
        state = self.server.state
        with state.lock:
            self._respond(200, {
                "virtual_now": state.virtual_now,
                "max_inflight": state.max_inflight,
                "req_counts": dict(state.req_counts),
                "open_scans": len(state.scans),
                "accessions": len(state.records),
                "versions": sum(len(v) for v in state.records.values()),
            })


def make_server(host="127.0.0.1", port=8734):
    class QuietHTTPServer(ThreadingHTTPServer):
        def handle_error(self, request, client_address):
            pass  # clients killed mid-request are expected in tests

    httpd = QuietHTTPServer((host, port), Handler)
    httpd.state = State()
    return httpd, httpd.state


def main():
    ap = argparse.ArgumentParser(description="Mock research-results scan API")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8734)
    args = ap.parse_args()
    httpd, _ = make_server(args.host, args.port)
    print(f"mock server on http://{args.host}:{args.port}")
    print("data API:    GET  /v1/results")
    print("control:     POST /control/{record,retract,time,fault,trigger,"
          "expire_cursors,latency,reset}")
    print("introspect:  GET  /control/requests, /control/stats")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
