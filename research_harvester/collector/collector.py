"""Multi-tenant incremental harvester (stdlib only).

Concurrency model
-----------------
One worker thread per tenant; a global BoundedSemaphore caps the number of
in-flight HTTP requests. Backoff sleeps happen inside the tenant's own thread,
so a throttled tenant never blocks the others. cancel() sets an event that is
checked before every request; persisted SQLite state makes restarts resumable.

Backoff testability: all waiting goes through Sleeper, which scales delays
(retry_scale). Tests use tiny scales instead of real long waits while the
requested Retry-After values are still logged and asserted.
"""
from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Optional

from . import db


class TenantStopped(Exception):
    """401 received: this tenant must not send further requests."""


class TenantFailed(Exception):
    """Unrecoverable error for this tenant."""


class Cancelled(Exception):
    """Cancellation requested via Collector.cancel()."""


class Sleeper:
    """Scalable, observable sleep used for all backoff/waiting."""

    def __init__(self, scale=1.0, sleep_fn=time.sleep):
        self.scale = float(scale)
        self._sleep_fn = sleep_fn
        self.log = []
        self._lock = threading.Lock()

    def sleep(self, seconds, tenant, reason):
        seconds = max(0.0, float(seconds))
        real = seconds * self.scale
        with self._lock:
            self.log.append({"tenant": tenant, "seconds": seconds,
                             "slept": real, "reason": reason,
                             "ts": time.time()})
        if real > 0:
            self._sleep_fn(real)


class HttpClient:
    def __init__(self, base_url, timeout=10.0):
        self.base = base_url.rstrip("/")
        self.timeout = timeout

    def get(self, path, params, tenant):
        q = urllib.parse.urlencode({k: v for k, v in params.items()
                                    if v is not None})
        url = f"{self.base}{path}?{q}" if q else f"{self.base}{path}"
        req = urllib.request.Request(url, headers={"X-Tenant-Id": tenant})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return (resp.status, dict(resp.headers.items()),
                        resp.read().decode("utf-8", "replace"), url)
        except urllib.error.HTTPError as e:
            try:
                body = e.read().decode("utf-8", "replace")
            except Exception:
                body = ""
            return (e.code, dict(e.headers.items()) if e.headers else {},
                    body, url)
        except Exception as e:  # network error / timeout -> transient (-1)
            return -1, {}, f"{type(e).__name__}: {e}", url


@dataclass
class TenantResult:
    tenant: str
    status: str = "ok"          # ok | stopped_401 | failed | cancelled
    rounds_done: int = 0
    error: Optional[str] = None


def _retry_after(headers, default):
    for k, v in (headers or {}).items():
        if k.lower() == "retry-after":
            try:
                return max(0.0, float(v))
            except (TypeError, ValueError):
                return default
    return default


class Collector:
    def __init__(self, base_url, db_path, tenants, *, page_size=100,
                 in_flight=4, retry_scale=1.0, max_transient=8, start=0.0,
                 request_timeout=10.0, sleeper=None):
        self.db_path = db_path
        self.tenants = list(tenants)
        self.page_size = int(page_size)
        self.max_transient = int(max_transient)
        self.start = float(start)
        self.http = HttpClient(base_url, timeout=request_timeout)
        self.sleeper = sleeper or Sleeper(retry_scale)
        self.sem = threading.BoundedSemaphore(max(1, int(in_flight)))
        self.stop_event = threading.Event()
        self.results = {}
        self.reopens = {t: 0 for t in self.tenants}
        self._lock = threading.Lock()
        self._threads = []
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        conn = db.connect(self.db_path)
        try:
            db.init(conn)
        finally:
            conn.close()

    # -- public control --------------------------------------------------
    def cancel(self):
        self.stop_event.set()

    def run(self, rounds=1, poll_interval=0.0):
        self._threads = []
        for t in self.tenants:
            th = threading.Thread(target=self._tenant_entry,
                                  args=(t, rounds, poll_interval),
                                  name=f"tenant-{t}", daemon=True)
            th.start()
            self._threads.append(th)
        for th in self._threads:
            th.join()
        return self.results

    def wait(self, timeout=None):
        for th in self._threads:
            th.join(timeout)

    # -- worker ----------------------------------------------------------
    def _tenant_entry(self, tenant, rounds, poll_interval):
        conn = db.connect(self.db_path)
        res = TenantResult(tenant=tenant)
        self.results[tenant] = res
        try:
            for i in range(rounds):
                if self.stop_event.is_set():
                    raise Cancelled()
                self._run_round(conn, tenant)
                res.rounds_done += 1
                if poll_interval and i < rounds - 1:
                    self.sleeper.sleep(poll_interval, tenant, "poll")
            res.status = "ok"
        except TenantStopped:
            res.status = "stopped_401"
        except Cancelled:
            res.status = "cancelled"
        except TenantFailed as e:
            res.status = "failed"
            res.error = str(e)
        finally:
            conn.close()

    def _round_base(self, conn, tenant):
        wm = db.load_watermark(conn, tenant)
        if wm and wm["high_modified_at"] is not None:
            return wm["high_modified_at"]
        return self.start

    def _run_round(self, conn, tenant):
        st = db.load_scan_state(conn, tenant)
        if st and st["status"] == "in_progress":
            # resume an interrupted round: same fixed upper bound, composite
            # boundary from the last confirmed page
            since = st["confirmed_modified_at"]
            if since is None:
                since = st["round_lower"]
            if since is None:
                since = self._round_base(conn, tenant)
            after = st["confirmed_accession"]
            until = st["scan_until"]
            round_lower = st["round_lower"] if st["round_lower"] is not None else since
        else:
            since = self._round_base(conn, tenant)
            after = None
            until = None  # server fixes the upper bound on the first response
            round_lower = since
            db.begin_round(conn, tenant, round_lower)

        cursor = None
        while True:
            if self.stop_event.is_set():
                raise Cancelled()
            params = {"limit": self.page_size}
            if cursor:
                params["cursor"] = cursor
            else:
                params["since"] = since
                if after:
                    params["after_accession"] = after
                if until is not None:
                    params["until"] = until
            status, headers, body, url = self._request_with_retries(
                conn, tenant, params)
            if status == 410:
                # cursor lost/expired: re-open from the persisted composite
                # boundary with the round's original fixed upper bound
                st = db.load_scan_state(conn, tenant)
                if st and st["status"] == "in_progress":
                    since = st["confirmed_modified_at"]
                    if since is None:
                        since = st["round_lower"]
                    if since is None:
                        since = self._round_base(conn, tenant)
                    after = st["confirmed_accession"]
                    until = st["scan_until"]
                else:
                    since = self._round_base(conn, tenant)
                    after = None
                    until = None
                    db.begin_round(conn, tenant, since)
                cursor = None
                with self._lock:
                    self.reopens[tenant] += 1
                continue
            payload = json.loads(body)
            items = payload.get("data", [])
            page = payload.get("page", {})
            if until is None:
                until = page.get("until")
            round_done = not page.get("next", False)
            db.commit_page(conn, tenant=tenant, scan_id=page.get("scan_id"),
                           url=url, status=status, body=body, items=items,
                           scan_until=until, round_done=round_done,
                           round_lower=round_lower)
            if round_done:
                return
            cursor = page.get("cursor")
            if not cursor:
                raise TenantFailed(
                    "server indicated more pages but returned no cursor")

    def _request_with_retries(self, conn, tenant, params):
        transient = 0
        while True:
            if self.stop_event.is_set():
                raise Cancelled()
            with self.sem:
                status, headers, body, url = self.http.get(
                    "/v1/results", params, tenant)
            if status == 200:
                return status, headers, body, url
            db.record_error(conn, tenant, url, status, body)
            if status == 410:
                return status, headers, body, url
            if status == 401:
                raise TenantStopped("401 unauthorized")
            if status == 429:
                self.sleeper.sleep(_retry_after(headers, 1.0), tenant, "429")
                continue
            if status in (503, -1):
                transient += 1
                if transient > self.max_transient:
                    raise TenantFailed(
                        f"too many transient failures ({transient})")
                reason = "503" if status == 503 else "net"
                backoff = min(0.1 * 2 ** transient, 5.0)
                self.sleeper.sleep(_retry_after(headers, backoff),
                                   tenant, reason)
                continue
            raise TenantFailed(f"unexpected HTTP status {status}")
