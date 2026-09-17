"""HTTP API（仅标准库）。

路由：
  POST /v1/events                 单条或 {"events":[...]} 批量写入
  GET  /v1/events?device=&from=&to=&limit=
                                  按设备查询，结果按业务序列号排序
  POST /v1/snapshots              冻结视图 {"name":...}
  GET  /v1/snapshots              视图列表
  GET  /v1/snapshots/{name}       视图详情（分段清单 + 校验和）
  GET  /v1/replay/{name}?cursor=&limit=
                                  从冻结视图回放，cursor 翻页，
                                  只返回冻结 HWM 之内的事件
  GET  /v1/devices                设备水位
  GET  /v1/segments               分段状态
  POST /v1/admin/verify           校验分段并隔离损坏者
  POST /v1/admin/flush            等待已确认数据全部落分段
  GET  /healthz
"""
from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .service import ArchiveService

MAX_BODY = 16 * 1024 * 1024


class Handler(BaseHTTPRequestHandler):
    server_version = "EventArchive/1.0"
    svc: ArchiveService = None  # 由 main 注入

    def log_message(self, fmt, *args):  # 简洁日志
        print(f"[{self.log_date_time_string()}] {fmt % args}")

    # ---- 工具 -----------------------------------------------------
    def _json(self, obj, status: int = 200) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _err(self, status: int, code: str, message: str) -> None:
        self._json({"error": code, "message": message}, status)

    def _body(self) -> dict | None:
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > MAX_BODY:
                self._err(413, "body_too_large", "请求体过大")
                return None
            raw = self.rfile.read(length) if length else b"{}"
            return json.loads(raw.decode())
        except (ValueError, UnicodeDecodeError):
            self._err(400, "bad_json", "请求体不是合法 JSON")
            return None

    # ---- 路由 -----------------------------------------------------
    def do_GET(self) -> None:
        u = urlparse(self.path)
        q = {k: v[-1] for k, v in parse_qs(u.query).items()}
        p = u.path.rstrip("/") or "/"
        try:
            if p == "/healthz":
                self._json({"ok": True})
            elif p == "/v1/events":
                self._handle_query(q)
            elif p == "/v1/snapshots":
                self._json({"snapshots": self.svc.list_snapshots()})
            elif p.startswith("/v1/snapshots/"):
                snap = self.svc.get_snapshot(p.split("/")[-1])
                self._json(snap if snap else {},
                           200 if snap else 404)
            elif p.startswith("/v1/replay/"):
                self._handle_replay(p.split("/")[-1], q)
            elif p == "/v1/devices":
                self._json({"devices": self.svc.devices()})
            elif p == "/v1/segments":
                self._json({"segments": self.svc.segments()})
            elif p == "/v1/stats":
                self._json(self.svc.stats())
            else:
                self._err(404, "not_found", p)
        except Exception as e:  # noqa: BLE001
            self._err(500, "internal", f"{type(e).__name__}: {e}")

    def do_POST(self) -> None:
        u = urlparse(self.path)
        p = u.path.rstrip("/") or "/"
        body = self._body()
        if body is None:
            return
        try:
            if p == "/v1/events":
                self._handle_ingest(body)
            elif p == "/v1/snapshots":
                self._handle_freeze(body)
            elif p == "/v1/admin/verify":
                reports = self.svc.verify()
                self._json({"quarantined": reports})
            elif p == "/v1/admin/rebuild":
                self._json(self.svc.rebuild_quarantined())
            elif p == "/v1/admin/flush":
                ok = self.svc.sync(timeout=float(body.get("timeout", 30)))
                self._json({"flushed": ok})
            else:
                self._err(404, "not_found", p)
        except ValueError as e:
            self._err(400, "bad_request", str(e))
        except KeyError as e:
            self._err(404, "not_found", f"未知资源: {e}")
        except Exception as e:  # noqa: BLE001
            self._err(500, "internal", f"{type(e).__name__}: {e}")

    # ---- 业务处理 -------------------------------------------------
    def _handle_ingest(self, body: dict) -> None:
        if "events" in body:
            items = body["events"]
            if not isinstance(items, list):
                raise ValueError("events 必须是数组")
        else:
            items = [body]
        results, errors = [], []
        for i, ev in enumerate(items):
            try:
                results.append(self.svc.ingest(
                    ev["device_id"], ev["seq"],
                    ev.get("event_ts"), ev.get("data")))
            except (KeyError, ValueError) as e:
                errors.append({"index": i, "error": str(e)})
        self._json({"results": results, "errors": errors},
                   200 if results else 400)

    def _handle_query(self, q: dict) -> None:
        device = q.get("device")
        if not device:
            self._err(400, "missing_param", "需要 device 参数")
            return

        def as_int(name: str) -> int | None:
            if name not in q:
                return None
            try:
                return int(q[name])
            except ValueError:
                raise ValueError(f"{name} 必须是整数")

        resp = self.svc.query(device, as_int("from"), as_int("to"),
                              min(int(q.get("limit", 1000)), 10_000))
        self._json(resp)

    def _handle_freeze(self, body: dict) -> None:
        name = body.get("name")
        if not name:
            raise ValueError("需要 name")
        snap = self.svc.freeze(name, body.get("note", ""))
        self._json(snap, 201)

    def _handle_replay(self, name: str, q: dict) -> None:
        cursor = int(q["cursor"]) if q.get("cursor") else None
        limit = min(int(q.get("limit", 1000)), 10_000)
        self._json(self.svc.replay(name, cursor, limit))


def serve(svc: ArchiveService, host: str, port: int) -> ThreadingHTTPServer:
    Handler.svc = svc
    httpd = ThreadingHTTPServer((host, port), Handler)
    return httpd
