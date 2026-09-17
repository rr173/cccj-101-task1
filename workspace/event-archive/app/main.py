"""进程入口。

环境变量：
  EA_DATA_DIR       数据目录（默认 /data）
  EA_HOST           监听地址（默认 0.0.0.0）
  EA_PORT           端口（默认 8080）
  EA_WINDOW_MS      分段事件时间窗口（默认 3600000 = 1 小时）
  EA_ROTATE_COUNT   单分段最大事件数（默认 100000）
  EA_MAX_OPEN       同时打开的活动分段句柄数（默认 16）
  EA_LATE_GRACE_MS  迟到判定宽限（默认 300000 = 5 分钟）
  EA_FLUSH_INTERVAL 刷盘空转间隔秒（默认 0.5）
"""
from __future__ import annotations

import os
import signal
import sys
import time

from .http_server import serve
from .service import ArchiveService


def _env(name: str, default):
    val = os.environ.get(name)
    return val if val is not None else default


def main() -> int:
    svc = ArchiveService(
        _env("EA_DATA_DIR", "/data"),
        window_ms=int(_env("EA_WINDOW_MS", 3_600_000)),
        rotate_count=int(_env("EA_ROTATE_COUNT", 100_000)),
        max_open=int(_env("EA_MAX_OPEN", 16)),
        late_grace_ms=int(_env("EA_LATE_GRACE_MS", 300_000)),
        flush_interval=float(_env("EA_FLUSH_INTERVAL", 0.5)))

    print("[boot] 开始崩溃恢复 ...")
    reports = svc.boot()
    if reports:
        for r in reports:
            print(f"[boot] 隔离分段: {r}")
    else:
        print("[boot] 未发现损坏分段")
    svc.start_flusher()

    host = _env("EA_HOST", "0.0.0.0")
    port = int(_env("EA_PORT", "8080"))
    httpd = serve(svc, host, port)
    print(f"[boot] 归档服务监听 {host}:{port}，durable_hwm={svc.durable_hwm}")

    def _graceful(_signum, _frame):
        # 不能在主线程里直接调 httpd.shutdown()（会死锁等待自身），
        # 置位让 serve_forever 的 select 循环自行退出
        httpd._BaseServer__shutdown_request = True

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)
    try:
        httpd.serve_forever()
    finally:
        print("[shutdown] 封存分段并关闭 ...")
        svc.sync(timeout=5)
        httpd.server_close()
        svc.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
