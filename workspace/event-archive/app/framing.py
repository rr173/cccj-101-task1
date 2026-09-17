"""帧编码：WAL 与分段文件共用的长度前缀记录格式。

帧布局（小端序）：
    MAGIC(2) | FLAGS(1) | LEN(4, payload 字节数) | PAYLOAD(LEN) | CRC32(4)

    FLAGS=1 表示该帧为文件结束标记（payload 为空），用于区分
    "干净封存的分段" 与 "尾部撕裂的分段"。

校验和覆盖 FLAGS/LEN/PAYLOAD。读到任何异常都抛出 FrameError，
调用方据此把损坏点之后的内容隔离。
"""
from __future__ import annotations

import struct
import zlib
from typing import BinaryIO, Iterator

MAGIC = b"EA"
FLAG_END = 0x01
HEADER = struct.Struct("<2sBI")  # magic, flags, payload length
TAIL = struct.Struct("<I")       # crc32
FRAME_OVERHEAD = HEADER.size + TAIL.size


class FrameError(Exception):
    """帧损坏：魔数不符、长度非法或 CRC 不匹配。"""


def encode(payload: bytes, end: bool = False) -> bytes:
    flags = FLAG_END if end else 0
    header = HEADER.pack(MAGIC, flags, len(payload))
    crc = zlib.crc32(header)
    crc = zlib.crc32(payload, crc)
    return header + payload + TAIL.pack(crc & 0xFFFFFFFF)


def read_frame(buf: BinaryIO) -> tuple[bytes, bool] | None:
    """读取一帧。文件正常结束返回 None；遇到撕裂/损坏抛 FrameError。"""
    head = buf.read(HEADER.size)
    if not head:
        return None
    if len(head) < HEADER.size:
        raise FrameError("截断的帧头")
    magic, flags, length = HEADER.unpack(head)
    if magic != MAGIC:
        raise FrameError("魔数不匹配")
    if length > 64 * 1024 * 1024:
        raise FrameError(f"帧长度异常: {length}")
    payload = buf.read(length)
    if len(payload) < length:
        raise FrameError("截断的 payload")
    crc_bytes = buf.read(TAIL.size)
    if len(crc_bytes) < TAIL.size:
        raise FrameError("截断的校验和")
    expect = TAIL.unpack(crc_bytes)[0]
    actual = zlib.crc32(head)
    actual = zlib.crc32(payload, actual) & 0xFFFFFFFF
    if actual != expect:
        raise FrameError("CRC 校验失败")
    return payload, bool(flags & FLAG_END)


def iter_frames(path: str) -> Iterator[tuple[int, int, bytes, bool]]:
    """逐帧产出 (起始偏移, 结束偏移, payload, is_end)，损坏时抛 FrameError。"""
    with open(path, "rb") as f:
        while True:
            start = f.tell()
            frame = read_frame(f)
            if frame is None:
                return
            payload, is_end = frame
            yield start, f.tell(), payload, is_end
            if is_end:
                return
