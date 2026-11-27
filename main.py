import asyncio
import os
import socket
import time
import uuid as _uuid

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI(title="Cloud Stream Telemetry", docs_url=None, redoc_url=None)

START_TS = time.time()
BUF_SIZE = 65536
HIGH_WATER = 262144


def _decode_frame_header(chunk: bytes, expected_id: str | None = None):
    if len(chunk) < 24:
        raise ValueError("invalid frame")

    header_id = chunk[1:17]
    if expected_id:
        expected_bytes = _uuid.UUID(str(expected_id)).bytes
        if header_id != expected_bytes:
            raise ValueError("session mismatch")

    pos = 17
    meta_len = chunk[pos]
    pos += 1 + meta_len
    op = chunk[pos]
    pos += 1
    port = int.from_bytes(chunk[pos : pos + 2], "big")
    pos += 2
    addr_kind = chunk[pos]
    pos += 1

    if addr_kind == 1:
        host = ".".join(str(b) for b in chunk[pos : pos + 4])
        pos += 4
    elif addr_kind == 2:
        hlen = chunk[pos]
        pos += 1
        host = chunk[pos : pos + hlen].decode("utf-8", errors="ignore")
        pos += hlen
    elif addr_kind == 3:
        raw = chunk[pos : pos + 16]
        pos += 16
        host = ":".join(f"{raw[i]:02x}{raw[i + 1]:02x}" for i in range(0, 16, 2))
    else:
        raise ValueError("unsupported endpoint")

    return op, host, port, chunk[pos:]


def _tune_socket(writer: asyncio.StreamWriter):
    try:
        sock = writer.get_extra_info("socket")
        if sock is not None:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception:
        pass


async def _pipe_upstream(ws: WebSocket, writer: asyncio.StreamWriter):
    try:
        while True:
            msg = await ws.receive()
            if msg["type"] == "websocket.disconnect":
                break
            data = msg.get("bytes") or (msg.get("text") or "").encode()
            if not data:
                continue
            writer.write(data)
            if writer.transport.get_write_buffer_size() > HIGH_WATER:
                await writer.drain()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        try:
            writer.write_eof()
        except Exception:
            pass


async def _pipe_downstream(ws: WebSocket, reader: asyncio.StreamReader):
    first = True
    try:
        while True:
            data = await reader.read(BUF_SIZE)
            if not data:
                break
            out = (b"\x00\x00" + data) if first else data
            first = False
            await ws.send_bytes(out)
    except Exception:
        pass


@app.get("/", response_class=HTMLResponse)
async def home():
    uptime = int(time.time() - START_TS)
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Cloud Stream Telemetry</title>"
        "<style>body{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;"
        "display:flex;align-items:center;justify-content:center;height:100vh;margin:0}"
        ".card{background:#1e293b;padding:32px 40px;border-radius:14px;border:1px solid #334155}"
        "h1{margin:0 0 8px;font-size:20px}p{margin:0;color:#94a3b8;font-size:14px}</style>"
        "</head><body><div class='card'><h1>Cloud Stream Telemetry Service</h1>"
        f"<p>Status: Operational &middot; Uptime: {uptime}s</p></div></body></html>"
    )


@app.get("/healthz")
async def healthz():
    return JSONResponse({"status": "ok", "uptime": int(time.time() - START_TS)})


@app.websocket("/ws/{stream_id}")
async def stream_endpoint(ws: WebSocket, stream_id: str):
    await ws.accept()
    writer = None
    try:
        first_msg = await asyncio.wait_for(ws.receive(), timeout=15.0)
        if first_msg["type"] == "websocket.disconnect":
            return
        first_chunk = first_msg.get("bytes") or (first_msg.get("text") or "").encode()
        if not first_chunk:
            return

        _, host, port, payload = _decode_frame_header(first_chunk, stream_id)
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=10.0
        )
        _tune_socket(writer)

        if payload:
            writer.write(payload)
            await writer.drain()

        _, pending = await asyncio.wait(
            {
                asyncio.create_task(_pipe_upstream(ws, writer)),
                asyncio.create_task(_pipe_downstream(ws, reader)),
            },
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    except Exception:
        pass
    finally:
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
