#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# MCP definitions adapted from xtell-cn/xtell-serial; see NOTICE.
"""Headless BootTerm MCP server with a shared human terminal console."""

from __future__ import annotations

import argparse
import asyncio
import base64
import codecs
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import signal
import sys

import anyio
import regex
import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions

MAX_LINES = 2000
MAX_LINE_CHARS = 4096
MAX_SEND_BYTES = 65536
OP_TIMEOUT = 5.0

mcp_server = Server("bootterm-mcp")


@mcp_server.list_tools()
async def list_tools():
    """Static discovery never opens a port or launches BootTerm."""
    tools = [
        types.Tool(
            name="list_serial_ports",
            description="List the serial ports on this computer.",
            inputSchema={
                "type": "object",
                "properties": {},
            }
        ),
        types.Tool(
            name="connect_serial",
            description="Connect a shared headless BootTerm serial session. A human terminal can attach to the same session.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string", "description": "Port path or device name, for example /dev/ttyUSB0 or /dev/cu.usbserial"},
                    "baudrate": {"type": "integer", "description": "Baud rate, for example 115200"},
                },
                "required": ["port", "baudrate"]
            }
        ),
        types.Tool(
            name="send_serial_data",
            description="Send one message to a serial port. The port must already be connected with connect_serial.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string", "description": "Target port"},
                    "data": {"type": "string", "description": "Text or HEX string to send"},
                    "hex_mode": {"type": "boolean", "description": "Send as hexadecimal bytes", "default": False},
                },
                "required": ["port", "data"]
            }
        ),
        types.Tool(
            name="disconnect_serial",
            description="Disconnect a serial port.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string", "description": "Target port"}
                },
                "required": ["port"]
            }
        ),
        types.Tool(
            name="start_auto_send",
            description="Start sending data repeatedly on a timer.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string"},
                    "data": {"type": "string", "description": "Data to send"},
                    "interval_ms": {"type": "integer", "description": "Interval in milliseconds", "default": 1000},
                    "hex_mode": {"type": "boolean", "default": False}
                },
                "required": ["port", "data", "interval_ms"]
            }
        ),
        types.Tool(
            name="stop_auto_send",
            description="Stop the repeating send.",
            inputSchema={
                "type": "object",
                "properties": {"port": {"type": "string"}},
                "required": ["port"]
            }
        ),
        types.Tool(
            name="set_filter",
            description="Set the live filter of a session.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string"},
                    "keyword": {"type": "string"},
                    "is_regex": {"type": "boolean", "default": False},
                    "enable": {"type": "boolean", "default": True}
                },
                "required": ["port", "keyword"]
            }
        ),
        types.Tool(
            name="start_saving_file",
            description="Append a serial log to an absolute local file path. Raw captures received bytes; annotated and jsonl log AI/human TX and DUT RX with timestamps.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string"},
                    "filepath": {"type": "string", "description": "Absolute file path, for example /tmp/console.log"},
                    "format": {"type": "string", "enum": ["raw", "annotated", "jsonl"], "default": "raw", "description": "raw: received bytes only; annotated: readable traffic with source/direction; jsonl: structured events with exact base64 bytes"}
                },
                "required": ["port", "filepath"]
            }
        ),
        types.Tool(
            name="stop_saving_file",
            description="Stop saving and close the file.",
            inputSchema={
                "type": "object",
                "properties": {"port": {"type": "string"}},
                "required": ["port"]
            }
        ),
        types.Tool(
            name="clear_data_buffers",
            description="Clear the text buffers of a session.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string"},
                    "target": {"type": "string", "description": "main, filter, history, or all", "default": "all"}
                },
                "required": ["port"]
            }
        ),
        types.Tool(
            name="read_received_data",
            description="Read the last N lines of the receive history. Optional start and end timestamps filter the range (format: yyyy-mm-dd HH:MM:SS.mmm).",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string"},
                    "lines_count": {"type": "integer", "default": 50},
                    "start_time": {"type": "string", "description": "Optional start timestamp"},
                    "end_time": {"type": "string", "description": "Optional end timestamp"}
                },
                "required": ["port"]
            }
        ),
        types.Tool(
            name="read_sent_data",
            description="Read the last N transmitted lines, labelled HUMAN or AI. Human keystrokes are combined into command lines; include unfinished commands.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string"},
                    "lines_count": {"type": "integer", "default": 50},
                    "start_time": {"type": "string", "description": "Optional start timestamp"},
                    "end_time": {"type": "string", "description": "Optional end timestamp"}
                },
                "required": ["port"]
            }
        ),
        types.Tool(
            name="read_filtered_data",
            description="Read the last N lines that match the current filter.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string"},
                    "lines_count": {"type": "integer", "default": 50},
                    "start_time": {"type": "string"},
                    "end_time": {"type": "string"}
                },
                "required": ["port"]
            }
        ),
        types.Tool(
            name="get_buffer_status",
            description="Report how many lines the session buffer holds, its limit and the percentage used.",
            inputSchema={
                "type": "object",
                "properties": {
                    "port": {"type": "string", "description": "Target port"}
                },
                "required": ["port"]
            }
        )
    ]

    for tool in tools:
        schema = tool.inputSchema
        schema["additionalProperties"] = False
        props = schema["properties"]
        for key in ("port", "filepath"):
            if key in props:
                props[key].update(minLength=1, maxLength=4096)
        if "baudrate" in props:
            props["baudrate"].update(minimum=0, maximum=2147483647)
        if "lines_count" in props:
            props["lines_count"].update(minimum=1, maximum=MAX_LINES)
        if "interval_ms" in props:
            props["interval_ms"].update(minimum=10, maximum=86400000)
        if "data" in props:
            props["data"]["maxLength"] = MAX_SEND_BYTES * 3
        if "keyword" in props:
            props["keyword"]["maxLength"] = 512
        if "target" in props:
            props["target"]["enum"] = ["main", "filter", "history", "all"]
    return tools



@dataclass
class Line:
    number: int
    time: datetime
    text: str
    source: str | None = None


class History:
    """Bounded lines, including the current unterminated prompt."""

    def __init__(self):
        self.lines = deque(maxlen=MAX_LINES)
        self.sequence = 0
        self.partial = False

    def feed(self, text, source=None):
        if self.partial and self.lines[-1].source != source:
            self.partial = False
        while text:
            if not self.partial:
                self.sequence += 1
                self.lines.append(Line(self.sequence, datetime.now(), "", source))
                self.partial = True
            line = self.lines[-1]
            newline = text.find("\n")
            length = len(text) if newline < 0 else newline
            take = min(length, MAX_LINE_CHARS - len(line.text))
            line.text += text[:take]
            text = text[take:]
            if text.startswith("\n"):
                text = text[1:]
                self.partial = False
            elif len(line.text) == MAX_LINE_CHARS:
                self.partial = False

    def clear(self):
        self.lines.clear()
        self.partial = False

    def render(self, count=50, start=None, end=None, predicate=None, after=0):
        selected = [line for line in self.lines
                    if line.number > after
                    and (start is None or line.time >= start)
                    and (end is None or line.time <= end)
                    and (predicate is None or predicate(line.text))]
        return "\n".join(
            f"[{line.time.isoformat(sep=' ', timespec='milliseconds')}] "
            + (f"[{line.source.upper()} TX -> DUT RX] " if line.source else "")
            + line.text.rstrip(chr(13))
            for line in selected[-count:]
        )


def child_environment():
    # Scan inclusion/exclusion remains useful, but terminal defaults must not
    # turn a tool call into an indefinite wait or open an unexpected capture.
    return {key: value for key, value in os.environ.items()
            if key not in {"BT_SCAN_WAIT_ANY", "BT_SCAN_WAIT_NEW",
                           "BT_CAPTURE_MODE", "BT_TIMESTAMP_MODE", "BT_PORT_CRLF"}}


def binary_path():
    override = os.environ.get("BOOTTERM_BINARY")
    local = Path(__file__).resolve().parent / "bin" / "bt"
    candidate = override or (str(local) if local.is_file() else shutil.which("bt"))
    resolved = shutil.which(candidate) if candidate else None
    if not resolved:
        raise ValueError("BootTerm binary not found. Run make in the BootTerm checkout, "
                         "or set BOOTTERM_BINARY to the absolute path of the built bt.")
    return resolved


def port_path(port):
    if not port or "\x00" in port:
        raise ValueError("A serial device path is required.")
    path = Path(port) if port.startswith("/") else Path("/dev") / port
    return str(path.resolve())


def encode_data(data, hex_mode=False):
    try:
        payload = bytes.fromhex(data) if hex_mode else data.encode("utf-8")
    except (ValueError, UnicodeError) as exc:
        raise ValueError(f"Invalid {'hexadecimal' if hex_mode else 'UTF-8'} data: {exc}") from exc
    if len(payload) > MAX_SEND_BYTES:
        raise ValueError(f"One send is limited to {MAX_SEND_BYTES} bytes.")
    return payload


def parse_time(value):
    if value is None:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError as exc:
        raise ValueError("Timestamp must have format yyyy-mm-dd HH:MM:SS.mmm.") from exc


class Session:
    def __init__(self, port, baudrate, process):
        self.port, self.baudrate, self.process = port, baudrate, process
        self.actual_baudrate = baudrate
        self.received, self.sent = History(), History()
        self.decoder = codecs.getincrementaldecoder("utf-8")("backslashreplace")
        self.human_decoder = codecs.getincrementaldecoder("utf-8")("backslashreplace")
        self.filter = None
        self.filter_after = 0
        self.capture = None
        self.capture_path = None
        self.capture_format = None
        self.error = None
        self.stderr = ""
        self.eof = False
        self.subscribers = set()
        self.write_lock = asyncio.Lock()
        self.auto_task = None
        self.reader_task = asyncio.create_task(self.read_output())
        self.stderr_task = None

    @property
    def live(self):
        return not self.eof and self.process.returncode is None

    async def ready(self):
        async def read_ready():
            while True:
                line = await self.process.stderr.readline()
                if not line:
                    raise ValueError(self.stderr.strip() or "BootTerm exited before opening the port.")
                if line.startswith(b"BT_READY "):
                    self.actual_baudrate = int(line.split()[1])
                    return
                self.stderr = (self.stderr + line.decode("utf-8", "replace"))[-4096:]
        try:
            await asyncio.wait_for(read_ready(), OP_TIMEOUT)
        except asyncio.TimeoutError as exc:
            raise ValueError("BootTerm did not become ready within 5 seconds. "
                             "Use the bt binary built from this checkout (with -R support).") from exc
        self.stderr_task = asyncio.create_task(self.read_stderr())

    async def read_stderr(self):
        while chunk := await self.process.stderr.read(4096):
            self.stderr = (self.stderr + chunk.decode("utf-8", "replace"))[-4096:]

    def broadcast(self, data):
        for queue in tuple(self.subscribers):
            if queue.full():
                # A slow viewer must never stop serial capture or other viewers.
                self.subscribers.discard(queue)
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(None)
            else:
                queue.put_nowait(data)

    async def read_output(self):
        try:
            while chunk := await self.process.stdout.read(4096):
                self.received.feed(self.decoder.decode(chunk))
                self.broadcast(chunk)
                self.record(chunk, source="dut")
            self.received.feed(self.decoder.decode(b"", final=True))
        except OSError as exc:
            self.error = f"Serial read failed: {exc}"
        finally:
            self.eof = True
            self.broadcast(None)
            self.stop_capture()
            await self.stop_auto()

    async def send(self, payload, source="ai"):
        if source not in ("ai", "human"):
            raise ValueError("Send source must be ai or human.")
        if len(payload) > MAX_SEND_BYTES:
            raise ValueError(f"One send is limited to {MAX_SEND_BYTES} bytes.")
        async with self.write_lock:
            if not self.live:
                raise ValueError(f"Port {self.port} is disconnected. Call connect_serial again.")
            try:
                self.process.stdin.write(payload)
                # Log when queued, before awaiting drain, so an immediate DUT
                # reply cannot appear before the command that triggered it.
                self.record(payload, source=source)
                self.broadcast({
                    "tx": base64.b64encode(payload).decode("ascii"),
                    "source": source,
                })
                text = (self.human_decoder.decode(payload) if source == "human"
                        else payload.decode("utf-8", "backslashreplace"))
                self.sent.feed(text, source=source)
                # Human keystrokes form a command across successive reads.
                # Separate AI calls remain separate messages without a newline.
                if source == "ai":
                    self.sent.partial = False
                await asyncio.wait_for(self.process.stdin.drain(), OP_TIMEOUT)
            except (OSError, asyncio.TimeoutError) as exc:
                self.error = "Serial write failed or timed out; some bytes may have been sent."
                if self.process.returncode is None:
                    with suppress(ProcessLookupError):
                        self.process.terminate()
                raise ValueError(self.error) from exc

    async def start_auto(self, payload, interval_ms):
        await self.stop_auto()
        await self.send(payload)

        async def repeat():
            try:
                while True:
                    await asyncio.sleep(interval_ms / 1000)
                    await self.send(payload)
            except (ValueError, OSError) as exc:
                self.error = f"Auto send stopped: {exc}"
        self.auto_task = asyncio.create_task(repeat())

    async def stop_auto(self):
        if self.auto_task:
            self.auto_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.auto_task
            self.auto_task = None

    def start_capture(self, filepath, log_format="raw"):
        if not self.live:
            raise ValueError("Port is disconnected. Call connect_serial first.")
        if log_format not in ("raw", "annotated", "jsonl"):
            raise ValueError("Log format must be raw, annotated, or jsonl.")
        filepath = Path(filepath)
        if not filepath.is_absolute():
            raise ValueError("Capture filepath must be absolute.")
        if self.capture_path == str(filepath) and self.capture_format != log_format:
            raise ValueError("Choose a different filepath when changing an active log's format.")
        # Open first, so a bad replacement path does not stop a good capture.
        capture = filepath.open("ab", buffering=0)
        self.stop_capture()
        self.capture, self.capture_path = capture, str(filepath)
        self.capture_format = log_format

    def record(self, payload, source):
        if self.capture is None or not payload:
            return
        if self.capture_format == "raw":
            if source != "dut":
                return
            data = payload
        else:
            event = {
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "source": source,
                "direction": "rx" if source == "dut" else "tx",
                "source_direction": "tx",
                "destination": "host" if source == "dut" else "dut",
                "destination_direction": "rx",
                "data_base64": base64.b64encode(payload).decode("ascii"),
                "text": payload.decode("utf-8", "backslashreplace"),
            }
            if self.capture_format == "jsonl":
                data = (json.dumps(event, ensure_ascii=True) + "\n").encode("utf-8")
            else:
                route = f"{source.upper()} TX -> {event['destination'].upper()} RX"
                text = json.dumps(event["text"], ensure_ascii=True)
                data = f"[{event['timestamp']}] [{route}] {text}\n".encode("utf-8")
        try:
            view = memoryview(data)
            while view:
                written = self.capture.write(view)
                if not written:
                    raise OSError("Log write made no progress")
                view = view[written:]
        except OSError as exc:
            self.error = f"Capture failed: {exc}"
            self.stop_capture()

    def stop_capture(self):
        if self.capture:
            try:
                self.capture.close()
            except OSError as exc:
                self.error = f"Capture close failed: {exc}"
            self.capture = None
            self.capture_path = None
            self.capture_format = None

    async def close(self):
        with anyio.CancelScope(shield=True):
            await self._close()

    async def _close(self):
        await self.stop_auto()
        if self.process.returncode is None:
            with suppress(ProcessLookupError):
                self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 2)
            except asyncio.TimeoutError:
                with suppress(ProcessLookupError):
                    self.process.kill()
                await self.process.wait()
        await asyncio.gather(self.reader_task, return_exceptions=True)
        if self.stderr_task:
            await asyncio.gather(self.stderr_task, return_exceptions=True)
        self.stop_capture()


class Backend:
    def __init__(self):
        self.sessions = {}
        self.lock = asyncio.Lock()

    async def connect(self, port, baudrate):
        path = port_path(port)
        old = self.sessions.get(path)
        if old and old.live:
            if old.baudrate != baudrate:
                raise ValueError(f"Port already connected at {old.baudrate}; disconnect before changing baud rate.")
            return old
        if old:
            await old.close()
            del self.sessions[path]
        # A cancelled MCP request must not leave an untracked serial owner.
        with anyio.CancelScope(shield=True):
            process = await asyncio.create_subprocess_exec(
                binary_path(), "-R", "-b", str(baudrate), "--", path,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE, env=child_environment(),
            )
            session = Session(path, baudrate, process)
            self.sessions[path] = session
        try:
            await session.ready()
        except BaseException:
            await session.close()
            del self.sessions[path]
            raise
        return session

    async def console_attach(self, port, baudrate, log_path=None, log_format="annotated"):
        if isinstance(baudrate, bool) or not isinstance(baudrate, int) or not 0 <= baudrate <= 2147483647:
            raise ValueError("baudrate must be an integer between 0 and 2147483647.")
        async with self.lock:
            path = port_path(port)
            existing = self.sessions.get(path)
            # A viewer of an existing session uses its current baud rate.
            session = existing if existing and existing.live else await self.connect(port, baudrate)
            if log_path is not None:
                session.start_capture(log_path, log_format)
            return session

    async def close(self):
        await asyncio.gather(*(s.close() for s in self.sessions.values()))
        self.sessions.clear()

    async def execute(self, name, args):
        async with self.lock:
            return await self._execute(name, args)

    async def _execute(self, name, args):
        if name == "list_serial_ports":
            with anyio.CancelScope(shield=True):
                process = await asyncio.create_subprocess_exec(
                    binary_path(), "-l", stdin=asyncio.subprocess.DEVNULL,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                    env=child_environment(),
                )
            try:
                out, err = await asyncio.wait_for(process.communicate(), OP_TIMEOUT)
            finally:
                with anyio.CancelScope(shield=True):
                    if process.returncode is None:
                        with suppress(ProcessLookupError):
                            process.kill()
                        await process.wait()
            if process.returncode:
                raise ValueError(err.decode("utf-8", "replace").strip() or "Port enumeration failed.")
            return out.decode("utf-8", "replace").strip() or "No serial ports found."
        if name == "connect_serial":
            session = await self.connect(args["port"], args["baudrate"])
            return f"Connected to {session.port} at {session.actual_baudrate} bps."
        path = port_path(args["port"])
        session = self.sessions.get(path)
        if not session:
            raise ValueError(f"Port {path} is not connected. Call connect_serial first.")
        if name == "disconnect_serial":
            await session.close()
            del self.sessions[path]
            return f"Disconnected {path}."
        if name == "send_serial_data":
            payload = encode_data(args["data"], args.get("hex_mode", False))
            await session.send(payload)
            return f"Sent {len(payload)} bytes to {path}."
        if name == "start_auto_send":
            await session.start_auto(encode_data(args["data"], args.get("hex_mode", False)), args["interval_ms"])
            return f"Auto send started every {args['interval_ms']} ms."
        if name == "stop_auto_send":
            await session.stop_auto()
            return "Auto send stopped."
        if name == "set_filter":
            if not args.get("enable", True):
                session.filter = None
            elif args.get("is_regex", False):
                pattern = regex.compile(args["keyword"])
                session.filter = lambda text: pattern.search(text, timeout=0.01) is not None
            else:
                keyword = args["keyword"]
                session.filter = lambda text: keyword in text
            session.filter_after = 0
            return "Filter updated."
        if name == "start_saving_file":
            log_format = args.get("format", "raw")
            session.start_capture(args["filepath"], log_format)
            return f"Appending {log_format} serial log to {session.capture_path}."
        if name == "stop_saving_file":
            session.stop_capture()
            return "File capture stopped."
        if name == "clear_data_buffers":
            target = args.get("target", "all")
            if target in ("main", "all"):
                session.received.clear()
                session.decoder.reset()
            if target in ("history", "all"):
                session.sent.clear()
                session.human_decoder.reset()
            if target in ("filter", "all"):
                session.filter_after = session.received.sequence
            return f"Cleared {target} buffer(s)."
        if name in ("read_received_data", "read_sent_data", "read_filtered_data"):
            start, end = parse_time(args.get("start_time")), parse_time(args.get("end_time"))
            if start and end and start > end:
                raise ValueError("start_time must be before or equal to end_time.")
            history = session.sent if name == "read_sent_data" else session.received
            if name == "read_filtered_data" and session.filter is None:
                return "No filter enabled."
            return history.render(
                args.get("lines_count", 50), start, end,
                session.filter if name == "read_filtered_data" else None,
                session.filter_after if name == "read_filtered_data" else 0,
            ) or "No data."
        if name == "get_buffer_status":
            return json.dumps({
                "port": path, "connected": session.live, "pid": session.process.pid,
                "baudrate": session.actual_baudrate,
                "received_lines": len(session.received.lines), "sent_lines": len(session.sent.lines),
                "max_lines": MAX_LINES, "max_line_chars": MAX_LINE_CHARS,
                "usage_percent": round(len(session.received.lines) * 100 / MAX_LINES, 2),
                "auto_send": session.auto_task is not None and not session.auto_task.done(),
                "capture_path": session.capture_path, "capture_format": session.capture_format,
                "viewers": len(session.subscribers),
                "error": session.error or (session.stderr.strip() if not session.live else None),
            })
        raise ValueError(f"Unknown tool: {name}")


backend = Backend()


@mcp_server.call_tool()
async def handle_call_tool(name, arguments):
    try:
        text = await backend.execute(name, arguments or {})
        return [types.TextContent(type="text", text=text)]
    except (ValueError, OSError, TimeoutError, regex.error) as exc:
        # The SDK turns handler exceptions into isError tool results (also on
        # older 1.x versions that cannot accept CallToolResult directly).
        raise ValueError(str(exc) or "Operation timed out.") from exc


def default_socket():
    return os.environ.get("BOOTTERM_MCP_SOCKET", str(Path.home() / ".cache" / "bootterm-mcp" / "console.sock"))


async def run_server(socket_path):
    from bootterm_console import serve_console

    socket_path = Path(socket_path).expanduser().absolute()
    socket_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    console = await serve_console(backend, str(socket_path))
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await mcp_server.run(
                read_stream, write_stream,
                InitializationOptions(
                    server_name="bootterm-mcp", server_version="0.1.0",
                    capabilities=mcp_server.get_capabilities(
                        notification_options=NotificationOptions(), experimental_capabilities={},
                    ),
                ),
            )
    finally:
        # SDK cancellation must not interrupt child-process and socket cleanup.
        with anyio.CancelScope(shield=True):
            console.close()
            await console.wait_closed()
            await backend.close()
            with suppress(FileNotFoundError):
                socket_path.unlink()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--console", metavar="PORT", help="attach a human terminal to a running MCP server")
    parser.add_argument("--baudrate", type=int, default=115200, help="baud rate when console opens a new port")
    parser.add_argument("--log", metavar="PATH", help="append a shared serial log to this absolute path when attaching")
    parser.add_argument("--log-format", choices=("raw", "annotated", "jsonl"), default="annotated", help="console log format (default: annotated)")
    parser.add_argument("--raw-output", action="store_true", help="show only live received bytes, without history replay or local AI TX notices")
    parser.add_argument("--socket", default=default_socket(), help="shared-console Unix socket path (or BOOTTERM_MCP_SOCKET)")
    options = parser.parse_args()
    if options.log and not options.console:
        parser.error("--log requires --console; MCP clients use start_saving_file")
    try:
        if options.console:
            from bootterm_console import attach_console
            asyncio.run(attach_console(options.socket, options.console, options.baudrate, options.log, options.log_format, show_ai_tx=not options.raw_output))
        else:
            asyncio.run(run_server(options.socket))
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    except (OSError, ValueError) as exc:
        print(f"bootterm-mcp: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
