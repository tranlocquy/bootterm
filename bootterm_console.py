# SPDX-License-Identifier: Apache-2.0
"""Local terminal access to serial sessions owned by the BootTerm MCP server.

The Unix socket carries newline-delimited JSON; serial bytes are base64 encoded.
This module uses only the standard library and never writes server diagnostics to
stdout. Closing a console connection leaves the serial session running.
"""

import asyncio
import base64
import binascii
import contextlib
import json
import os
import signal
import socket
import stat
import sys
import termios
import tty


MAX_PAYLOAD = 64 * 1024
MAX_REQUEST_LINE = 128 * 1024
MAX_RESPONSE_LINE = 8 * 1024 * 1024
IO_TIMEOUT = 10
DETACH = b"\x1d"


async def _read_message(reader):
    line = await reader.readline()
    if not line:
        return None
    try:
        message = json.loads(line)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("Console message must be valid JSON") from exc
    if not isinstance(message, dict):
        raise ValueError("Console message must be a JSON object")
    return message


def _decode_data(message):
    data = message.get("data")
    if not isinstance(data, str):
        raise ValueError("Console data must be a base64 string")
    if len(data) > 4 * ((MAX_PAYLOAD + 2) // 3):
        raise ValueError("Console payload exceeds 64 KiB")
    try:
        payload = base64.b64decode(data, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("Console data is not valid base64") from exc
    if len(payload) > MAX_PAYLOAD:
        raise ValueError("Console payload exceeds 64 KiB")
    return payload


async def _send_message(writer, message):
    writer.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")
    await asyncio.wait_for(writer.drain(), IO_TIMEOUT)


async def _run_pair(first, second):
    tasks = [asyncio.create_task(first), asyncio.create_task(second)]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        await asyncio.gather(*done)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def _handle_console(backend, reader, writer):
    session = None
    queue = asyncio.Queue(maxsize=256)
    try:
        request = await asyncio.wait_for(_read_message(reader), IO_TIMEOUT)
        if request is None:
            return
        port = request.get("port")
        baudrate = request.get("baudrate", 115200)
        log_path = request.get("log_path")
        log_format = request.get("log_format", "annotated")
        if type(request.get("show_ai_tx", True)) is not bool:
            raise ValueError("show_ai_tx must be a boolean")
        if not isinstance(port, str) or not port or len(port) > 4096:
            raise ValueError("A serial port path is required")
        if type(baudrate) is not int or baudrate < 0:
            raise ValueError("Baud rate must be a nonnegative integer")
        if log_path is not None and (
            not isinstance(log_path, str) or not log_path or len(log_path) > 4096
        ):
            raise ValueError("Log path must be a nonempty string of at most 4096 characters")
        if log_format not in ("raw", "annotated", "jsonl"):
            raise ValueError("Log format must be raw, annotated, or jsonl")
        session = await backend.console_attach(
            port, baudrate, log_path=log_path, log_format=log_format
        )
        if not session.live:
            raise ConnectionError("Serial session has closed")
        # No await between subscribing and taking the snapshot: newly received
        # bytes go to the queue only after the history has been captured.
        session.subscribers.add(queue)
        history = session.received.render()
        await _send_message(writer, {"ok": True, "history": history})

        async def receive_input():
            while True:
                message = await _read_message(reader)
                if message is None:
                    return
                payload = _decode_data(message)
                if payload:
                    await session.send(payload, source="human")

        async def send_output():
            while True:
                payload = await queue.get()
                if payload is None:
                    raise ConnectionError("Serial session closed or console could not keep up")
                if isinstance(payload, dict):
                    await _send_message(writer, payload)
                    continue
                for offset in range(0, len(payload), MAX_PAYLOAD):
                    await _send_message(writer, {
                        "data": base64.b64encode(payload[offset:offset + MAX_PAYLOAD]).decode("ascii")
                    })

        await _run_pair(receive_input(), send_output())
    except Exception as exc:
        with contextlib.suppress(Exception):
            await _send_message(writer, {"error": str(exc) or type(exc).__name__})
    finally:
        if session is not None:
            session.subscribers.discard(queue)
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


class ConsoleServer(asyncio.AbstractServer):
    """An asyncio server that also closes its active console connections."""

    def __init__(self):
        self.server = None
        self.clients = set()
        self.writers = set()
        self.closing = False

    @property
    def sockets(self):
        return self.server.sockets

    def get_loop(self):
        return self.server.get_loop()

    def is_serving(self):
        return self.server.is_serving()

    async def start_serving(self):
        await self.server.start_serving()

    async def serve_forever(self):
        await self.server.serve_forever()

    def close(self):
        self.closing = True
        self.server.close()
        for writer in self.writers:
            writer.close()
        for task in self.clients:
            task.cancel()

    async def wait_closed(self):
        await self.server.wait_closed()
        if self.clients:
            await asyncio.gather(*tuple(self.clients), return_exceptions=True)


async def serve_console(backend, socket_path):
    """Bind a private console socket; refuse any existing filesystem entry.

    The caller owns directory creation and unlinking the socket after shutdown.
    """
    server = ConsoleServer()

    def accept(reader, writer):
        if server.closing:
            writer.close()
            return
        task = asyncio.create_task(_handle_console(backend, reader, writer))
        server.clients.add(task)
        server.writers.add(writer)

        def finished(task):
            server.clients.discard(task)
            server.writers.discard(writer)

        task.add_done_callback(finished)

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    bound = False
    try:
        # Supplying a bound socket prevents asyncio from removing an existing
        # socket path, which might belong to another running MCP server.
        listener.bind(os.fspath(socket_path))
        bound = True
        listener.setblocking(False)
        os.chmod(socket_path, 0o600)
        server.server = await asyncio.start_unix_server(
            accept, sock=listener, limit=MAX_REQUEST_LINE, start_serving=False
        )
        await server.start_serving()
        return server
    except BaseException:
        listener.close()
        if bound:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(socket_path)
        raise


async def close_console(server):
    """Close the listener and all console handlers without closing sessions."""
    server.close()
    await server.wait_closed()


def _write_stdout(data):
    view = memoryview(data)
    while view:
        written = os.write(sys.stdout.fileno(), view)
        view = view[written:]


async def _read_stdin(fd):
    # Regular files cannot be registered with epoll on Linux; their reads do
    # not block, so redirected input can be consumed directly.
    if stat.S_ISREG(os.fstat(fd).st_mode):
        return os.read(fd, MAX_PAYLOAD)
    loop = asyncio.get_running_loop()
    result = loop.create_future()

    def ready():
        try:
            data = os.read(fd, MAX_PAYLOAD)
        except BlockingIOError:
            return
        except OSError as exc:
            loop.remove_reader(fd)
            if not result.done():
                result.set_exception(exc)
        else:
            loop.remove_reader(fd)
            if not result.done():
                result.set_result(data)

    loop.add_reader(fd, ready)
    try:
        return await result
    finally:
        loop.remove_reader(fd)


async def attach_console(
    socket_path, port, baudrate, log_path=None, log_format="annotated", show_ai_tx=True
):
    """Attach a human terminal; Ctrl-] or stdin EOF detaches just this client."""
    socket_path = os.path.abspath(os.path.expanduser(os.fspath(socket_path)))
    reader, writer = await asyncio.open_unix_connection(
        socket_path, limit=MAX_RESPONSE_LINE
    )
    terminal_settings = None
    stdin_fd = sys.stdin.fileno()
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    loop.add_signal_handler(signal.SIGTERM, task.cancel)
    try:
        await _send_message(writer, {
            "port": port, "baudrate": baudrate,
            "log_path": log_path, "log_format": log_format,
            "show_ai_tx": show_ai_tx,
        })
        reply = await asyncio.wait_for(_read_message(reader), IO_TIMEOUT)
        if reply is None:
            raise ConnectionError("Console server closed before accepting the connection")
        if "error" in reply:
            raise ConnectionError(reply["error"])
        if reply.get("ok") is not True or not isinstance(reply.get("history"), str):
            raise ValueError("Invalid console server response")
        if show_ai_tx:
            _write_stdout(reply["history"].encode("utf-8", errors="replace"))
        logging_notice = f" Logging to {log_path} ({log_format})." if log_path else ""
        print(
            f"Connected to {port}.{logging_notice} Press Ctrl-] to detach.",
            file=sys.stderr, flush=True,
        )
        if os.isatty(stdin_fd):
            terminal_settings = termios.tcgetattr(stdin_fd)
            tty.setraw(stdin_fd, termios.TCSANOW)

        async def send_input():
            while True:
                payload = await _read_stdin(stdin_fd)
                if not payload:
                    return
                before, separator, _ = payload.partition(DETACH)
                if before:
                    await _send_message(writer, {"data": base64.b64encode(before).decode("ascii")})
                if separator:
                    return

        async def receive_output():
            while True:
                message = await _read_message(reader)
                if message is None:
                    return
                if "error" in message:
                    raise ConnectionError(message["error"])
                if "tx" in message:
                    payload = _decode_data({"data": message["tx"]})
                    if message.get("source") not in ("ai", "human"):
                        raise ValueError("Console transmit source must be ai or human")
                    if show_ai_tx and message["source"] == "ai":
                        visible = json.dumps(payload.decode("utf-8", "backslashreplace"), ensure_ascii=True)
                        _write_stdout(f"\r\n[AI TX -> DUT RX] {visible}\r\n".encode("utf-8"))
                    continue
                _write_stdout(_decode_data(message))

        await _run_pair(send_input(), receive_output())
    finally:
        try:
            if terminal_settings is not None:
                termios.tcsetattr(stdin_fd, termios.TCSANOW, terminal_settings)
        finally:
            loop.remove_signal_handler(signal.SIGTERM)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
