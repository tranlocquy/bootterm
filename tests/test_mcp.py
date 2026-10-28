"""End-to-end tests against the real MCP stdio server and BootTerm binary.

PTYs emulate serial devices, so these tests never open physical serial ports.
Run `make` followed by `python -m pytest tests` in the project environment.
"""

import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import pty
import sys
import tempfile

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


ROOT = Path(__file__).resolve().parents[1]
BINARY = ROOT / "bin" / "bt"
SCRIPT = ROOT / "bootterm_mcp.py"
TOOLS = {
    "list_serial_ports", "connect_serial", "send_serial_data",
    "disconnect_serial", "start_auto_send", "stop_auto_send", "set_filter",
    "start_saving_file", "stop_saving_file", "clear_data_buffers",
    "read_received_data", "read_sent_data", "read_filtered_data",
    "get_buffer_status",
}


@pytest.fixture(scope="session", autouse=True)
def require_binary():
    assert BINARY.is_file(), "Build BootTerm with `make` before running tests"


@pytest.fixture
def serial_device():
    master, slave = pty.openpty()
    os.set_blocking(master, False)
    try:
        yield master, os.ttyname(slave)
    finally:
        os.close(master)
        os.close(slave)


@asynccontextmanager
async def server(binary=BINARY, extra_env=None, socket_path=None):
    with tempfile.TemporaryDirectory(prefix="bt-test-") as directory:
        params = StdioServerParameters(
            command=sys.executable,
            args=[str(SCRIPT)],
            cwd=str(ROOT),
            env={
                "BOOTTERM_BINARY": str(binary),
                "BOOTTERM_MCP_SOCKET": str(socket_path or Path(directory) / "control.sock"),
                "PYTHONUNBUFFERED": "1",
                **(extra_env or {}),
            },
        )
        async with stdio_client(params) as (reader, writer):
            async with ClientSession(
                reader, writer, read_timeout_seconds=timedelta(seconds=5)
            ) as session:
                await session.initialize()
                yield session


def result_text(result):
    return "\n".join(item.text for item in result.content if item.type == "text")


async def call(session, name, **arguments):
    result = await session.call_tool(name, arguments)
    assert not result.isError, result_text(result)
    return result_text(result)


async def wait_for_text(session, tool, port, expected, **arguments):
    deadline = asyncio.get_running_loop().time() + 3
    while True:
        text = await call(session, tool, port=port, **arguments)
        if expected in text:
            return text
        assert asyncio.get_running_loop().time() < deadline, (expected, text)
        await asyncio.sleep(0.01)


async def read_device(master, count, timeout=3):
    data = bytearray()
    deadline = asyncio.get_running_loop().time() + timeout
    while len(data) < count:
        try:
            chunk = os.read(master, count - len(data))
        except BlockingIOError:
            chunk = b""
        if chunk:
            data.extend(chunk)
        else:
            assert asyncio.get_running_loop().time() < deadline, bytes(data)
            await asyncio.sleep(0.01)
    return bytes(data)


def drain_device(master):
    data = bytearray()
    while True:
        try:
            chunk = os.read(master, 65536)
        except BlockingIOError:
            break
        if not chunk:
            break
        data.extend(chunk)
    return bytes(data)


@pytest.mark.asyncio
async def test_discovery_is_lazy_without_bootterm(tmp_path):
    async with server(binary=tmp_path / "missing-bt") as session:
        listing = await session.list_tools()
        assert {tool.name for tool in listing.tools} == TOOLS
        result = await session.call_tool("list_serial_ports", {})
        assert result.isError
        result = await session.call_tool(
            "connect_serial", {"port": "/dev/nonexistent-bootterm-test", "baudrate": 115200}
        )
        assert result.isError
        assert result_text(result)


@pytest.mark.asyncio
async def test_text_and_all_byte_values_round_trip(serial_device, tmp_path):
    master, port = serial_device
    capture = tmp_path / "received.bin"
    async with server() as session:
        await call(session, "connect_serial", port=port, baudrate=115200)
        await call(session, "send_serial_data", port=port, data="status\r\n")
        assert await read_device(master, 8) == b"status\r\n"
        history = await call(session, "read_sent_data", port=port)
        assert "status" in history

        # Escape bytes and standalone C1 controls must not trigger terminal
        # commands or be translated into printable <0xHH> sequences.
        payload = bytes(range(256))
        await call(session, "send_serial_data", port=port, data=payload.hex(), hex_mode=True)
        assert await read_device(master, len(payload)) == payload

        await call(session, "start_saving_file", port=port, filepath=str(capture))
        os.write(master, payload)
        deadline = asyncio.get_running_loop().time() + 3
        while not capture.exists() or capture.stat().st_size < len(payload):
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        await call(session, "stop_saving_file", port=port)
        assert capture.read_bytes() == payload
        await call(session, "disconnect_serial", port=port)


@pytest.mark.asyncio
async def test_split_utf8_prompt_and_time_range(serial_device):
    master, port = serial_device
    async with server() as session:
        await call(session, "connect_serial", port=port, baudrate=115200)
        os.write(master, b"boot \xe2")
        await asyncio.sleep(0.03)
        os.write(master, b"\x82\xac ready\r\nlogin: ")
        text = await wait_for_text(session, "read_received_data", port, "login: ")
        assert "boot € ready" in text
        assert "\ufffd" not in text
        full = await call(
            session, "read_received_data", port=port,
            start_time="2000-01-01 00:00:00.000", end_time="2100-01-01 00:00:00.000",
        )
        assert "boot € ready" in full
        excluded = await call(
            session, "read_received_data", port=port,
            end_time="2000-01-01 00:00:00.000",
        )
        assert "boot € ready" not in excluded


@pytest.mark.asyncio
async def test_filters_and_buffer_clear(serial_device):
    master, port = serial_device
    async with server() as session:
        await call(session, "connect_serial", port=port, baudrate=115200)
        await call(session, "set_filter", port=port, keyword="ERROR")
        os.write(master, b"INFO ordinary\nERROR broken\n")
        filtered = await wait_for_text(session, "read_filtered_data", port, "ERROR broken")
        assert "INFO ordinary" not in filtered
        received = await call(session, "read_received_data", port=port)
        assert "INFO ordinary" in received

        await call(session, "set_filter", port=port, keyword=r"WARN \d+", is_regex=True)
        os.write(master, b"WARN text\nWARN 42\n")
        filtered = await wait_for_text(session, "read_filtered_data", port, "WARN 42")
        assert "WARN text" not in filtered

        await call(session, "send_serial_data", port=port, data="sent command")
        assert await read_device(master, 12) == b"sent command"
        await call(session, "clear_data_buffers", port=port, target="filter")
        assert "WARN 42" not in await call(session, "read_filtered_data", port=port)
        assert "WARN 42" in await call(session, "read_received_data", port=port)
        assert "sent command" in await call(session, "read_sent_data", port=port)
        os.write(master, b"WARN 43\n")
        filtered = await wait_for_text(session, "read_filtered_data", port, "WARN 43")
        assert "WARN 42" not in filtered
        await call(session, "clear_data_buffers", port=port, target="history")
        assert "sent command" not in await call(session, "read_sent_data", port=port)
        assert "WARN 43" in await call(session, "read_filtered_data", port=port)
        assert "WARN 42" in await call(session, "read_received_data", port=port)

        status = json.loads(await call(session, "get_buffer_status", port=port))
        assert isinstance(status, dict)
        await call(session, "clear_data_buffers", port=port, target="main")
        received = await call(session, "read_received_data", port=port)
        assert "INFO ordinary" not in received
        assert "WARN 42" not in received
        await call(session, "clear_data_buffers", port=port, target="all")
        filtered = await call(session, "read_filtered_data", port=port)
        assert "WARN 42" not in filtered


@pytest.mark.asyncio
async def test_auto_send_stops(serial_device):
    master, port = serial_device
    async with server() as session:
        await call(session, "connect_serial", port=port, baudrate=115200)
        await call(session, "start_auto_send", port=port, data="ping", interval_ms=30)
        assert await read_device(master, 12) == b"pingpingping"
        await call(session, "stop_auto_send", port=port)
        await asyncio.sleep(0.05)
        drain_device(master)
        await asyncio.sleep(0.12)
        assert drain_device(master) == b""
        history = await call(session, "read_sent_data", port=port)
        assert "ping" in history


@pytest.mark.asyncio
async def test_file_capture_appends_and_stops(serial_device, tmp_path):
    master, port = serial_device
    capture = tmp_path / "capture.bin"
    capture.write_bytes(b"existing\n")
    async with server() as session:
        await call(session, "connect_serial", port=port, baudrate=115200)
        await call(session, "start_saving_file", port=port, filepath=str(capture))
        await call(session, "send_serial_data", port=port, data="transmit is not raw capture\r\n")
        assert await read_device(master, 29) == b"transmit is not raw capture\r\n"
        os.write(master, b"saved\x00\r\n")
        await wait_for_text(session, "read_received_data", port, "saved")
        await call(session, "stop_saving_file", port=port)
        os.write(master, b"not saved\n")
        await wait_for_text(session, "read_received_data", port, "not saved")
        assert capture.read_bytes() == b"existing\nsaved\x00\r\n"


@pytest.mark.asyncio
async def test_errors_are_tool_results_and_server_remains_usable(serial_device):
    master, port = serial_device
    async with server() as session:
        failures = [
            ("connect_serial", {"port": "/dev/nonexistent-bootterm-test", "baudrate": 115200}),
            ("send_serial_data", {"port": port, "data": "before connect"}),
            ("connect_serial", {"port": port, "baudrate": -1}),
            ("connect_serial", {"port": port}),
            ("unknown_tool", {}),
        ]
        for name, arguments in failures:
            result = await session.call_tool(name, arguments)
            assert result.isError, (name, result_text(result))
        await call(session, "connect_serial", port=port, baudrate=115200)
        failures = [
            ("send_serial_data", {"port": port, "data": "not hex", "hex_mode": True}),
            ("start_auto_send", {"port": port, "data": "x", "interval_ms": 0}),
            ("set_filter", {"port": port, "keyword": "[", "is_regex": True}),
            ("start_saving_file", {"port": port, "filepath": "relative.log"}),
            ("clear_data_buffers", {"port": port, "target": "invalid"}),
            ("read_received_data", {"port": port, "lines_count": -1}),
            ("read_received_data", {"port": port, "start_time": "not a timestamp"}),
        ]
        for name, arguments in failures:
            result = await session.call_tool(name, arguments)
            assert result.isError, (name, result_text(result))
        await call(session, "send_serial_data", port=port, data="ok")
        assert await read_device(master, 2) == b"ok"


@pytest.mark.asyncio
async def test_disconnect_and_reconnect(serial_device):
    master, port = serial_device
    async with server() as session:
        await call(session, "connect_serial", port=port, baudrate=115200)
        await call(session, "disconnect_serial", port=port)
        result = await session.call_tool("send_serial_data", {"port": port, "data": "closed"})
        assert result.isError
        await call(session, "connect_serial", port=port, baudrate=9600)
        await call(session, "send_serial_data", port=port, data="reconnected")
        assert await read_device(master, 11) == b"reconnected"


@pytest.mark.asyncio
async def test_server_shutdown_reaps_bootterm(serial_device):
    _, port = serial_device
    async with server() as session:
        await call(session, "connect_serial", port=port, baudrate=115200)
        status = json.loads(await call(session, "get_buffer_status", port=port))
        pid = status["pid"]
        os.kill(pid, 0)
        await call(session, "start_auto_send", port=port, data="tick", interval_ms=100)
    deadline = asyncio.get_running_loop().time() + 3
    while True:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        assert asyncio.get_running_loop().time() < deadline, f"BootTerm process {pid} leaked"
        await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_human_console_and_ai_share_one_serial_connection(serial_device):
    master, port = serial_device
    # Unix socket paths are length-limited on macOS; keep this path short.
    with tempfile.TemporaryDirectory(prefix="bt-console-") as directory:
        socket_path = Path(directory) / "console.sock"
        async with server(socket_path=socket_path) as session:
            await call(session, "connect_serial", port=port, baudrate=115200)
            before = json.loads(await call(session, "get_buffer_status", port=port))
            console = await asyncio.create_subprocess_exec(
                sys.executable, str(SCRIPT), "--console", port,
                "--baudrate", "115200", "--socket", str(socket_path),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                ready = await asyncio.wait_for(console.stderr.readline(), timeout=5)
                assert ready, "Console exited before attaching"
                # A real terminal sends keystrokes separately. The AI should
                # still receive one complete human command in sent history.
                human_command = "human € command\r\n".encode()
                for byte in human_command:
                    console.stdin.write(bytes([byte]))
                    await console.stdin.drain()
                    assert await read_device(master, 1) == bytes([byte])
                history = await wait_for_text(session, "read_sent_data", port, "human € command")
                assert "[HUMAN TX -> DUT RX]" in history
                assert "\\xe2" not in history
                await call(session, "send_serial_data", port=port, data="ai\r\n")
                assert await read_device(master, 4) == b"ai\r\n"
                # This notice must arrive without relying on the DUT echoing
                # the AI's command back over the serial connection.
                notice = b'\r\n[AI TX -> DUT RX] "ai\\r\\n"\r\n'
                visible = await asyncio.wait_for(console.stdout.readexactly(len(notice)), timeout=3)
                assert visible == notice
                history = await call(session, "read_sent_data", port=port)
                assert "[AI TX -> DUT RX]" in history
                assert "human € command" in history
                os.write(master, b"shared boot output\r\n")
                output = await asyncio.wait_for(console.stdout.readexactly(20), timeout=3)
                assert output == b"shared boot output\r\n"
                await wait_for_text(session, "read_received_data", port, "shared boot output")
                after = json.loads(await call(session, "get_buffer_status", port=port))
                assert before["pid"] == after["pid"]

                console.stdin.write(b"\x1d")
                await console.stdin.drain()
                assert await asyncio.wait_for(console.wait(), timeout=3) == 0
                await call(session, "send_serial_data", port=port, data="still connected")
                assert await read_device(master, 15) == b"still connected"
            finally:
                if console.returncode is None:
                    console.terminate()
                    await asyncio.wait_for(console.wait(), timeout=3)


@pytest.mark.asyncio
async def test_console_without_server_reports_failure():
    with tempfile.TemporaryDirectory(prefix="bt-absent-") as directory:
        console = await asyncio.create_subprocess_exec(
            sys.executable, str(SCRIPT), "--console", "/dev/nonexistent-bootterm-test",
            "--socket", str(Path(directory) / "absent.sock"),
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(console.communicate(), timeout=5)
            assert console.returncode != 0
            assert stdout == b""
            assert stderr
            assert b"Traceback" not in stderr
        finally:
            if console.returncode is None:
                console.kill()
                await console.wait()


@pytest.mark.asyncio
async def test_device_hangup_preserves_history_and_stops_background_work(tmp_path):
    master, slave = pty.openpty()
    port = os.ttyname(slave)
    os.set_blocking(master, False)
    os.close(slave)
    capture = tmp_path / "hangup.bin"
    try:
        async with server() as session:
            await call(session, "connect_serial", port=port, baudrate=115200)
            await call(session, "start_saving_file", port=port, filepath=str(capture))
            os.write(master, b"last boot message\r\n")
            await wait_for_text(session, "read_received_data", port, "last boot message")
            await call(session, "start_auto_send", port=port, data="poll", interval_ms=100)
            assert await read_device(master, 4) == b"poll"
            os.close(master)
            master = None

            deadline = asyncio.get_running_loop().time() + 3
            while True:
                status = json.loads(await call(session, "get_buffer_status", port=port))
                if not status["connected"] and not status["auto_send"] and status["capture_path"] is None:
                    break
                assert asyncio.get_running_loop().time() < deadline, status
                await asyncio.sleep(0.01)
            assert "last boot message" in await call(session, "read_received_data", port=port)
            assert "poll" in await call(session, "read_sent_data", port=port)
            assert capture.read_bytes() == b"last boot message\r\n"
            result = await session.call_tool("send_serial_data", {"port": port, "data": "closed"})
            assert result.isError
    finally:
        if master is not None:
            os.close(master)


@pytest.mark.asyncio
async def test_console_jsonl_log_attributes_human_ai_auto_send_and_dut(serial_device, tmp_path):
    master, port = serial_device
    logfile = tmp_path / "session.jsonl"
    human_payload = b"human command\r\n"
    ai_payload = b"\x00\x1d\x80\xff"
    dut_payload = b"DUT reply\x00\xff\r\n"
    with tempfile.TemporaryDirectory(prefix="bt-log-") as directory:
        socket_path = Path(directory) / "console.sock"
        async with server(socket_path=socket_path) as session:
            await call(session, "connect_serial", port=port, baudrate=115200)
            # Raw viewers receive only future DUT bytes, without formatted
            # history replay or AI transmit notices mixed into their stream.
            os.write(master, b"before raw viewer attached\r\n")
            await wait_for_text(session, "read_received_data", port, "before raw viewer attached")
            console = await asyncio.create_subprocess_exec(
                sys.executable, str(SCRIPT), "--console", port,
                "--socket", str(socket_path), "--log", str(logfile), "--log-format", "jsonl",
                "--raw-output",
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                ready = await asyncio.wait_for(console.stderr.readline(), timeout=5)
                assert b"Connected to" in ready, ready
                console.stdin.write(human_payload)
                await console.stdin.drain()
                assert await read_device(master, len(human_payload)) == human_payload
                await call(session, "send_serial_data", port=port, data=ai_payload.hex(), hex_mode=True)
                assert await read_device(master, len(ai_payload)) == ai_payload
                await call(session, "start_auto_send", port=port, data="auto", interval_ms=100)
                assert await read_device(master, 4) == b"auto"
                await call(session, "stop_auto_send", port=port)

                os.write(master, dut_payload)
                received = await asyncio.wait_for(console.stdout.readexactly(len(dut_payload)), timeout=3)
                assert received == dut_payload
                await wait_for_text(session, "read_received_data", port, "DUT reply")
                await call(session, "stop_saving_file", port=port)

                events = [json.loads(line) for line in logfile.read_text().splitlines()]
                assert {event["source"] for event in events} == {"ai", "human", "dut"}
                for event in events:
                    timestamp = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))
                    assert timestamp.utcoffset() == timedelta(0)
                    assert event["source_direction"] == "tx"
                    assert event["destination_direction"] == "rx"
                    assert event["direction"] == ("rx" if event["source"] == "dut" else "tx")
                    assert event["destination"] == ("host" if event["source"] == "dut" else "dut")
                    payload = base64.b64decode(event["data_base64"], validate=True)
                    assert event["text"] == payload.decode("utf-8", "backslashreplace")
                assert b"".join(base64.b64decode(e["data_base64"]) for e in events if e["source"] == "human") == human_payload
                assert b"".join(base64.b64decode(e["data_base64"]) for e in events if e["source"] == "dut") == dut_payload
                ai_events = [base64.b64decode(e["data_base64"]) for e in events if e["source"] == "ai"]
                assert ai_payload in ai_events
                assert b"auto" in ai_events
            finally:
                if console.returncode is None:
                    console.terminate()
                    await asyncio.wait_for(console.wait(), timeout=3)


@pytest.mark.asyncio
async def test_annotated_capture_labels_directions_and_escapes_data(serial_device, tmp_path):
    master, port = serial_device
    logfile = tmp_path / "session.log"
    sent = 'say "hello"\r\n'
    received = b"ready\x00\x80\r\n"
    async with server() as session:
        await call(session, "connect_serial", port=port, baudrate=115200)
        await call(session, "start_saving_file", port=port, filepath=str(logfile), format="annotated")
        await call(session, "send_serial_data", port=port, data=sent)
        assert await read_device(master, len(sent)) == sent.encode()
        os.write(master, received)
        await wait_for_text(session, "read_received_data", port, "ready")
        await call(session, "stop_saving_file", port=port)

    lines = logfile.read_text().splitlines()
    assert any("[AI TX -> DUT RX]" in line for line in lines)
    assert any("[DUT TX -> HOST RX]" in line for line in lines)
    ai_text = "".join(json.loads(line.split("] ", 2)[2]) for line in lines if "[AI TX -> DUT RX]" in line)
    dut_text = "".join(json.loads(line.split("] ", 2)[2]) for line in lines if "[DUT TX -> HOST RX]" in line)
    assert ai_text == sent
    assert dut_text == received.decode("utf-8", "backslashreplace")
