"""Cancellation regressions using a subprocess that never opens hardware."""

import asyncio
from contextlib import suppress
import os
import signal
import sys

import anyio
import pytest

import bootterm_mcp


@pytest.mark.asyncio
async def test_cancelled_startup_reaps_unregistered_child(tmp_path, monkeypatch):
    ready = tmp_path / "started"
    binary = tmp_path / "waiting-bt"
    # A stuck child exercises the terminate timeout and subsequent kill/reap;
    # ignoring SIGTERM makes a skipped cleanup observable, not timing-dependent.
    binary.write_text(
        f"#!{sys.executable}\n"
        "import signal, time\n"
        "from pathlib import Path\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        f"Path({str(ready)!r}).touch()\n"
        "time.sleep(60)\n"
    )
    binary.chmod(0o700)
    monkeypatch.setenv("BOOTTERM_BINARY", str(binary))
    monkeypatch.setattr(bootterm_mcp, "OP_TIMEOUT", 10)
    backend = bootterm_mcp.Backend()
    processes = []
    spawn = asyncio.create_subprocess_exec

    async def record_process(*args, **kwargs):
        process = await spawn(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", record_process)
    try:
        with anyio.CancelScope() as scope:
            async with anyio.create_task_group() as group:
                group.start_soon(backend.connect, "/dev/null", 115200)
                with anyio.fail_after(5):
                    while not ready.exists():
                        await anyio.sleep(0.01)
                scope.cancel()
        assert len(processes) == 1
        assert processes[0].returncode is not None, "Cancelled startup leaked bt"
        with pytest.raises(ProcessLookupError):
            os.kill(processes[0].pid, 0)
        assert not backend.sessions
    finally:
        # Keep this regression safe to run against the broken implementation.
        for process in processes:
            if process.returncode is None:
                with suppress(ProcessLookupError):
                    process.send_signal(signal.SIGKILL)
                await process.wait()
        await backend.close()
