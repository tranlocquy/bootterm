# Shared embedded console over MCP

The optional `bootterm_mcp.py` server lets a person and an AI debug the same
embedded console on Linux or macOS. One `bt` process owns each serial port. MCP
tools and any attached human terminals share that process, its input, and its
received output. There is no GUI, TCP listener, or xtell hub requirement.

The server runs over stdio under your MCP client. It creates a private local Unix
socket for terminal viewers. Tool discovery does not open serial ports or need
the `bt` binary; ports are opened on demand. Closing the MCP server stops its
serial processes, timed sends and captures, and disconnects its terminal viewers.

## Build and configure

Build this checkout's `bt`, then install the optional Python dependencies.
Python 3.10 or newer is required; on macOS the system Python may be older.

```sh
make
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-mcp.txt
```

Register this stdio server in your MCP client's configuration. Replace both
`/absolute/path/bootterm` paths with your checkout location; use absolute paths
because clients may launch servers from another working directory.

```json
{
  "mcpServers": {
    "bootterm": {
      "command": "/absolute/path/bootterm/.venv/bin/python",
      "args": ["/absolute/path/bootterm/bootterm_mcp.py"]
    }
  }
}
```

The adapter finds `bin/bt` beside the script, then searches `PATH`. Set
`BOOTTERM_BINARY` to select another binary built with this checkout's `-R` option.
A previously installed upstream `bt` lacks that option.

## Debug together

1. Start the configured server in your MCP client. Ask the AI to call
   `list_serial_ports`, then `connect_serial` with a port and baud rate.
2. In your terminal, attach to that port:

   ```sh
   /absolute/path/bootterm/.venv/bin/python /absolute/path/bootterm/bootterm_mcp.py \
     --console /dev/ttyUSB0 --baudrate 115200
   ```

   On macOS, use the listed `/dev/cu.*` device, for example
   `/dev/cu.usbserial-0001`. A terminal can also open the first connection while
   the MCP server is running. Attaching to an existing connection uses its
   current baud rate, regardless of the CLI default.
3. The terminal shows the last 50 received lines, followed by live serial bytes.
   It also displays `[AI TX -> DUT RX]` notices for MCP commands, even when the
   device does not echo them. These notices stay in your local terminal and
   are never sent to the DUT. Type commands as usual. The AI can read what you
   type using `read_sent_data` (labelled `HUMAN`), read the same DUT output with
   `read_received_data`, and send commands with `send_serial_data`. Consecutive
   human keystrokes are combined into command lines, including unfinished input.
4. Press **Ctrl-]** to detach your terminal. The serial session and AI access
   continue. `disconnect_serial` closes the port for everyone.

For a byte-only live receive stream, pass `--raw-output`. This disables local
AI TX notices and the formatted history replay; source-labelled file logging
and the MCP's access to human input still work.

Example tool calls:

```text
connect_serial(port="/dev/ttyUSB0", baudrate=115200)
send_serial_data(port="/dev/ttyUSB0", data="help\r\n")
read_received_data(port="/dev/ttyUSB0", lines_count=100)
set_filter(port="/dev/ttyUSB0", keyword="ERROR")
read_filtered_data(port="/dev/ttyUSB0")
start_saving_file(port="/dev/ttyUSB0", filepath="/tmp/board-boot.bin")
```

## Log traffic with source and direction

Add `--log` to the human console command to start a shared annotated transcript:

```sh
/absolute/path/bootterm/.venv/bin/python /absolute/path/bootterm/bootterm_mcp.py \
  --console /dev/ttyUSB0 --baudrate 115200 --log /tmp/board-debug.log
```

The log includes traffic from **both** the human terminal and the AI, plus output
from the device under test (DUT). For example:

```text
[2026-10-28T04:30:00.000+00:00] [HUMAN TX -> DUT RX] "help\r\n"
[2026-10-28T04:30:00.010+00:00] [DUT TX -> HOST RX] "Available commands...\r\n"
[2026-10-28T04:30:01.000+00:00] [AI TX -> DUT RX] "status\r\n"
[2026-10-28T04:30:01.020+00:00] [DUT TX -> HOST RX] "status: OK\r\n"
```

Timestamps are UTC. TX means transmit, RX means receive, and HOST is the shared
console. Control characters are escaped to keep one event per log line. Timed
sends initiated through MCP are labelled AI. A DUT echo is still DUT output;
the log records the actual source of each event.

The AI can start the same transcript using:

```text
start_saving_file(port="/dev/ttyUSB0", filepath="/tmp/board-debug.log", format="annotated")
```

Choose `--log-format jsonl` (or tool `format="jsonl"`) for structured events.
Each event includes `timestamp`, `source` (`ai`, `human`, or `dut`), `direction`
(`tx`/`rx` from the host's perspective), explicit source/destination directions,
decoded `text`, and `data_base64` preserving the exact bytes. Receive events
follow stream read boundaries, so a response or UTF-8 character may span events;
use the base64 bytes to reconstruct the original stream.

Choose `raw` for received bytes only. The terminal's `--log` defaults to
`annotated`; the MCP tool defaults to `raw` for compatibility. TX events record
bytes queued to BootTerm, not firmware acknowledgements. All formats append to
the named file. Use a separate file for each format. A port has one active log;
starting another replaces its destination. Logging continues when a human
terminal detaches, until `stop_saving_file`, serial disconnect, or server exit.

Text is sent as UTF-8 exactly as supplied; include `\r`, `\n`, or `\r\n` when
needed. `hex_mode=true` accepts hexadecimal bytes, including control characters.
Ctrl-] is the human terminal's detach key; send that byte through the MCP tool if
the device needs it. Individual writes are serialized, but a human and AI can
still interrupt each other's command sequences. Coordinate who is typing.

An attached console uses the shared process, so do not also run ordinary `bt`,
`screen`, or another serial application against the same device. BootTerm asks
the OS for exclusive access where available; that cannot evict an already-open
reader. Reads from independent serial processes can steal bytes from each other.

## Tools

| Tools | Behavior |
| --- | --- |
| `list_serial_ports` | Uses BootTerm's native port enumeration. |
| `connect_serial`, `disconnect_serial` | Open or close a shared port. Reconnecting the same path and baud is idempotent. Disconnect before changing baud. |
| `send_serial_data` | Send UTF-8 text or hex bytes, at most 64 KiB per call. No newline is added. |
| `start_auto_send`, `stop_auto_send` | Send immediately, then repeat at an interval of 10 ms to 24 hours until stopped or disconnected. |
| `set_filter`, `read_filtered_data` | Search retained received lines by literal substring or regular expression. `enable=false` disables the filter. Regex execution has a time limit. |
| `start_saving_file`, `stop_saving_file` | Append `raw` received bytes, an `annotated` AI/human/DUT transcript, or structured `jsonl` traffic to an absolute path. The parent directory must exist. Logging begins when requested. |
| `read_received_data`, `read_sent_data` | Read the latest timestamped history, including prompts without newlines. Sent lines identify HUMAN or AI; typed human fragments are combined. Optional inclusive `start_time`/`end_time` use local server time: `yyyy-mm-dd HH:MM:SS.mmm`. |
| `clear_data_buffers` | Clear `main`, `filter`, `history` (sent), or `all`. Clearing `filter` hides earlier received lines from that view; it does not erase the main history. |
| `get_buffer_status` | Report connection state, PID, baud, viewer count, buffer use, capture path/format, auto-send state, and errors. |

Each received/sent history retains up to 2,000 lines of 4,096 characters each.
Long lines are split to keep memory bounded. UTF-8 is decoded across read chunks;
invalid bytes are displayed as `\xNN`. Raw captures and `--raw-output` retain
the original bytes. The default terminal adds local AI TX notices. History reads
are snapshots, not consuming reads. A send success
means bytes were queued to BootTerm; it is not an acknowledgement from firmware.

If a device disappears, its history remains readable until disconnect or
reconnect. `connect_serial` opens a fresh session and clears old history. Commands
are never automatically replayed after reconnect. Capture and timed-send errors
are available in `get_buffer_status`.

## Socket and lifecycle

The default console socket is `~/.cache/bootterm-mcp/console.sock`, with mode
`0600`; the default directory is created with mode `0700`. Only local processes
running as your user can attach. The socket also allows sending serial input.

Set `BOOTTERM_MCP_SOCKET` in the MCP client's server environment and the terminal,
or pass `--socket /absolute/path/console.sock` to both invocations, to use a
separate server. One server owns each socket path. Keep paths short enough for
your OS's Unix-socket limit (macOS commonly allows about 100 bytes).

An existing socket is never replaced automatically. If startup reports that the
address is in use, use the running server or choose another path. After an
unclean kill, first verify that the prior server has exited, then remove its
stale socket. Normal stdio closure, SIGINT and SIGTERM clean up the socket and
child processes. The terminal command requires a running MCP server; it does
not start a background service.

## Development

```sh
.venv/bin/python -m pip install -r requirements-test.txt
make test-mcp PYTHON=.venv/bin/python
```

Tests run real MCP clients and the compiled `bt` against pseudoterminals, with
no hardware required. CI runs them on Linux and macOS. The C-only `make` and
`make install` workflow does not require Python.

The Python MCP tool definitions and stdio setup were adapted from
[xtell-serial](https://github.com/xtell-cn/xtell-serial/blob/main/xtell_serial_mcp.py).
See `NOTICE` and `LICENSES/Apache-2.0.txt` for attribution and licensing.
