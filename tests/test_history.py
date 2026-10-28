"""History remains bounded for both line-oriented output and binary-like streams."""

import pytest

from bootterm_mcp import History, MAX_LINE_CHARS, MAX_LINES


@pytest.mark.parametrize("terminated", [False, True], ids=["unterminated-stream", "many-lines"])
def test_history_bounds_preserve_latest_prompt(terminated):
    history = History()
    chunk = "x" * MAX_LINE_CHARS
    for index in range(MAX_LINES + 17):
        history.feed(f"line {index}\n" if terminated else chunk)
    history.feed("latest login: ")

    assert len(history.lines) <= MAX_LINES
    assert all(len(line.text) <= MAX_LINE_CHARS for line in history.lines)
    assert sum(len(line.text) for line in history.lines) <= MAX_LINES * MAX_LINE_CHARS
    assert history.lines[0].number > 1
    assert history.render(1).endswith("latest login: ")
    assert "line 0\n" not in history.render(MAX_LINES)
    assert len(history.render(3).splitlines()) == 3
