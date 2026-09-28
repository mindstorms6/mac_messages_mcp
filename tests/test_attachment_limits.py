"""Transport-sized results and failure isolation, without private attachments."""

import asyncio
import json
import subprocess
from unittest.mock import patch

from mac_messages_mcp.messages import get_attachment, _heic_to_png_bytes
from mac_messages_mcp.server import mcp
from tests.test_attachments import make_attachment_row


def test_converted_bytes_rechecked_against_requested_limit(tmp_path):
    path = tmp_path / "tiny.heic"
    path.write_bytes(b"heic")
    with (
        patch(
            "mac_messages_mcp.messages.query_messages_db",
            return_value=[make_attachment_row(filename=str(path))],
        ),
        patch(
            "mac_messages_mcp.messages._heic_to_png_bytes", return_value=b"png" * 100
        ),
    ):
        result = get_attachment(1, max_bytes=100)
    assert isinstance(result, str) and "Converted image exceeds" in result


def test_large_requested_limit_cannot_bypass_transport_limit(tmp_path):
    path = tmp_path / "large.jpg"
    path.write_bytes(b"x" * 3_000_001)
    with patch(
        "mac_messages_mcp.messages.query_messages_db",
        return_value=[make_attachment_row(filename=str(path), mime_type="image/jpeg")],
    ):
        result = get_attachment(1, max_bytes=80_000_000)
    assert isinstance(result, str) and "inline render skipped" in result


def test_worker_timeout_and_crash_are_local_errors():
    with patch(
        "mac_messages_mcp.messages.subprocess.run",
        side_effect=subprocess.TimeoutExpired("worker", 20),
    ):
        assert _heic_to_png_bytes(b"bad") is None
    with patch(
        "mac_messages_mcp.messages.subprocess.run",
        return_value=subprocess.CompletedProcess([], -11, b"", b""),
    ):
        assert _heic_to_png_bytes(b"bad") is None


def test_actual_mcp_serialization_stays_under_tunnel_limit(tmp_path):
    path = tmp_path / "image.png"
    path.write_bytes(b"x" * 3_000_000)
    with patch(
        "mac_messages_mcp.messages.query_messages_db",
        return_value=[make_attachment_row(filename=str(path), mime_type="image/png")],
    ):
        result = asyncio.run(
            mcp.call_tool(
                "tool_get_attachment", {"attachment_id": 1, "max_bytes": 8_000_000}
            )
        )

    def serialize(obj):
        if hasattr(obj, "model_dump"):
            return obj.model_dump(mode="json")
        raise TypeError(type(obj))

    wire = json.dumps(result, default=serialize).encode()
    assert len(wire) < 10_000_000
    assert b'"type": "image"' in wire
