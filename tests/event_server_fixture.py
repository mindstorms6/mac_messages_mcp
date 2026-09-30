"""Subprocess fixture: real MCP stdio, synthetic Messages DB, fake HTTPS peer.

Only tests launch this module. The production server has no test URL bypass.
"""

import json
import os

from mac_messages_mcp import events
from mac_messages_mcp.event_webhooks import WebhookSender


class RecordingPeer(WebhookSender):
    def post(self, url, body, headers):
        data = json.loads(body)
        with open(os.environ["TEST_DELIVERIES"], "a") as stream:
            stream.write(json.dumps({"body": data, "headers": headers}) + "\n")
        if data.get("type") == "verification":
            return 200, json.dumps({"challenge": data["challenge"]}).encode()
        return 204, b""


events.WebhookSender = RecordingPeer

if __name__ == "__main__":
    from mac_messages_mcp.server import run_server

    run_server()
