"""Bounded Standard Webhooks delivery with DNS-rebinding-resistant HTTPS.

Never follow redirects, use proxy environment variables, log callback URLs, or
connect to an address other than the public address validated for this attempt.
"""

import base64
import hashlib
import hmac
import http.client
import ipaddress
import json
import secrets
import socket
import ssl
import time
from urllib.parse import urlsplit

MAX_BODY = 256 * 1024


class WebhookError(Exception):
    """A deliberately non-sensitive delivery failure."""


def secret_bytes(secret: str) -> bytes:
    if not isinstance(secret, str) or not secret.startswith("whsec_"):
        raise ValueError("Signing secret must start with whsec_")
    try:
        key = base64.b64decode(secret[6:], validate=True)
    except (ValueError, UnicodeError) as exc:
        raise ValueError("Signing secret must contain valid base64") from exc
    if not 24 <= len(key) <= 64:
        raise ValueError("Signing secret must decode to 24-64 bytes")
    return key


def callback_parts(url: str):
    if not isinstance(url, str) or len(url) > 4096:
        raise ValueError("Invalid callback URL")
    if any(ord(c) < 33 or ord(c) > 126 for c in url):
        raise ValueError("Callback URL must be an ASCII HTTPS URL")
    try:
        parts = urlsplit(url)
        if (
            parts.scheme != "https"
            or not parts.hostname
            or parts.username is not None
            or parts.password is not None
            or parts.fragment
            or parts.port not in (None, 443)
            or "%" in parts.hostname
            or "\\" in url
        ):
            raise ValueError("Only public HTTPS callbacks on port 443 are supported")
    except ValueError as exc:
        raise ValueError("Invalid HTTPS callback URL") from exc
    return parts


def public_address(host: str) -> str:
    try:
        answers = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
    except OSError as exc:
        raise WebhookError("dns_failed") from exc
    addresses = []
    for answer in answers:
        ip = ipaddress.ip_address(answer[4][0])
        # Reject transition mechanisms as well as private, loopback, link-local,
        # multicast, reserved and unspecified destinations. Reject mixed DNS.
        if (
            not ip.is_global
            or ip.is_multicast
            or ip.is_reserved
            or (
                isinstance(ip, ipaddress.IPv6Address)
                and (
                    ip.ipv4_mapped is not None
                    or ip.sixtofour is not None
                    or ip.teredo is not None
                    or ip in ipaddress.ip_network("64:ff9b::/96")
                )
            )
        ):
            raise WebhookError("non_public_callback")
        addresses.append(str(ip))
    if not addresses:
        raise WebhookError("dns_failed")
    return addresses[0]


class _PinnedHTTPS(http.client.HTTPSConnection):
    def __init__(self, hostname: str, address: str):
        super().__init__(hostname, timeout=10, context=ssl.create_default_context())
        self.address = address

    def connect(self) -> None:
        sock = socket.create_connection((self.address, 443), timeout=self.timeout)
        try:
            self.sock = self._context.wrap_socket(sock, server_hostname=self.host)
        except BaseException:
            sock.close()
            raise


def signed_headers(
    body: bytes, event_id: str, subscription_id: str, keys: list[str], now: float
) -> dict[str, str]:
    timestamp = str(int(now))
    message = event_id.encode() + b"." + timestamp.encode() + b"." + body
    signatures = [
        "v1,"
        + base64.b64encode(
            hmac.new(secret_bytes(key), message, hashlib.sha256).digest()
        ).decode()
        for key in keys
    ]
    return {
        "Content-Type": "application/json",
        "webhook-id": event_id,
        "webhook-timestamp": timestamp,
        "webhook-signature": " ".join(signatures),
        "X-MCP-Subscription-Id": subscription_id,
    }


class WebhookSender:
    def post(self, url: str, body: bytes, headers: dict[str, str]) -> tuple[int, bytes]:
        if len(body) > MAX_BODY:
            raise WebhookError("payload_too_large")
        parts = callback_parts(url)
        connection = _PinnedHTTPS(parts.hostname, public_address(parts.hostname))
        try:
            path = parts.path or "/"
            if parts.query:
                path += "?" + parts.query
            connection.request("POST", path, body=body, headers=headers)
            response = connection.getresponse()
            # Verification responses are tiny; never read unbounded remote data.
            return response.status, response.read(8193)
        except (OSError, http.client.HTTPException) as exc:
            raise WebhookError("connection_failed") from exc
        finally:
            connection.close()

    def verify(self, url: str, key: str, subscription_id: str) -> None:
        challenge = secrets.token_urlsafe(32)
        body = json.dumps(
            {"type": "verification", "challenge": challenge}, separators=(",", ":")
        ).encode()
        headers = signed_headers(
            body, "verify_" + secrets.token_hex(16), subscription_id, [key], time.time()
        )
        status, response = self.post(url, body, headers)
        try:
            echoed = json.loads(response).get("challenge")
        except (ValueError, AttributeError, UnicodeError):
            echoed = None
        if (
            not 200 <= status < 300
            or not isinstance(echoed, str)
            or not hmac.compare_digest(echoed.encode(), challenge.encode())
        ):
            raise WebhookError("challenge_failed")
