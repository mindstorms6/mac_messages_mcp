# Messages and reaction events

This fork implements the webhook subset of [OpenAI MCP Events](https://developers.openai.com/plugins/build/mcp-events)
over MCP protocol `2026-07-28`, using the MCP Python SDK 2.x. Existing tools
continue to support older MCP clients. Events are protocol methods, not tools
that an agent needs to poll.

## Enable

Install **this fork/revision**, with its locked dependencies (not the upstream
PyPI package), using your normal package manager. For source development:

```sh
uv sync --frozen
MAC_MESSAGES_EVENTS=1 uv run --frozen mac-messages-mcp
```

The server speaks stdio; the command waits for an MCP client, not an interactive
shell prompt. Full Disk Access is required to read the local Messages database.
No subscriptions or outgoing webhooks are created merely by enabling events.

Optional environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `MAC_MESSAGES_EVENTS` | `0` | Set to `1` to enable discovery, subscriptions and the worker. |
| `MAC_MESSAGES_EVENTS_STATE_DIR` | `~/Library/Application Support/mac-messages-mcp/events` | Durable, private state directory; must be owned by the process user and mode `0700`. |
| `MAC_MESSAGES_EVENTS_INTERVAL` | `2` | Local database polling interval in seconds, from `0.25` through `60`. Delivery drains independently during bursts. |
| `MAC_MESSAGES_EVENTS_DB` | `~/Library/Messages/chat.db` | Optional database path, primarily for development/testing. Always opened read-only. |
| `MAC_MESSAGES_PROTOCOL_TRACE_FILE` | unset | Opt-in owner-only JSONL trace of bounded stdio protocol metadata for diagnosing discovery. Raw frames and application data are never logged. |

Use one long-lived event-enabled process per state directory. A lock prevents
competing workers. Short-lived stdio sessions cannot deliver while stopped:
run under a supervisor/tunnel that keeps the process alive for ongoing events.
Existing tool-only processes can remain event-disabled.

### Bounded protocol diagnostics

Set `MAC_MESSAGES_PROTOCOL_TRACE_FILE` only while diagnosing MCP negotiation.
The trace records a fixed allowlist: protocol method category, offered/returned
date-form protocol versions, known capability names, event catalog count, fixed
result type, and numeric error code. It never records
request IDs, arbitrary method/capability names, tool arguments, message data,
headers, authentication material, callback URLs, signing secrets, or raw
frames. The original request string and response object are forwarded unchanged.

The file and directory are owner-only (`0600`/`0700`). Rotation retains at most
the active 256 KiB file plus two backups. Disable tracing by unsetting the
variable and restarting the supervised process. After review, remove the trace
file and its `.1`/`.2` backups with the process stopped or tracing disabled.

### Plugin package

Root `plugin.json` and `mcp.json` provide a portable Agent Plugins wrapper. The
wrapper launches the **already-installed** `mac-messages-mcp` from `PATH` and
uses `${PLUGIN_DATA}/events` for persistent state. Install the matching fork
revision first; the plugin does not download an unpinned binary or modify any
Nix, Home Manager, Codex, launch-agent, or host configuration.

Import/register the package using your client's supported local-plugin workflow.
For ChatGPT web, configure an authenticated MCP 2.0-capable tunnel/connection
and register it with your plugin. Do not copy a personal connection ID into
this repository. Packaging alone does not make an older client or tunnel
forward `server/discover` and `events/*`.

The automated tests cover the stdio wire and webhook contract with synthetic
data; they do **not** assert a live ChatGPT installation or tunnel was deployed.

## Event catalog and subscriptions

- `message.created`: a newly observed normal message, including attachment-only
  messages, direct chats, group chats, incoming and outgoing messages.
- `reaction.added`: a newly observed added Tapback, custom emoji or sticker
  reaction; unfamiliar Apple reaction codes retain their raw code and use
  `kind: "unknown"`.
- `reaction.removed`: a newly observed reaction removal.

Subscribe to all three for any message/reaction activity. Reactions are not
also emitted as `message.created`. System/chat-management records, edits,
read receipts and arbitrary in-place database changes are not events.

`server/discover` advertises `capabilities.events: {}` only when enabled.
`events/list` returns the full catalog, filter schemas and payload schemas.
These methods use the same stdio connection as the tools. On MCP 2.0 requests,
include the protocol's `_meta` envelope, for example:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "events/subscribe",
  "params": {
    "_meta": {
      "io.modelcontextprotocol/protocolVersion": "2026-07-28",
      "io.modelcontextprotocol/clientCapabilities": {}
    },
    "name": "message.created",
    "arguments": {},
    "delivery": {
      "mode": "webhook",
      "url": "https://callback-provided-by-your-client.example/events",
      "secret": "whsec_BASE64_KEY_PROVIDED_BY_YOUR_CLIENT"
    },
    "cursor": null,
    "ttlMs": 86400000
  }
}
```

The example URL/key are placeholders. The subscribing client supplies the
actual callback and a `whsec_` base64 key decoding to 24–64 bytes.

Empty arguments match **all conversations and both directions**. Optional
`chat_guids` and `sender_addresses` are nonempty arrays of exact IDs (maximum
128); `direction` is `all`, `incoming` or `outgoing`. Different filter fields
are ANDed; entries within an array are ORed. Sender addresses are exact stored
Messages handle IDs, not fuzzy contact names or automatically inferred aliases.
Chat filters include any sender within a matching group. Outgoing rows often
have no sender address; prefer direction/chat filters for those.

The result contains `id`, `refreshBefore`, `cursor: null`, `truncated: false`.
Identity is deterministic over the local owner, URL, name and canonicalized
filters. Repeating a subscribe refreshes that subscription, not a duplicate.
TTL defaults to 24 hours and is capped at 7 days; `null` requests are granted
the finite 24-hour default. A shorter positive TTL is honored without rounding
it up. Refresh before the returned timestamp.

`events/unsubscribe` takes the same `name`, `arguments` and `delivery` with just
`mode` and `url`, without `secret`. It is idempotent and deletes queued
deliveries. It returns only after any already-started local send has finished;
the receiver may still be processing that request.

## Payload and privacy

Events carry stable `eventId`, `name`, the original Messages timestamp (UTC),
`cursor: null`, and `data["untrusted-mcp-output"]`. Data contains message ROWID
and GUID, associated chat IDs/GUIDs, exact sender address if known, direction,
attachment presence, and reaction action/kind/raw Apple code/target GUID/emoji.
Apple's target GUID may contain a part prefix such as `p:0/`; it is preserved.

No message bodies, transcripts, contact names, attachment contents or local
attachment paths are pushed. Use existing read/search tools to retrieve content
when needed. All third-party strings are neutralized and explicitly untrusted;
an event is data, never authorization to send a reply or perform another action.
To avoid feedback loops in an agent that sends replies, subscribe with
`direction: "incoming"` or explicitly ignore outgoing events.

Callback URLs and signing secrets are stored in the owner-only state database,
not in the Messages database. Logs/status never include them. Protect backups
of this directory as credentials; deleting subscriptions does not promise
forensic erasure of filesystem backups or freed SQLite pages.

## Delivery and lifecycle guarantees

- Before storing a subscription, send a signed, fresh challenge verification
  request. Require a 2xx response echoing `challenge`, compared in constant time.
  Failed verification returns JSON-RPC `-32015`. Every refresh is reverified.
- Sign exact JSON bytes using Standard Webhooks HMAC-SHA256 and
  `webhook-id`, `webhook-timestamp`, `webhook-signature`, and
  `X-MCP-Subscription-Id`. Refreshing with a new key signs with both keys for a
  five-minute overlap; the old key is then removed.
- Only public HTTPS port-443 callbacks are accepted. DNS is checked on each
  attempt; connections are pinned to the validated IP with the original TLS
  hostname/certificate validation. Private/mixed DNS, transition addresses,
  credentials in URLs and redirects are rejected. Proxy environment variables
  are not used. Requests are capped at 256 KiB and response reads at 8 KiB + 1.
- Messages observations and outbox insertion commit atomically with a durable
  high-water mark. Delayed chat associations are retried. Both subscriptions
  and pending deliveries survive process restarts. A second worker cannot use
  the same state directory simultaneously.
- Delivery uses **at-least-once retry semantics**, not exactly once or guaranteed
  receipt. A crash after the receiver accepts a request can cause a duplicate.
  Deduplicate by subscription ID plus `eventId`; do not assume arrival order.
  Retries retain the body/event ID and get fresh timestamps/signatures.
- Retry connection failures, 408, 429 and 5xx responses with jittered exponential
  backoff, up to eight attempts or 24 hours. Other responses are permanent
  failures, including 413. A 410 removes the subscription and its pending queue.
  Expiry and unsubscribe stop future sends and delete queued deliveries.
- Limits are 100 subscriptions, 10,000 queued deliveries and 1,000 pending chat
  associations. Full queues pause observation instead of dropping newly scanned
  rows; each scan stops before a message's fan-out would exceed capacity. Invalid
  source records (for example missing GUID or invalid timestamp), oversize
  events and exhausted/permanent delivery failures increment health counters.
- Initial startup and each new subscription begin at the current local message
  ROWID; no historical flood. There is no protocol replay API (`cursor` must be
  null). Active subscriptions can catch up after a restart while still valid,
  but expired subscriptions cannot recover missed events by refreshing.
- Observation means **newly inserted into this Mac's local database**, not
  necessarily newly sent: iCloud history/backfills can appear later. Rows
  inserted and removed between polls, messages never downloaded to this Mac,
  and in-place changes are not guaranteed to be observed. Database replacement
  or a ROWID rewind fails closed; stop the worker, select a fresh private state
  directory and resubscribe after reviewing the database change. This pauses
  events without taking ordinary Messages tools offline.
- On macOS the database identity uses its resolved path, inode and creation
  time, so a reboot changing the disk device number does not invalidate the
  checkpoint. Legacy checkpoints upgrade without changing event IDs. If their
  device number has already changed, automatic migration is limited to stores
  with no subscriptions, queued deliveries or pending observations; active
  legacy stores require review and resubscription.
- Database access may be unavailable early during login. Event initialization
  retries automatically while normal MCP tools remain available. The saved
  checkpoint and subscriptions are preserved, and event delivery resumes only
  after the source checks pass.

`tool_event_status` reports enabled state, subscription/queue/pending counts,
delivery/failure counters, and the last worker error type, without private
content. During initialization failures it reports `configured: true`,
`enabled: false`, `retrying: true` and `last_error`; a real replacement reports
`SourceChangedError` and still requires the review above. A healthy worker does not prove the receiver displayed a notification.

## Authorization boundary

This package runs as **one local Messages account owner over stdio**. Access to
that process is access to that owner's tools and event catalog. No client-sent
principal or `clientInfo` field can select another owner. HTTP embedding rejects
events rather than treating anonymous clients as the owner. Separate accounts
must have separate OS processes and private state directories.

A tunnel must authenticate its owner and forward MCP 2.0 without broadening
that boundary. When revoking remote access, unsubscribe first or stop/disable
the event process; revoking only inbound tunnel credentials cannot cancel an
already-authorized outbound subscription. Restart with a fresh state directory
if discarding all subscriptions is intended. Do not expose this stdio process
through an anonymous or multi-tenant proxy. A multi-user HTTP service would need
its own principal resolver and ongoing per-principal revocation checks.

## Development

```sh
uv sync --frozen --extra dev
uv run --frozen --extra dev pytest
```

Tests use synthetic SQLite databases and fake callback peers, covering actual
MCP stdio discovery/subscriptions, legacy tools, schema validation, signatures,
verification failures, DNS safety, filters, reactions, expiry, unsubscribe,
restarts, deduplication, secret rotation and bounded retries. They never send
a real message, mark a real conversation read or register a live subscription.
