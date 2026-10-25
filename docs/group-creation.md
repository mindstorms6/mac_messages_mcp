# New iMessage group creation

`tool_create_group_and_send` creates one new iMessage group from exact addresses
and sends its initial text. It is a privileged external side effect: the MCP
client must obtain authorization for the exact recipients and message before
calling it.

## Input contract

- `recipients`: 2-32 distinct phone numbers or plain email addresses. Phone
  numbers are normalized using `MAC_MESSAGES_REGION` or the Mac's configured
  region; E.164 is preferred. Names, `contact:N`, short codes, chat IDs, display
  names, and duplicate normalized addresses are rejected.
- `message`: non-whitespace text of at most 64,000 UTF-8 bytes.
- `request_id`: a new canonical lowercase UUID. It identifies the authorized
  group creation and initial send, not just an individual network attempt.

Before group creation, every address must have successful local iMessage
evidence. During the native operation, Messages must provide an enabled,
connected iMessage account and resolve every exact address back to the same
participant handle. All participants are resolved before the chat is created.
There is no SMS or RCS fallback.

On current macOS releases, the Messages scripting dictionary exposes chat
participants as the singular Apple-event class `pres`. Spelling `participants`
directly in a `make` properties record therefore tries to coerce the entire
recipient list to one participant and fails with AppleScript error `-1700`.
The native script creates the chat at the resolved iMessage account and uses
Messages' list-valued `ICpt` chat-participants property. Synthetic regression
tests cover six-recipient argument transport and durable duplicate suppression.

## Result and reconciliation

A verified `sent` or `delivered` result contains:

- `chat_id`: the new full Messages chat GUID.
- `message_id`: the outgoing Messages message GUID.
- `evidence`: local service, send/delivery flags, and database row identifiers.

The server records a database baseline and a SHA-256 digest of the message; it
does not store the message plaintext in its durable ledger. Reconciliation
requires one new style-43 group whose exact normalized participant set and
outgoing message digest match the request. Multiple matches return `ambiguous`
and do not choose one.

The UUID claim is committed before AppleScript runs. Reusing the same UUID,
including after a process restart, only reconciles local evidence and never
dispatches again. A timeout or native error may have happened after the group
or message was created, so `unverified` and `ambiguous` are always
`retry_safe=false`. Call `tool_get_group_creation_status` or reuse the same UUID;
never retry with a new UUID. A different UUID always means a separately
authorized new group and message.

## Permissions and enablement

The MCP server host needs:

- Messages configured with an enabled, connected iMessage account;
- Automation permission to control Messages;
- Full Disk Access to read `~/Library/Messages/chat.db` for validation and exact
  result reconciliation; and
- an owner-only writable Application Support directory for the durable group
  outbox ledger.

The test suite uses synthetic SQLite fixtures and mocked native calls. It does
not create chats or send messages.
