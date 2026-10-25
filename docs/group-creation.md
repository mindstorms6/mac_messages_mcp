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

On current macOS releases, the Messages scripting dictionary declares chat
participants as a to-many element rather than a property. Spelling
`participants` in a `make` properties record compiles as the singular
Apple-event class `pres`, so Messages tries to coerce the whole list to one
participant and fails with `-1700`. Supplying raw `ICpt` in that record also
fails because an element is not a member of the chat properties record. The
native script instead passes the resolved participant list through the create
command's separate contents channel: `with data groupParticipants`. Synthetic
regression tests cover six-recipient argument transport, both historical
pre-creation errors, and durable duplicate suppression.

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

Two historical failures are deterministically pre-creation: coercing the list
of `participant id` objects to singular `participant`, and coercing a record
whose raw `ICpt` key is not a chat property to `properties of chat`. Both
`-1700` errors are raised on the script's only `make new chat` statement, so no
chat exists and the following `send` is unreachable. Status reconciliation
reports either exact signature as `failed_before_creation`, with
`side_effects=no_chat_or_message_created` and `replacement_request_safe=true`.
The original UUID still remains permanently closed and `retry_safe=false`.
After fresh authorization, the safe recovery is one replacement call with the
same recipients/message and a new UUID. Status lookup never performs that
replacement automatically.

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
