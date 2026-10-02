# Native attachment sending

`tool_send_attachment` sends one file over iMessage to one exact E.164 phone
number or plain email address. The MCP client must obtain authorization for the
recipient and file before calling it. Names, contact selectors and groups are
not supported by this tool. Existing text tools and event subscriptions remain
unchanged.

Required arguments: `recipient`, `request_id` (canonical lowercase UUID), and
`filename` (a basename including its extension, at most 180 UTF-8 bytes).
Supply exactly one file source:

- `content_base64`: strict base64, no data URL, 1–3,000,000 decoded bytes. This is
  the portable choice for connected clients and files from a client library.
- `file_path`: absolute path **on the MCP server's Mac**, 1–20,000,000 bytes. The
  server opens a regular file without following a leaf symlink, rejects special
  files, and copies bounded bytes before sending. It does not fetch URLs or
  interpret a path on another computer. Parent directories may be symlinks (for
  example `/tmp` on macOS); the final file itself cannot be a symlink.

The native Messages `send` command accepts a file, as documented by the installed
Messages scripting dictionary. The backend passes recipient/path as arguments,
never interpolates them into script code, and uses no UI controls, accessibility,
application activation, keystrokes, network endpoint workaround, or SMS fallback.
Existing Automation permission and an enabled iMessage account are required.
Apple documents the file-reference representation in its
[automation guide](https://developer.apple.com/library/archive/documentation/LanguagesUtilities/Conceptual/MacAutomationScriptingGuide/ReferenceFilesandFolders.html).

The materialized copy is mode 0600 under the mode 0700 directory
`~/Library/Application Support/mac-messages-mcp/outbox` (override:
`MAC_MESSAGES_OUTBOX_DIR`). Its sent filename is `REQUEST_UUID-original-name.ext`.
The prefix allows exact message correlation even after Messages copies the file.
Files are retained because ingestion may be asynchronous. Operators may remove
an individual staged file after reconciling completion, but must retain
`requests.sqlite3` to preserve duplicate suppression. No automatic cleanup
removes request records.

A durable SQLite claim is committed before the single native send attempt.
Concurrent/repeated calls with the same UUID and content cannot dispatch twice.
Reusing a UUID with different content or recipient fails. After any timeout or
transport failure, call `tool_get_attachment_send_status(request_id=...)`.
Do not automatically choose a new UUID: it represents a new send. A crash between
the durable claim and dispatch intentionally leaves an uncertain record that
requires reconciliation. This favors avoiding duplicates over automatic retry.

Results contain `dispatch` (`accepted` or `uncertain`), SHA-256, byte length,
filename, recipient and `status`. Status is `unverified`, `pending`, `sent`,
`delivered`, `failed`, or `ambiguous`. `not_found` means no known request record,
not permission to resend. Exact evidence uses the new outgoing message's ROWID,
the request filename and byte length, and the exact recipient in a direct chat.
It never uses an unrelated latest-message fallback. Evidence includes
message/attachment ROWIDs and GUIDs, bounded native error detail, transfer state, sent and
delivered flags, and raw Apple-epoch delivery/read timestamps where available.
When the outgoing local file is readable, its SHA-256 and `local_bytes_match`
verify it against the submitted bytes. A service delivery flag does not prove which device downloaded a file or that a
person opened it. All returned strings are untrusted data.

The tool sends the file only. A caption needs a separate, explicitly authorized
`tool_send_message` call after the file's outcome has been reconciled. There is
no atomic file-and-caption operation in this implementation.
