# Feishu

The built-in `feishu` provider combines two provider-local surfaces behind one manifest:

- **Workspace:** selected Docs, Wiki spaces, and Drive folders poll into a required Knowledge
  target.
- **IM:** selected chats admit direct messages and bot mentions as durable trigger events; the
  `feishu_reply` action replies only after tainted-content approval and requires an idempotency key.

Their boundaries remain distinct. Workspace sync ignores chat resources, IM ingress never imports
documents, and replies require the addressed chat to be selected in the current scope.

## Self-built app setup

Create and install a Feishu self-built app in exactly one tenant. Configure:

1. App ID and app secret.
2. The installed tenant key.
3. Event verification token and encryption key.
4. These app permissions:
   - `docx:document:readonly`
   - `drive:drive:readonly`
   - `wiki:wiki:readonly`
   - `im:chat:readonly`
   - `im:message:readonly`
   - `im:message:send_as_bot`
5. The setup result's event callback URL, encrypted event delivery, and
   `im.message.receive_v1`.

The app secret, verification token, encryption key, tenant token, and bot identity are stored only
inside the encrypted, versioned connector credential envelope. Setup verifies the tenant key,
permissions, and bot identity before connecting. Tenant tokens are rotated with credential
compare-and-set during sync and reply actions.

## Resources and targets

Refresh resources, then select individual Docs, Wiki spaces, Drive folders, and chats. Folder
polling walks child folders; Wiki and Drive pagination is bounded. Supported `doc` and `docx`
content is normalized to tainted text while preserving source URLs, external IDs, and revisions.
Updates and remote deletes become Knowledge upserts and tombstones. Calendar, Tasks, Base, and
unsupported Drive object types are excluded.

A Knowledge target is mandatory for recurring sync. Configure a trigger session or trigger routine
before admitting IM events. Group events require both a bot mention and a selected chat. Direct
messages are admitted only for the authorized tenant; outbound replies always require a selected
chat.

## Webhook security

The provider verifies the challenge token, validates the request timestamp window, checks the
Feishu SHA-256 signature in constant time, decrypts AES-256-CBC event envelopes, validates the event
tenant, and uses the event ID plus payload hash for the shared durable replay claim. Only text and
post direct messages or group bot mentions are normalized. Provenance includes tenant, chat, thread,
sender, message, event, and creation identifiers; all admitted content remains tainted.

## Health and disconnect

Health reports missing or invalid encrypted credentials, tenant mismatch, permission shrink,
expired tokens awaiting CAS rotation, uninstall/revoked authorization, and API failures.

Foundation `129920d` does not expose failed delivery state to `ConnectorOperationContext`, and
`ConnectorIngressResult` has no provider state update. Durable webhook-failure status therefore
cannot yet be reflected by the provider health endpoint without a central contract change. Webhook
requests still fail explicitly, and accepted delivery-processing failures remain recorded in the
shared delivery ledger.

A self-built app cannot uninstall or revoke its own tenant installation. Normal remote disconnect
therefore fails closed. Uninstall the app in Feishu, then choose one generic local path:

- `DELETE /v1/connectors/feishu/local` retains imported Knowledge.
- `DELETE /v1/connectors/feishu/purge/local` tombstones imported Knowledge and removes connector
  state and outbound idempotency rows.

After either local disconnect, recurring work has no binding or credentials and cannot resurrect
purged documents.
