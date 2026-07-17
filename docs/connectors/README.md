# Connector provider foundation

Built-in connectors live under `keel_core.connector_providers`. A provider contribution adds
one module or package exporting:

- `manifest`: a `ConnectorManifest` with a unique lowercase id, capabilities, generic setup /
  callback fields, optional recurring sync and subscription-renewal cadence, typed Knowledge or
  trigger targets, and action schemas;
- `factory()`: a zero-argument factory returning a `ConnectorProvider`.
- optional import-safe `enabled()` / `availability()` probes. Optional SDKs must be imported by
  the probe, factory, or operation, never while the manifest module is discovered.

`keel_core.connector_registry` discovers manifests without constructing providers, isolates
provider-local dependency failures, and constructs providers only when an operation or action
needs one. Provider additions therefore do not edit the API/auth dispatcher, digest runtime,
worker function list, or React catalog.

## Shared runtime

- Contracts: `connector_contracts.py`
- Encrypted credential envelopes plus atomic credential versions: `connector_credentials.py`
  (stored in the existing `connector_tokens` table)
- Scope-bound repository: `connector_repository.py`
- Single-resolution, IP-pinned HTTP transport (original Host and TLS SNI retained):
  `connector_network.py`
- Setup/sync/ingress/health/revoke orchestration: `connector_service.py`
- Generic API: `keel_server.api.connectors`
- Generic durable sync/renew jobs and recurring reconciliation: `keel_worker.connectors`

Migration `0015_connector_foundation` adds bindings, generic next-sync/next-renewal state and fenced
schedule leases, typed binding targets, user-selected root resources, provider-synced item mappings,
per-resource cursors, and delivery replay rows. All are scope-bound with RLS and
`FORCE ROW LEVEL SECURITY`; metadata/config reject plaintext secret keys. Delivery rows store
only ids, hashes, status, and bounded error summaries, not webhook payloads.

Every stateful provider operation receives one `ConnectorOperationContext`. Setup receives the
public connector callback base URL. Browser auth start and completion receive the current binding
and decrypted credential, so providers can store app/client configuration as `configured`, move to
`authorizing`, and atomically finish credential/binding/status updates as `connected`. Ingress also
receives method, repeated query parameters, headers, raw body, and public URL before delivery claim.
Sync receives selected resources plus their state; renew and revoke receive the complete
binding/resource/target/item/cursor state needed to maintain or remove remote subscriptions. Sync
and renewal may return compare-and-set credential rotation and binding state through
`ConnectorStateUpdate`. Credential values are excluded from object reprs and are never stored in
metadata.

Resource discovery declares each response `authoritative` or `incremental`. Authoritative refresh
prunes missing resources and their item/cursor children; incremental refresh only upserts. A
successful downstream delete removes its item mapping so a later provider recreation creates a new
destination. Repository delete/prune methods remain scope- and binding-bound.

Setup completion may return typed instructions, URLs, and explicitly one-time secrets. Secrets are
never stored in connector metadata or React state. Knowledge item mappings carry both the
destination base and document id so disconnect-and-purge can tombstone every imported document
before connector state is removed.

External content returned as `ConnectorChange`/`ConnectorEvent` must remain tainted and carry
connector, binding, external resource, URL, and revision/event provenance. Webhooks may return a
bounded immediate provider response for verification challenges without creating a delivery.
Normal events must verify first, then return a stable delivery id and SHA-256 payload hash before
the shared replay claim. Provider response status/content type/body are preserved, while
hop-by-hop, cookie, framing, and CR/LF-injected response headers are rejected. Outbound actions
continue through permission/approval and `connector_outbox`. `ConnectorActionContext` supplies
scope-bound binding, selected-resource, target, item, and cursor readers; registry wrappers also
reject a tool invocation whose runtime scope differs from the action's configured scope.

The worker's single recurring reconciler claims bounded due bindings with expiring fencing tokens,
enqueues the same generic durable sync/renew jobs used elsewhere, advances cadence only after the
durable enqueue, and uses stable due-time idempotency keys so crash reclamation cannot duplicate a
job. Dispatch failures back off explicitly and update connector health. Provider enablement and
manifest cadence are consulted dynamically; configured, authorizing, revoked, and disabled
bindings never execute recurring provider work. RSS/Atom polling and expiring watch renewal remain
provider-local manifest/operation implementations.

URL/feed providers use the shared pinned transport. Every initial and redirected address is
validated once and pinned while retaining the original Host and TLS SNI. Private, loopback,
link-local, reserved, IPv4-mapped private, and RFC6598 `100.64.0.0/10` shared address space are
rejected. `ConnectorHttpClient.get()` remains the bytes-only convenience API. Providers that need
conditional polling use `get_response()` with `If-None-Match` / `If-Modified-Since` request headers
and an explicit accepted-status set such as `{200, 304}`; the immutable response includes the final
URL, status, case-insensitive duplicate-preserving headers, and body. Conditional headers survive
only same-origin redirects, and other caller-supplied request headers are rejected.

Normal disconnect remains fail-closed when remote revoke fails. If a provider module or optional
SDK cannot load, operators can explicitly use local forget (`DELETE .../{id}/local`) or forced
local purge (`DELETE .../{id}/purge/local`); these paths never pretend that remote revoke succeeded.

Gmail is the compatibility proof: its callback schema and inbox/send action factories are
provider-local while existing encrypted credentials, reads, approved sends, status, and revoke
contracts remain available at `/v1/connectors/gmail/...`.
