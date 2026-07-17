# Connector provider foundation

Built-in connectors live under `keel_core.connector_providers`. A provider contribution adds
one module or package exporting:

- `manifest`: a `ConnectorManifest` with a unique lowercase id, capabilities, generic setup /
  callback fields, typed Knowledge or trigger targets, and action schemas;
- `factory()`: a zero-argument factory returning a `ConnectorProvider`.
- optional import-safe `enabled()` / `availability()` probes. Optional SDKs must be imported by
  the probe, factory, or operation, never while the manifest module is discovered.

`keel_core.connector_registry` discovers manifests without constructing providers, isolates
provider-local dependency failures, and constructs providers only when an operation or action
needs one. Provider additions therefore do not edit the API/auth dispatcher, digest runtime,
worker function list, or React catalog.

## Shared runtime

- Contracts: `connector_contracts.py`
- Encrypted, versioned credential envelopes: `connector_credentials.py` (stored in the
  existing `connector_tokens` table)
- Scope-bound repository: `connector_repository.py`
- Single-resolution, IP-pinned HTTP transport (original Host and TLS SNI retained):
  `connector_network.py`
- Setup/sync/ingress/health/revoke orchestration: `connector_service.py`
- Generic API: `keel_server.api.connectors`
- Generic durable sync job: `keel_worker.connectors`

Migration `0015_connector_foundation` adds bindings, typed binding targets, user-selected root
resources, provider-synced item mappings, per-resource cursors, and delivery replay rows. All are
scope-bound with RLS and
`FORCE ROW LEVEL SECURITY`; metadata/config reject plaintext secret keys. Delivery rows store
only ids, hashes, status, and bounded error summaries, not webhook payloads.

Setup completion may return typed instructions, URLs, and one-time secrets. Secrets are never
stored in connector metadata or React state. Knowledge item mappings carry both the destination
base and document id so disconnect-and-purge can tombstone every imported document before
connector state is removed.

External content returned as `ConnectorChange`/`ConnectorEvent` must remain tainted and carry
connector, binding, external resource, URL, and revision/event provenance. Webhooks must verify
provider signatures before claiming the shared delivery replay key. Outbound actions continue
through permission/approval and `connector_outbox`.

Gmail is the compatibility proof: its callback schema and inbox/send action factories are
provider-local while existing encrypted credentials, reads, approved sends, status, and revoke
contracts remain available at `/v1/connectors/gmail/...`.
