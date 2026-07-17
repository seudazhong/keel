# Connector provider foundation

Built-in connectors live under `keel_core.connector_providers`. A provider contribution adds
one module or package exporting:

- `manifest`: a `ConnectorManifest` with a unique lowercase id, auth kind, capabilities,
  scopes, and generic setup fields;
- `factory()`: a zero-argument factory returning a `ConnectorProvider`.

`keel_core.connector_registry` discovers that namespace, validates ids/factories, and sorts by
connector id. Provider additions therefore do not edit the API catalog, OAuth dispatcher,
worker function list, React catalog, or Alembic.

## Shared runtime

- Contracts: `connector_contracts.py`
- Encrypted, versioned credential envelopes: `connector_credentials.py` (stored in the
  existing `connector_tokens` table)
- Scope-bound repository: `connector_repository.py`
- SSRF/redirect/DNS/timeout/size/rate-limit bounds: `connector_network.py`
- Setup/sync/ingress/health/revoke orchestration: `connector_service.py`
- Generic API: `keel_server.api.connectors`
- Generic durable sync job: `keel_worker.connectors`

Migration `0015_connector_foundation` adds `connector_bindings`, `connector_resources`,
`connector_cursors`, and `connector_deliveries`. All four are scope-bound with RLS and
`FORCE ROW LEVEL SECURITY`; metadata/config reject plaintext secret keys. Delivery rows store
only ids, hashes, status, and bounded error summaries, not webhook payloads.

External content returned as `ConnectorChange`/`ConnectorEvent` must remain tainted and carry
connector, binding, external resource, URL, and revision/event provenance. Webhooks must verify
provider signatures before claiming the shared delivery replay key. Outbound actions continue
through permission/approval and `connector_outbox`.

Gmail is the compatibility provider. Existing OAuth routes, encrypted credentials, inbox reads,
approved sends, status, and revoke contracts remain available at `/v1/connectors/gmail/...`.
