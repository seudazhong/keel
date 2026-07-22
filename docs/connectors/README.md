# Connector providers

Keel's connector subsystem owns external-account lifecycle, selected resources, provenance,
taint, triggers, and effects. Connectors enter the Agent loop through the same Tool contract as
other capabilities; they do not create a second execution path.

## Shared contract

Provider modules expose a manifest and factory. The shared runtime supplies:

- encrypted/versioned credentials;
- setup, browser callback, health, sync, renewal, revoke, local-forget, and purge;
- selected provider resources and Knowledge/trigger targets;
- routed authenticated webhooks and replay protection;
- recurring durable jobs across Agent scopes;
- connector action schemas, provenance, taint, approval, and the durable Effect ledger
  (idempotent at-most-once execution + ambiguous-outcome reconciliation, see below).

The current schema permits one binding per provider per Agent scope. The target Connection model
supports multiple accounts and reusable resource grants.

## Current provider implementations

| Provider | Reference | Main capability |
|---|---|---|
| Gmail | Compatibility API plus generic catalog | Mail read and approval-gated send. |
| Google Calendar | [Google Calendar](./google-calendar.md) | Calendar read/sync and approval-gated create/update. |
| Google Drive/Docs | [Google Drive/Docs](./google-drive-docs.md) | Selected content into Knowledge. |
| Microsoft 365 | [Microsoft 365](./microsoft-365.md) | Read-only Outlook Mail and Calendar. |
| Notion | [Notion](./notion.md) | Read-only pages/data sources into Knowledge. |
| Feishu | [Feishu](./feishu.md) | Docs/Drive/Wiki Knowledge sync plus IM events/replies. |
| GitHub collaboration | [GitHub](./github.md) | Issues, pull requests, comments, and status; separate from managed Projects. |
| RSS and Atom | [Feeds](./feeds.md) | Safe recurring feed ingestion. |
| Generic webhook | [Webhook](./webhook.md) | Signed JSON trigger events. |

Provider implementation and tests do not automatically make a product-supported Connection.
Product qualification requires common lifecycle, scope, refresh/revoke, webhook, effect,
reconciliation, provenance, health, and UI acceptance.

Gmail and Google Calendar are the initial personal-Agent product candidates. Other providers remain
available for preview/qualification.

## Effects and reconciliation (R1B, invariants C4/C5)

Every outbound connector action with an idempotency key is reserved, executed, and confirmed
through the generic durable **Effect** ledger (`keel_core.effects`/`keel_core.effect_store`,
migration `0026_effect_ledger`) instead of a per-provider ad hoc claim: `reserved -> executing ->
confirmed`, with `executing` also able to land on `unknown` (a possible provider success followed
by response loss — never an ordinary failure, never a deleted claim) or `failed` (an ordinary,
provably pre-send failure). An `unknown` Effect blocks every further retry until provider
reconciliation proves `reconciled_confirmed` or `reconciled_absent` (which then permits exactly
one controlled retry).

A provider action signals ambiguity by raising `keel_core.connectors.ProviderAmbiguousError` —
never inferred from a generic exception — and may optionally implement
`ConnectorProvider.build_reconciler` to let the worker's reconciliation cron
(`keel_worker.effects_reconciliation`) prove confirmed/absent for an `unknown` Effect:

| Provider | Reconciliation identity | Capability |
|---|---|---|
| Gmail (`email_send`) | A deterministic RFC 5322 `Message-ID` derived from `(scope_id, idempotency_key)`, embedded in every send attempt; reconciled by a bare-value Gmail `rfc822msgid:` search. Search can prove existence, but not-found never proves permanent absence, so it does not unlock retry. | Confirm-only |
| Google Calendar (create/update) | A deterministic event id / `request_id` marker derived from `(scope_id, calendar_id, idempotency_key)` (or `event_id:idempotency_key` for update), already used by the action itself for idempotent retry; reconciled by a direct `get_event` lookup + marker check. | Implemented |
| Every other current provider | — | **Not yet implemented** — an `unknown` Effect from these providers stays `unknown` and is surfaced via `/v1/effects` for an operator/user decision; it is never guessed at or silently retried. |

The API (`/v1/effects`, `keel_server.api.effects`) and the typed SDK
(`KeelClient.list_effects`/`get_effect`/`reconcile_effect`/`check_effect_retry_eligibility`) expose
this ledger for read and authorized manual reconciliation/retry-eligibility requests. See
[`docs/INVARIANTS.md`](../INVARIANTS.md) C4/C5 for the acceptance statement.

## Webhook routing

Cloud deliveries use a high-entropy routed URL:

```text
/v1/connectors/{connector_id}/r/{route_token}/webhook
```

The route token selects the bound Agent scope; provider-specific authentication still authorizes the
delivery. Unknown, revoked, or cross-provider routes fail closed without scope disclosure.

The tokenless webhook path is local-preview compatibility only and fails closed in cloud mode.

## Network and content safety

URL providers use a single-resolution, IP-pinned HTTP client with redirect revalidation and
loopback/private/link-local/reserved blocking.

External content remains tainted with connector, binding, external resource, URL, revision, and
delivery provenance. An outbound effect influenced by tainted content cannot silently bypass the
approval policy.

## Operational escape hatches

Normal disconnect attempts remote revoke first. When a provider or optional SDK is unavailable,
operators may explicitly choose local forget or local purge. These paths never claim remote revoke
succeeded.
