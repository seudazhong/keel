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
- connector action schemas, provenance, taint, approval, and idempotency.

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
