# Google Drive / Docs connector

Connector id: `google_drive_docs`

This MVP imports selected Google Drive folders or files into an existing Knowledge Base. It is
read-only: Keel never edits, uploads, comments on, or otherwise writes to Drive.

## Authorization

The connector reuses the Google OAuth client configured by
`KEEL_GMAIL_CLIENT_SECRETS_PATH`. It is enabled independently when that client file exists and
stores a separate encrypted token under the `google_drive_docs` connector id. Its grant is
intentionally limited to:

```text
https://www.googleapis.com/auth/drive.readonly
```

Authorization disables incremental grants so Gmail scopes are not folded into the Drive token.
Refresh-token rotation is persisted with the connector foundation's credential version fence.
Authentication failures require reconnecting; quota/rate-limit failures are retryable and surfaced
through connector health.

## Resources and target

Resource discovery is authoritative and paginated. Users can select:

- Drive folders, recursively;
- Google Docs (`application/vnd.google-apps.document`);
- UTF-8 text (`text/plain`);
- Markdown (`text/markdown`).

PDF, Office, OCR, images, arbitrary `text/*`, and other binary formats are excluded. Sync requires
an explicit `knowledge` target through the shared binding-target model before provider work starts.

## Sync and provenance

Initial sync takes a Drive changes start token before crawling selected roots, then stores a
`drive_changes` cursor for each selected resource. Recurring sync paginates the Drive changes feed,
deduplicates file deliveries, and handles content/title updates, renames, moves into or out of a
selected root, and source deletion.

Expired/invalid Drive cursors trigger one controlled full resync of all selected resources. Full
resync compares the authoritative crawl with connector item mappings and emits deletes for content
that disappeared. Folder topology changes also use this path so descendant moves cannot leave stale
Knowledge content.

Google Docs are exported as `text/plain`; text and Markdown files retain their supported Knowledge
source type. Raw files use trustworthy Drive `size` metadata for an early rejection. All downloads,
including Google Docs exports where size is unavailable, use bounded 256 KiB streaming chunks and
abort as soon as the reported or accumulated content exceeds the existing 1 MiB Knowledge input
limit. Content is then normalized to UTF-8/LF and checked again after normalization.

Every upsert is tainted and includes:

- Drive file id (`external_resource_id`);
- source URL;
- Drive `version` and `modifiedTime` in the stable revision value;
- connector id and binding id;
- the connector resource/item mapping maintained by the foundation.

Knowledge creates, updates, versions, chunks, citations, and idempotency continue through the
existing `DurableConnectorChangeSink` and `KnowledgeService`.

## Deletion and revoke

A Drive deletion, trash, unsupported-type transition, or move outside every selected root emits a
delete immediately. The shared Knowledge lifecycle tombstones/hides the mapped document before its
durable delete job purges versions/chunks. The connector item mapping is removed only after that
handoff, preventing stale destination reuse.

Normal disconnect remotely revokes the Google token and supports the foundation's two choices:

- **retain**: remove connector credentials/state while retaining imported Knowledge;
- **purge**: tombstone and durably purge every mapped Knowledge document before connector state is
  removed.

After revoke, recurring reconciliation no longer runs the binding, so retained or purged content
cannot be resurrected by the connector.

## Operational behavior

- Discovery, folder crawl, and changes use explicit page tokens and bounded page sizes.
- Drive 429 and quota reasons are retryable; auth failures are fail-closed.
- Health performs a read-only Drive `about` request.
- Remote revoke is attempted before local state deletion; operators can still use the foundation's
  explicit local-only forget/purge routes when the provider is unavailable.
- The connector has no actions or write capability.

Tests use sanitized fake Drive API fixtures and cover pagination, initial crawl, changes cursors,
duplicate delivery, update/rename/move/delete, cursor invalidation, scope isolation, required
Knowledge targets, taint/provenance, credential refresh, health, retain/purge, and no resurrection.
