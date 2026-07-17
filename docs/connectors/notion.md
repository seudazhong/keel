# Notion connector

The `notion` connector is a read-only importer for Notion pages and data sources. It uses
plain HTTP against Notion API version `2025-09-03`; the official SDK is not required.

## Setup

1. Create a Notion **internal integration**.
2. Copy its Internal Integration Token.
3. Explicitly share each page or data source root with the integration in Notion.
4. In Keel, save the token, refresh resources, select the roots to import, and choose an
   explicit Knowledge Base target.

Keel validates the token with `GET /users/me`, stores it only in the existing encrypted
connector credential store, and never returns it in setup artifacts or binding metadata.
Rotating or revoking the token is done in Notion; disconnecting removes Keel's encrypted copy.

## Visibility and sync

Discovery uses Notion search and exposes only pages/data sources visible to the integration.
Selecting a page imports that page and nested child pages. Selecting a data source imports its
schema, queried row pages, nested blocks, and nested child pages. A page reachable through
multiple selected roots is imported once.

Initial and recurring 15-minute syncs paginate search, block children, and data-source queries.
The durable connector cursor records root membership plus object last-edited/revision state.
Unchanged objects do not emit Knowledge updates. Cursor writes occur only after all emitted
changes have been durably handed to the existing Knowledge lifecycle, so retries safely replay
the same revision after interruption.

Rename/content updates become deterministic Markdown upserts. Archived, trashed, or no-longer
visible objects become deletes; the durable Knowledge delete completes before the item mapping
is removed. Disconnect supports:

- **Retain:** remove connector credentials/state while leaving imported Knowledge documents.
- **Purge:** durably delete mapped Knowledge documents before removing connector state.

Old scheduled jobs carry the prior binding id and cannot sync after reconnect.

## Content and provenance

Markdown includes the Notion object kind, page/data-source id, URL, last-edited timestamp,
derived revision, parent/database provenance, page properties, data-source schema, and supported
block content. Connector changes are always tainted. Knowledge citations use the original Notion
URL, while the cited document text retains the remaining provenance.

Supported rendering includes rich text, headings, paragraphs, lists, to-dos, toggles, quotes,
callouts, code, equations, dividers, child pages/data sources, bookmarks/embeds, media links,
tables, columns, synced blocks, and nested children. Unknown blocks and properties are retained
as explicit `[Unsupported Notion ...]` markers rather than being silently discarded.

## Health behavior

| Condition | Health | Retryable |
|---|---|---|
| `401` invalid/revoked token | error | no |
| `403` denied resource | error | no |
| `404` missing or no longer shared | error for direct health/API access; sync treats missing inventory as deletion | no |
| `429` rate limit | degraded | yes |
| timeout or Notion `5xx`/network outage | degraded | yes |

Errors are sanitized and do not include tokens, response bodies, or imported content.

## Read-only guarantee

The connector calls only read-semantic endpoints: user validation, search, page/data-source
retrieval and query, and block-child retrieval. It never creates, edits, archives, or deletes
Notion pages, databases, data sources, blocks, comments, or users.
