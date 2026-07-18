# Google Calendar connector

Connector id: `google_calendar`

Google Calendar is independent from Gmail. It reuses the deployment's Google OAuth client
file (`KEEL_GMAIL_CLIENT_SECRETS_PATH`) but stores a separate encrypted credential under the
`google_calendar` connector id.

## Authorization and scopes

The first connection requests only:

- `https://www.googleapis.com/auth/calendar.readonly`

Create and update actions fail closed until the connector is reconnected. That reconnect uses
Google incremental authorization (`include_granted_scopes=true`) and additionally requests:

- `https://www.googleapis.com/auth/calendar.events`

The callback preserves an existing refresh token when Google omits it during incremental consent.
Missing, malformed, revoked, insufficiently scoped, or refresh-failed credentials produce explicit
errors; they are never treated as a successful connection.

## Setup

1. Configure the same Google OAuth client JSON used by the deployment's Gmail connector.
2. Add the generic callback URL shown by Keel to the Google OAuth client's redirect URIs.
3. Connect Google Calendar and approve read-only access.
4. Discover calendars, select the calendars Keel may access, and select the session that receives
   tainted sync events.
5. If create/update is needed, request the action and reconnect Google Calendar to grant the
   incremental write scope.

Calendar discovery records only non-secret metadata: calendar id, display name, access role,
primary flag, time zone, and URL.

## Read actions

- `google_calendar_events_list`: upcoming events with bounded pagination.
- `google_calendar_events_search`: upcoming event search using Google's `q` filter.
- `google_calendar_event_get`: one event by id.

Every action validates that the calendar is selected in the current scope. Results retain
pagination tokens, requested/returned time zone, all-day dates, timed values, recurrence and
recurring-instance fields, source URL, revision, calendar/event ids, binding provenance, and
tainted content status.

## Recurring sync

The provider stores one Google `nextSyncToken` cursor per selected calendar. Incremental calls use
that token and preserve deleted/cancelled changes. If Google returns `410 Gone`, only that calendar
performs a controlled full resync:

- a 30-day lookback;
- expanded recurring instances;
- at most 20 pages of 2,500 events;
- a new token is stored only after the bounded resync completes.

Sync runs every five minutes through the generic connector reconciler. Changes are admitted as
tainted `google_calendar.event_changed` events with calendar and event provenance.

## Outbound actions

- `google_calendar_event_create`
- `google_calendar_event_update`

Both actions:

- require tainted-content approval;
- require an `idempotency_key`;
- require a selected calendar grant whose discovered access role is `writer` or `owner`;
- require the incremental `calendar.events` scope;
- use the durable connector outbox;
- store a hashed request marker in the external event.

Create derives a stable Google event id from the scope, calendar, and idempotency key, then
reconciles an existing event or an insert conflict before returning. Update patches a specific
event and reconciles the external request marker after ambiguous failures. These checks cover the
crash window between Google's successful write and local outbox finalization.

## Health and revoke

Health performs an authenticated, bounded Calendar API request. Refresh/auth failures are
non-retryable errors; rate limits and temporary Google failures are explicit retryable degraded
states. Disconnect remotely revokes the refresh/access token first. A network or Google revoke
failure keeps local credentials and reports an error; operators may use the foundation's explicit
local-forget path if remote revoke is impossible.

## Limits

- No event deletion.
- No room/resource booking.
- No conference-provider-specific behavior.
- No live Google credentials or payloads are used in CI; fixtures are sanitized.
- Calendar API calls use a 10-second transport timeout, bounded pagination, and library retries for
  transient responses.
