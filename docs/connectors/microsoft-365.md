# Microsoft 365 connector

Connector id: `microsoft_365`

This MVP uses a customer-owned, **single-tenant** Microsoft Entra application and delegated
authorization for one account. It is read-only: Outlook Mail and Calendar are included; mail send,
calendar writes, Contacts, Teams, OneDrive, and SharePoint are not.

## Entra application

1. Register a single-tenant application in the target Entra directory.
2. Add the Keel callback shown by the deployment:
   `https://<keel-host>/v1/connectors/microsoft_365/callback`.
3. Add only these delegated Microsoft Graph permissions:
   - `User.Read`
   - `Mail.Read`
   - `Calendars.Read`
4. Create a client secret. Store its value in the connector setup form; Keel puts it in the
   encrypted, scope-bound connector credential envelope, never connector metadata.
5. Save the tenant ID, client ID, and client secret, then choose **Authorize Microsoft 365**.

Keel requests `openid profile offline_access User.Read Mail.Read Calendars.Read`. The callback
rejects a token tenant different from the configured tenant, records the authorized Graph account,
and stores the refresh credential encrypted at rest. Refresh rotation uses the foundation's
credential-version compare-and-set fence.

## Resources and tools

Refresh resources after authorization, then explicitly select the mail folders and calendars the
scope may use. Tool calls fail closed if a requested resource is not selected.

- `m365_mail_search`: search one selected folder; supports Graph continuation pages.
- `m365_mail_get`: get one message from one selected folder.
- `m365_calendar_list`: list a selected calendar in a supplied time range.
- `m365_calendar_get`: get one event from a selected calendar.
- `m365_calendar_upcoming`: list upcoming events for 1–90 days.

Calendar reads accept an Outlook/Graph timezone string and send it through the
`Prefer: outlook.timezone="..."` header. Results include the effective timezone, continuation URL,
connector/binding/resource provenance, source URL and revision. Connector read results remain
tainted external content.

## Recurring synchronization

The manifest schedules synchronization every five minutes through the generic connector scheduler.
Configure a Knowledge target before synchronization. Each selected mail folder and calendar has an
independent Graph delta link:

- mail uses `mailFolders/{id}/messages/delta`;
- calendar uses a bounded `calendarView/delta` window (30 days past through 365 days future).

All `@odata.nextLink` pages are consumed before the new `@odata.deltaLink` is committed. A rejected
or expired cursor triggers one controlled full delta pass; items absent from that full pass are
deleted from the mapped target. HTTP 429 honors bounded `Retry-After` delays before failing for the
durable job retry. Authentication, missing-scope, tenant mismatch, and account mismatch errors fail
closed and do not advance the cursor.

## Health and disconnect

Health calls `/me` with the current access token and verifies the configured account, tenant, and
read scopes. It does not perform an unfenced refresh; expired credentials are refreshed by sync or
tool execution using CAS, otherwise health reports an error.

Microsoft Entra does not expose an RFC 7009-style endpoint to revoke one delegated refresh token.
Normal remote disconnect therefore fails closed. Revoke the application's user grant in Entra
first, then use the connector foundation's explicit local-forget endpoint. This avoids requesting
the broad, write-like permission needed to invalidate all user sessions.

Graph webhooks are intentionally not enabled in this MVP; recurring delta synchronization is the
complete change path.
