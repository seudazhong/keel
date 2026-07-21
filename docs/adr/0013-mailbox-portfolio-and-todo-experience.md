# ADR-0013: Mailbox portfolio and ToDo product experience

- **Status:** Accepted
- **Date:** 2026-07-21
- **Refines:** [ADR-0012](./0012-user-mailboxes-todos-notifications.md)
- **Supersedes:** ADR-0012's one-to-one mailbox cardinality and conceptual dotted ToDo tool names

## Context

ADR-0012 established that Keel email belongs to the User rather than a persisted Agent, and that
ToDos are user-owned product data. It intentionally left the detailed product experience open.

The user experience must answer:

- whether a user can have several Keel email identities;
- how an address is provisioned, routed, configured, and retired;
- how mail remains understandable beside Connections and Agents;
- how ToDos work as a focused personal task list rather than a project-management product;
- which tools the Agent loop receives, and which contexts may mutate versus only propose.

## Decision summary

Keel supports a **mailbox portfolio**:

- when Mail is enabled, exactly one active **Primary Mailbox** per User;
- zero or more optional **Purpose Mailboxes**, bounded by deployment quota;
- all mailboxes remain private and user-owned;
- a mailbox may route new threads to a selected personal Agent, but routing is not ownership;
- organization/team mailboxes are a separate future resource and are not simulated by sharing a
  personal mailbox.

R2 ships both the Primary Mailbox journey and the ability to add, switch, configure, and archive a
Purpose Mailbox. The exit scenario proves two private mailboxes without cross-routing. Team/shared
mailboxes remain later work.

The product labels are **Mail**, **Keel Mailbox**, and **ToDos**. "Agentic Email" may appear in
explanatory copy, but not as the primary navigation term because Keel already has a distinct Agent
domain concept.

## 1. Mailbox portfolio

### 1.1 Primary Mailbox

The Primary Mailbox:

- is provisioned during onboarding or on first entry to Mail;
- is the default sender for versioned Keel notifications;
- is the default mailbox shown in the Mail workspace;
- cannot be archived until another active mailbox becomes primary;
- may be changed explicitly, affecting future notifications and new conversations only.

Switching the primary mailbox never rewrites historical sender identity, thread routing, approvals,
or Effect evidence.

The Primary Mailbox sends versioned Keel notifications regardless of which mailbox, Session, email,
or Routine produced the underlying ToDo or event.

### 1.2 Purpose Mailboxes

A user may create additional private mailboxes for roles such as:

- subscriptions and receipts;
- travel planning;
- recruiting or job search;
- finance administration;
- a project-specific external identity.

Each Purpose Mailbox has its own display name, purpose, signature, default personal-Agent route,
inbound automation mode, sender rules, and budget. It uses the same verified human delivery endpoint
and immutable outbound approval rules as the Primary Mailbox.

The UI shows the deployment quota and remaining capacity. The quota is operator-configurable rather
than hard-coded into the product contract.

### 1.3 Address and lifecycle

The user chooses an available local part when the provider/domain permits it, or accepts a generated
address. Available operator-verified custom domains may be offered; users cannot add DNS domains
from the personal Mail flow.

An address is immutable. "Rename" changes display name, purpose, or signature, not the email address.
Changing the address creates a new mailbox and optionally makes it primary.

Archive is the normal reversible action:

- stop new automatic routing and sending;
- retain history and existing thread visibility;
- remove it from the default switcher;
- allow restore while the remote inbox still exists.

Permanent remote deletion is an explicit advanced lifecycle action with erasure status. Keel never
claims the provider address was deleted until remote deletion is verified.

An archived provider inbox may still technically receive mail until remote deletion or a provider
disable rule succeeds. Keel stores such deliveries without admitting model work and shows an
archived-address warning; it does not silently discard or act on them.

The sole active Primary Mailbox cannot be permanently deleted through the per-mailbox action. The
user must either make another mailbox primary first or choose the separate **Disable Keel Mail**
flow, which retires all mailboxes and returns the user to the valid zero-mailbox, Mail-disabled
state. While Mail is enabled and any active mailbox exists, exactly one is primary.

### 1.4 Provisioning idempotency

Keel persists a unique opaque local `mailbox_id` before enqueueing provider work. The AgentMail
`client_id` is deterministic per local mailbox, for example `keel-mailbox-{mailbox_id}`, not merely
per User. Retrying one provisioning job resolves to the same provider inbox, while a second Purpose
Mailbox has a different local mailbox ID and cannot collide.

## 2. Information architecture

The primary sidebar becomes:

```text
Workspace
  Chat
  Mail
  ToDos
  Sessions
  Memory
  Knowledge
  Projects

Configuration
  Agents
  Connectors
  Approvals

Automation
  Routines / Schedules
```

Mail and ToDos are high-frequency personal work surfaces, so they belong in Workspace rather than
Configuration or Automation.

Target routes:

```text
/mail
/mail/:mailboxId
/mail/:mailboxId/thread/:threadId
/mail/:mailboxId/settings
/todos
/todos/:todoId
```

The current React shell remains the global navigation. Mail and ToDos use the available width rather
than inheriting the existing `max-w-[900px]` management-page constraint.

## 3. Mail setup experience

### 3.1 First-use flow

Opening Mail without a mailbox shows one primary action: **Enable Keel Mail**.

The setup wizard has five short steps:

| Step | User decision | Default |
|---|---|---|
| 1. Identity | Address/local part, available domain, display name | Generated address and "Keel for {name}" |
| 2. Route | Personal Agent used for new admitted mail threads | Current personal Agent |
| 3. Inbound mode | Receive only; triage and draft; triage plus ToDo proposals | Triage and draft |
| 4. Notify me | Verify human email, Web/email channels, timezone, quiet hours | Web on; email after verification |
| 5. Test | Send a safe template test notification and show delivery status | Explicit user action |

If no personal Agent exists, the wizard links to Agent creation and preserves the mailbox draft.

The wizard explains the two addresses:

```text
Keel Mailbox:     your Keel-owned sender/receiver address
Notify me at:     your verified human address for reminders and alerts
```

No step exposes the AgentMail API key, webhook secret, provider organization ID, or raw routing
identifier.

### 3.2 Adding another mailbox

**Add mailbox** opens a compact variant of the wizard:

1. purpose and display name;
2. address/domain;
3. default personal Agent;
4. inbound mode and daily triage budget;
5. review fixed outbound policy and create.

The user is not asked to verify the human delivery endpoint again unless it changed.

## 4. Mail workspace

### 4.1 Desktop layout

At wide desktop widths, Mail is a three-pane workspace:

```text
+--------------------------------------------------------------------------+
| Mail | [Mailbox switcher v] [Search................] [New draft] [Settings]|
+---------------+---------------------------+------------------------------+
| Mailboxes     | Threads                   | Selected thread              |
| - Primary     | sender / subject / preview| safety + provenance banner   |
| - Travel      | unread / time / badges    | normalized message history   |
|               |                           | attachments / remote images  |
| Views         |                           | Keel summary                 |
| - Inbox       |                           | proposed ToDo / draft reply  |
| - Drafts      |                           | [Edit draft] [Send/Approve]  |
| - Approvals   |                           |                              |
| - Sent        |                           |                              |
| - Failed      |                           |                              |
+---------------+---------------------------+------------------------------+
```

At medium widths, the mailbox/view rail becomes a switcher and the page uses thread-list plus detail.
On mobile, list, thread, draft, and settings are separate stacked routes with preserved back
navigation.

### 4.2 Thread list

Each row shows:

- sender and subject;
- one-line sanitized preview;
- received/sent time;
- unread, attachment, draft, approval, failed, and proposed-ToDo badges;
- mailbox color/initial only when viewing a combined inbox.

Filters include unread, has attachment, needs approval, failed delivery, and date range. Search spans
sender, recipients, subject, and normalized body while preserving mailbox ownership filters.

### 4.3 Thread detail and safe rendering

The thread opens with a persistent provenance banner:

```text
External email - untrusted content
Links and attachments cannot authorize actions. Remote images are blocked.
```

HTML is rendered in a sanitized, isolated view. Remote images require a user click. Attachments show
size, type, scan/quarantine state, and a separate download/open action.

Beside the message history, Keel may show:

- concise summary;
- extracted dates, people, and requested actions;
- **Proposed ToDo** cards with Accept, Edit, and Dismiss;
- a reply draft with cited source messages;
- why an action needs approval.

Model interpretation is visually separate from original email content.

### 4.4 Draft and approval experience

A user can start a draft directly or ask Keel to prepare one. The editor always shows sender mailbox,
recipients, subject, body, attachments, and source thread.

If the authenticated user reviews the exact draft in Mail and clicks **Send**, that click is the
human decision bound to the draft hash; Keel does not require a second redundant approval dialog.
Autonomously prepared drafts appear both in Mail > Approvals and the global Approvals page.

Any edit after approval invalidates the decision and returns the draft to pending review.

Template notifications to the verified user address are shown in Sent/Notifications but do not
create noisy approval cards.

## 5. Mailbox settings

Each mailbox settings page contains:

| Section | Controls |
|---|---|
| Identity | Display name, purpose, immutable address, signature, locale |
| Routing | Default personal Agent for new threads; current pinned-thread count |
| Inbound automation | Receive only; triage and draft; triage plus ToDo proposals |
| Sender controls | Allow/block entries, spam/unauthenticated visibility, rate-limit status |
| Budget | Daily automatic-triage messages/tokens and circuit-breaker state |
| Attachments | Allowed types/size policy; always quarantine executable or unknown content |
| Outbound policy | Read-only explanation of template-to-self exemption and exact-draft approval |
| Lifecycle | Make primary, archive, restore, export, or request permanent deletion |

The settings UI cannot enable arbitrary auto-reply or widen recipients that bypass approval.

## 6. ToDo workspace

### 6.1 Desktop layout

ToDos use a focused three-column layout:

```text
+--------------------------------------------------------------------------+
| ToDos | [Quick add: "Friday 5pm submit report #work !high"] [Add] [Search]|
+---------------+--------------------------------+-------------------------+
| Views         | Task list                      | Task detail             |
| - Inbox       | [ ] Submit report      Fri 5pm | title / notes           |
| - Today       | [ ] Review invoice     overdue | status / priority       |
| - Upcoming    | [x] Book hotel                 | due / timezone          |
| - All         |                                | reminders / channels    |
| - Completed   | Suggested from Mail            | tags / provenance       |
| - Proposals   | [Accept] [Edit] [Dismiss]      | activity / source link  |
+---------------+--------------------------------+-------------------------+
```

At medium widths the detail panel becomes a drawer. On mobile, views, list, and detail are separate
routes. Completing a ToDo is keyboard-operable and immediately offers Undo.

### 6.2 Views

- **Inbox:** active ToDos without a due time.
- **Today:** due today plus overdue.
- **Upcoming:** future due items grouped by date.
- **All:** active items with filters.
- **Completed:** completed and cancelled history.
- **Proposals:** ToDos suggested from email or autonomous work but not yet accepted.

Filters cover status, priority, tag, due range, reminder channel, and provenance. Sorting supports
due time, priority, created time, and updated time. Manual Kanban ordering is not an initial goal.

### 6.3 Quick add

Plain text creates a title immediately. Natural-language dates, priority, tags, and reminders are
parsed into visible chips before save:

```text
"周五下午 5 点提醒我提交报告 #工作 !高"
 -> Due: Fri 17:00 Asia/Shanghai
 -> Reminder: Email + Web at Fri 16:00
 -> Tag: 工作
 -> Priority: High
```

If a date or timezone is materially ambiguous, Keel asks instead of silently choosing. The persisted
tool boundary receives RFC 3339 timestamps and IANA timezone names, not unresolved natural language.

### 6.4 Task row and detail

A task row shows checkbox/status, title, due/overdue state, priority, reminder icon, tags, and a small
provenance indicator when Keel or Mail created it.

The detail panel supports:

- title and notes;
- `open`, `in_progress`, `completed`, and `cancelled`;
- priority;
- due time and timezone;
- zero or more Web/email reminders;
- tags;
- source Session/email/Routine link;
- append-only activity;
- complete, cancel, reopen, archive, and restore.

Hard delete is not an Agent tool. User erasure and explicit lifecycle actions own physical deletion.

### 6.5 Mail proposals

Untrusted email creates a proposal, not an active ToDo. A proposal shows:

- proposed title, due time, priority, and reminders;
- source sender/thread and quoted evidence;
- why Keel suggested it;
- Accept, Edit and accept, or Dismiss.

Accepting creates the ToDo with source provenance and an idempotency link so webhook or model retries
cannot create duplicates.

### 6.6 Initial scope

Initial ToDos include dates, reminders, tags, provenance, search, and audit. The following are
fast-follow capabilities rather than hidden partial implementations:

- recurrence;
- subtasks and dependencies;
- shared/team task lists;
- attachments;
- project boards and arbitrary custom fields.

## 7. Agent ToDo tools

Provider-facing tool names follow the repository's `snake_case` convention. ADR-0012's dotted names
were conceptual operations, not the final tool API.

### 7.1 Public Agent tools

| Tool | Purpose | Important boundary |
|---|---|---|
| `todo_create` | Create one active ToDo | Owner/org derived from run; stable idempotency key required |
| `todo_list` | List/search/filter the user's ToDos and, in trusted direct sessions, proposals | Cursor pagination; bounded result count |
| `todo_get` | Read one ToDo with reminders and provenance | Foreign IDs return not-found without disclosure |
| `todo_update` | Patch title, notes, due time, priority, tags, and optionally replace reminders atomically | Requires `expected_version`; cannot change owner/status |
| `todo_transition` | Start, complete, cancel, reopen, archive, or restore | Explicit transition plus `expected_version` |
| `todo_set_reminders` | Atomically replace reminder set | Absolute timestamps/channels; stale reminders cancelled |

`todo_list` covers search; a separate `todo_search` tool would add selection ambiguity without new
authority. A trusted caller can request active records, proposals, or both; untrusted contexts cannot
use the listing tool. `todo_transition` keeps lifecycle changes out of a broad generic update schema.

### 7.2 Restricted proposal tool

`todo_propose` is available only to admitted untrusted/email/autonomous contexts. It creates a
reviewable proposal and cannot schedule a live reminder. Accepting the proposal is a user/API action,
not another untrusted tool call.

The trusted adapter supplies a proposal idempotency namespace derived from the accepted source
delivery/occurrence and candidate ordinal. The model cannot choose the owner or source occurrence.
Retries return the existing proposal instead of creating duplicate cards.

### 7.3 Schema rules

The tools never accept `user_id`, `org_id`, `scope_id`, or `agent_id` as authority-bearing input.

`todo_propose` accepts the candidate fields plus a bounded source evidence reference and reason. Its
trusted context supplies the source occurrence and idempotency namespace; these are not
model-controlled.

`todo_create` accepts:

```text
title
notes?
priority?
due_at?          RFC 3339
timezone?        IANA name
tags?
reminders?
idempotency_key
```

`todo_list` accepts bounded filters:

```text
statuses?
query?
due_from?
due_to?
priorities?
tags?
source_kinds?
record_kinds?      todo | proposal
limit?
cursor?
```

`todo_update`, `todo_transition`, and `todo_set_reminders` require `todo_id` and
`expected_version`. Empty patches, invalid transitions, reminders after completion, and reminders in
the past fail explicitly. When a due-time change also changes relative reminder intent,
`todo_update` replaces the reminder set in the same transaction; `todo_set_reminders` is the
convenience operation for reminder-only changes.

All successful mutations return the canonical ToDo, new version, reminder state, and provenance
summary so the Agent can confirm exactly what changed.

### 7.4 Permission matrix

| Context | Read | Create/update/transition | Proposal |
|---|---:|---:|---:|
| Authenticated direct Web/CLI user session | allow own | allow own, audited, with Undo in UI | allow |
| Personal Routine | allow if admitted | only with explicit `todo.write`; otherwise propose | allow |
| Inbound email or untrusted IM | deny active-list discovery by default | deny | allow bounded proposal |
| Team Agent | deny personal ToDos by default | deny | deny |
| Operator/admin | support diagnostics only; no model tool access | lifecycle APIs, audited | n/a |

Low-risk ToDo writes requested in an authenticated direct session do not require a separate Approval
modal. They remain visible, versioned, auditable, and reversible. Email delivery caused by a ToDo is
a separate Notification/Effect decision and follows ADR-0012.

## 8. UX acceptance

The release is not complete until:

- a new user provisions a Primary Mailbox without seeing provider credentials;
- the user adds and switches between two private mailboxes without cross-routing threads;
- an archived mailbox cannot send or admit new automatic work;
- a received untrusted message is visibly distinct from Keel's interpretation and produces only a
  proposal;
- editing an approved mail draft invalidates the approval;
- desktop, tablet, keyboard-only, screen-reader, and mobile paths cover Mail and ToDos;
- a user creates a ToDo in UI and chat, edits it under optimistic concurrency, receives one reminder,
  completes it with Undo, and sees the same state after restart;
- an untrusted email cannot invoke the active ToDo mutation tools.

## Consequences

- Multiple email identities are supported without turning persisted Agents into mailbox owners.
- The Primary Mailbox keeps onboarding simple and supplies one predictable notification sender.
- Mail and ToDos require workspace-style responsive layouts rather than more 900-pixel management
  tables.
- The ToDo toolset stays small, explicit, and safer for model selection.
- Team mailboxes, recurrence, subtasks, and project management remain visible future work instead of
  leaking into an unstable first release.
