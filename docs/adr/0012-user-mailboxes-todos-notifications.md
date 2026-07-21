# ADR-0012: User-scoped Keel mailboxes, ToDos, and notifications

- **Status:** Accepted
- **Date:** 2026-07-21
- **Refines:** [ADR-0011](./0011-product-boundary-and-domain-model.md)
- **Provider:** [AgentMail](https://docs.agentmail.to/welcome)

## Context

Keel can currently use a user's Gmail Connection, but that address represents the user and carries
user-provided credentials. The product also needs a communication identity of its own: an address
from which Keel can receive mail, prepare replies, and notify the user.

In this decision, "Agent Email" means email owned and operated by **Keel itself**, not an inbox owned
by one persisted Agent record. Each user receives a distinct Keel address. The mailbox must survive
personal-Agent replacement and must not become visible to a team Agent merely because the user is an
organization member.

Keel also needs a durable personal task model. Jobs schedule internal execution, Routines describe
autonomous behavior, and approvals authorize exact effects; none of those is a user-visible ToDo
list.

## Decision

### 1. Product concepts and ownership

Three concepts are added:

| Concept | Meaning |
|---|---|
| **Keel Mailbox** | A platform-managed email address bound one-to-one to a User within a Keel deployment. |
| **ToDo** | A user-owned action record that Keel may create and manage under the user's authority. |
| **Notification** | A durable user-facing notice with one or more channel delivery attempts. |

A Keel Mailbox is:

- owned by the User, not by a persisted Agent;
- provisioned and operated by Keel, not configured as a user Connection;
- independent of the user's Gmail, Outlook, or other external accounts;
- private by default and unavailable to team Agents without a future explicit grant;
- retained across personal-Agent changes and erased with the User.

A ToDo belongs to `(organization, user)` so it remains isolated by the active organization while
surviving Agent replacement or deletion. The creating Actor, Agent, Session, Run, email message, or
Routine is provenance, never ownership.

These objects introduce an explicit user-owned partition axis rather than overloading Agent
`scope_id`:

- mailbox, mail, delivery-endpoint, and user Notification rows carry `owner_user_id` and use
  `FORCE ROW LEVEL SECURITY` against request-scoped `app.user_id`;
- ToDo, reminder, and organization-originated Notification rows require both `org_id` and
  `owner_user_id`, with RLS checking `app.org_id` and `app.user_id`;
- a small global route index may map an opaque provider inbox ID to a mailbox/User, but contains no
  mail body or user-facing content;
- after route resolution, the receiver sets the user/organization database context before storing
  or reading protected content;
- only a subsequently admitted Session/Run uses the Agent-derived `scope_id`.

Agent scope therefore remains the runtime partition for Agent work, while User/Organization keys
partition user-owned product data. Neither partition key grants authority by itself.

The initial production profile permits one active organization per deployment. If a future profile
allows one user to operate in several active organizations, automatic mailbox-to-Agent routing must
remain disabled until the user selects an organization or an explicit routing rule resolves it. R2
mail processing fails closed when a user has no unique active personal-Agent route: it stores the
mail under the User, marks the thread `routing_required`, and starts no model Run or external Effect.
The R2 release is gated on this ambiguity test even before the R4 multi-user product.

### 2. AgentMail integration

AgentMail is the first Keel Mailbox provider. Keel uses an existing AgentMail account supplied by the
operator; it never calls AgentMail's agent sign-up endpoint during normal startup because repeating
that flow can rotate credentials.

The deployment credential is a secret such as `AGENTMAIL_API_KEY`. It is loaded from an environment
variable or secret manager and never stored in Git, browser state, model context, run events, or the
sandbox.

Provisioning is asynchronous and idempotent:

1. persist the desired local mailbox row;
2. enqueue a durable provisioning job;
3. create the AgentMail inbox with a deterministic opaque `client_id`;
4. record the provider inbox ID/address and activate the mailbox;
5. reconcile incomplete provisioning after crashes or timeouts.

The deterministic provider key is derived from the Keel user ID, not the user's human email address.
Only opaque Keel identifiers are placed in AgentMail metadata.

The trusted control plane owns inbox and webhook lifecycle. Production should mint an inbox-scoped,
least-privilege key for message/thread/draft operations where supported. Provisioning credentials
and webhook administration remain unavailable to the Agent loop. All runtime operations pass
through a narrow trusted mail broker.

### 3. Human delivery address

The Keel Mailbox address and the user's human email address are different resources.

Keel records a primary human delivery endpoint with verification state, notification opt-in,
timezone, quiet hours, and channel preferences. An OIDC claim with `email_verified=true` may seed a
verified endpoint, but the user still explicitly enables email notifications. Local preview uses an
explicit verification flow rather than treating a configured string as verified.

Changing the primary delivery address invalidates the previous auto-send exemption until the new
address is verified.

### 4. Inbound mail

Cloud delivery uses AgentMail webhooks. Keel:

- verifies the exact raw body with the webhook's Svix secret before parsing;
- deduplicates both the provider event ID and delivery ID;
- durably records an accepted delivery before returning success;
- fetches the full message when the webhook omits a large body;
- reconciles webhook gaps with bounded provider polling;
- excludes spam, blocked, trash, and unauthenticated mail from automatic processing by default.

All inbound email, links, HTML, and attachments are untrusted external content. HTML is sanitized,
remote content is disabled or proxied, and attachments remain quarantined until size, type, and
malware policy permits access. Mail content never writes durable Memory or Knowledge implicitly.

A mailbox thread may be routed to a private email Session under the user's selected personal Agent.
The route is pinned when the thread is first admitted so later Agent changes do not silently change
an existing conversation. Webhook authenticity proves AgentMail delivered the event; it does not
prove that the human named in the `From` header authorized an action.

Default inbound behavior is triage and draft preparation. Untrusted mail may propose a ToDo or reply,
but it cannot directly authorize an external effect. Future email-command policies may grant a
verified sender a deliberately attenuated capability set; address matching alone is insufficient.

Persisting an inbound message does not automatically spend model budget. Automatic triage is
subject to user opt-in, per-mailbox and per-sender rate limits, bounded queue depth, Agent/Routine
budgets, and a circuit breaker that degrades to unread-mail notification or digest mode. Spam,
blocked, unauthenticated, and over-quota deliveries never invoke the model automatically.

### 5. Outbound mail and notification policy

Every outbound email is a durable Effect with an immutable payload hash, attempt history, provider
IDs, and `unknown`/reconciliation handling. AgentMail sends use a stable provider
`Idempotency-Key`; Keel's local Effect remains authoritative after the provider's idempotency window
expires.

One narrow action may bypass per-message approval:

- recipient is exactly the user's active verified delivery endpoint;
- content comes from a versioned Keel notification template;
- all substitutions are structured and escaped;
- there are no CC/BCC recipients, attachments, arbitrary reply targets, or model-supplied HTML;
- notification preferences, quiet hours, quotas, and deduplication permit delivery.

Examples include a ToDo reminder, approval request, Routine completion, or failure notice.

Every other send, reply, or forward requires an Approval bound to the exact mailbox, recipients,
subject/body hash, attachment hashes, source thread/message, policy context, and expiry. Keel stores
the canonical draft locally. AgentMail Drafts may be used as a provider artifact, but provider-side
scheduled sending is not the source of truth and may not bypass Keel's approval and Effect ledger.

Delivery, bounce, complaint, and rejection events update the Notification delivery and Effect state.
A possible provider success followed by a timeout becomes `unknown`; Keel reconciles before retry.

### 6. ToDo model

The initial ToDo contains:

- title and optional notes;
- status: `open`, `in_progress`, `completed`, or `cancelled`;
- priority;
- optional due time and user timezone;
- zero or more reminder times/channels;
- tags;
- optimistic version;
- creator and source provenance;
- completion, cancellation, archive, and audit timestamps.

Completion and cancellation are reversible through an explicit reopen action. Archive hides a ToDo
without pretending it never existed; erasure is handled by the user lifecycle.

Keel exposes user-scoped operations:

```text
todo.create
todo.list
todo.get
todo.update
todo.complete
todo.cancel
todo.reopen
todo.archive
```

The tools never accept an arbitrary owner or organization ID from model arguments. An authenticated
interactive session may create and update its user's ToDos under explicit policy. A Routine or email
session needs a separately admitted `todo.write` capability; destructive or ambiguous changes may
be proposed instead of applied.

ToDo reminders create durable Notifications. A due timestamp is not a scheduler cursor and a
reminder is not considered delivered merely because a job was enqueued. Editing, completing,
cancelling, or archiving a ToDo atomically cancels or replaces pending reminder occurrences.

### 7. Target persistence and surfaces

The target store includes:

- `user_mailboxes` and encrypted provider credential references;
- `user_email_endpoints`;
- `mail_threads`, `mail_messages`, and `mail_drafts`;
- a content-free global `mailbox_route_index`;
- `mail_webhook_deliveries`;
- `todos`, `todo_reminders`, and append-only `todo_activity`;
- `notifications` and per-channel `notification_deliveries`.

Mailbox, mail, delivery-endpoint, and user Notification rows are globally user-owned and protected
by the explicit User RLS axis. ToDos and their reminders are partitioned by organization and user.
Routing mail into an Agent Session adds an explicit organization/Agent/session reference without
changing mail ownership. Notification delivery credentials remain in the trusted effect plane.

The product surfaces are:

- **Mail**: address/status, threads, message detail, quarantined attachments, and pending drafts;
- **ToDos**: Today, Upcoming, All, Completed, create/edit/complete/cancel/reopen/archive, and
  provenance;
- **Settings**: verified delivery address, notification channels, quiet hours, and mailbox state;
- chat tools for natural-language ToDo management and mail draft preparation.

The API remains additive under `/v1`, with user-scoped mailbox/mail, ToDo, and notification
resources. Normal clients do not provide raw `user_id`, `scope_id`, or provider credentials.

### 8. Lifecycle and acceptance

Mail bodies and ToDos are user content and remain until user deletion, explicit archive/delete
policy, or retention policy. Operational webhook deduplication and delivery attempts use finite
retention. User erasure revokes scoped keys, deletes the remote AgentMail inbox, removes local
content, and reports `partial` rather than success if remote deletion cannot be verified.

Release acceptance must prove:

- retrying provisioning creates one mailbox for one user;
- two users cannot discover, read, send from, or route through each other's mailboxes or ToDos;
- direct SQL access under a foreign `app.user_id`, or a mismatched `app.org_id` for ToDos, is denied
  even when an Agent `scope_id` is supplied;
- an absent or ambiguous personal-Agent/organization route stores mail safely but starts no Run;
- spoofed, replayed, stale, spam, and unauthenticated webhook deliveries cannot start trusted work;
- a crash at every outbound boundary produces one logical email, or an `unknown` effect that is
  reconciled before retry;
- the template auto-send exemption cannot be widened by model output or caller arguments;
- editing/completing a ToDo cannot produce stale or duplicate reminders;
- the Mail and ToDo journeys work through the shipped React UI and real provider integration.

## Consequences

- Email becomes another surface of the same durable runtime, not a second Agent implementation.
- Keel gains a stable user-facing identity without conflating it with user Connections or Agent
  records.
- Notification delivery becomes reusable by ToDos, Routines, approvals, and operations.
- The trusted effect plane gains another credential-bearing broker and must be isolated and
  observable accordingly.
- Provider cost, deliverability, abuse handling, retention, and erasure become production concerns.

## Rejected alternatives

- **One mailbox per persisted Agent:** rejected because mailbox identity would disappear or fragment
  when the user changes Agents.
- **Model AgentMail as a normal Connection:** rejected because the platform, not the user, owns the
  provider account and credential.
- **Use provider scheduled drafts as Keel's scheduler:** rejected because approval, cancellation,
  restart recovery, and exact-effect evidence would move outside Keel's authority.
- **Represent ToDos as Jobs or Routines:** rejected because internal execution state is not a
  user-owned task list.
