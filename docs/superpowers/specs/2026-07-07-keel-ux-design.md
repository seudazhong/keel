# Keel — UX design & product-shape review (mockups)

**Date:** 2026-07-07 · **Status:** Draft for review · **Owner:** @dazhongguo
**Artifacts:** `docs/mockups/` (10 static HTML screens, open `index.html`)
**Grounded in:** ADR-0009 (product form & use cases), ADR-0004 (frontend stack)

## 1. Why this exists

M0+M1 built a solid **engine** (durable loop, provider, tools, state/memory/scope,
connectors subsystem, search, observability, three surfaces) and validated it live.
But there was **no UX-perspective design** — we shipped a bare chat page while the
product's actual differentiation (per ADR-0009) lives in **connectors + memory +
agents-as-scoped-entities + messaging + safety/approvals**, none of which was
visualized. Two risks motivated this pass:

1. **Direction drift** — building fast without a picture of the end product.
2. **Late-discovered design gaps** — decisions deferred until they're expensive.

Static mockups are the cheap forcing function: they make the product concrete for
alignment and surface the open decisions **before** we build more. Each mockup
carries yellow dashed **design-gap callouts**; §5 consolidates them.

## 2. Product framing (recap, ADR-0009)

Keel is a **self-hostable connected AI assistant**, not a coding tool. One
abstraction — **an agent is a scoped, persisted entity** (persona + memory +
toolset + connectors + permission boundary + provider + trust) — is projected onto
**Web + IM** surfaces. Center of gravity: **connectors (email/calendar/docs) +
memory + retrieval + messaging first**; code/shell second. Headline security goal:
**per-scope isolation + confused-deputy** (tainted content must never drive an
unapproved action).

## 3. Design language

Hi-fi **light "admin console"**: light background (`#f6f7f9`), white cards, thin
borders (`#e6e8ec`), indigo accent (`#4f46e5`), subtle shadows, system font + mono
for tool/step lines. One shared stylesheet `assets/app.css` (design tokens +
components: cards, tables, badges, toggles, banners, timeline, wizard, chat). Every
screen shares a left-nav **app shell** (Workspace / Config / Ops groups) + a top bar
with a **scope switcher** (the scope switch is the UX embodiment of ADR-0009).

## 4. Screen inventory & journeys

| Screen | Purpose | Key elements |
|---|---|---|
| **index** | Product overview / entry to mockups | 9-screen grid, framing |
| **onboarding** | First run (ADR-0008) | Wizard: deploy profile → provider/model (Copilot device-login) → first agent |
| **chat** | Hero: connected-assistant conversation | Sessions rail · streaming · **tool/step timeline** · connector action · **inline approval** (taint banner) · context sidebar (agent, connectors, memory, run cost) |
| **agents** | Agent = scoped entity | Agent list (personal/group + trust) · config: persona, model, **per-tool allow/ask/deny**, granted connectors |
| **connectors** | OAuth integrations | Connected accounts (least-scope grants, refresh state) · taint rule banner · catalog · IM channels |
| **approvals** | Human-in-the-loop gate | Pending queue with **confused-deputy warning** on tainted outbound · resolved history |
| **memory** | Editable long-term memory | Memory blocks table · **version history** · consolidation |
| **sessions** | Cross-surface history | Hybrid search (FTS⊕vector, RRF) · filters (surface/agent/date) |
| **observability** | Runs + cost | Stat tiles · runs table · **run trace timeline** (per-step tokens/cost, cache-read) |
| **admin** | Operate the instance | Health · users/**RBAC** · scopes · **IM channels** (wake rules, rate limits) |

Primary journeys the mockups tell:
- **UC-B personal:** onboarding → connect Gmail/Calendar → chat "arrange tomorrow" →
  agent reads calendar (connector) → drafts email → **approval** → sent; memory
  remembers "张伟 = project lead".
- **UC-A team/IM:** admin connects QQ (OneBot) → group agent (untrusted → safe
  toolset) answers @-mentions under a rate limit → sessions/observability show it.

## 5. Open design decisions & risks (the point of this pass)

Grouped by theme; each needs a decision before the corresponding build.

### A. Permissions & approvals (safety UX — highest priority)
- **Approval delivery across surfaces.** Web has a queue, but an approval raised by
  an **IM or background/scheduled** run must reach the user somehow (DM? email? push?).
  Undecided — blocks async autonomy (UC-B digests/reminders).
- **Fail-closed timeout.** Approvals fail closed on timeout (good). Is the timeout
  **configurable per policy**? What does the user see for an expired approval?
- **"Why am I being asked?"** The approval card should explain the trigger (taint,
  first-use, policy). What's the minimum explanation set?
- **Outbound default policy.** Always-ask / first-time-ask / per-recipient-domain —
  where is this configured (agent? connector? global)?
- **Who may approve a group agent's action?** Ties to RBAC.

### B. Connectors & the scope model
- **agent ↔ connector is many-to-many.** Grant entry point: from the agent, or from
  the connector? (Recommend: choose target agent(s) **at connect time**, not
  default-all-scope.)
- **Isolation as a hard rule.** A **group agent must never** be grantable a personal
  connector (ADR-0009). How does the UI *prevent* (not just discourage) this?
- **Refresh failure.** Notify the user proactively and **pause dependent auto-tasks**?
- **Scope deletion** → revoke + purge tokens (G18): confirmation UX + audit.

### C. Memory model
- **Auto-write vs manual.** Which memory does the agent write itself vs the user? Should
  auto-writes be **reviewable / rollback-able** (they already carry versions)?
- **Sharing granularity.** Is memory **per-agent** or **per-scope** (do a user's
  multiple personal agents share "关于我")? Affects the data model.

### D. Cross-surface parity
- **IM ↔ Web session continuity.** Opening an IM session on Web: read-only replay or
  continue the conversation? (Sessions are already scope-keyed `platform:type:id`.)
- **Search & scope.** Search must respect isolation (personal can't find group).
- **Mobile / IM degradation** of rich surfaces (approval cards, context sidebar).

### E. Admin / RBAC / multi-tenant
- **RBAC granularity.** Are owner/admin/member enough, or are **per-scope roles** needed?
- **Secret/key management.** `KEEL_SECRET_KEY` rotation (connector-token encryption) in Admin?
- **Multi-tenant (M4).** Does an **"organization"** layer above scope become necessary?
- **Audit log** viewer (cross-scope denials, token use, approvals) — entry point.

### F. Onboarding & first-run
- **Deploy profiles** (lite/standard/full, ADR-0008): what each enables.
- **First-run must set** an admin account **and** `KEEL_SECRET_KEY` (else connectors
  can't be stored) — force it in the wizard.

### G. Model / provider
- **Responses-API-only models** (Copilot `gpt-5.x`/codex) aren't reachable via the
  chat path today — the model picker should **grey them out with a reason** (or we
  land `provider-responses-api`).

## 6. Direction check — engine vs. product shape

The mockups make the gap explicit and non-alarming: **we built the engine; the
product-facing API + frontend is the large remaining body of work.** Concretely, the
screens imply backend surfaces that **do not yet exist**:

- **Agents CRUD API** (list/create/configure scoped agents) — today agents are
  constructed in code per surface.
- **Connectors OAuth flows + management API** — the `connectors`/`tokens` subsystem
  and confused-deputy guard exist in `keel-core`, but there are **no OAuth flows, no
  connector tools wired, no management endpoints**.
- **Approvals queue + RBAC** — today approval is an in-process HTTP handshake for one
  web run; no durable queue, no cross-surface delivery, no roles.
- **Memory editing API**, **sessions search API**, **admin/RBAC/IM-channel APIs**,
  **cost aggregation**.
- **The real React frontend** (ADR-0004: React/Vite/Tailwind/shadcn) vs. the current
  single server-rendered HTML. Decision needed: **when** to invest in it.

This reframes the roadmap: M2's "scheduler/jobs/scale" is the *autonomy* track, but
there is a parallel **"product shell" track** (agents/connectors/approvals/admin
API + React frontend) that these mockups scope. Sequencing the two is the main
planning decision coming out of this review.

## 7. Out of scope (this pass)

Visual polish beyond hi-fi wireframing; interaction/motion; empty/error/loading
states for every screen; accessibility audit; i18n strings; the actual React
implementation. These follow once the §5 decisions land.

## 8. Next steps

1. **Review** these mockups + this doc; correct any drift in the product picture.
2. **Resolve the §5 decisions** that block near-term work (A + B first — they gate
   connectors and safe autonomy).
3. **Decide sequencing:** product-shell track (agents/connectors/approvals/admin +
   React) vs. M2 autonomy track — and whether to start the React frontend now or
   keep extending the inline HTML through the next slice.
4. Fold resolved decisions into `ARCHITECTURE.md` (§6.5 connectors, §9 API) and the
   `IMPLEMENTATION-PLAN.md`; capture new ADRs where a decision is load-bearing
   (e.g., agent↔connector grant model, approval delivery).
