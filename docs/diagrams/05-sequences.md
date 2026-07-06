# 05 — Key sequences

## A. Interactive run (any surface)

```mermaid
sequenceDiagram
    autonumber
    participant S as Surface (CLI/Web/IM)
    participant API as keel-server
    participant Q as Redis (arq + bus)
    participant W as keel-worker
    participant SB as keel-sandbox
    participant DB as Postgres
    participant LF as Langfuse

    S->>API: POST /sessions/{id}/messages (input)
    API->>DB: admit input (durable)
    API->>Q: enqueue run (arq)
    API-->>S: 202 + run_id
    S->>API: open SSE /sessions/{id}/events
    Q->>W: deliver run job
    loop agent turns
        W->>W: keel-core.run() assemble + provider stream
        W->>SB: execute risky tool (RPC)
        SB-->>W: tool result (bounded)
        W->>Q: publish events
        Q-->>API: fan-out
        API-->>S: SSE (deltas, tool, approval)
    end
    W->>DB: projections + cost
    W->>LF: trace + score
    W->>Q: run.ended (reason)
    Q-->>API: fan-out
    API-->>S: run.ended
```

## B. Approval round-trip (durable, fail-closed — DESIGN-REVIEW G5)

```mermaid
sequenceDiagram
    autonumber
    participant W as keel-worker
    participant DB as Postgres (events)
    participant Q as Redis bus
    participant S as Surface

    W->>W: tool call hits permission gate (ask)
    W->>DB: persist approval.requested (pending, TTL)
    W->>Q: publish approval.requested (correlation_id)
    Q-->>S: approval prompt
    alt user responds in time
        S->>Q: approval.resolved (allow/deny)
        Q-->>W: resolution
        W->>DB: persist approval.resolved
    else TTL expires or no subscriber
        W->>W: fail-closed, deny
        W->>DB: persist approval.resolved (denied, timeout)
    end
    W->>W: continue or skip tool per resolution
```

## C. Scheduled job (at-most-once)

```mermaid
sequenceDiagram
    autonumber
    participant SC as keel-scheduler (leader)
    participant DB as Postgres (schedules)
    participant Q as Redis (arq)
    participant W as keel-worker

    Note over SC: holds Redis leader lock
    loop every tick
        SC->>DB: select due rows
        SC->>DB: advance next_run_at (before enqueue)
        SC->>Q: enqueue run (arq)
    end
    Q->>W: deliver job
    W->>W: run agent in target session
    W->>DB: events + result
    Note over W: overrun leads to hard interrupt
```

## D. IM message (QQ / OneBot)

```mermaid
sequenceDiagram
    autonumber
    participant OB as OneBot (QQ)
    participant AD as adapter
    participant API as keel-server
    participant W as keel-worker

    OB->>AD: raw event
    AD->>AD: normalize to InboundEvent (session_key)
    AD->>AD: wake rules · rate limit · safe toolset
    AD->>API: admit (untrusted input)
    API->>W: run (constrained toolset)
    W-->>AD: events to MessageChain
    AD->>OB: reply
```
