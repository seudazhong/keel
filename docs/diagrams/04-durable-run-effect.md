# Durable run and effect lifecycle

```mermaid
flowchart LR
    REQUEST["Web / IM / API request"]
    AUTH["Resolve actor, Agent, grants"]
    INPUT["Persist admitted input"]
    RUNROW["Create / repair durable run"]
    DISPATCH["Queue transition + dispatch intent<br/>atomic"]
    QUEUE["Redis / arq delivery"]
    RECONCILE["Postgres reconciler"]
    CLAIM["Worker fenced claim"]
    LOOP["Bounded Agent loop"]
    APPROVAL["Durable approval"]
    EFFECT["Trusted effect broker"]
    UNKNOWN["Target effect ledger<br/>unknown -> reconcile"]
    SANDBOX["Untrusted sandbox"]
    END["Terminal run + events"]

    REQUEST --> AUTH
    AUTH --> INPUT
    INPUT --> RUNROW
    RUNROW --> DISPATCH
    DISPATCH --> QUEUE
    RECONCILE --> DISPATCH
    QUEUE --> CLAIM
    CLAIM --> LOOP
    LOOP --> SANDBOX
    SANDBOX --> LOOP
    LOOP --> APPROVAL
    APPROVAL --> LOOP
    LOOP --> EFFECT
    EFFECT --> LOOP
    EFFECT -. ambiguous outcome .-> UNKNOWN
    UNKNOWN -. reconciled result .-> EFFECT
    LOOP --> END
```

Admission is staged and idempotently repairable; the queued transition and dispatch intent share
the critical atomic boundary. Duplicate delivery is fenced by durable state. The `unknown` effect
ledger is the target generic contract; current providers implement differing reconciliation
strength. Approval binds the exact effect or immutable patch revision.
