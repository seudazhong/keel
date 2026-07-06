# 06 — Data model (core tables)

Abbreviated Postgres schema. `events` is the append-only source of truth; `messages`/`parts` are projections. Additions: `events.version` (G3, schema evolution), `passages.embedding_model`/`dim` (G8, model pinning), and the **scope / connector** model (ADR-0009) — agents are scoped entities, connectors are granted per scope, and `audit_log` records connector actions.

```mermaid
erDiagram
    users ||--o{ api_keys : owns
    users ||--o{ sessions : starts
    agents ||--o{ sessions : runs
    agents ||--o{ memory_blocks : has
    agents ||--o{ schedules : owns
    agents ||--o{ connector_grants : grants
    connectors ||--o{ connector_grants : granted_via
    sessions ||--o{ events : appends
    sessions ||--o{ messages : projects
    sessions ||--o{ audit_log : records
    messages ||--o{ parts : contains
    memory_blocks ||--o{ block_history : versions
    schedules ||--o{ jobs : enqueues
    kb_docs ||--o{ kb_chunks : chunked_into

    users {
        uuid id PK
        string email
        string role
    }
    api_keys {
        uuid id PK
        uuid user_id FK
        string hash
        jsonb scopes
    }
    agents {
        uuid id PK
        string name
        string scope
        jsonb model_slots
        jsonb tools_policy
        jsonb memory_config
    }
    sessions {
        uuid id PK
        uuid agent_id FK
        string key
        string status
        bigint tokens
        numeric cost
    }
    connectors {
        uuid id PK
        string kind
        string scope
        string token_ref
        string status
    }
    connector_grants {
        uuid id PK
        uuid agent_id FK
        uuid connector_id FK
    }
    audit_log {
        uuid id PK
        uuid session_id FK
        string actor
        string action
        string target
        timestamp ts
    }
    events {
        uuid id PK
        uuid session_id FK
        bigint seq
        string type
        int version
        jsonb payload
        timestamp ts
    }
    messages {
        uuid id PK
        uuid session_id FK
        bigint seq
        string role
    }
    parts {
        uuid id PK
        uuid message_id FK
        string kind
        jsonb content
    }
    memory_blocks {
        uuid id PK
        uuid agent_id FK
        string label
        text value
        int version
    }
    block_history {
        uuid id PK
        uuid block_id FK
        text value
        int version
    }
    passages {
        uuid id PK
        string scope
        text body
        vector embedding
        string embedding_model
        int dim
    }
    kb_docs {
        uuid id PK
        string source
        string status
    }
    kb_chunks {
        uuid id PK
        uuid doc_id FK
        text body
        vector embedding
    }
    schedules {
        uuid id PK
        uuid agent_id FK
        string spec
        timestamp next_run_at
        string status
    }
    jobs {
        uuid id PK
        uuid schedule_id FK
        string type
        string status
        int progress
        string result_ref
    }
```
