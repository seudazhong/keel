# 02 — Containers (C4 level 2)

Deployable units and their runtime dependencies. Every service imports `keel-core`; `keel-server` is stateless/scale-out, `keel-worker` scales horizontally, `keel-scheduler` is singleton-by-election. Tool execution is a **pluggable environment** (sandbox now; a user-machine `LocalDaemon` deferred). Connectors reach external SaaS over **per-scope OAuth**.

```mermaid
flowchart TB
    subgraph clients["Clients"]
        cli["keel-cli<br/>(admin / power-user)"]
        web["keel-web<br/>(React, nginx) — primary"]
        adp["adapters — primary<br/>(OneBot / Telegram / WeCom)"]
    end

    server["keel-server (FastAPI)<br/>API · auth · admin · SSE/WS hub<br/>gateway host · OAuth · run admission"]

    subgraph compute["Compute"]
        worker["keel-worker ×N<br/>agent turns + jobs"]
        sched["keel-scheduler<br/>leader-elected cron"]
        sandbox["keel-sandbox ×N<br/>tool exec (least-priv)"]
        local["LocalDaemon<br/>user machine · local files<br/>(deferred)"]
    end

    pg[("Postgres + pgvector")]
    redis[("Redis<br/>queue · bus · lock · cache")]
    minio[("MinIO (S3)<br/>blobs / spill")]
    lf["Langfuse<br/>traces / evals"]
    otel["OTel + Prometheus"]
    saas["External SaaS<br/>Gmail · MS Graph · Calendar · Notion"]

    cli -->|"HTTP/SSE/WS"| server
    web -->|"HTTP/SSE/WS"| server
    adp -->|"internal API / Redis"| server
    server -->|"enqueue (arq)"| redis
    redis -->|"deliver job"| worker
    server -->|"pub/sub events"| redis
    sched -->|"leader lock + enqueue"| redis
    worker -->|"exec RPC"| sandbox
    worker -.->|"exec RPC (deferred)"| local
    worker -->|"OAuth connectors (per-scope)"| saas
    server -->|"OAuth connect flow"| saas
    server --> pg
    worker --> pg
    sched --> pg
    worker --> minio
    worker --> lf
    worker --> otel
    server --> otel
```
