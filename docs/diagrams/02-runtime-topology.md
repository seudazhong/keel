# Runtime topology

```mermaid
flowchart TB
    WEB["keel-web<br/>nginx + React"]
    SERVER["keel-server<br/>API / identity / admission"]
    WORKER["keel-worker<br/>runs / jobs / current effect brokers"]
    SANDBOX["keel-sandbox<br/>untrusted file execution"]

    PG[("PostgreSQL<br/>source of truth")]
    REDIS[("Redis<br/>delivery / fan-out / locks")]
    STORAGE[("Shared project storage")]
    PROVIDERS["Model + connector providers"]
    GITHUB["GitHub App API"]

    WEB --> SERVER
    SERVER --> PG
    SERVER --> REDIS
    WORKER --> PG
    WORKER --> REDIS
    SERVER --> SANDBOX
    WORKER --> SANDBOX
    WORKER --> STORAGE
    SERVER --> STORAGE
    WORKER --> PROVIDERS
    SERVER --> PROVIDERS
    WORKER --> GITHUB
    SERVER --> GITHUB
```

Current trust boundaries:

- server and worker are trusted control/orchestration processes;
- trusted connector/Git effect authority currently exists in both server routes and workers;
- sandbox is credential-less and untrusted;
- Postgres owns lifecycle state; Redis is not the sole durable record.

The production target separates orchestration workers from credentialed effect brokers.
