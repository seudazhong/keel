# 07 — Deployment profiles

Compose profiles from `ARCHITECTURE.md §15` and `ADR-0008`. Dashed edges show the footprint ladder from single-container `lite` up to `full`, plus the optional zero-key `demo` overlay.

```mermaid
flowchart TB
    subgraph lite["Profile: lite — single container"]
        l1["keel (embedded core)<br/>SQLite + local FS<br/>in-process scheduler / exec"]
    end

    subgraph dev["Profile: dev — fast inner loop"]
        d1["keel-server"]
        d2["keel-worker"]
        d3["keel-web"]
        d4[("postgres + pgvector")]
        d5[("redis")]
    end

    subgraph full["Profile: full — adds to dev"]
        f1["keel-scheduler"]
        f2["keel-sandbox"]
        f3[("minio")]
        f4["langfuse (+clickhouse)"]
        f5["otel-collector"]
        f6["prometheus"]
        f7["keel-embed"]
        f8["adapters/*"]
    end

    subgraph demo["Overlay: demo — zero-key showcase"]
        m1["ollama"]
        m2["seed data"]
    end

    subgraph ext["External — per-scope OAuth"]
        e1["Gmail · MS Graph<br/>Calendar · Notion"]
    end

    subgraph future["Deferred — pluggable execution"]
        fd1["LocalDaemon<br/>user machine · local files"]
    end

    lite -.->|"scale up"| dev
    dev -.->|"add services"| full
    full -.->|"zero-key demo"| demo
    full -->|"OAuth connectors"| ext
    full -.->|"local exec (later)"| future
```
