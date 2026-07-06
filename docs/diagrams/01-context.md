# 01 — System context (C4 level 1)

Who talks to Keel, and what Keel talks to. Keel's primary form is a **server-side connected assistant** for teams/IM and individuals; personal data is reached via **per-scope OAuth connectors**.

```mermaid
flowchart TB
    dev["Dev / Ops<br/>(CLI, Web)"]
    team["Team member<br/>(Web, IM group)"]
    indiv["Individual<br/>(Web, IM DM)"]
    chat["Chat platforms<br/>(QQ / Telegram / WeCom)"]

    subgraph keelSys["Keel — self-hosted connected AI assistant"]
        core["Scoped agents · connectors · tools<br/>memory · scheduler · gateway"]
    end

    llm["LLM providers<br/>OpenAI / Anthropic / Google / local"]
    conn["Personal data via OAuth<br/>email · calendar · docs · knowledge"]
    mcp["MCP servers<br/>(local & remote)"]
    targets["Tool targets<br/>web · filesystem · shell"]

    dev -->|"HTTP / SSE / WS"| keelSys
    team -->|"HTTP / SSE / WS"| keelSys
    indiv -->|"HTTP / SSE / WS"| keelSys
    chat -->|"IM adapter"| keelSys
    keelSys -->|"chat / embed / rerank"| llm
    keelSys -->|"OAuth: read / send (per-scope)"| conn
    keelSys -->|"tools / resources"| mcp
    keelSys -->|"fetch / read / write / exec"| targets
```
