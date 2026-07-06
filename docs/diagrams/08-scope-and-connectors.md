# 08 — Agent scope, connectors & isolation [ADR-0009]

The pivot's core idea: **an agent is a scoped, persisted entity.** A group agent and a personal agent are the *same* abstraction with a different scope; **per-scope isolation** keeps a group/untrusted agent away from a personal agent's connectors, memory, and tokens.

```mermaid
flowchart TB
    subgraph inst["One Keel instance — hard per-scope isolation"]
        subgraph g["Scope: chat:qq:12345 — group agent"]
            ga["Group agent<br/>safe toolset · untrusted input"]
            gm["Shared group memory"]
            ga --- gm
        end
        subgraph p["Scope: user:ivy — personal agent"]
            pa["Personal agent<br/>trusted toolset"]
            pm["Private memory"]
            pc["Connectors<br/>Gmail · Calendar (granted to scope)"]
            pa --- pm
            pa --- pc
        end
        ga -.->|"cross-scope read DENIED"| pc
        ga -.->|"DENIED"| pm
    end
    note["Headline threat: cross-scope data exfiltration / confused deputy —<br/>injection (group msg · web page · email) coercing a leak; ranked above sandbox escape.<br/>Personal connectors need explicit grants; outbound actions need approval + audit.<br/>Recommend separate instances for the most sensitive personal use."]
```

## Pluggable execution environment

*Where* a tool runs is a swappable backend, so "server vs personal-machine" is a backend choice, not an architecture fork.

```mermaid
flowchart LR
    te["ToolExecutor"] --> env{{"ExecutionEnvironment"}}
    env -->|"v1 default"| sb["SandboxContainer<br/>server-side keel-sandbox · safest"]
    env -->|"lite"| ip["InProcess<br/>reduced isolation"]
    env -.->|"deferred"| ld["LocalDaemon<br/>user machine · local files<br/>own fail-closed gate"]
```
