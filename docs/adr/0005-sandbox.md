# ADR-0005 — Sandbox & tool execution isolation

**Status:** Accepted · **Date:** 2026-07-06

## Context
Keel's `bash`/`powershell`, code, and JS-rendering web tools execute model-chosen actions. Untrusted input (IM/web content) can attempt prompt-injection → tool abuse. We need containerisable, defense-in-depth isolation that still works under `docker compose`.

## Decision
- **Two-level sandbox** (per the field manual):
  1. **Process/container level** — a dedicated **`keel-sandbox`** service/image running shell/code tools with least privilege: read-only root FS, `tmpfs` scratch, dropped Linux capabilities, **network denied by default**, workspace bind-mount only, CPU/memory limits, non-root user.
  2. **Per-command policy** — every command passes the permission engine (allow/ask/deny) and path/egress checks (workspace-only; deny `.git`/`.env`; SSRF-safe fetch).
- Workers dispatch tool execution to the sandbox (RPC), keeping dangerous execution out of the core/worker address space.

## Alternatives considered
- **Run tools in-worker** with `bubblewrap`/`nsjail`: lighter, but couples execution to the worker and is Linux-host-specific. Available as an advanced local option.
- **Ephemeral container per command** via Docker socket: strong isolation but needs a mounted Docker socket (privilege risk) and higher latency. Offered as an opt-in "strict" mode.
- **microVM (Firecracker) / gVisor**: strongest isolation; documented as an advanced hardening path, not the default (heavier ops).

## Consequences
- Safe-by-default execution that composes cleanly; network-off default may require explicit allow for tools that need egress.
- A `powershell` sandbox implies a Windows-capable executor variant or `pwsh` on Linux; documented per-target.
