# ADR-0004 — Web frontend stack

**Status:** Accepted · **Date:** 2026-07-06

## Context
The web app needs rich streaming chat, a tool/step timeline, approvals, session search, admin panels, and trace views. It must be maintainable, componentised, and reusable inside an optional desktop shell.

## Decision
- **React + Vite + TypeScript**, **Tailwind CSS + shadcn/ui**, **TanStack Query** (server state), **Zustand** (UI state), and a small **SSE/WebSocket** client for run streams.
- Served as static assets by **nginx**, which proxies the `keel-server` API. An optional **Tauri** desktop shell (P2) reuses the same bundle.

## Alternatives considered
- **SvelteKit**: smaller/faster and great DX, but a smaller component/hiring ecosystem for a complex admin+chat app. Close second.
- **Next.js**: SSR is unnecessary for a self-hosted SPA behind auth and adds a Node server. Rejected.
- **Vue** (à la AstrBot/n8n/AIRI): viable; React chosen for ecosystem breadth and streaming-UI examples.

## Consequences
- Large, well-supported ecosystem and easy desktop reuse (Tauri).
- SPA-only (no SSR); fine for an authenticated self-hosted tool.
