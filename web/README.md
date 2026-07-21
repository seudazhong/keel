# Keel React application

This directory contains the React 19 + TypeScript + Vite product UI.

The standard Compose `keel-web` image builds this application and serves it through nginx on
`http://localhost:3000/`, with API/health/readiness proxying and SPA fallback.

## Run locally

Start the API at `http://localhost:8000`, then:

```powershell
npm ci
npm run dev
```

Vite normally serves `http://localhost:5173`.

## Checks

```powershell
npm run lint
npm run test
npm run build
npm run test:e2e
```

## Current routes

- onboarding;
- chat and durable session resume;
- sessions/search/history;
- Memory and consolidation proposals;
- Knowledge;
- Agents (Preview);
- Projects (Preview);
- Connectors;
- Approvals;
- Schedules;
- Jobs;
- Observability;
- Settings.

Read-only review and controlled patch proposals do not have routes yet.

## Authentication boundary

The UI supports explicit local preview and directly supplied API-key/bearer credentials with
organization/Agent selection. It does not implement OIDC authorization code + PKCE or a secure
server-managed browser session.

Secrets are tab-scoped in `sessionStorage`, never `localStorage`, but this remains a preview
compatibility design rather than the production login target.

## Architecture

API access is centralized in `src/lib/api.ts`; feature queries/mutations live under
`src/features/`; routing is in `src/router.tsx`.

Use the running server's `/docs` for endpoint schemas and
[`docs/STATUS.md`](../docs/STATUS.md) for product maturity.
