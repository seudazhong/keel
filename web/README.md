# Keel React web application

This directory contains the React 19 + TypeScript + Vite application. It is the current
product UI source, but it is **not** served by the Compose `keel-web` service: Compose still
mounts `web/stub/` into nginx as a static placeholder.

## Run locally

Start the API at `http://localhost:8000`, then:

```powershell
npm ci
npm run dev
```

Vite normally serves `http://localhost:5173` and proxies `/v1` and `/health` to the API.

## Checks

```powershell
npm run lint
npm run test
npm run build
```

Current routes cover chat, sessions/history, connectors, Knowledge, schedules, approvals,
overview, and model settings. The UI still uses the server's hard-coded `web:local` scope.
Identity, Agents CRUD/switching, Calendar, onboarding, Memory/Admin governance completeness,
responsive/i18n/a11y work, and production nginx packaging are on the
[roadmap](../docs/ROADMAP.md).

API access is centralized in `src/lib/api.ts`; feature requests/hooks live under
`src/features/`. The running server's `/docs` is authoritative for endpoint schemas.
