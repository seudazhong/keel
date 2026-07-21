# IM adapters

Keel currently hosts OneBot v11 and Telegram gateway implementations inside `keel-server`.

They provide:

- authenticated webhook ingestion;
- durable replay protection;
- normalized inbound messages and wake/rate policy;
- organization/Agent channel mappings;
- worker-owned durable runs;
- durable encrypted reply intents and retry;
- constrained read-oriented tools for untrusted IM input.

Current limitations:

- there is no complete React IM administration surface;
- Web/IM continuity has backend primitives but no finished team product journey;
- adapters are hosted in the server rather than independently scaled gateway services;
- WeCom, Slack, Discord, and personal-WeChat bridges are not implemented.

Do not expose an IM endpoint without its provider signing secret and cloud-mode fail-closed
configuration.

See [Architecture](../docs/ARCHITECTURE.md), [Identity](../docs/IDENTITY.md), and
[Status](../docs/STATUS.md).
