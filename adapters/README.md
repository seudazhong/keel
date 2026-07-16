# IM adapters

Keel currently hosts gateway implementations inside `keel-server`:

- OneBot v11 (QQ): `packages/keel-server/src/keel_server/gateway/onebot.py`
- Telegram: `packages/keel-server/src/keel_server/gateway/telegram.py`

The webhook endpoints are under `/v1/gateway`. They normalize inbound messages, apply wake
and rate-limit policy, run a constrained toolset, and render replies through the platform
API.

Important current limits:

- adapters do not yet run as separate packages in this directory;
- webhook authentication/replay protection is not implemented;
- Web and IM do not yet share a complete durable identity/Agent/run topology;
- the active scope/Agent model is still hard-coded;
- WeCom, Discord, Slack, and personal-WeChat bridges are not implemented.

Do not expose gateway endpoints to the public internet until the
[Cloud Safety Foundation](../docs/ROADMAP.md#m31--cloud-safety-foundation) gates are complete.
See [Architecture](../docs/ARCHITECTURE.md#123-im-gateway-adapters) for the target and
[Operations](../docs/OPERATIONS.md) for current deployment warnings.
