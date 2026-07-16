# Mounted configuration

This directory is reserved for non-secret configuration files mounted into Keel services.
The current Compose stack does not yet mount a complete production configuration set; most
runtime settings come from defaults, `.env`, and `KEEL_*` environment variables.

Configuration precedence is intended to be:

1. package defaults;
2. mounted configuration files;
3. environment variables;
4. durable/runtime overrides where implemented.

Do not store provider keys, OAuth client secrets/tokens, `KEEL_SECRET_KEY`, or API keys
here. Use environment variables or an external secret manager. Production-grade secret
envelope/KMS integration and rotation remain roadmap work.

See [Operations](../../docs/OPERATIONS.md), [`.env.example`](../../.env.example), and the
current settings model in `packages/keel-core/src/keel_core/config.py`.
