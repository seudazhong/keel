# Mounted configuration

This directory is reserved for non-secret configuration mounted into Keel services.

Current services primarily use typed defaults plus `KEEL_*` environment variables. Mounted
configuration may be added where it improves operability, but secret values do not belong here.

Never store:

- provider or API keys;
- OAuth client secrets or refresh/access tokens;
- GitHub private keys or webhook secrets;
- sandbox RPC secret;
- runtime database password;
- envelope-encryption key material.

Use environment/file references or an external secret manager. Keel supports versioned
envelope-encryption keys for connector credentials, but production KMS/secret-manager integration is
still a roadmap gate.

See [Operations](../../docs/OPERATIONS.md), [`.env.example`](../../.env.example), and
`packages/keel-core/src/keel_core/config.py`.
