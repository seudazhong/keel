# Webhook connector

The `webhook` provider receives JSON events and has no outbound actions.

## Setup

Run generic connector setup with no fields. Keel creates a unique endpoint and a random signing
secret. The endpoint and secret are returned as setup artifacts with `Cache-Control: no-store`.
The secret is stored only in encrypted connector credentials and is never placed in binding
metadata. Copy it immediately; Keel does not display it again.

Running setup again keeps the endpoint identifier and rotates the signing secret. Normal connector
revoke deletes the local binding, replay state, and encrypted credential.

Configure the provider's required trigger-session target before sending deliveries.

## Request format

Send `POST` with `Content-Type: application/json` (an optional `charset=utf-8` is accepted) and:

- `X-Keel-Webhook-Id`: stable delivery identifier containing 1-128 letters, digits, `.`, `_`, `:`,
  or `-`;
- `X-Keel-Webhook-Timestamp`: Unix timestamp in decimal seconds;
- `X-Keel-Webhook-Signature`: lowercase SHA-256 signature prefixed by `v1=`.

The signed bytes are:

```text
timestamp + "." + delivery_id + "." + raw_request_body
```

Compute the header as:

```text
v1=hex(HMAC-SHA256(signing_secret, signed_bytes))
```

Keel requires the timestamp to be within five minutes, limits the raw body to 60,000 bytes,
validates UTF-8 JSON, and rejects non-finite JSON constants. Authentication occurs before the
shared replay claim. Reusing a delivery id with the same payload is acknowledged without emitting
another event; reusing it with another payload is rejected.

Accepted payloads become `webhook.received` tainted durable events with connector, binding,
delivery, source URL, payload hash, and event-id provenance. The provider does not implement a
verification challenge because its generated endpoint and HMAC setup do not require one.
