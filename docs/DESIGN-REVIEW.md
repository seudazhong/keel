# Original design review — historical summary

The full pre-implementation review was removed from the working tree to reduce stale-document
weight. Retrieve the complete snapshot with:

```powershell
git show d0dd310:docs/DESIGN-REVIEW.md
```

Current authority:

- [Product requirements](./PRD.md)
- [Architecture](./ARCHITECTURE.md)
- [Invariants](./INVARIANTS.md)
- [Roadmap](./ROADMAP.md)

## Historical finding identifiers

Some code comments and early migrations retain these identifiers:

| Finding | Historical topic | Current authority |
|---|---|---|
| G3 | Event evolution/upcasting | [Event versioning](./EVENT-VERSIONING.md) |
| G4 | Retention and erasure | [Data lifecycle](./DATA-LIFECYCLE.md) |
| G5 | Durable approvals | [Invariants](./INVARIANTS.md) |
| G6 | Import is not trust | [Invariants](./INVARIANTS.md) |
| G8 | Embedding model/dimension pinning | [Memory](./MEMORY.md), [Knowledge](./KNOWLEDGE.md) |
| G9 | Secret envelope encryption | [Architecture](./ARCHITECTURE.md) |
| G10 | Provider/session/chat rate limiting | [Architecture](./ARCHITECTURE.md) |
| G14 | Additive API compatibility | [Event versioning](./EVENT-VERSIONING.md) |
| G16 | Scope/data isolation | [ADR-0011](./adr/0011-product-boundary-and-domain-model.md) |
| G17 | Taint/confused-deputy defense | [Architecture](./ARCHITECTURE.md), [Invariants](./INVARIANTS.md) |
| G18 | Connector credential lifecycle | [Connector providers](./connectors/README.md) |
| G19 | Connectors use the single Tool interface | [Connector providers](./connectors/README.md) |
| G20 | External-effect idempotency | [Invariants](./INVARIANTS.md) |

See [Historical documentation](./HISTORY.md) for the remaining removed snapshots.
