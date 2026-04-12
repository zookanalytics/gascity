# Supervisor WebSocket Contract

This directory contains the machine-readable contract for the supervisor/city
WebSocket transport introduced by issue `#646`.

Artifacts:

- `asyncapi.yaml` — the AsyncAPI contract for `GET /v0/ws`
- `generated/typescript` — Modelina-generated TypeScript DTOs
- `generated/golang` — Modelina-generated Go DTOs
- `generated/rust` — Modelina-generated Rust DTOs

The generated DTOs are contract artifacts, not the runtime transport layer.
The runtime websocket implementation in `internal/api` remains hand-written.

## Regenerate

```bash
cd contracts/supervisor-ws
npm ci
npm run generate
```

## Scope

The contract currently covers the shared websocket envelope types plus the
request payload models for the transport actions implemented in this branch.
