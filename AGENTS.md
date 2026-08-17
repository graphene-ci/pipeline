# AGENTS.md — pipeline

The user-facing pipeline library of graphene vision v3 (`../GRAPHENE.MD`
at the org root) and the shared vocabulary of the system: the server and
the agent import types from here — nothing here imports them back.

## Before making changes

1. Read `../GRAPHENE.MD`. A change that contradicts the vision updates
   the vision first.
2. `make lint` and `make test` must be green before push.

## Code rules

- Go; code, names, and comments in English. Commits are Conventional
  Commits, no `Co-Authored-By`.
- Identifiers are the `id` types, suffix `Id` (the var-naming exception
  is recorded in `.golangci.yaml`).
- Secrets and large data never enter specs, logs, or Temporal history —
  references only (`ref`).
- Machine-execution helpers take named registered functions, never
  closures.
- The machine-execution pair naming (`OnMachine`/`Action`) is
  provisional — do not spread the names further until decided.

## Package boundaries

- `id`, `ref` — vocabulary; import nothing from this repository and no
  Temporal packages.
- `wire` — cross-component conventions (queue names, server activity
  names, search attribute keys).
- root (`pipeline`) — the user-facing workflow helpers; the server
  contract they speak is the activity names in `wire`.
- `flow/*` — temporal flows of the system resources: definition + `Ops`
  contract; `Ops` implementations live in the graphene server. Flow
  packages import the root package for shared types, never the other way
  around.
- No server code and no agent code here — those live in the graphene and
  agent repositories.
