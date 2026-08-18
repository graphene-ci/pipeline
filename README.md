# pipeline

The library a [graphene](https://github.com/graphene-ci/graphene)
pipeline author writes against — and the home of the types shared across
the system (the server and the agent import them from here).

A pipeline is an ordinary Temporal workflow. This library adds what plain
Temporal does not have:

- **the Resource handle** — declaring a resource returns a handle
  immediately; outputs are reachable only through `Ready()`, so an
  unready resource cannot be used by construction, and declaring several
  in a row runs their creation in parallel (Temporal's Future model
  lifted to resources);
- **resources with owned lifetimes** — `Machine` (a LINK between a real
  machine created by whatever the user chose and its agent; the record
  acts only in the ssh-install case and never creates machines),
  `Artifact`; the owner defaults to the current run, and the run's end
  tears down everything it owns;
- **`Main`** — the entry point of a pipeline binary (one main == one
  pipeline): reads its role from the environment (run worker / machine
  container) and serves the right queue;
- **execution on machines** — a converging call (at-least-once, retried)
  and a one-shot `Action` (MaximumAttempts=1; an undeterminable outcome
  surfaces as `ErrUnknown`, never as a silent re-execution) — both are
  plain Temporal activities on the per-(machine × run) queue served by
  the user's own code hosted by the agent;
- **references instead of values** — `SecretRef`, `BlobRef`: only names
  travel through specs, logs, and history.

The naming of the machine-execution pair (`OnMachine`/`Action`) is
provisional.

## Layout

| Package | What it is |
|---|---|
| `pkg/pipeline` | The user-facing library: resource declaration, machine execution, references |
| `pkg/id` | Identifier dictionary (suffix `Id`) |
| `pkg/ref` | `SecretRef`, `BlobRef`, `OwnerRef` |
| `pkg/wire` | Cross-component conventions: queue names, server activity names, search attributes |
| `pkg/flow/machine`, `pkg/flow/artifact` | Temporal flows of the system resources (definitions on the temporal-entity chassis + lifecycle code); the graphene server registers them on its worker and implements their `Ops` |

## Build and check

```bash
make configure   # pinned tools into bin/, nothing global
make lint
make test
```
