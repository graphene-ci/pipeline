# pipeline

The library a [graphene](https://github.com/graphene-ci/graphene)
pipeline author writes against — and the home of the types shared across
the system (the server and the agent import them from here).

A pipeline is an ordinary Temporal workflow. This library adds what plain
Temporal does not have:

- **resources with owned lifetimes** — `Machine` (cloud-created and
  owned, or ssh-recognized and untouchable), `Artifact`; the owner
  defaults to the current run, and the run's end tears down everything it
  owns;
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
| root (`pipeline`) | The user-facing library: resource declaration, machine execution, references |
| `id` | Identifier dictionary (suffix `Id`) |
| `ref` | `SecretRef`, `BlobRef`, `OwnerRef` |
| `wire` | Cross-component conventions: queue names, server activity names, search attributes |

## Build and check

```bash
make configure   # pinned tools into bin/, nothing global
make lint
make test
```
