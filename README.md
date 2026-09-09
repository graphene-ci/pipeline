# pipeline

The Go SDK an author writes a Graphene pipeline with. One ordinary Go binary
holds the typed params and result, the run workflow, resource declarations and
machine actions. That same binary exposes the `plan`, `push` and `run` commands
and, in server mode, serves the worker queues it needs.

On top of Temporal the SDK adds Graphene's product model:

- non-blocking typed resource handles and an explicit `Ready`;
- the ownership tree, cascading delete, stands and bounded lifetime;
- agents for existing and created machines, selections and action fan-out;
- activity retry guarantees, including at-most-once for irreversible work;
- artifacts, secret/var references, triggers and cross-pipeline links;
- declared network/data flows and attribution of events, logs, metrics and traces.

The user guide and SDK reference are in the
[Graphene docs](https://graphene-ci.github.io/docs/sdk/main).

## Install

Releases: [github.com/graphene-ci/pipeline/releases](https://github.com/graphene-ci/pipeline/releases).

Add the SDK to a pipeline module:

```bash
go get github.com/graphene-ci/pipeline@latest   # or @v0.1.1
```

A minimal pipeline:

```go
package main

import "github.com/graphene-ci/pipeline/pkg/pipeline"

type Params struct {
	Msg string `json:"msg" validate:"required"`
}
type Result struct {
	Echo string `json:"echo"`
}

func run(_ pipeline.Context, p Params) (Result, error) {
	return Result{Echo: "echo: " + p.Msg}, nil
}

func main() { pipeline.Main("echo", run) }
```

The binary then offers `plan` (local, no server), `push` and `run`.

## Packages

| Path | Purpose |
|---|---|
| `pkg/pipeline` | `Main`, `Prepare`, the run context, resources, agents, flows and the built-in CLI |
| `pkg/pipelinetest` | isolated Graphene contract simulator on Temporal testsuite |
| `pkg/activity` | agent actions and execution guarantees |
| `pkg/artifact`, `pkg/file` | artifact and file sources |
| `pkg/trigger` | manual, cron, webhook and upstream triggers |
| `pkg/obs` | attributed telemetry from user code |
| `pkg/id`, `pkg/ref`, `pkg/wire` | identifiers, references and wire conventions |
| `pkg/flow/*` | durable system-resource definitions |

## Build and check

```bash
make configure
make lint
make test
make build
```

## Локальные тесты пайплайнов

`pkg/pipelinetest` подключается к Temporal `TestWorkflowEnvironment` и проверяет
пайплайн с моделью агентов, ресурсов, артефактов и владения. Пользовательские
activities требуют явных подмен; инфраструктура не запускается. Руководство и
границы модели: [локальные тесты](https://graphene-ci.github.io/docs/sdk/testing).

`make test` включает тесты readiness, capability selection, cleanup, TTL,
отмены, retry и at-most-once. Для проверки конкурентного исполнения:

```bash
go test -race ./pkg/pipelinetest/...
```

## Release

A pushed semver tag (`vX.Y.Z`) is the release — the Go module proxy serves it,
and the release workflow creates the matching GitHub Release page:

```bash
make ver v=0.1.1        # or: make bump TYPE=patch
```
