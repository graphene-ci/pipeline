package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/graphene-ci/temporal-entity/pkg/entclient"
	"github.com/graphene-ci/temporal-entity/pkg/entdefine"
	entity "github.com/graphene-ci/temporal-entity/pkg/entity"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
)

type recState struct {
	Info string `json:"info"`
}

// phasesClient answers describes with a sequence of phases.
type phasesClient struct {
	client.Client
	phases []entity.Phase
	calls  int
}

type phaseValue struct{ phase entity.Phase }

func (phaseValue) HasValue() bool { return true }

func (v phaseValue) Get(ptr any) error {
	raw, err := json.Marshal(map[string]any{"phase": v.phase, "state": recState{Info: "pg"}})
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, ptr)
}

func (c *phasesClient) QueryWorkflow(context.Context, string, string, string, ...any) (converter.EncodedValue, error) {
	if len(c.phases) == 0 {
		return nil, serviceerror.NewNotFound("no workflow")
	}
	p := c.phases[min(c.calls, len(c.phases)-1)]
	c.calls++
	return phaseValue{phase: p}, nil
}

func TestAwaitRecord(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		phases []entity.Phase
		code   string
	}{
		{name: "ready", phases: []entity.Phase{entity.PhaseReady}},
		{name: "ready after creating", phases: []entity.Phase{entity.PhaseCreating, entity.PhaseReady}},
		{name: "never declared", code: CodeRecordMissing},
		{name: "being deleted", phases: []entity.Phase{entity.PhaseDeleting}, code: CodeRecordGone},
		{name: "failed", phases: []entity.Phase{entity.PhaseCreateFailed}, code: CodeRecordGone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			def := entdefine.New[struct{}, recState]("test.rec")
			cl := entclient.Bind(def, &phasesClient{phases: c.phases}, "q")
			st, err := AwaitRecord(context.Background(), cl, "test.rec", "pg")
			if c.code == "" {
				if err != nil || st.Info != "pg" {
					t.Fatalf("state %+v err %v", st, err)
				}
				return
			}
			var app *temporal.ApplicationError
			if !errors.As(err, &app) || app.Type() != c.code || !app.NonRetryable() {
				t.Fatalf("err = %v, want %s", err, c.code)
			}
		})
	}
}
