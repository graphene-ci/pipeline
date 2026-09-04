package pipeline

import (
	"errors"
	"testing"
)

type tagParams struct {
	Count int    `json:"count" validate:"min=1,max=10"`
	Env   string `json:"env" validate:"oneof=dev prod"`
}

type customParams struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

func (p customParams) Validate() error {
	if p.End <= p.Start {
		return errors.New("end must be after start")
	}
	return nil
}

func TestCheckParamsTags(t *testing.T) {
	if err := checkParams(tagParams{Count: 3, Env: "dev"}); err != nil {
		t.Fatalf("valid params rejected: %v", err)
	}
	if err := checkParams(tagParams{Count: 0, Env: "dev"}); err == nil {
		t.Error("min=1 not enforced (count=0 accepted)")
	}
	if err := checkParams(tagParams{Count: 3, Env: "staging"}); err == nil {
		t.Error("oneof not enforced (env=staging accepted)")
	}
}

func TestCheckParamsValidatable(t *testing.T) {
	if err := checkParams(customParams{Start: 1, End: 5}); err != nil {
		t.Fatalf("valid custom params rejected: %v", err)
	}
	if err := checkParams(customParams{Start: 5, End: 1}); err == nil {
		t.Error("Validate() not run (end<=start accepted)")
	}
}

func TestCheckParamsNonStruct(t *testing.T) {
	// A map/slice payload has no field tags — not a failure, nothing to do.
	if err := checkParams(map[string]int{"n": 1}); err != nil {
		t.Errorf("non-struct params must not error: %v", err)
	}
}
