package manifest

import (
	"reflect"
	"testing"
	"time"

	"github.com/gopherex/schemapb/go/schemapb"
)

type testParams struct {
	AgentId   string            `json:"agentId"`
	MarkerDir string            `json:"markerDir,omitempty"`
	Keep      time.Duration     `json:"keep"`
	Labels    map[string]string `json:"labels,omitempty"`
	Retries   *int              `json:"retries,omitempty"`
	Nested    struct {
		Deadline time.Time `json:"deadline"`
		Tags     []string  `json:"tags"`
	} `json:"nested"`
	Skipped string `json:"-"`
}

func TestSchemaOfShapes(t *testing.T) {
	s, err := SchemaOf(reflect.TypeFor[testParams](),
		schemapb.ID("graphene", "test-params", schemapb.Ver(0, 1, 0)))
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	byName := map[string]*schemapb.Schema_Field{}
	for _, f := range s.GetFields() {
		byName[f.GetName()] = f
	}
	if _, ok := byName["Skipped"]; ok {
		t.Fatal(`json:"-" must skip the field`)
	}
	if !byName["agentId"].GetRequired() {
		t.Fatal("plain field must be required")
	}
	if byName["markerDir"].GetRequired() || byName["retries"].GetRequired() {
		t.Fatal("omitempty/pointer must be optional")
	}
	if byName["keep"].GetDuration() == nil {
		t.Fatalf("keep must be a duration: %v", byName["keep"])
	}
	if byName["labels"].GetMap() == nil {
		t.Fatal("labels must be a map")
	}
	nested := byName["nested"].GetObject()
	if nested == nil {
		t.Fatal("nested must be an object")
	}
}

func TestBuildManifest(t *testing.T) {
	m, err := Build[testParams, struct {
		Report string `json:"report"`
	}]("perf-nightly", []string{"b", "a"}, []string{"vpc.network"}, nil, "")
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if m.GetPipelineId() != "perf-nightly" || m.GetActivities()[0] != "a" {
		t.Fatalf("manifest: %+v", m)
	}
	if m.GetParamsSchema() == nil || m.GetResultSchema() == nil {
		t.Fatal("schemas missing")
	}
}
