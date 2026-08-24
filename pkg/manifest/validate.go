package manifest

// Params normalization against the manifest's schema — ONE language for
// every door: the server validates a submit with it, the binary's own
// CLI runs the same code so a value the door would coerce ("15m" into a
// duration) is never refused client-side.

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	schemapb "github.com/gopherex/schemapb/go/schemapb"
	"google.golang.org/protobuf/encoding/protojson"

	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
)

// NormalizeParams checks the params JSON against the manifest's params
// schema and returns the normalized form (coerced strings resolved,
// durations in Go wire form — nanosecond numbers). Best-effort by
// design: an absent or unreadable manifest/schema never blocks — only
// a schema violation does.
func NormalizeParams(manifestJSON json.RawMessage, params []byte) ([]byte, error) {
	var m manifestpb.Manifest
	if protojson.Unmarshal(manifestJSON, &m) != nil {
		return params, nil
	}
	schema := m.GetParamsSchema()
	if schema == nil {
		return params, nil
	}
	values := map[string]any{}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &values); err != nil {
			return nil, fmt.Errorf("params are not a JSON object: %w", err)
		}
	}
	numericDurations(schema, values)
	_, res, err := schema.Bake(values)
	if err != nil {
		// A schema that does not compile is the record's problem, not
		// the caller's.
		return params, nil
	}
	if res != nil && res.Blocking() {
		parts := make([]string, 0, len(res.GetErrors()))
		for _, e := range res.GetErrors() {
			part := e.GetCode().String()
			if e.GetPath() != "" {
				part = e.GetPath() + ": " + part
			}
			if e.GetConstraint() != "" {
				part += " (" + e.GetConstraint() + ")"
			}
			parts = append(parts, part)
		}
		return nil, fmt.Errorf("params do not match the pipeline's manifest: %s", strings.Join(parts, "; "))
	}
	normalized, err := json.Marshal(values)
	if err != nil {
		return params, nil
	}
	return normalized, nil
}

// numericDurations rewrites nanosecond numbers on duration-typed fields
// into time.Duration, walking nested objects, lists, and maps — the Go
// wire form schemapb does not know.
func numericDurations(s *schemapb.Schema, values map[string]any) {
	if s == nil {
		return
	}
	for _, f := range s.GetFields() {
		v, ok := values[f.GetName()]
		if !ok {
			continue
		}
		values[f.GetName()] = numericValue(f, v)
	}
}

func numericValue(f *schemapb.Schema_Field, v any) any {
	switch {
	case f.GetDuration() != nil:
		if n, ok := v.(float64); ok {
			return time.Duration(int64(n))
		}
	case f.GetObject() != nil:
		if m, ok := v.(map[string]any); ok {
			numericDurations(f.GetObject().GetSchema(), m)
		}
	case f.GetList() != nil:
		items := f.GetList().GetItems()
		if list, ok := v.([]any); ok && len(items) == 1 {
			for i, item := range list {
				list[i] = numericValue(items[0], item)
			}
		}
	case f.GetMap() != nil:
		if m, ok := v.(map[string]any); ok {
			for _, mv := range m {
				if mm, ok := mv.(map[string]any); ok {
					numericDurations(f.GetMap().GetValueSchema(), mm)
				}
			}
		}
	}
	return v
}
