// Package manifest renders what a pipeline binary IS: the recording pass
// discovers activities and kinds, and schemapb's own reflection turns the
// Params and Result Go types into schemapb schemas — the runtime
// form/validation descriptor a UI renders and a CLI validates against, in
// any of schemapb's languages. The one thing schemapb cannot know is our
// domain type ref.SecretRef; SchemaOf teaches it that through WithType.
package manifest

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/gopherex/schemapb/go/schemapb"

	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
	"github.com/graphene-ci/pipeline/pkg/ref"
)

// Build assembles the manifest of one pipeline. Triggers and the
// concurrency policy are pass-through declarations from the Main
// options; nil/empty mean none and the default policy.
func Build[P, R any](pipelineId string, activities, kinds []string,
	triggers []*manifestpb.Trigger, concurrency string, graph *manifestpb.Graph,
	kindDecls ...*manifestpb.KindDecl,
) (*manifestpb.Manifest, error) {
	params, err := SchemaOf(reflect.TypeFor[P](),
		schemapb.ID("graphene", schemapb.SchemaName(pipelineId+"-params"), schemapb.Ver(0, 1, 0)))
	if err != nil {
		return nil, fmt.Errorf("params schema: %w", err)
	}
	result, err := SchemaOf(reflect.TypeFor[R](),
		schemapb.ID("graphene", schemapb.SchemaName(pipelineId+"-result"), schemapb.Ver(0, 1, 0)))
	if err != nil {
		return nil, fmt.Errorf("result schema: %w", err)
	}
	sort.Strings(activities)
	sort.Strings(kinds)
	return &manifestpb.Manifest{
		PipelineId:   pipelineId,
		ParamsSchema: params,
		ResultSchema: result,
		Activities:   activities,
		Kinds:        kinds,
		KindDecls:    kindDecls,
		Triggers:     triggers,
		Concurrency:  concurrency,
		Graph:        graph,
	}, nil
}

// secretRefType is the one domain type schemapb's reflection cannot know.
var secretRefType = reflect.TypeFor[ref.SecretRef]()

// SchemaOf reflects a Go type into a schemapb schema. Shapes, stdlib types
// (time.Time, time.Duration, json.RawMessage) and the go-playground/
// validator tag vocabulary are all schemapb.Reflect's own job now; the only
// thing we add is our domain type: ref.SecretRef becomes a secret-name
// field — a picker in a UI, existence-checked at the door, masked in errors.
func SchemaOf(t reflect.Type, id *schemapb.SchemaIdentity) (*schemapb.Schema, error) {
	return schemapb.Reflect(t, id,
		schemapb.WithType(secretRefType, func(name schemapb.FieldName) *schemapb.Schema_Field {
			return schemapb.Str(name).Secret().Done()
		}))
}
