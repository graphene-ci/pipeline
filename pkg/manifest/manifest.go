// Package manifest renders what a pipeline binary IS: the recording
// pass discovers activities and kinds, reflection turns the Params and
// Result Go types into gopherex schemapb schemas — the runtime
// form/validation descriptor a UI renders and a CLI validates against,
// in any of schemapb's four languages.
package manifest

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/gopherex/schemapb/go/schemapb"

	manifestpb "github.com/graphene-ci/pipeline/pkg/proto/manifest/v1"
)

// Build assembles the manifest of one pipeline. Triggers and the
// concurrency policy are pass-through declarations from the Main
// options; nil/empty mean none and the default policy.
func Build[P, R any](pipelineId string, activities, kinds []string,
	triggers []*manifestpb.Trigger, concurrency string, graph *manifestpb.Graph,
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
		Triggers:     triggers,
		Concurrency:  concurrency,
		Graph:        graph,
	}, nil
}

// SchemaOf reflects a Go type into a schemapb schema. The type's JSON
// shape is what travels, so json tags decide the field names.
func SchemaOf(t reflect.Type, id *schemapb.SchemaIdentity) (*schemapb.Schema, error) {
	// Coercion on: human input arrives as strings ("1h", "256") — the
	// schema converts them itself on resolve.
	root := schemapb.NewSchema(id).Coerce()
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct && t != timeType {
		fields, err := fieldsOf(t, map[reflect.Type]bool{})
		if err != nil {
			return nil, err
		}
		return root.Fields(rawDefs(fields)...).Build()
	}
	// A non-struct payload becomes one "value" field.
	f, err := fieldOf("value", t, false, map[reflect.Type]bool{})
	if err != nil {
		return nil, err
	}
	return root.Fields(f).Build()
}

var (
	timeType     = reflect.TypeFor[time.Time]()
	durationType = reflect.TypeFor[time.Duration]()
	rawJSONType  = reflect.TypeFor[json.RawMessage]()
)

func fieldsOf(t reflect.Type, visited map[reflect.Type]bool) ([]*schemapb.Schema_Field, error) {
	if visited[t] {
		return nil, nil
	}
	visited[t] = true
	defer delete(visited, t)
	var out []*schemapb.Schema_Field
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}
		name, omit, skip := jsonName(sf)
		if skip {
			continue
		}
		if sf.Anonymous && sf.Tag.Get("json") == "" {
			// Embedded struct: fields flatten, like encoding/json.
			et := sf.Type
			for et.Kind() == reflect.Pointer {
				et = et.Elem()
			}
			if et.Kind() == reflect.Struct {
				nested, err := fieldsOf(et, visited)
				if err != nil {
					return nil, err
				}
				out = append(out, nested...)
				continue
			}
		}
		required := sf.Type.Kind() != reflect.Pointer && !omit
		field, err := fieldOf(name, sf.Type, required, visited)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", sf.Name, err)
		}
		out = append(out, field)
	}
	return out, nil
}

// fieldOf builds one field descriptor for a Go type.
func fieldOf(name string, t reflect.Type, required bool, visited map[reflect.Type]bool) (*schemapb.Schema_Field, error) {
	fname := schemapb.FieldName(name)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
		required = false
	}
	var field *schemapb.Schema_Field
	switch {
	case t == timeType:
		field = schemapb.Timestamp(fname).Done()
	case t == durationType:
		field = schemapb.Duration(fname).Done()
	case t == rawJSONType:
		field = schemapb.JSON(fname).Done()
	case visited[t] && t.Kind() == reflect.Struct:
		// A cycle: the shape cannot be described finitely.
		field = schemapb.JSON(fname).Done()
	default:
		switch t.Kind() { //nolint:exhaustive // the default arm rejects everything unrepresentable
		case reflect.Struct:
			fields, err := fieldsOf(t, visited)
			if err != nil {
				return nil, err
			}
			field = schemapb.Object(fname, rawDefs(fields)...).Done()
		case reflect.Slice, reflect.Array:
			if t.Elem().Kind() == reflect.Uint8 {
				field = schemapb.Bytes(fname).Done()
				break
			}
			item, err := fieldOf("item", t.Elem(), true, visited)
			if err != nil {
				return nil, err
			}
			field = schemapb.List(fname, item).Done()
		case reflect.Map:
			value, err := fieldOf("value", t.Elem(), true, visited)
			if err != nil {
				return nil, err
			}
			field = schemapb.Map(fname, value).Done()
		case reflect.String:
			field = schemapb.Str(fname).Done()
		case reflect.Bool:
			field = schemapb.Bool(fname).Done()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field = schemapb.Int64(fname).Done()
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			field = schemapb.UInt64(fname).Done()
		case reflect.Float32, reflect.Float64:
			field = schemapb.Double(fname).Done()
		case reflect.Interface:
			field = schemapb.JSON(fname).Done()
		default:
			return nil, fmt.Errorf("unsupported kind %s", t.Kind())
		}
	}
	field.Required = required
	return field, nil
}

func rawDefs(fields []*schemapb.Schema_Field) []schemapb.FieldDef {
	out := make([]schemapb.FieldDef, 0, len(fields))
	for _, f := range fields {
		out = append(out, f)
	}
	return out
}

// jsonName resolves the traveling name of a struct field.
func jsonName(sf reflect.StructField) (name string, omitempty, skip bool) {
	tag := sf.Tag.Get("json")
	if tag == "-" {
		return "", false, true
	}
	parts := strings.Split(tag, ",")
	name = parts[0]
	if name == "" {
		name = sf.Name
	}
	for _, opt := range parts[1:] {
		if opt == "omitempty" {
			omitempty = true
		}
	}
	return name, omitempty, false
}
