package pipeline

import "reflect"

// optimisticZero builds the value a resource "returns" during the
// recording pass: a zero value with every reachable bool set to TRUE.
// The pass walks the pipeline once to DISCOVER declarations; user code
// legitimately guards on readiness ("if !state.AgentConnected return") —
// pessimistic zeros would end the walk before the declarations after
// the guard. Optimism keeps the common path walking; values are still
// fakes and nothing executes.
func optimisticZero[T any]() T {
	var v T
	rv := reflect.ValueOf(&v).Elem()
	setBoolsTrue(rv)
	return v
}

func setBoolsTrue(v reflect.Value) {
	switch v.Kind() {
	case reflect.Bool:
		if v.CanSet() {
			v.SetBool(true)
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			setBoolsTrue(v.Field(i))
		}
	case reflect.Ptr:
		if v.CanSet() && v.IsNil() && v.Type().Elem().Kind() == reflect.Struct {
			v.Set(reflect.New(v.Type().Elem()))
		}
		if !v.IsNil() {
			setBoolsTrue(v.Elem())
		}
	default:
	}
}
