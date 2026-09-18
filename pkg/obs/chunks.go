package obs

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// bisectMessage halves an OTLP request at one of its collection boundaries
// (the named repeated fields, outermost first). Cloning preserves all
// metadata and unknown protobuf fields without mutating the caller's
// request; ok is false when nothing can be cut any further.
func bisectMessage(message protoreflect.Message, names []protoreflect.Name) (protoreflect.Message, protoreflect.Message, bool) {
	for _, name := range names {
		field := message.Descriptor().Fields().ByName(name)
		if field == nil || field.Kind() != protoreflect.MessageKind || !message.Has(field) {
			continue
		}
		left := proto.Clone(message.Interface()).ProtoReflect()
		right := proto.Clone(message.Interface()).ProtoReflect()
		if field.IsList() {
			list := message.Get(field).List()
			if list.Len() > 1 {
				middle := list.Len() / 2
				left.Mutable(field).List().Truncate(middle)
				tail := right.Mutable(field).List()
				for i := range list.Len() - middle {
					tail.Set(i, tail.Get(middle+i))
				}
				tail.Truncate(list.Len() - middle)
				return left, right, true
			}
			if list.Len() == 1 {
				first, second, ok := bisectMessage(list.Get(0).Message(), names)
				if ok {
					left.Mutable(field).List().Set(0, protoreflect.ValueOfMessage(first))
					right.Mutable(field).List().Set(0, protoreflect.ValueOfMessage(second))
					return left, right, true
				}
			}
		} else if first, second, ok := bisectMessage(message.Get(field).Message(), names); ok {
			left.Set(field, protoreflect.ValueOfMessage(first))
			right.Set(field, protoreflect.ValueOfMessage(second))
			return left, right, true
		}
	}
	return nil, nil, false
}
