package protoyaml

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"gopkg.in/yaml.v3"
)

type FieldLesser func(md protoreflect.MessageDescriptor, fd1, fd2 protoreflect.FieldDescriptor) bool

func AlphabetFieldLesser(md protoreflect.MessageDescriptor, fd1, fd2 protoreflect.FieldDescriptor) bool {
	return fd1.TextName() < fd2.TextName()
}

func NumberFieldLesser(md protoreflect.MessageDescriptor, fd1, fd2 protoreflect.FieldDescriptor) bool {
	return fd1.Number() < fd2.Number()
}

func ScoredFieldLesser(scores map[string]int) FieldLesser {
	return func(md protoreflect.MessageDescriptor, fd1, fd2 protoreflect.FieldDescriptor) bool {
		fn1, fn2 := string(md.FullName())+"."+fd1.TextName(), string(md.FullName())+"."+fd2.TextName()
		return scores[fn1] < scores[fn2]
	}
}

type MarshalOptions struct {
	UseProtoNames  bool
	UseEnumNumbers bool
	FieldLesser    FieldLesser
	IndentSpaces   int
}

type Encoder struct {
	Options MarshalOptions
}

func Marshal(msg proto.Message) ([]byte, error) {
	return MarshalOptions{UseProtoNames: true, FieldLesser: NumberFieldLesser}.Marshal(msg)
}

func (m MarshalOptions) Marshal(msg proto.Message) ([]byte, error) {
	node := Encoder{Options: m}.Encode(msg)
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	if m.IndentSpaces > 0 {
		encoder.SetIndent(m.IndentSpaces)
	} else {
		encoder.SetIndent(2)
	}
	if err := encoder.Encode(node); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func (e Encoder) Encode(m proto.Message) *yaml.Node {
	enc := &nodeEncoder{
		useProtoNames:  e.Options.UseProtoNames,
		useEnumNumbers: e.Options.UseEnumNumbers,
		fieldLesser:    e.Options.FieldLesser,
		resolver:       protoregistry.GlobalTypes,
		current: &nodeEntry{
			node: &yaml.Node{
				Kind: yaml.DocumentNode,
			},
		},
	}
	enc.marshalMessage(m.ProtoReflect(), false)
	return enc.current.node
}

type nodeEncoder struct {
	useProtoNames  bool
	useEnumNumbers bool
	fieldLesser    FieldLesser
	resolver       interface {
		protoregistry.ExtensionTypeResolver
		protoregistry.MessageTypeResolver
	}
	current *nodeEntry
}

type nodeEntry struct {
	parent *nodeEntry
	node   *yaml.Node
}

type fieldValuePair struct {
	f protoreflect.FieldDescriptor
	v protoreflect.Value
}

type keyValuePair struct {
	k protoreflect.MapKey
	v protoreflect.Value
}

func (e *nodeEncoder) push(node *yaml.Node) {
	e.add(node)
	e.current = &nodeEntry{parent: e.current, node: node}
}

func (e *nodeEncoder) pop() {
	if e.current.parent == nil {
		panic("stack becoming empty")
	}
	e.current = e.current.parent
}

func (e *nodeEncoder) add(node *yaml.Node) {
	e.current.node.Content = append(e.current.node.Content, node)
}

func (e *nodeEncoder) marshalMessage(msg protoreflect.Message, addOnly bool) {
	md := msg.Descriptor()
	if name := md.FullName(); name.Parent() == googlePackage {
		switch name.Name() {
		case anyName:
			typeURL := msg.Get(md.Fields().ByNumber(1)).String()
			data := msg.Get(md.Fields().ByNumber(2))
			e.push(&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"})
			defer e.pop()
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: typeURLFieldName})
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: typeURL})
			msgType, err := e.resolver.FindMessageByURL(typeURL)
			if err == nil {
				msg = msgType.New()
				err = proto.UnmarshalOptions{
					AllowPartial: true, // never check required fields inside an Any
					Resolver:     e.resolver,
				}.Unmarshal(data.Bytes(), msg.Interface())
			}
			if err != nil {
				e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: invalidFieldName})
				e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: "true"})
				return
			}
			e.marshalMessage(msg, true)
			return
		case timestampName:
			secs, nanos := msg.Get(md.Fields().ByNumber(1)).Int(), msg.Get(md.Fields().ByNumber(2)).Int()
			val := time.Unix(secs, nanos).UTC().Format("2006-01-02T15:04:05.000000000")
			val = strings.TrimSuffix(val, "000")
			val = strings.TrimSuffix(val, "000")
			val = strings.TrimSuffix(val, ".000")
			val += "Z"
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: val})
			return
		case durationName:
			secs, nanos := msg.Get(md.Fields().ByNumber(1)).Int(), msg.Get(md.Fields().ByNumber(2)).Int()
			dur := time.Duration(nanos) + time.Duration(secs)*time.Second
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: dur.String()})
			return
		case emptyName:
			e.add(&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"})
			return
		}
	}

	if !addOnly {
		e.push(&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"})
		defer e.pop()
	}

	var fieldValues []fieldValuePair
	msg.Range(func(fd protoreflect.FieldDescriptor, val protoreflect.Value) bool {
		fieldValues = append(fieldValues, fieldValuePair{f: fd, v: val})
		return true
	})
	if e.fieldLesser != nil {
		sort.Slice(fieldValues, func(i, j int) bool {
			return e.fieldLesser(md, fieldValues[i].f, fieldValues[j].f)
		})
	}
	for _, fv := range fieldValues {
		name := fv.f.JSONName()
		if e.useProtoNames {
			name = fv.f.TextName()
		}
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: name})
		switch {
		case fv.f.IsList():
			e.marshalList(fv.v.List(), fv.f)
		case fv.f.IsMap():
			e.marshalMap(fv.v.Map(), fv.f)
		default:
			e.marshalValue(fv.v, fv.f)
		}
	}
}

func (e *nodeEncoder) marshalList(l protoreflect.List, fd protoreflect.FieldDescriptor) {
	e.push(&yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"})
	defer e.pop()
	for i := 0; i < l.Len(); i++ {
		e.marshalValue(l.Get(i), fd)
	}
}

func (e *nodeEncoder) marshalMap(m protoreflect.Map, fd protoreflect.FieldDescriptor) {
	e.push(&yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"})
	defer e.pop()
	var keyValues []keyValuePair
	m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		keyValues = append(keyValues, keyValuePair{k: k, v: v})
		return true
	})
	sort.Slice(keyValues, func(i, j int) bool {
		ki, kj := keyValues[i], keyValues[j]
		switch ki.k.Interface().(type) {
		case bool:
			return !ki.k.Bool() && kj.k.Bool()
		case int32, int64:
			return ki.k.Int() < kj.k.Int()
		case uint32, uint64:
			return ki.k.Uint() < kj.k.Uint()
		case string:
			return ki.k.String() < kj.k.String()
		default:
			panic("invalid map key")
		}
	})
	for _, kv := range keyValues {
		switch kv.k.Interface().(type) {
		case bool:
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(kv.k.Bool())})
		case int32, int64:
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatInt(kv.k.Int(), 10)})
		case uint32, uint64:
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatUint(kv.k.Uint(), 10)})
		case string:
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: kv.k.String()})
		default:
			panic("invalid map key")
		}
		e.marshalValue(kv.v, fd.MapValue())
	}
}

func (e *nodeEncoder) marshalValue(val protoreflect.Value, fd protoreflect.FieldDescriptor) {
	if !val.IsValid() {
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"})
		return
	}

	switch kind := fd.Kind(); kind {
	case protoreflect.BoolKind:
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!bool", Value: strconv.FormatBool(val.Bool())})
	case protoreflect.StringKind:
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: val.String()})
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatInt(val.Int(), 10)})
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatUint(val.Uint(), 10)})
	case protoreflect.FloatKind:
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!float", Value: strconv.FormatFloat(val.Float(), 'g', -1, 32)})
	case protoreflect.DoubleKind:
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!float", Value: strconv.FormatFloat(val.Float(), 'g', -1, 64)})
	case protoreflect.BytesKind:
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!binary", Value: base64.StdEncoding.EncodeToString(val.Bytes())})
	case protoreflect.EnumKind:
		if fd.Enum().FullName() == nullValueFullName {
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"})
			return
		}
		if e.useEnumNumbers {
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatInt(int64(val.Enum()), 10)})
			return
		}
		desc := fd.Enum().Values().ByNumber(val.Enum())
		if desc == nil {
			e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!int", Value: strconv.FormatInt(int64(val.Enum()), 10)})
			return
		}
		e.add(&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: string(desc.Name())})
	case protoreflect.MessageKind, protoreflect.GroupKind:
		e.marshalMessage(val.Message(), false)
	default:
		panic(fmt.Sprintf("%v has unknown kind: %v", fd.FullName(), kind))
	}
}
