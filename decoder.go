package protoyaml

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

type DecodeError struct {
	Node *yaml.Node
	Err  error
}

func (e DecodeError) Error() string {
	return fmt.Sprintf("%d:%d: %v", e.Node.Line, e.Node.Column, e.Err)
}

type UnmarshalOptions struct {
	DiscardUnknown bool
}

type Decoder struct {
	Options UnmarshalOptions
}

func Unmarshal(data []byte, msg proto.Message) error {
	return UnmarshalOptions{}.Unmarshal(data, msg)
}

func (m UnmarshalOptions) Unmarshal(data []byte, msg proto.Message) error {
	var node yaml.Node
	if err := yaml.Unmarshal(data, &node); err != nil {
		return err
	}
	return Decoder{Options: m}.Decode(&node, msg)
}

func (d Decoder) Decode(node *yaml.Node, m proto.Message) error {
	proto.Reset(m)
	if node.Kind == yaml.DocumentNode {
		if len(node.Content) == 0 {
			return nil
		}
		node = node.Content[0]
	}
	dec := &nodeDecoder{
		discardUnknown: d.Options.DiscardUnknown,
		resolver:       protoregistry.GlobalTypes,
	}
	return dec.unmarshalMessage(node, m.ProtoReflect())
}

type nodeDecoder struct {
	discardUnknown bool
	resolver       interface {
		protoregistry.ExtensionTypeResolver
		protoregistry.MessageTypeResolver
	}
}

func (d *nodeDecoder) unmarshalMessage(node *yaml.Node, m protoreflect.Message) error {
	origNode := node
	if node = derefAliasNode(node); node == nil {
		return nil
	}

	md := m.Descriptor()

	if name := md.FullName(); name.Parent() == googlePackage {
		switch name.Name() {
		case anyName:
			return d.unmarshalAny(node, m)
		case timestampName:
			if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
				return &DecodeError{Node: origNode, Err: fmt.Errorf("not timestamp")}
			}
			t, err := time.Parse(time.RFC3339Nano, node.Value)
			if err != nil {
				return &DecodeError{Node: origNode, Err: fmt.Errorf("invalid timestamp %q: %w", node.Value, err)}
			}
			tsPb := timestamppb.New(t)
			m.Set(md.Fields().ByNumber(1), protoreflect.ValueOfInt64(tsPb.GetSeconds()))
			m.Set(md.Fields().ByNumber(2), protoreflect.ValueOfInt32(tsPb.GetNanos()))
			return nil
		case durationName:
			if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
				return &DecodeError{Node: origNode, Err: fmt.Errorf("not duration")}
			}
			dur, err := time.ParseDuration(node.Value)
			if err != nil {
				return &DecodeError{Node: origNode, Err: fmt.Errorf("invalid duration %q: %w", node.Value, err)}
			}
			durPb := durationpb.New(dur)
			m.Set(md.Fields().ByNumber(1), protoreflect.ValueOfInt64(durPb.GetSeconds()))
			m.Set(md.Fields().ByNumber(2), protoreflect.ValueOfInt32(durPb.GetNanos()))
			return nil
		case emptyName:
			if node.Kind != yaml.MappingNode {
				return &DecodeError{Node: origNode, Err: fmt.Errorf("not a mapping node")}
			}
			if len(node.Content) > 0 && !d.discardUnknown {
				return &DecodeError{Node: node.Content[0], Err: fmt.Errorf("unexpected content for %s", md.FullName())}
			}
			return nil
		}
	}

	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return &DecodeError{Node: origNode, Err: fmt.Errorf("not a mapping node")}
	}
	if len(node.Content)&1 != 0 {
		return &DecodeError{Node: origNode, Err: fmt.Errorf("mapping node contains non-even children")}
	}
	return d.unmarshalMessageWithContent(node, m, node.Content)
}

func (d *nodeDecoder) unmarshalMessageWithContent(node *yaml.Node, m protoreflect.Message, content []*yaml.Node) error {
	if node = derefAliasNode(node); node == nil {
		return nil
	}

	md := m.Descriptor()
	visitedField := make(map[uint64]bool)
	specifiedOneOf := make(map[uint64]bool)
	for n := 0; n < len(content); n += 2 {
		fieldNode, valueNode := content[n], content[n+1]
		if fieldNode.Kind != yaml.ScalarNode || fieldNode.Tag != "!!str" {
			return &DecodeError{Node: fieldNode, Err: fmt.Errorf("field name is not a string")}
		}
		fd := md.Fields().ByTextName(fieldNode.Value)
		if fd == nil {
			fd = md.Fields().ByJSONName(fieldNode.Value)
		}
		if fd == nil && d.discardUnknown {
			continue
		}
		if fd == nil {
			return &DecodeError{Node: fieldNode, Err: fmt.Errorf("unknown field: %s", fieldNode.Value)}
		}
		if visitedField[uint64(fd.Number())] {
			return &DecodeError{Node: fieldNode, Err: fmt.Errorf("duplicated field: %s", fieldNode.Value)}
		}
		visitedField[uint64(fd.Number())] = true
		switch {
		case fd.IsList():
			if err := d.unmarshalList(valueNode, m.Mutable(fd).List(), fd); err != nil {
				return err
			}
		case fd.IsMap():
			if err := d.unmarshalMap(valueNode, m.Mutable(fd).Map(), fd); err != nil {
				return err
			}
		default:
			if oneof := fd.ContainingOneof(); oneof != nil {
				index := uint64(oneof.Index())
				if specifiedOneOf[index] {
					return &DecodeError{Node: fieldNode, Err: fmt.Errorf("oneof %s already set", fieldNode.Value)}
				}
				specifiedOneOf[index] = true
			}
			var val protoreflect.Value
			switch fd.Kind() {
			case protoreflect.MessageKind, protoreflect.GroupKind:
				val = m.NewField(fd)
				if err := d.unmarshalMessage(valueNode, val.Message()); err != nil {
					return err
				}
			default:
				var err error
				if val, err = d.unmarshalValue(valueNode, fd); err != nil {
					return err
				}
			}
			m.Set(fd, val)
		}
	}
	return nil
}

func (d *nodeDecoder) unmarshalAny(node *yaml.Node, m protoreflect.Message) error {
	origNode := node
	if node = derefAliasNode(node); node == nil {
		return nil
	}

	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return &DecodeError{Node: origNode, Err: fmt.Errorf("not a mapping node")}
	}
	if len(node.Content)&1 != 0 {
		return &DecodeError{Node: origNode, Err: fmt.Errorf("mapping node contains non-even children")}
	}
	content := make([]*yaml.Node, 0, len(node.Content))
	var msgType protoreflect.MessageType
	var typeURL string
	for i := 0; i < len(node.Content); i += 2 {
		child, valueNode := node.Content[i], node.Content[i+1]
		if child.Kind == yaml.ScalarNode && child.Tag == "!!str" {
			switch child.Value {
			case typeURLFieldName:
				if msgType != nil {
					return &DecodeError{Node: child, Err: fmt.Errorf("duplicated field %s", child.Value)}
				}
				if valueNode.Kind != yaml.ScalarNode || valueNode.Tag != "!!str" {
					return &DecodeError{Node: valueNode, Err: fmt.Errorf("expect string")}
				}
				typeURL = valueNode.Value
				var err error
				if msgType, err = d.resolver.FindMessageByURL(typeURL); err != nil {
					return &DecodeError{Node: valueNode, Err: fmt.Errorf("resolve type %q: %w", typeURL, err)}
				}
				continue
			case invalidFieldName:
				return nil
			}
		}
		content = append(content, child, valueNode)
	}
	if msgType == nil {
		return &DecodeError{Node: origNode, Err: fmt.Errorf("field %s not found", typeURLFieldName)}
	}
	msg := msgType.New()
	if err := d.unmarshalMessageWithContent(node, msg, content); err != nil {
		return err
	}
	data, err := proto.Marshal(msg.Interface())
	if err != nil {
		return &DecodeError{Node: origNode, Err: fmt.Errorf("encode as any: %w", err)}
	}
	md := m.Descriptor()
	m.Set(md.Fields().ByNumber(1), protoreflect.ValueOfString(typeURL))
	m.Set(md.Fields().ByNumber(2), protoreflect.ValueOfBytes(data))
	return nil
}

func (d *nodeDecoder) unmarshalList(node *yaml.Node, l protoreflect.List, fd protoreflect.FieldDescriptor) error {
	origNode := node
	if node = derefAliasNode(node); node == nil {
		return nil
	}
	if node.Kind != yaml.SequenceNode {
		return &DecodeError{Node: origNode, Err: fmt.Errorf("expect a list for field: %s", fd.Name())}
	}
	switch fd.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		for _, child := range node.Content {
			val := l.NewElement()
			if err := d.unmarshalMessage(child, val.Message()); err != nil {
				return err
			}
			l.Append(val)
		}
	default:
		for _, child := range node.Content {
			val, err := d.unmarshalValue(child, fd)
			if err != nil {
				return err
			}
			l.Append(val)
		}
	}
	return nil
}

func (d *nodeDecoder) unmarshalMap(node *yaml.Node, m protoreflect.Map, fd protoreflect.FieldDescriptor) error {
	origNode := node
	if node = derefAliasNode(node); node == nil {
		return nil
	}
	if node.Kind == yaml.ScalarNode && node.Tag == "!!null" {
		return nil
	}
	if node.Kind != yaml.MappingNode {
		return &DecodeError{Node: origNode, Err: fmt.Errorf("expect a map for field: %s", fd.Name())}
	}
	if len(node.Content)&1 != 0 {
		return &DecodeError{Node: origNode, Err: fmt.Errorf("map contains non-even children")}
	}
	for i := 0; i < len(node.Content); i += 2 {
		keyNode, valNode := node.Content[i], node.Content[i+1]
		key, err := d.unmarshalMapKey(keyNode, fd.MapKey())
		if err != nil {
			return err
		}
		if m.Has(key) {
			return &DecodeError{Node: keyNode, Err: fmt.Errorf("duplicated map key: %s", keyNode.Value)}
		}
		var val protoreflect.Value
		switch fd.MapValue().Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			val = m.NewValue()
			if err := d.unmarshalMessage(valNode, val.Message()); err != nil {
				return err
			}
		default:
			if val, err = d.unmarshalValue(valNode, fd.MapValue()); err != nil {
				return err
			}
		}
		m.Set(key, val)
	}
	return nil
}

func (d *nodeDecoder) unmarshalMapKey(node *yaml.Node, fd protoreflect.FieldDescriptor) (protoreflect.MapKey, error) {
	if node.Kind != yaml.ScalarNode {
		return protoreflect.MapKey{}, &DecodeError{Node: node, Err: fmt.Errorf("invalid map key type")}
	}
	var val protoreflect.Value
	var err error
	switch fd.Kind() {
	case protoreflect.StringKind:
		val, err = parseString(node)
	case protoreflect.BoolKind:
		val, err = parseBool(node)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		val, err = parseInt(node, 32)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		val, err = parseInt(node, 64)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		val, err = parseUint(node, 32)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		val, err = parseUint(node, 64)
	default:
		panic(fmt.Sprintf("invalid kind for map key: %v", fd.Kind()))
	}
	return val.MapKey(), err
}

func (d *nodeDecoder) unmarshalValue(node *yaml.Node, fd protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	origNode := node
	if node = derefAliasNode(node); node == nil {
		return protoreflect.Value{}, &DecodeError{Node: origNode, Err: fmt.Errorf("expect a scalar value")}
	}
	if node.Kind != yaml.ScalarNode {
		return protoreflect.Value{}, &DecodeError{Node: origNode, Err: fmt.Errorf("expect a scalar value")}
	}
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return parseBool(node)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return parseInt(node, 32)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return parseInt(node, 64)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return parseUint(node, 32)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return parseUint(node, 64)
	case protoreflect.FloatKind:
		return parseFloat(node, 32)
	case protoreflect.DoubleKind:
		return parseFloat(node, 64)
	case protoreflect.StringKind:
		return parseString(node)
	case protoreflect.BytesKind:
		return parseBytes(node)
	case protoreflect.EnumKind:
		return parseEnum(node, fd)
	default:
		panic(fmt.Sprintf("invalid kind of value: %v", fd.Kind()))
	}
}

func derefAliasNode(node *yaml.Node) *yaml.Node {
	for node != nil && node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	return node
}

func parseBool(node *yaml.Node) (protoreflect.Value, error) {
	if node.Tag != "!!bool" {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("expect bool")}
	}
	switch node.Value {
	case "true":
		return protoreflect.ValueOfBool(true), nil
	case "false":
		return protoreflect.ValueOfBool(false), nil
	default:
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("invalid bool value: %s", node.Value)}
	}
}

func parseString(node *yaml.Node) (protoreflect.Value, error) {
	if node.Tag != "!!str" {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("expect string")}
	}
	return protoreflect.ValueOfString(node.Value), nil
}

func parseInt(node *yaml.Node, bitSize int) (protoreflect.Value, error) {
	// Also try to decode string for 64 bit integer as json encoding will make it a string.
	if node.Tag != "!!int" && (bitSize != 64 || node.Tag != "!!str") {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("expect int")}
	}
	val, err := strconv.ParseInt(node.Value, 10, bitSize)
	if err != nil {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("invalid int value %q: %w", node.Value, err)}
	}
	if bitSize == 32 {
		return protoreflect.ValueOfInt32(int32(val)), nil
	}
	return protoreflect.ValueOfInt64(val), nil
}

func parseUint(node *yaml.Node, bitSize int) (protoreflect.Value, error) {
	// Also try to decode string for 64 bit integer as json encoding will make it a string.
	if node.Tag != "!!int" && (bitSize != 64 || node.Tag != "!!str") {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("expect int")}
	}
	val, err := strconv.ParseUint(node.Value, 10, bitSize)
	if err != nil {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("invalid unsigned int value %q: %w", node.Value, err)}
	}
	if bitSize == 32 {
		return protoreflect.ValueOfUint32(uint32(val)), nil
	}
	return protoreflect.ValueOfUint64(val), nil
}

func parseFloat(node *yaml.Node, bitSize int) (protoreflect.Value, error) {
	if node.Tag != "!!float" {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("expect float")}
	}
	val, err := strconv.ParseFloat(node.Value, bitSize)
	if err != nil {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("invalid float value %q: %w", node.Value, err)}
	}
	if bitSize == 32 {
		return protoreflect.ValueOfFloat32(float32(val)), nil
	}
	return protoreflect.ValueOfFloat64(val), nil
}

func parseBytes(node *yaml.Node) (protoreflect.Value, error) {
	if node.Tag != "!!binary" {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("expect binary")}
	}
	enc := base64.StdEncoding
	if strings.ContainsAny(node.Value, "-_") {
		enc = base64.URLEncoding
	}
	if len(node.Value)%4 != 0 {
		enc = enc.WithPadding(base64.NoPadding)
	}
	data, err := enc.DecodeString(node.Value)
	if err != nil {
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("invalid base64: %w", err)}
	}
	return protoreflect.ValueOfBytes(data), nil
}

func parseEnum(node *yaml.Node, fd protoreflect.FieldDescriptor) (protoreflect.Value, error) {
	switch node.Tag {
	case "!!str":
		if val := fd.Enum().Values().ByName(protoreflect.Name(node.Value)); val != nil {
			return protoreflect.ValueOfEnum(val.Number()), nil
		}
		return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("unknown enum: %s", node.Value)}
	case "!!int":
		val, err := strconv.ParseUint(node.Value, 10, 64)
		if err != nil {
			return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("invalid enum index %q: %w", node.Value, err)}
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(val)), nil
	case "!!null":
		if fd.FullName() == nullValueFullName {
			return protoreflect.ValueOfEnum(0), nil
		}
	}
	return protoreflect.Value{}, &DecodeError{Node: node, Err: fmt.Errorf("invalid enum: %s", node.Value)}
}
