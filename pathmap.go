package protoyaml

import (
	"strconv"

	"gopkg.in/yaml.v3"
)

type NodeRef struct {
	Key *yaml.Node
	Val *yaml.Node
}

func BuildNodeMapByPath(node *yaml.Node) map[string]*NodeRef {
	m := make(map[string]*NodeRef)
	buildNodeMapByPath(node, "", m)
	return m
}

func buildNodeMapByPath(node *yaml.Node, path string, m map[string]*NodeRef) {
	switch node.Kind {
	case yaml.DocumentNode:
		for _, n := range node.Content {
			buildNodeMapByPath(n, "", m)
		}
	case yaml.MappingNode:
		for i := 0; i < len(node.Content); i += 2 {
			ref := &NodeRef{Key: node.Content[i]}
			if i+1 < len(node.Content) {
				ref.Val = node.Content[i+1]
			}
			subPath := pathExtendKey(path, ref.Key.Value)
			m[subPath] = ref
			if ref.Val != nil {
				buildNodeMapByPath(ref.Val, subPath, m)
			}
		}
	case yaml.SequenceNode:
		for i := 0; i < len(node.Content); i++ {
			ref := &NodeRef{Val: node.Content[i]}
			subPath := pathExtendAt(path, i)
			m[subPath] = ref
			buildNodeMapByPath(ref.Val, subPath, m)
		}
	case yaml.AliasNode:
		if node.Value != "" {
			path = pathExtendKey(path, node.Value)
			m[path] = &NodeRef{Key: node}
		}
		buildNodeMapByPath(node.Alias, path, m)
	}
}

func pathExtendKey(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func pathExtendAt(path string, at int) string {
	return path + "[" + strconv.Itoa(at) + "]"
}
