package store

import "encoding/json"
import "sort"

// jsonUnmarshal 作为一个小间接层，便于在 gc.go 中替换/测试损坏场景。
func jsonUnmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// sortPaths 对路径升序排序，便于之后自下而上删除空目录。
func sortPaths(paths []string) {
	sort.Strings(paths)
}
