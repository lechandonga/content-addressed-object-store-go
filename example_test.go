package cas_test

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	cas "github.com/lechandonga/content-addressed-object-store-go"
)

// Example 演示：去重写入、多版本读取、完整性校验与垃圾回收。
func Example() {
	dir, err := os.MkdirTemp("", "cas-example-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	s, err := cas.Open(dir)
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// 相同内容写入不同键，只会落盘一份。
	v1, _ := s.Put("greeting", strings.NewReader("hello world"))
	v2, _ := s.Put("greeting-copy", strings.NewReader("hello world"))
	fmt.Println("去重:", v1.Hash == v2.Hash)

	// 覆盖写产生新版本。
	s.Put("greeting", strings.NewReader("hello again"))

	versions, _ := s.Versions("greeting")
	fmt.Println("版本数:", len(versions))

	// 读取最新版与历史版本。
	rc, _, _ := s.Get("greeting")
	latest, _ := io.ReadAll(rc)
	rc.Close()
	fmt.Println("最新版:", string(latest))

	rc, _, _ = s.GetVersion("greeting", 0)
	oldest, _ := io.ReadAll(rc)
	rc.Close()
	fmt.Println("历史版:", string(oldest))

	// GC：两个版本仍被引用，不会删除任何块。
	st, _ := s.GC()
	fmt.Printf("GC: kept=%d removed=%d\n", st.ObjectsKept, st.ObjectsRemoved)

	// 错误可区分：缺失 vs 损坏。
	_, _, err = s.Get("missing-key")
	fmt.Println("缺失判定:", errors.Is(err, cas.ErrNotFound))

	// Output:
	// 去重: true
	// 版本数: 2
	// 最新版: hello again
	// 历史版: hello world
	// GC: kept=2 removed=0
	// 缺失判定: true
}
