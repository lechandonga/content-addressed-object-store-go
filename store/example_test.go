package store_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/lechandonga/content-addressed-object-store-go/store"
)

func Example() {
	dir, err := os.MkdirTemp("", "caos-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	s, err := store.Open(filepath.Join(dir, "repo"), nil)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()

	// 写入两个版本。
	v1, _ := s.Put(ctx, "greeting.txt", stringReader("hello\n"))
	v2, _ := s.Put(ctx, "greeting.txt", stringReader("hello, world\n"))
	fmt.Println("versions:", v1.ID, "->", v2.ID)

	// 当前版本读回。
	fmt.Print("current: ")
	if rc, _, err := s.Get(ctx, "greeting.txt"); err == nil {
		b, _ := io.ReadAll(rc)
		_ = rc.Close()
		os.Stdout.Write(b)
	}

	// 历史版本仍可读。
	rc, _, err := s.GetVersion(ctx, "greeting.txt", v1.ID)
	if err != nil {
		log.Fatal(err)
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	fmt.Print("history: ")
	os.Stdout.Write(b)

	// 缺失与损坏可被明确区分。
	if _, _, err := s.Get(ctx, "missing"); err != nil {
		var nf *store.NotFoundError
		var ce *store.CorruptionError
		switch {
		case errors.As(err, &nf):
			fmt.Println("missing: object does not exist")
		case errors.As(err, &ce):
			fmt.Println("corrupt:", ce.Got)
		}
	}

	// 回收只删除无引用数据块。
	stats, _ := s.GC(ctx)
	fmt.Printf("gc: scanned=%d live=%d removed=%d\n", stats.BlobsScanned, stats.BlobsLive, stats.BlobsRemoved)

	// Output:
	// versions: 1 -> 2
	// current: hello, world
	// history: hello
	// missing: object does not exist
	// gc: scanned=2 live=2 removed=0
}

type stringReaderCloser interface {
	io.Reader
}

func stringReader(s string) io.Reader {
	return strReader(s)
}
