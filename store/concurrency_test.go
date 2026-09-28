package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestConcurrentPutSameObject 判定依据：大量 goroutine 并发覆盖同一对象时，
// 每次提交都必须产生唯一且单调递增的版本号，外部永远读不到半成品：
// 任意时刻读到的当前版本内容都必须是某个完整输入。
func TestConcurrentPutSameObject(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in -short mode")
	}
	s := newTestStore(t)
	const n = 40
	t.Logf("input: %d 个 goroutine 各向同一对象写入唯一内容", n)

	inputs := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		inputs[fmt.Sprintf("writer-%02d-payload-xxxx", i)] = struct{}{}
	}
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for in := range inputs {
		wg.Add(1)
		go func(data string) {
			defer wg.Done()
			if _, err := s.Put(context.Background(), "hot", strings.NewReader(data)); err != nil {
				errCh <- err
			}
		}(in)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent put: %v", err)
	}

	vs, err := s.Versions(context.Background(), "hot")
	if err != nil {
		t.Fatalf("versions: %v", err)
	}
	t.Logf("判定依据(1): 版本数应为 %d 且版本号 1..%d 唯一连续，实际 %d", n, n, len(vs))
	if len(vs) != n {
		t.Fatalf("version count = %d, want %d（可能有提交被覆盖丢失）", len(vs), n)
	}
	seen := make(map[int64]bool)
	gotContents := make(map[string]struct{})
	for _, v := range vs {
		if seen[v.ID] {
			t.Fatalf("duplicate version id %d", v.ID)
		}
		seen[v.ID] = true
		c := mustGetVersion(t, s, "hot", v.ID)
		if _, ok := inputs[c]; !ok {
			t.Fatalf("version %d content %q is not any complete input", v.ID, short(c))
		}
		gotContents[c] = struct{}{}
	}
	for id := int64(1); id <= n; id++ {
		if !seen[id] {
			t.Fatalf("version id %d missing", id)
		}
	}
	if len(gotContents) != n {
		t.Fatalf("distinct readable contents = %d, want %d", len(gotContents), n)
	}
	cur := mustGet(t, s, "hot")
	if _, ok := inputs[cur]; !ok {
		t.Fatalf("current content is not a complete input: %q", short(cur))
	}
	t.Logf("判定依据(2): %d 个版本全部可读且内容各不相同，当前版本是完整输入之一", n)

	// 唯一内容 => n 个块；GC 不得误删任何被引用块。
	if got := countBlobs(t, s); got != n {
		t.Fatalf("blob count = %d, want %d", got, n)
	}
	stats, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if stats.BlobsRemoved != 0 || countBlobs(t, s) != n {
		t.Fatalf("GC removed %d referenced blobs", stats.BlobsRemoved)
	}
	t.Logf("判定依据(3): 并发写入后 GC 删除=%d，%d 个被引用块全部保留", stats.BlobsRemoved, n)
}

// TestConcurrentGCAndIO 判定依据：读写与 GC 长时间并发执行时，
// 读操作只可能返回成功或 NotFound（对象尚未发布），绝不允许 Corruption
// 或其它 I/O 错误；结束后所有版本仍完整可读，GC 后引用块一块不少。
func TestConcurrentGCAndIO(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in -short mode")
	}
	s := newTestStore(t)
	const objects = 8
	const writers = 8
	const readers = 8
	const rounds = 30
	t.Logf("input: %d writers / %d readers / GC 并发，每 writer 写 %d 轮，对象数 %d",
		writers, readers, rounds, objects)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var writerWG, readerWG, gcWG sync.WaitGroup
	stop := make(chan struct{})
	var failMu sync.Mutex
	var failures []error
	addFail := func(format string, args ...any) {
		failMu.Lock()
		failures = append(failures, fmt.Errorf(format, args...))
		failMu.Unlock()
	}

	// writers：不断给不同对象追加新版本。
	writerWG.Add(writers)
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer writerWG.Done()
			for r := 0; r < rounds; r++ {
				name := fmt.Sprintf("obj-%d", r%objects)
				data := fmt.Sprintf("writer-%d-round-%d-content", id, r)
				if _, err := s.Put(ctx, name, strings.NewReader(data)); err != nil {
					addFail("put %s: %v", name, err)
					return
				}
			}
		}(w)
	}

	// readers：轮询各对象当前版本，校验返回原因。
	readerWG.Add(readers)
	for rd := 0; rd < readers; rd++ {
		go func(id int) {
			defer readerWG.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				name := fmt.Sprintf("obj-%d", i%objects)
				rc, _, err := s.Get(ctx, name)
				switch {
				case err == nil:
					b, rerr := io.ReadAll(rc)
					_ = rc.Close()
					if rerr != nil {
						addFail("reader %d read %s: %v", id, name, rerr)
						return
					}
					if len(b) == 0 {
						addFail("reader %d got empty content for %s", id, name)
						return
					}
				default:
					var nf *NotFoundError
					var ce *CorruptionError
					switch {
					case errors.As(err, &ce):
						addFail("reader %d saw CORRUPTION for %s: %v", id, name, err)
						return
					case errors.As(err, &nf):
						if nf.Key != "" { // 对象已发布但数据块缺失，绝不允许
							addFail("reader %d saw missing blob for published object %s: %v", id, name, err)
							return
						}
						// 对象尚未首次发布：允许。
					default:
						addFail("reader %d unexpected error for %s: %T %v", id, name, err, err)
						return
					}
				}
				i++
				if i >= objects*rounds {
					return
				}
			}
		}(rd)
	}

	// GC goroutine：持续回收，直到 stop 关闭。
	gcWG.Add(1)
	go func() {
		defer gcWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := s.GC(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
				addFail("gc: %v", err)
				return
			}
		}
	}()

	writerWG.Wait()
	close(stop)
	readerWG.Wait()
	gcWG.Wait()

	for _, f := range failures {
		t.Error(f)
	}
	if len(failures) > 0 {
		t.Fatalf("并发期间出现 %d 个异常", len(failures))
	}
	t.Logf("判定依据(1): 并发期间无 Corruption / 无意外 I/O 错误")

	// 收尾：再做一次 GC，然后校验每个对象的每个版本仍可读。
	stats, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("final gc: %v", err)
	}
	total := 0
	for o := 0; o < objects; o++ {
		name := fmt.Sprintf("obj-%d", o)
		vs, verr := s.Versions(context.Background(), name)
		if verr != nil {
			t.Fatalf("versions %s: %v", name, verr)
		}
		for _, v := range vs {
			if got := mustGetVersion(t, s, name, v.ID); len(got) == 0 {
				t.Fatalf("%s v%d empty", name, v.ID)
			}
			total++
		}
	}
	if stats.BlobsRemoved != 0 {
		t.Fatalf("final GC removed %d blobs although all versions remain referenced", stats.BlobsRemoved)
	}
	t.Logf("判定依据(2): 收尾 GC 删除=%d；共 %d 个历史版本全部可读，被引用块无一丢失",
		stats.BlobsRemoved, total)
}
