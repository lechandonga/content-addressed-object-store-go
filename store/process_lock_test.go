package store

import (
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

// 子进程模式环境变量：存在时，子进程持有独占锁指定毫秒后退出。
const (
	envChild     = "STORE_TEST_LOCK_CHILD_MS"
	envChildRoot = "STORE_TEST_ROOT"
)

// TestMain 拦截子进程模式；正常模式下运行全部测试。
func TestMain(m *testing.M) {
	if ms := os.Getenv(envChild); ms != "" {
		n, _ := strconv.Atoi(ms)
		s, err := Open(os.Getenv(envChildRoot), nil)
		if err != nil {
			os.Exit(2)
		}
		lk, err := s.beginGC()
		if err != nil {
			os.Exit(3)
		}
		time.Sleep(time.Duration(n) * time.Millisecond)
		s.endGC(lk)
		_ = s.Close()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestCrossProcessLock 判定依据：另一个进程持有独占（GC）锁期间，
// 本进程的共享读锁必须阻塞等待；对方释放后才拿到，且等待时间近似于对方持有时长。
func TestCrossProcessLock(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cross-process test in -short mode")
	}
	dir := t.TempDir()
	// 先初始化仓库布局。
	s, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("init: %v", err)
	}

	const holdMs = 600
	cmd := exec.Command(os.Args[0], "-test.run", "^TestCrossProcessLock$")
	cmd.Env = append(os.Environ(), envChild+"=600", envChildRoot+"="+dir)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}

	// 等待子进程拿到锁。
	time.Sleep(200 * time.Millisecond)
	t.Logf("输入: 子进程持有 GC 独占锁 %dms，期间本进程尝试获取读共享锁", holdMs)

	start := time.Now()
	lk, err := s.beginRead()
	waited := time.Since(start)
	if err != nil {
		t.Fatalf("beginRead: %v", err)
	}
	s.endRead(lk)

	if err := cmd.Wait(); err != nil {
		t.Fatalf("child exited: %v", err)
	}
	t.Logf("判定依据: 共享锁等待了 %v（应接近 %dms，说明被跨进程独占锁阻塞）", waited, holdMs)
	if waited < 250*time.Millisecond {
		t.Fatalf("shared lock was acquired too soon (%v); cross-process exclusion broken", waited)
	}
	_ = s.Close()
}
