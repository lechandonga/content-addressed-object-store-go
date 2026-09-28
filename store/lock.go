package store

import (
	"fmt"
	"os"
	"syscall"
)

// fileLock 封装跨进程文件锁（POSIX flock）。
//
// 锁文件本身位于仓库目录内，所有打开同一仓库的进程都对同一把锁竞争：
//   - 读 / 写操作取共享锁（LOCK_SH），彼此兼容，可完全并行；
//   - 垃圾回收取独占锁（LOCK_EX），与一切读写互斥。
//
// flock 绑定的是打开文件描述项，多个 goroutine 在同一 fd 上并发加解锁
// 由内核仲裁，因此本类型不维护任何 Go 侧的加锁状态字段，避免数据竞争。
// 这是建议性（advisory）锁，所有协调都必须经由本类型进行。
type fileLock struct {
	path string
	file *os.File
}

// newFileLock 打开（必要时创建）锁文件。readOnly 模式下不创建文件。
func newFileLock(path string, readOnly bool) (*fileLock, error) {
	flag := os.O_RDWR
	if readOnly {
		flag = os.O_RDONLY
	} else {
		flag |= os.O_CREATE
	}
	f, err := os.OpenFile(path, flag, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock file %s: %w", path, err)
	}
	return &fileLock{path: path, file: f}, nil
}

// rLock 加共享锁（阻塞直到获得）。
func (l *fileLock) rLock() error {
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_SH); err != nil {
		return fmt.Errorf("acquire shared lock %s: %w", l.path, err)
	}
	return nil
}

// lock 加独占锁（阻塞直到获得）。
func (l *fileLock) lock() error {
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("acquire exclusive lock %s: %w", l.path, err)
	}
	return nil
}

// unlock 释放当前持有的锁。
func (l *fileLock) unlock() error {
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("release lock %s: %w", l.path, err)
	}
	return nil
}

// Close 解锁并关闭锁文件。
func (l *fileLock) Close() error {
	uerr := l.unlock()
	cerr := l.file.Close()
	if uerr != nil {
		return uerr
	}
	return cerr
}
