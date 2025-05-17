package backend

import (
	"fmt"
	"io"
	"time"

	"github.com/pojntfx/go-nbd/pkg/backend"
)

// LogBackend 是一个包装器，用于记录对后端的所有操作
type LogBackend struct {
	backend backend.Backend
	logger  io.Writer
}

// NewLogBackend 创建一个新的日志后端
func NewLogBackend(backend backend.Backend, logger io.Writer) *LogBackend {
	return &LogBackend{
		backend: backend,
		logger:  logger,
	}
}

// ReadAt 实现 backend.Backend 接口
func (b *LogBackend) ReadAt(p []byte, off int64) (n int, err error) {
	start := time.Now()
	n, err = b.backend.ReadAt(p, off)
	duration := time.Since(start)
	fmt.Fprintf(b.logger, "[%s] ReadAt(offset=%d(0x%X), size=%d(0x%X)) = %d, %v (took %v)\n",
		time.Now().Format("2006-01-02 15:04:05.000"),
		off, off, len(p), len(p), n, err, duration)
	return n, err
}

// WriteAt 实现 backend.Backend 接口
func (b *LogBackend) WriteAt(p []byte, off int64) (n int, err error) {
	start := time.Now()
	n, err = b.backend.WriteAt(p, off)
	duration := time.Since(start)
	fmt.Fprintf(b.logger, "[%s] WriteAt(offset=%d(0x%X), size=%d(0x%X)) = %d, %v (took %v)\n",
		time.Now().Format("2006-01-02 15:04:05.000"),
		off, off, len(p), len(p), n, err, duration)
	return n, err
}

// Size 实现 backend.Backend 接口
func (b *LogBackend) Size() (int64, error) {
	start := time.Now()
	size, err := b.backend.Size()
	duration := time.Since(start)
	fmt.Fprintf(b.logger, "[%s] Size() = %d(0x%X), %v (took %v)\n",
		time.Now().Format("2006-01-02 15:04:05.000"),
		size, size, err, duration)
	return size, err
}

// Sync 实现 backend.Backend 接口
func (b *LogBackend) Sync() error {
	start := time.Now()
	err := b.backend.Sync()
	duration := time.Since(start)
	fmt.Fprintf(b.logger, "[%s] Sync() = %v (took %v)\n",
		time.Now().Format("2006-01-02 15:04:05.000"),
		err, duration)
	return err
}
