package backend

import (
	"io"
	"sync"

	"github.com/pojntfx/go-nbd/pkg/backend"
)

// PrefetchBackend 实现预读取缓存策略的Backend
type PrefetchBackend struct {
	base               backend.Backend
	sectorSize         int64
	prefetchMultiplier int // 预读取倍数
	mutex              sync.RWMutex

	// 记录上次读取的位置和长度，用于检测顺序读取
	lastReadOffset   int64
	lastReadLength   int
	consecutiveReads int // 连续顺序读取的次数

	// 单个预读取缓冲区
	prefetchBuffer      []byte // 预读取的数据
	prefetchStartOffset int64  // 预读取缓冲区的起始偏移量
	prefetchEndOffset   int64  // 预读取缓冲区的结束偏移量
	prefetchValid       bool   // 预读取缓冲区是否有效
}

// NewPrefetchBackend 创建一个新的预读取缓存Backend
func NewPrefetchBackend(base backend.Backend, sectorSize int64, prefetchMultiplier int) (*PrefetchBackend, error) {
	return &PrefetchBackend{
		base:               base,
		sectorSize:         sectorSize,
		prefetchMultiplier: prefetchMultiplier,
		consecutiveReads:   0,
		prefetchValid:      false,
	}, nil
}

// ReadAt 实现预读取策略的读取
func (b *PrefetchBackend) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	// 检查是否可以从预读取缓冲区读取
	b.mutex.RLock()
	if b.prefetchValid && off >= b.prefetchStartOffset && off+int64(len(p)) <= b.prefetchEndOffset {
		// 命中缓冲区，直接从缓冲区读取
		bufferOffset := off - b.prefetchStartOffset
		copy(p, b.prefetchBuffer[bufferOffset:bufferOffset+int64(len(p))])
		b.mutex.RUnlock()
		return len(p), nil
	}
	b.mutex.RUnlock()

	// 检测是否为顺序读取
	isSequential := false
	b.mutex.Lock()
	if b.lastReadOffset != 0 && b.lastReadLength != 0 {
		if off == b.lastReadOffset+int64(b.lastReadLength) {
			isSequential = true
			b.consecutiveReads++
		} else {
			// 不是顺序读取，重置计数
			b.consecutiveReads = 0
		}
	}

	// 更新最后一次读取信息
	b.lastReadOffset = off
	b.lastReadLength = len(p)

	// 判断是否需要预读取
	needPrefetch := b.consecutiveReads >= 2 && isSequential
	b.mutex.Unlock()

	// 从底层读取数据
	n, err := b.base.ReadAt(p, off)
	if err != nil && err != io.EOF {
		return n, err
	}

	// 执行预读取
	if needPrefetch {
		go b.prefetch(off + int64(len(p)))
	}

	return n, err
}

// prefetch 预读取数据
func (b *PrefetchBackend) prefetch(startOffset int64) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	// 计算预读取大小
	prefetchSize := b.sectorSize * int64(b.prefetchMultiplier)

	// 检查现有缓冲区是否已覆盖请求范围
	if b.prefetchValid && startOffset >= b.prefetchStartOffset &&
		startOffset+prefetchSize <= b.prefetchEndOffset {
		// 已经有缓冲区覆盖了这个范围，不需要再预读取
		return
	}

	// 分配或重用缓冲区
	if b.prefetchBuffer == nil || int64(len(b.prefetchBuffer)) < prefetchSize {
		b.prefetchBuffer = make([]byte, prefetchSize)
	}

	// 从底层读取数据
	n, err := b.base.ReadAt(b.prefetchBuffer[:prefetchSize], startOffset)
	if err != nil && err != io.EOF {
		// 预读取失败，标记缓冲区为无效
		b.prefetchValid = false
		return
	}

	// 如果实际读取长度小于预期，调整有效长度
	validLength := int64(n)

	// 更新缓冲区信息
	b.prefetchStartOffset = startOffset
	b.prefetchEndOffset = startOffset + validLength
	b.prefetchValid = true
}

// WriteAt 将写入操作委托给底层Backend
func (b *PrefetchBackend) WriteAt(p []byte, off int64) (int, error) {
	b.mutex.Lock()
	// 检查写入是否影响预读取缓冲区，如果是则立即清除缓冲区
	if b.prefetchValid &&
		((off >= b.prefetchStartOffset && off < b.prefetchEndOffset) ||
			(off+int64(len(p)) > b.prefetchStartOffset && off+int64(len(p)) <= b.prefetchEndOffset) ||
			(off <= b.prefetchStartOffset && off+int64(len(p)) >= b.prefetchEndOffset)) {
		// 写入命中缓冲区，清除缓冲区
		b.prefetchBuffer = nil
		b.prefetchValid = false
	}

	// 写入会打断顺序读取模式
	b.consecutiveReads = 0
	b.mutex.Unlock()

	return b.base.WriteAt(p, off)
}

// Size 返回底层设备的大小
func (b *PrefetchBackend) Size() (int64, error) {
	return b.base.Size()
}

// Sync 同步底层设备
func (b *PrefetchBackend) Sync() error {
	// 不需要特别的同步操作
	return b.base.Sync()
}
