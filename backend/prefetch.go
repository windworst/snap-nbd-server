package backend

import (
	"io"
	"sync"

	"github.com/pojntfx/go-nbd/pkg/backend"
)

// PrefetchBackend 实现预读取缓存策略的Backend
type PrefetchBackend struct {
	base                backend.Backend
	sectorSize          int64
	prefetchMultiplier  int // 预读取倍数
	maxConsecutiveReads int // 连击点最大值
	mutex               sync.RWMutex

	// 记录上次读取的位置和长度，用于检测顺序读取
	lastReadOffset   int64
	lastReadLength   int
	consecutiveReads int // 连续读取"连击点"

	// 单个预读取缓冲区
	prefetchBuffer      []byte // 预读取的数据
	prefetchStartOffset int64  // 预读取缓冲区的起始偏移量
	prefetchEndOffset   int64  // 预读取缓冲区的结束偏移量
	prefetchValid       bool   // 预读取缓冲区是否有效
}

// NewPrefetchBackend 创建一个新的预读取缓存Backend
func NewPrefetchBackend(base backend.Backend, sectorSize int64, prefetchMultiplier int, maxConsecutiveReads ...int) (*PrefetchBackend, error) {
	// 默认连击点最大值为2，如果提供了参数则使用提供的值
	maxReads := 2
	if len(maxConsecutiveReads) > 0 && maxConsecutiveReads[0] > 0 {
		maxReads = maxConsecutiveReads[0]
	}

	return &PrefetchBackend{
		base:                base,
		sectorSize:          sectorSize,
		prefetchMultiplier:  prefetchMultiplier,
		maxConsecutiveReads: maxReads,
		consecutiveReads:    0,
		prefetchValid:       false,
	}, nil
}

// ReadAt 实现预读取策略的读取
func (b *PrefetchBackend) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	// 判断是否为顺序读取和是否需要预读取
	isSequential := false
	shouldPrefetch := false

	b.mutex.Lock()
	if b.lastReadOffset != 0 && b.lastReadLength != 0 {
		if off == b.lastReadOffset+int64(b.lastReadLength) {
			isSequential = true
			// 增加连击点，上限为maxConsecutiveReads
			if b.consecutiveReads < b.maxConsecutiveReads {
				b.consecutiveReads++
			}
			// 连击点达到maxConsecutiveReads，触发预读取标志
			shouldPrefetch = (b.consecutiveReads >= b.maxConsecutiveReads)
		} else {
			// 非连续读取，重置连击点
			b.consecutiveReads = 0
		}
	}

	// 更新最后一次读取信息
	b.lastReadOffset = off
	b.lastReadLength = len(p)
	b.mutex.Unlock()

	// 首先检查是否完全命中缓存
	b.mutex.RLock()
	if b.prefetchValid && off >= b.prefetchStartOffset && off+int64(len(p)) <= b.prefetchEndOffset {
		// 完全命中缓存，直接从缓存中读取数据
		bufferOffset := off - b.prefetchStartOffset
		copy(p, b.prefetchBuffer[bufferOffset:bufferOffset+int64(len(p))])
		b.mutex.RUnlock()

		// 即使命中缓存也更新连击点（仅在连续读取时）
		if isSequential {
			b.mutex.Lock()
			if b.consecutiveReads < b.maxConsecutiveReads {
				b.consecutiveReads++
			}
			b.mutex.Unlock()
		}
		return len(p), nil
	}

	// 检查是否部分命中缓存
	partialHit := false
	var partialStart, partialEnd int64

	if b.prefetchValid {
		// 检查读取区域是否与缓存有重叠
		// 情况1: 读取区域的前半部分在缓存中
		if off >= b.prefetchStartOffset && off < b.prefetchEndOffset &&
			off+int64(len(p)) > b.prefetchEndOffset {
			partialHit = true
			partialStart = off
			partialEnd = b.prefetchEndOffset
		}
		// 情况2: 读取区域的后半部分在缓存中
		if off < b.prefetchStartOffset &&
			off+int64(len(p)) > b.prefetchStartOffset &&
			off+int64(len(p)) <= b.prefetchEndOffset {
			partialHit = true
			partialStart = b.prefetchStartOffset
			partialEnd = off + int64(len(p))
		}
	}
	b.mutex.RUnlock()

	// 处理部分命中
	if partialHit {
		// 计算部分命中的长度
		hitLength := partialEnd - partialStart
		hitOffset := partialStart - off
		if hitOffset < 0 {
			hitOffset = 0
		}

		// 首先从缓存复制部分命中的数据
		b.mutex.RLock()
		bufferOffset := partialStart - b.prefetchStartOffset
		copy(p[hitOffset:hitOffset+hitLength], b.prefetchBuffer[bufferOffset:bufferOffset+hitLength])
		b.mutex.RUnlock()

		// 读取未命中部分
		if hitOffset > 0 {
			// 如果前半部分未命中，读取前半部分
			_, err := b.base.ReadAt(p[:hitOffset], off)
			if err != nil && err != io.EOF {
				return 0, err
			}
		}

		if hitOffset+hitLength < int64(len(p)) {
			// 如果后半部分未命中，读取后半部分
			_, err := b.base.ReadAt(p[hitOffset+hitLength:], off+hitOffset+hitLength)
			if err != nil && err != io.EOF {
				return 0, err
			}
		}

		return len(p), nil
	}

	// 到这里表示完全未命中缓存
	// 只有当shouldPrefetch为true（连击点达到maxConsecutiveReads）且未命中缓存时，才触发预读取
	if shouldPrefetch {
		// 计算预读取大小
		prefetchSize := b.sectorSize * int64(b.prefetchMultiplier)

		// 预读取起始位置就是当前读取的位置
		readStartOffset := off

		b.mutex.Lock()
		// 分配或重用缓冲区
		if b.prefetchBuffer == nil || int64(len(b.prefetchBuffer)) < prefetchSize {
			b.prefetchBuffer = make([]byte, prefetchSize)
		}

		// 从底层一次性读取当前需要的数据和预读取数据
		n, err := b.base.ReadAt(b.prefetchBuffer[:prefetchSize], readStartOffset)
		if err != nil && err != io.EOF {
			b.mutex.Unlock()
			return 0, err
		}

		// 如果实际读取长度小于预期，调整有效长度
		validLength := int64(n)

		// 更新缓冲区信息
		b.prefetchStartOffset = readStartOffset
		b.prefetchEndOffset = readStartOffset + validLength
		b.prefetchValid = true

		// 从预读取缓冲区复制出当前需要的数据
		if int64(len(p)) <= validLength {
			copy(p, b.prefetchBuffer[:len(p)])
			b.mutex.Unlock()
			return len(p), nil
		} else {
			// 如果实际读取长度小于请求长度，只返回能读到的部分
			copy(p, b.prefetchBuffer[:validLength])
			b.mutex.Unlock()
			return int(validLength), io.EOF
		}
	}

	// 常规读取，直接从底层读取（完全未命中缓存且不需要预读取）
	return b.base.ReadAt(p, off)
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
	return b.base.Sync()
}
