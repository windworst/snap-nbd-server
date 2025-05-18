package backend

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	bloom "github.com/bits-and-blooms/bloom/v3"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pojntfx/go-nbd/pkg/backend"
)

type CowBackend struct {
	base       backend.Backend
	dir        string
	sectorSize int64
	filter     *bloom.BloomFilter
	cache      *lru.Cache // LRU缓存
}

func NewCowBackend(base backend.Backend, dir string, sectorSize int64, filterSize uint, filterFalsePositiveRate float64, cacheSize int) (*CowBackend, error) {
	// Check if sector size is a multiple of 512 and a power of 2
	if sectorSize < 512 || sectorSize&(sectorSize-1) != 0 {
		return nil, fmt.Errorf("sector size must be a multiple of 512 and a power of 2")
	}

	// 创建布隆过滤器，使用命令行传入的参数
	filter := bloom.NewWithEstimates(filterSize, filterFalsePositiveRate)

	// 创建LRU缓存，大小使用传入的参数
	cache, err := lru.New(cacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %v", err)
	}

	// 初始化CowBackend实例
	cowBackend := &CowBackend{
		base:       base,
		dir:        dir,
		sectorSize: sectorSize,
		filter:     filter,
		cache:      cache,
	}

	// 扫描现有的扇区文件，将其添加到布隆过滤器
	if err := cowBackend.scanExistingSectors(); err != nil {
		return nil, fmt.Errorf("failed to scan existing sectors: %v", err)
	}

	return cowBackend, nil
}

// sectorToBytes 将扇区号转换为字节数组，用于布隆过滤器
func (b *CowBackend) sectorToBytes(sector int64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(sector))
	return key
}

// sectorToCacheKey 将扇区号转换为缓存键
func (b *CowBackend) sectorToCacheKey(sector int64) uint64 {
	return uint64(sector)
}

// 扫描现有扇区文件并将其添加到布隆过滤器
func (b *CowBackend) scanExistingSectors() error {
	// 确保目录存在
	if _, err := os.Stat(b.dir); os.IsNotExist(err) {
		return nil // 目录不存在，不需要扫描
	}

	// 遍历目录
	return filepath.Walk(b.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 只处理文件
		if info.IsDir() {
			return nil
		}

		// 检查是否是扇区文件
		if filepath.Ext(path) == ".sector" {
			// 从文件名中提取扇区号
			filename := filepath.Base(path)
			var sector int64
			_, err := fmt.Sscanf(filename, "%016x", &sector)
			if err == nil {
				// 将扇区添加到布隆过滤器
				b.filter.Add(b.sectorToBytes(sector))
			}
		}
		return nil
	})
}

func (b *CowBackend) sectorPath(sector int64) string {
	levels := 4
	dirs := []string{}
	for i := 0; i < levels; i++ {
		shift := uint(i * 8)
		dirs = append(dirs, fmt.Sprintf("%02x", (sector>>shift)&0xff))
	}
	filename := fmt.Sprintf("%016x_%08x.sector", sector, b.sectorSize)
	return filepath.Join(append([]string{b.dir}, append(dirs, filename)...)...)
}

func (b *CowBackend) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// 计算起始扇区和结束扇区
	startSector := off / b.sectorSize
	endSector := (off + int64(len(p)) - 1) / b.sectorSize

	// 处理读取
	remaining := p
	currentOff := off

	for sector := startSector; sector <= endSector; sector++ {
		// 计算当前扇区需要读取的数据长度
		sectorStart := int(currentOff % b.sectorSize)
		sectorRemaining := int(b.sectorSize) - sectorStart
		readLen := min(sectorRemaining, len(remaining))

		// 读取当前扇区
		n, err = b.readSector(remaining[:readLen], currentOff, sector)
		if err != nil && err != io.EOF {
			return len(p) - len(remaining), err
		}
		if n > 0 {
			remaining = remaining[n:]
			currentOff += int64(n)
		}
		if err == io.EOF {
			break
		}
	}

	return len(p) - len(remaining), nil
}

func (b *CowBackend) readSector(p []byte, off int64, sector int64) (n int, err error) {
	// 使用布隆过滤器快速检查此扇区是否已被修改
	if !b.filter.Test(b.sectorToBytes(sector)) {
		// 扇区未被修改，直接从原始文件读取
		return b.base.ReadAt(p, off)
	}

	// 从缓存读取数据（如果有的话）
	inSectorOffset := off % b.sectorSize
	cacheKey := b.sectorToCacheKey(sector)
	if cachedData, ok := b.cache.Get(cacheKey); ok {
		// 缓存命中，从缓存读取
		sectorData := cachedData.([]byte)
		copy(p, sectorData[inSectorOffset:inSectorOffset+int64(len(p))])
		return len(p), nil
	}

	// 缓存未命中，从文件读取
	sectorFile := b.sectorPath(sector)

	// 尝试打开扇区文件
	f, err := os.OpenFile(sectorFile, os.O_RDONLY, 0666)
	if err == nil {
		defer f.Close()
		// 扇区文件存在，直接从文件读取所需部分
		_, err = f.Seek(inSectorOffset, io.SeekStart)
		if err != nil {
			return 0, err
		}
		return f.Read(p)
	}

	// 如果布隆过滤器误报（扇区文件实际不存在），从原始文件读取
	return b.base.ReadAt(p, off)
}

func (b *CowBackend) WriteAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// 计算起始扇区和结束扇区
	startSector := off / b.sectorSize
	endSector := (off + int64(len(p)) - 1) / b.sectorSize

	// 处理写入
	remaining := p
	currentOff := off

	for sector := startSector; sector <= endSector; sector++ {
		// 计算当前扇区需要写入的数据长度
		sectorStart := int(currentOff % b.sectorSize)
		sectorRemaining := int(b.sectorSize) - sectorStart
		writeLen := min(sectorRemaining, len(remaining))

		// 写入当前扇区
		n, err = b.writeSector(remaining[:writeLen], currentOff, sector)
		if err != nil {
			return len(p) - len(remaining), err
		}
		if n > 0 {
			remaining = remaining[n:]
			currentOff += int64(n)
		}
	}

	return len(p) - len(remaining), nil
}

func (b *CowBackend) writeSector(p []byte, off int64, sector int64) (n int, err error) {
	sectorFile := b.sectorPath(sector)
	cacheKey := b.sectorToCacheKey(sector)

	// 写入前确保目录存在
	dir := filepath.Dir(sectorFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create sector directory: %v", err)
	}

	// 将扇区添加到布隆过滤器
	b.filter.Add(b.sectorToBytes(sector))

	// 准备扇区数据
	var sectorData []byte
	inSectorOffset := off % b.sectorSize

	// 检查扇区文件是否存在
	_, err = os.Stat(sectorFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return 0, err
		}

		// 扇区文件不存在，在内存中准备数据
		sectorData = make([]byte, b.sectorSize)

		// 尝试从缓存获取
		if cachedData, ok := b.cache.Get(cacheKey); ok {
			// 从缓存复制数据
			copy(sectorData, cachedData.([]byte))
		} else {
			// 从原始文件读取
			_, err = b.base.ReadAt(sectorData, sector*b.sectorSize)
			if err != nil && err != io.EOF {
				return 0, err
			}
		}
	} else {
		// 扇区文件存在，读取现有数据
		sectorData = make([]byte, b.sectorSize)
		f, err := os.OpenFile(sectorFile, os.O_RDONLY, 0666)
		if err != nil {
			return 0, err
		}
		_, err = f.ReadAt(sectorData, 0)
		f.Close()
		if err != nil && err != io.EOF {
			return 0, err
		}
	}

	// 在内存中写入新数据
	copy(sectorData[inSectorOffset:], p)

	// 更新缓存
	b.cache.Add(cacheKey, sectorData)

	// 一次性写入文件
	err = os.WriteFile(sectorFile, sectorData, 0666)
	if err != nil {
		return 0, err
	}

	return len(p), nil
}

func (b *CowBackend) Size() (int64, error) {
	return b.base.Size()
}

func (b *CowBackend) Sync() error {
	// 不需要特别的同步操作，因为每次操作都是直接打开文件
	return nil
}

func (b *CowBackend) Close() error {
	// 不需要特别的关闭操作，因为每次操作都是直接打开文件
	return nil
}
