package backend

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pojntfx/go-nbd/pkg/backend"
)

type CowBackend struct {
	base       backend.Backend
	dir        string
	sectorSize int64
}

func NewCowBackend(base backend.Backend, dir string, sectorSize int64) (*CowBackend, error) {
	// Check if sector size is a multiple of 512 and a power of 2
	if sectorSize < 512 || sectorSize&(sectorSize-1) != 0 {
		return nil, fmt.Errorf("sector size must be a multiple of 512 and a power of 2")
	}

	return &CowBackend{
		base:       base,
		dir:        dir,
		sectorSize: sectorSize,
	}, nil
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
	sectorFile := b.sectorPath(sector)

	// 尝试打开扇区文件
	f, err := os.OpenFile(sectorFile, os.O_RDONLY, 0666)
	if err == nil {
		defer f.Close()
		// 扇区文件存在，从扇区文件读取
		_, err = f.Seek(off%b.sectorSize, io.SeekStart)
		if err != nil {
			return 0, err
		}
		return f.Read(p)
	}

	// 扇区文件不存在，从原始文件读取
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

	// 写入前确保目录存在
	dir := filepath.Dir(sectorFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create sector directory: %v", err)
	}

	// 检查扇区文件是否存在
	_, err = os.Stat(sectorFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return 0, err
		}
		// 扇区文件不存在，在内存中准备数据
		buf := make([]byte, b.sectorSize)
		// 读取原始扇区数据
		_, err = b.base.ReadAt(buf, sector*b.sectorSize)
		if err != nil && err != io.EOF {
			return 0, err
		}
		// 在内存中写入新数据
		copy(buf[off%b.sectorSize:], p)
		// 一次性写入文件
		err = os.WriteFile(sectorFile, buf, 0666)
		if err != nil {
			return 0, err
		}
		return len(p), nil
	}

	// 扇区文件存在，直接写入
	f, err := os.OpenFile(sectorFile, os.O_RDWR, 0666)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	_, err = f.Seek(off%b.sectorSize, io.SeekStart)
	if err != nil {
		return 0, err
	}
	return f.Write(p)
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
