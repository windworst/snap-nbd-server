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
	cache      *lru.Cache // LRU cache
}

func NewCowBackend(base backend.Backend, dir string, sectorSize int64, filterSize uint, filterFalsePositiveRate float64, cacheSize int) (*CowBackend, error) {
	// Check if sector size is a multiple of 512 and a power of 2
	if sectorSize < 512 || sectorSize&(sectorSize-1) != 0 {
		return nil, fmt.Errorf("sector size must be a multiple of 512 and a power of 2")
	}

	// Create bloom filter using command line parameters
	filter := bloom.NewWithEstimates(filterSize, filterFalsePositiveRate)

	// Create LRU cache with the specified size
	cache, err := lru.New(cacheSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create LRU cache: %v", err)
	}

	// Initialize CowBackend instance
	cowBackend := &CowBackend{
		base:       base,
		dir:        dir,
		sectorSize: sectorSize,
		filter:     filter,
		cache:      cache,
	}

	// Scan existing sector files and add them to the bloom filter
	if err := cowBackend.scanExistingSectors(); err != nil {
		return nil, fmt.Errorf("failed to scan existing sectors: %v", err)
	}

	return cowBackend, nil
}

// sectorToBytes converts a sector number to a byte array for bloom filter
func (b *CowBackend) sectorToBytes(sector int64) []byte {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, uint64(sector))
	return key
}

// sectorToCacheKey converts a sector number to a cache key
func (b *CowBackend) sectorToCacheKey(sector int64) uint64 {
	return uint64(sector)
}

// Scan existing sector files and add them to the bloom filter
func (b *CowBackend) scanExistingSectors() error {
	fmt.Printf("Starting to scan sector files directory: %s\n", b.dir)

	// Ensure directory exists
	if _, err := os.Stat(b.dir); os.IsNotExist(err) {
		fmt.Println("Directory does not exist, no need to scan")
		return nil // Directory does not exist, no need to scan
	}

	count := 0
	dirCounts := make(map[string]int)
	// Use custom method to scan all .sector files
	err := b.walkAllSectorFiles(b.dir, &count, dirCounts)
	if err != nil {
		return err
	}

	fmt.Printf("Scan completed, loaded %d sectors in total\n", count)
	return err
}

// walkAllSectorFiles recursively scans directories and processes all .sector files
func (b *CowBackend) walkAllSectorFiles(dir string, count *int, dirCounts map[string]int) error {
	// Read directory contents
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		path := filepath.Join(dir, entry.Name())

		// If it's a directory, process recursively
		if entry.IsDir() {
			if err := b.walkAllSectorFiles(path, count, dirCounts); err != nil {
				return err
			}
			continue
		}

		// Get detailed info
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Handle symbolic links
		if info.Mode()&os.ModeSymlink != 0 {
			realPath, err := filepath.EvalSymlinks(path)
			if err != nil {
				continue
			}

			realInfo, err := os.Stat(realPath)
			if err != nil {
				continue
			}

			// If it points to a directory, process recursively
			if realInfo.IsDir() {
				if err := b.walkAllSectorFiles(realPath, count, dirCounts); err != nil {
					return err
				}
				continue
			}

			path = realPath // Use the actual path for further processing
		}

		// Check if it's a sector file
		if filepath.Ext(path) == ".sector" {
			// Extract sector number from filename
			filename := filepath.Base(path)
			var sector int64
			var sectorSize int64
			_, err := fmt.Sscanf(filename, "%016x_%08x.sector", &sector, &sectorSize)
			if err == nil {
				// Add sector to bloom filter
				b.filter.Add(b.sectorToBytes(sector))
				*count++
				// Update directory statistics
				dirCounts[filepath.Dir(path)]++
			}
		}
	}

	return nil
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

// readBlackSectorToBuffer reads black sector data directly into the target buffer
func (b *CowBackend) readBlackSectorToBuffer(sector int64, targetBuf []byte, sectorOffset int64) bool {
	// Try to get data from cache
	cacheKey := b.sectorToCacheKey(sector)
	if cachedData, ok := b.cache.Get(cacheKey); ok {
		// Copy data from cache directly to target buffer
		sectorData := cachedData.([]byte)
		copy(targetBuf, sectorData[sectorOffset:sectorOffset+int64(len(targetBuf))])
		return true
	}

	// Cache miss, try to read from file
	sectorFile := b.sectorPath(sector)
	f, err := os.OpenFile(sectorFile, os.O_RDONLY, 0666)
	if err != nil {
		return false // File open failed
	}

	// Read directly into target buffer
	_, err = f.ReadAt(targetBuf, sectorOffset)
	if err != nil && err != io.EOF {
		f.Close()
		return false // File read failed
	}

	// After successful read, add the entire sector to cache
	if b.cache != nil {
		sectorData := make([]byte, b.sectorSize)
		f.Seek(0, io.SeekStart)
		_, err = f.ReadAt(sectorData, 0)
		if err == nil || err == io.EOF {
			b.cache.Add(cacheKey, sectorData)
		}
	}

	f.Close()
	return true
}

func (b *CowBackend) ReadAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// 1. First read all requested data from the base device
	n, err = b.base.ReadAt(p, off)
	if err != nil && err != io.EOF {
		return n, err
	}

	// If the read data length is insufficient, return EOF
	if int64(n) < int64(len(p)) {
		err = io.EOF
	} else {
		err = nil
	}

	// 2. Calculate the sector range to process
	startSector := off / b.sectorSize
	endSector := (off + int64(len(p)) - 1) / b.sectorSize

	// 3. Check each sector and overlay black sector data
	for sector := startSector; sector <= endSector; sector++ {
		// Use bloom filter to quickly check if this sector has been modified
		if b.filter.Test(b.sectorToBytes(sector)) {
			// Calculate the start position and length of this sector in the request range
			sectorStartOffset := sector * b.sectorSize
			sectorEndOffset := sectorStartOffset + b.sectorSize - 1

			// Calculate the intersection with the current request
			readStart := max(sectorStartOffset, off)
			readEnd := min(sectorEndOffset, off+int64(len(p))-1)

			if readStart <= readEnd {
				// Calculate the offset within the sector and in the buffer
				sectorOffset := readStart - sectorStartOffset
				bufOffset := readStart - off
				length := readEnd - readStart + 1

				// Read black sector data and overlay to the corresponding position in the buffer
				b.readBlackSectorToBuffer(sector, p[bufOffset:bufOffset+length], sectorOffset)
			}
		}
	}

	return n, err
}

func (b *CowBackend) WriteAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	// Calculate start sector and end sector
	startSector := off / b.sectorSize
	endSector := (off + int64(len(p)) - 1) / b.sectorSize

	// Process write
	remaining := p
	currentOff := off

	for sector := startSector; sector <= endSector; sector++ {
		// Calculate current sector data length to write
		sectorStart := int(currentOff % b.sectorSize)
		sectorRemaining := int(b.sectorSize) - sectorStart
		writeLen := min(sectorRemaining, len(remaining))

		// Write current sector
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

	// Write before ensuring directory exists
	dir := filepath.Dir(sectorFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return 0, fmt.Errorf("failed to create sector directory: %v", err)
	}

	// Add sector to bloom filter
	b.filter.Add(b.sectorToBytes(sector))

	// Prepare sector data
	var sectorData []byte
	inSectorOffset := off % b.sectorSize

	// Check if sector file exists
	_, err = os.Stat(sectorFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return 0, err
		}

		// Sector file does not exist, prepare data in memory
		sectorData = make([]byte, b.sectorSize)

		// Try to get data from cache
		if cachedData, ok := b.cache.Get(cacheKey); ok {
			// Copy data from cache
			copy(sectorData, cachedData.([]byte))
		} else {
			// Read from original file
			_, err = b.base.ReadAt(sectorData, sector*b.sectorSize)
			if err != nil && err != io.EOF {
				return 0, err
			}
		}
	} else {
		// Sector file exists, read existing data
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

	// Write new data into memory
	copy(sectorData[inSectorOffset:], p)

	// Update cache
	b.cache.Add(cacheKey, sectorData)

	// Write once into file
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
	return b.base.Sync()
}
