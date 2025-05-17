package backend

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

const (
	// BLKGETSIZE64 是获取块设备大小的 ioctl 命令
	BLKGETSIZE64 = 0x80081272
)

// DeviceBackend 实现了 backend.Backend 接口，用于处理块设备
type DeviceBackend struct {
	file *os.File
	size int64
}

// NewDeviceBackend 创建一个新的块设备后端
func NewDeviceBackend(device string) (*DeviceBackend, error) {
	// 打开设备
	f, err := os.OpenFile(device, os.O_RDWR|syscall.O_DIRECT, 0666)
	if err != nil {
		return nil, err
	}

	// 获取设备大小
	var size int64
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), uintptr(BLKGETSIZE64), uintptr(unsafe.Pointer(&size)))
	if errno != 0 {
		f.Close()
		return nil, errno
	}

	return &DeviceBackend{
		file: f,
		size: size,
	}, nil
}

// ReadAt 实现 backend.Backend 接口
func (b *DeviceBackend) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= b.size {
		return 0, io.EOF
	}

	// 确保读取不会超出设备大小
	if off+int64(len(p)) > b.size {
		p = p[:b.size-off]
	}

	return b.file.ReadAt(p, off)
}

// WriteAt 实现 backend.Backend 接口
func (b *DeviceBackend) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= b.size {
		return 0, io.EOF
	}

	// 确保写入不会超出设备大小
	if off+int64(len(p)) > b.size {
		p = p[:b.size-off]
	}

	return b.file.WriteAt(p, off)
}

// Size 实现 backend.Backend 接口
func (b *DeviceBackend) Size() (int64, error) {
	return b.size, nil
}

// Sync 实现 backend.Backend 接口
func (b *DeviceBackend) Sync() error {
	return b.file.Sync()
}

// Close 关闭设备
func (b *DeviceBackend) Close() error {
	return b.file.Close()
}
