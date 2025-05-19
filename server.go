package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	nbdbackend "nbd/backend"

	"github.com/pojntfx/go-nbd/pkg/backend"
	"github.com/pojntfx/go-nbd/pkg/server"
)

type AppendWriter struct {
	path string
}

func NewAppendWriter(path string) *AppendWriter {
	return &AppendWriter{path: path}
}

func (w *AppendWriter) Write(p []byte) (n int, err error) {
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.Write(p)
}

func (w *AppendWriter) Close() error {
	// 无需关闭，因每次写入都自动关闭
	return nil
}

func startServer(device, sectorDir, listenAddr string, sectorSize int64, logFile string, filterSize uint, filterFalsePositiveRate float64, cacheSize int, enablePrefetch bool, prefetchMultiplier, maxConsecutiveReads int) error {
	// 设置日志输出
	var logger io.Writer = os.Stderr
	if logFile != "" {
		writer := NewAppendWriter(logFile)
		logger = writer
	}

	// 检查设备类型
	fi, err := os.Stat(device)
	if err != nil {
		return fmt.Errorf("device or file does not exist: %v", err)
	}

	// 创建基础后端
	var baseBackend backend.Backend
	if fi.Mode()&os.ModeDevice != 0 {
		// 块设备
		devBackend, err := nbdbackend.NewDeviceBackend(device)
		if err != nil {
			return fmt.Errorf("failed to create block device backend: %v", err)
		}
		defer devBackend.Close()
		baseBackend = devBackend
	} else {
		// 普通文件
		f, err := os.OpenFile(device, os.O_RDWR, 0666)
		if err != nil {
			return fmt.Errorf("failed to open file: %v", err)
		}
		defer f.Close()
		baseBackend = backend.NewFileBackend(f)
	}

	// 创建 COW 后端
	cowBackend, err := nbdbackend.NewCowBackend(baseBackend, sectorDir, sectorSize, filterSize, filterFalsePositiveRate, cacheSize)
	if err != nil {
		return fmt.Errorf("failed to create COW backend: %v", err)
	}

	// 如果启用预读取缓存，创建预读取后端
	var backend backend.Backend = cowBackend
	if enablePrefetch {
		prefetchBackend, err := nbdbackend.NewPrefetchBackend(cowBackend, sectorSize, prefetchMultiplier, maxConsecutiveReads)
		if err != nil {
			return fmt.Errorf("failed to create prefetch cache backend: %v", err)
		}
		backend = prefetchBackend
	}

	// 创建日志后端
	logBackend := nbdbackend.NewLogBackend(backend, logger)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}
	fmt.Printf("NBD server started, listening on %s\n", listenAddr)

	// 优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		fmt.Println("\nReceived exit signal, shutting down server...")
		ln.Close()
		os.Exit(0)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept failed: %v", err)
			break
		}
		go func(c net.Conn) {
			defer c.Close()
			err := server.Handle(
				c,
				[]server.Export{{
					Name:        "disk",
					Description: "cow disk",
					Backend:     logBackend,
				}},
				&server.Options{
					ReadOnly: false,
				},
			)
			if err != nil {
				log.Printf("NBD handling failed: %v", err)
			}
		}(conn)
	}

	return nil
}
