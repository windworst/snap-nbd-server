package main

import (
	"flag"
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

func main() {
	var (
		device                  = flag.String("device", "", "块设备或镜像文件路径（必需）")
		sectorDir               = flag.String("sector-dir", "", "CopyOnWrite扇区文件目录（必需）")
		listenAddr              = flag.String("listen", ":10809", "监听地址，格式如 :10809")
		sectorSize              = flag.Int64("sector-size", 4096, "扇区大小（必须是 512 的 2 次方倍数）")
		logFile                 = flag.String("log", "", "日志文件路径（可选，默认输出到标准错误）")
		filterSize              = flag.Uint("filter-size", 100000, "布隆过滤器预计元素数量")
		filterFalsePositiveRate = flag.Float64("filter-fpr", 0.01, "布隆过滤器错误率（0-1之间）")
		cacheSize               = flag.Int("cache-size", 5000, "LRU缓存大小（缓存的扇区数量）")
	)
	flag.Parse()

	// 检查必需参数
	if *device == "" {
		log.Fatal("必须指定块设备或镜像文件路径（-device）")
	}
	if *sectorDir == "" {
		log.Fatal("必须指定扇区文件目录（-sector-dir）")
	}

	// 设置日志输出
	var logger io.Writer = os.Stderr
	if *logFile != "" {
		writer := NewAppendWriter(*logFile)
		logger = writer
	}

	// 检查设备类型
	fi, err := os.Stat(*device)
	if err != nil {
		log.Fatalf("设备或文件不存在: %v", err)
	}

	// 创建基础后端
	var baseBackend backend.Backend
	if fi.Mode()&os.ModeDevice != 0 {
		// 块设备
		devBackend, err := nbdbackend.NewDeviceBackend(*device)
		if err != nil {
			log.Fatalf("创建块设备后端失败: %v", err)
		}
		defer devBackend.Close()
		baseBackend = devBackend
	} else {
		// 普通文件
		f, err := os.OpenFile(*device, os.O_RDWR, 0666)
		if err != nil {
			log.Fatalf("打开文件失败: %v", err)
		}
		defer f.Close()
		baseBackend = backend.NewFileBackend(f)
	}

	// 创建 COW 后端
	cowBackend, err := nbdbackend.NewCowBackend(baseBackend, *sectorDir, *sectorSize, *filterSize, *filterFalsePositiveRate, *cacheSize)
	if err != nil {
		log.Fatalf("创建 COW 后端失败: %v", err)
	}

	// 创建日志后端
	logBackend := nbdbackend.NewLogBackend(cowBackend, logger)

	ln, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("监听失败: %v", err)
	}
	fmt.Printf("NBD 服务器已启动，监听 %s\n", *listenAddr)

	// 优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-quit
		fmt.Println("\n收到退出信号，关闭服务器...")
		ln.Close()
		os.Exit(0)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept 失败: %v", err)
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
				log.Printf("NBD 处理失败: %v", err)
			}
		}(conn)
	}
}
