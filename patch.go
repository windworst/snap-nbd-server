package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type SectorInfo struct {
	Path   string
	Offset int64
	Size   int64
}

func walkSectorFiles(dir string) ([]SectorInfo, error) {
	var sectors []SectorInfo
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 如果是目录，继续遍历
		if info.IsDir() {
			return nil
		}

		// 检查是否是扇区文件
		if filepath.Ext(path) == ".sector" {
			// 解析扇区文件名
			// 格式：0000000012345678_00001000.sector
			filename := filepath.Base(path)
			parts := strings.Split(strings.TrimSuffix(filename, ".sector"), "_")
			if len(parts) != 2 {
				log.Printf("Invalid sector filename format: %s", filename)
				return nil
			}

			// 解析偏移量和大小
			offset, err := strconv.ParseInt(parts[0], 16, 64)
			if err != nil {
				log.Printf("Invalid offset in filename %s: %v", filename, err)
				return nil
			}
			size, err := strconv.ParseInt(parts[1], 16, 64)
			if err != nil {
				log.Printf("Invalid size in filename %s: %v", filename, err)
				return nil
			}

			sectors = append(sectors, SectorInfo{
				Path:   path,
				Offset: offset,
				Size:   size,
			})
		}
		return nil
	})

	return sectors, err
}

func patchSectors(sectorDir, device string, deviceOffset int64, dryRun bool) error {
	// 显示警告信息（只在非 dry-run 模式下显示）
	if !dryRun {
		fmt.Println("\n" + strings.Repeat("!", 80))
		fmt.Println("WARNING: This program will write data directly to the target device.")
		fmt.Println("         Incorrect usage may result in data loss or system damage.")
		fmt.Println("         Make sure you have a backup of your data.")
		fmt.Println("         Double-check the target device and offset.")
		fmt.Println(strings.Repeat("!", 80) + "\n")
	}

	// 遍历并收集扇区文件信息
	fmt.Println("Scanning sector files...")
	sectors, err := walkSectorFiles(sectorDir)
	if err != nil {
		return fmt.Errorf("failed to scan sector files: %v", err)
	}

	// 显示统计信息
	var totalSize int64
	for _, s := range sectors {
		totalSize += s.Size
	}
	fmt.Printf("\nFound %d sector files, total size: %d bytes (%.2f MB)\n",
		len(sectors), totalSize, float64(totalSize)/1024/1024)
	fmt.Printf("Target device: %s (offset: 0x%x)\n", device, deviceOffset)
	if dryRun {
		fmt.Println("\nDRY RUN MODE: No data will be written to the device")
	}

	// 只在非 dry-run 模式下请求确认
	if !dryRun {
		fmt.Print("\nTo proceed, type 'YES' (case sensitive): ")
		reader := bufio.NewReader(os.Stdin)
		response, err := reader.ReadString('\n')
		if err != nil {
			return fmt.Errorf("failed to read response: %v", err)
		}
		if strings.TrimSpace(response) != "YES" {
			fmt.Println("Operation cancelled by user")
			return nil
		}
	}

	// 打开目标设备/文件
	var dev *os.File
	if dryRun {
		dev, err = os.OpenFile(device, os.O_RDONLY, 0666) // 只读模式打开
		if err != nil {
			return fmt.Errorf("failed to open device in read-only mode: %v", err)
		}
	} else {
		dev, err = os.OpenFile(device, os.O_RDWR, 0666)
		if err != nil {
			return fmt.Errorf("failed to open device/file: %v", err)
		}
	}
	defer dev.Close()

	// 写入扇区文件
	fmt.Println("\nApplying sectors...")
	for _, s := range sectors {
		// 计算实际写入位置（扇区号 * 扇区大小 + 设备偏移）
		actualOffset := (s.Offset * s.Size) + deviceOffset

		if dryRun {
			// 尝试 seek 到目标位置
			if _, err := dev.Seek(actualOffset, io.SeekStart); err != nil {
				return fmt.Errorf("failed to seek to offset 0x%x: %v", actualOffset, err)
			}
			fmt.Printf("Would apply sector %s to offset 0x%x (sector: 0x%x * size: %d + device-offset: 0x%x), size %d bytes\n",
				filepath.Base(s.Path), actualOffset, s.Offset, s.Size, deviceOffset, s.Size)
			continue
		}

		// 打开扇区文件
		sectorFile, err := os.Open(s.Path)
		if err != nil {
			log.Printf("Failed to open sector file %s: %v", s.Path, err)
			continue
		}

		// 定位到目标位置
		if _, err := dev.Seek(actualOffset, io.SeekStart); err != nil {
			log.Printf("Failed to seek to offset %d in device: %v", actualOffset, err)
			sectorFile.Close()
			continue
		}

		// 写入数据
		if _, err := io.CopyN(dev, sectorFile, s.Size); err != nil {
			log.Printf("Failed to write sector file %s to device: %v", s.Path, err)
			sectorFile.Close()
			continue
		}

		fmt.Printf("Applied sector %s to offset 0x%x (sector: 0x%x * size: %d + device-offset: 0x%x), size %d bytes\n",
			filepath.Base(s.Path), actualOffset, s.Offset, s.Size, deviceOffset, s.Size)
		sectorFile.Close()
	}

	if dryRun {
		fmt.Println("\nDry run completed successfully (no data was written)")
	} else {
		fmt.Println("Note: The data is still being written to the device in the background.")
		fmt.Println("Please wait for this program to exit before proceeding.")
		fmt.Println("\n!!! DO NOT MANUALLY CLOSE THIS PROGRAM !!!")

		// 强制同步所有写入到设备
		if err := dev.Sync(); err != nil {
			return fmt.Errorf("failed to sync device: %v", err)
		}

		fmt.Println("\nApply completed successfully")
	}

	return nil
}
