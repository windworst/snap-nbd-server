package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage:")
		fmt.Println("  snap-nbd server [options]")
		fmt.Println("  snap-nbd patch [options]")
		fmt.Println("\nOptions:")
		fmt.Println("  server:")
		fmt.Println("    -device string                Block device or image file path (required)")
		fmt.Println("    -sector-dir string            CopyOnWrite sector file directory (required)")
		fmt.Println("    -listen string                Listen address, format like :10809 (default :10809)")
		fmt.Println("    -sector-size int              Sector size (must be a multiple of 512 and power of 2) (default 4096)")
		fmt.Println("    -log string                   Log file path (optional, default to stderr)")
		fmt.Println("    -filter-size uint             Bloom filter estimated element count (default 100000)")
		fmt.Println("    -filter-fpr float             Bloom filter false positive rate (0-1) (default 0.01)")
		fmt.Println("    -cache-size int               LRU cache size (number of sectors to cache) (default 5000)")
		fmt.Println("    -enable-prefetch              Enable prefetch cache")
		fmt.Println("    -prefetch-multiplier int      Prefetch multiplier (relative to sector size) (default 16)")
		fmt.Println("    -max-consecutive-reads int    Maximum consecutive reads before prefetch (default 4)")
		fmt.Println("\n  patch:")
		fmt.Println("    -sector-dir string            Sector file directory (required)")
		fmt.Println("    -device string                Target block device or image file path (required)")
		fmt.Println("    -device-offset int            Offset in the target device to start writing (in bytes)")
		fmt.Println("    -dry-run                      Dry run mode (don't actually write to device)")
		os.Exit(0)
	}

	command := os.Args[1]
	os.Args = append(os.Args[:1], os.Args[2:]...)

	switch command {
	case "server":
		var (
			device                  = flag.String("device", "", "Block device or image file path (required)")
			sectorDir               = flag.String("sector-dir", "", "CopyOnWrite sector file directory (required)")
			listenAddr              = flag.String("listen", ":10809", "Listen address, format like :10809")
			sectorSize              = flag.Int64("sector-size", 4096, "Sector size (must be a multiple of 512 and power of 2)")
			logFile                 = flag.String("log", "", "Log file path (optional, default to stderr)")
			filterSize              = flag.Uint("filter-size", 100000, "Bloom filter estimated element count")
			filterFalsePositiveRate = flag.Float64("filter-fpr", 0.01, "Bloom filter false positive rate (0-1)")
			cacheSize               = flag.Int("cache-size", 5000, "LRU cache size (number of sectors to cache)")
			enablePrefetch          = flag.Bool("enable-prefetch", false, "Enable prefetch cache")
			prefetchMultiplier      = flag.Int("prefetch-multiplier", 16, "Prefetch multiplier (relative to sector size)")
			maxConsecutiveReads     = flag.Int("max-consecutive-reads", 4, "Maximum consecutive reads before prefetch")
		)
		flag.Parse()

		if *device == "" {
			log.Fatal("Block device or image file path is required (-device)")
		}
		if *sectorDir == "" {
			log.Fatal("Sector file directory is required (-sector-dir)")
		}

		if err := startServer(*device, *sectorDir, *listenAddr, *sectorSize, *logFile, *filterSize, *filterFalsePositiveRate, *cacheSize, *enablePrefetch, *prefetchMultiplier, *maxConsecutiveReads); err != nil {
			log.Fatalf("Server error: %v", err)
		}

	case "patch":
		var (
			sectorDir    = flag.String("sector-dir", "", "Sector file directory (required)")
			device       = flag.String("device", "", "Target block device or image file path (required)")
			deviceOffset = flag.Int64("device-offset", 0, "Offset in the target device to start writing (in bytes)")
			dryRun       = flag.Bool("dry-run", false, "Dry run mode (don't actually write to device)")
		)
		flag.Parse()

		if *sectorDir == "" {
			log.Fatal("Sector file directory is required (-sector-dir)")
		}
		if *device == "" {
			log.Fatal("Target device or image file path is required (-device)")
		}

		if err := patchSectors(*sectorDir, *device, *deviceOffset, *dryRun); err != nil {
			log.Fatalf("Patch error: %v", err)
		}

	default:
		log.Fatalf("Unknown command: %s", command)
	}
}
