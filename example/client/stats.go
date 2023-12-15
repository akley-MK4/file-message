package main

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
)

type memorySnapshot struct {
	AllocBytes uint64
	AllocMBs   uint64
	TotalAlloc uint64
}

func printMemoryStatus() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	memStats := memorySnapshot{
		AllocBytes: ms.Alloc,
		AllocMBs:   ms.Alloc / (1024 * 1024),
		TotalAlloc: ms.TotalAlloc,
	}
	data, _ := json.Marshal(memStats)
	log.Println("=============MemoryStats=============")
	fmt.Println(string(data))
	log.Println("====================================")
}
