package sandbox

import (
	// "bytes"
	"bytes"
	"fmt"

	"github.com/Jakub-Woszczek/kvdb/memtable"

	// "math/rand"
	"runtime"
	"unsafe"
)

// Global counters to track exactly what we "think" we allocated
var manualDataTotal uint64

func insertVariable(depth int, kSize int, vSize int) *memtable.Node {
	if depth <= 0 {
		return nil
	}

	// Create slices of specific lengths
	key := make([]byte, kSize)
	val := make([]byte, vSize)

	// Increment manual counter:
	// We count the bytes we requested for the backing arrays
	manualDataTotal += uint64(len(key) + len(val))

	newNode := &memtable.Node{
		Key:   key,
		Value: val,
		Color: true,
	}

	// Varying sizes slightly for children to simulate real-world fragmentation
	newNode.Left = insertVariable(depth-1, kSize+1, vSize)
	newNode.Right = insertVariable(depth-1, kSize, vSize+1)
	return newNode
}

func Sandbox() {
	const depth = 15
	manualDataTotal = 0 // Reset counter

	var m1, m2 runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&m1)

	// Build tree with starting key/val sizes of 10 and 20
	table := memtable.MEMTABLE{}
	table.Root = insertVariable(depth, 10, 20)

	runtime.ReadMemStats(&m2)

	// --- Calculations ---

	numNodes := (1 << depth) - 1
	nodeStructSize := uint64(unsafe.Sizeof(memtable.Node{}))

	// Total Manual = (Count * StructSize) + (Sum of all slice lengths)
	theoreticalTotal := (uint64(numNodes) * nodeStructSize) + manualDataTotal
	actualAllocated := m2.HeapAlloc - m1.HeapAlloc

	fmt.Printf("--- Results for %d Nodes ---\n", numNodes)
	fmt.Printf("Fixed Struct Size:    %d bytes\n", nodeStructSize)
	fmt.Printf("Total Raw Data Size:  %d bytes\n", manualDataTotal)
	fmt.Printf("Manual Estimate:      %d bytes\n", theoreticalTotal)
	fmt.Printf("ReadMemStats (Heap):  %d bytes\n", actualAllocated)

	diff := int64(actualAllocated) - int64(theoreticalTotal)
	fmt.Printf("Unaccounted Overhead: %d bytes (%.2f%%)\n", diff, float64(diff)/float64(theoreticalTotal)*100)

	// Insight: This overhead is usually the "Size Class" padding
	fmt.Printf("Overhead per node:    %d bytes\n", diff/int64(numNodes))

	runtime.KeepAlive(table)
}

func HowMuchMemo() {
	// table := memtable.MEMTABLE{}
	key := make([]byte, 100)
	val := make([]byte, 200)
	node := memtable.Node{
		Key:   key,
		Value: val,
		Color: true,
	}
	entrySize := uint64(unsafe.Sizeof(node)) + uint64(cap(key)+cap(val))

	fmt.Printf("Size of Node struct: %d bytes\n", unsafe.Sizeof(node))
	fmt.Printf("Computed size: %d bytes ", entrySize)
}

func EasyCompareXD() {
	s := []byte("costam")
	fmt.Println(string(s[1 : 1+2]))

	// l,,m so b is on right side
	a := []byte("clq")
	b := []byte("cni")
	fmt.Printf("compare: %v", bytes.Compare(a, b))
}
