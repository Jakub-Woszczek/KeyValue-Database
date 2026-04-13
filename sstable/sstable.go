/*
SSTable file layout:

[ Vals block		          ]  ← raw values, variable size
[ Keys block                  ]  ← raw keys, variable size
[ Index block                 ]  ← fixed 24 bytes per entry
[ Footer                      ]  ← points to where index block starts

Each index block:
[ key_len : i32 ][ key_offset : i64 ][ val_len : i32 ][ val_offset : i64 ]

	4 bytes           8 bytes             4 bytes           8 bytes

Footer:
[ index_block_offset : i64 ]

	8 bytes
*/
package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	datastructures "github.com/Jakub-Woszczek/kvdb/dataStructures"
	"github.com/Jakub-Woszczek/kvdb/memtable"
)

type SSTable struct {
	SSTableFilePath string
}

const IndexEntrySize = 24

type IndexEntry struct {
	Key       []byte
	KeyLen    uint32
	KeyOffset uint64
	ValLen    uint32
	ValOffset uint64
}

func (s *SSTable) BuildSSTable(mTable *memtable.MEMTABLE) ([]byte, error) {
	f, err := os.Create(s.SSTableFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer f.Close()

	sstableWriter := bufio.NewWriter(f)
	defer sstableWriter.Flush()

	var index []IndexEntry
	var offset uint64 = 0

	stack := datastructures.Stack[*memtable.Node]{}
	node := mTable.Root

	for stack.Len() > 0 || node != nil {
		for node != nil {
			stack.Push(node)
			node = node.Left
		}

		node, _ = stack.Pop()

		valOffset := offset
		n, _ := sstableWriter.Write(node.Value)
		valLen := uint32(n)

		// Debug print
		// fmt.Printf(
		// 	"[VAL] key=%s val=%s valOffset=%d valLen=%d runningOffset=%d\n",
		// 	string(node.Key),
		// 	string(node.Value),
		// 	valOffset,
		// 	valLen,
		// 	offset,
		// )

		offset += uint64(n)

		index = append(index, IndexEntry{
			Key:       node.Key, // reference
			ValOffset: valOffset,
			ValLen:    valLen,
		})
		// Debug print
		// fmt.Printf(
		// 	"[IDX-APPEND] key=%s valOffset=%d valLen=%d\n",
		// 	string(node.Key),
		// 	valOffset,
		// 	valLen,
		// )

		node = node.Right
	}

	// keyBlockOffset := offset

	for i := range index {
		entry := &index[i]

		entryKeyOffset := offset
		n, _ := sstableWriter.Write(entry.Key)

		entry.KeyOffset = entryKeyOffset
		entry.KeyLen = uint32(n)

		offset += uint64(n)

		// Debug print
		// fmt.Printf(
		// 	"[KEY] key=%s keyOffset=%d keyLen=%d runningOffset=%d\n",
		// 	string(entry.Key),
		// 	entryKeyOffset,
		// 	entry.KeyLen,
		// 	offset,
		// )
	}

	index_block_offset := offset
	for i := range index {
		entry := &index[i]
		n, err := sstableWriter.Write(EncodeIndexBlock(entry))
		if err != nil {
			return nil, err
		}
		offset += uint64(n)
	}

	// Write footer
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf[0:], index_block_offset)
	_, err = sstableWriter.Write(buf)

	return nil, nil
}

func EncodeIndexBlock(entry *IndexEntry) []byte {
	buf := make([]byte, IndexEntrySize)

	binary.LittleEndian.PutUint32(buf[0:], entry.KeyLen)
	binary.LittleEndian.PutUint64(buf[4:], entry.KeyOffset)
	binary.LittleEndian.PutUint32(buf[12:], entry.ValLen)
	binary.LittleEndian.PutUint64(buf[16:], entry.ValOffset)

	return buf
}
