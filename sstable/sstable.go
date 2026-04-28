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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	datastructures "github.com/Jakub-Woszczek/kvdb/dataStructures"
	"github.com/Jakub-Woszczek/kvdb/memtable"
)

type SSTable struct {
	FileName string
}

const IndexEntrySize = 24

type IndexEntry struct {
	Key       []byte
	KeyLen    int32
	KeyOffset int64
	ValLen    int32
	ValOffset int64
}

func (s *SSTable) BuildSSTable(mTable *memtable.Memtable) ([]byte, error) {
	f, err := os.Create(s.FileName)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer f.Close()

	sstableWriter := bufio.NewWriter(f)
	defer sstableWriter.Flush()

	var index []IndexEntry // TODO: Make this static size (given size of memtable)
	var offset int64 = 0

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
		valLen := int32(n)

		offset += int64(n)

		index = append(index, IndexEntry{
			Key:       node.Key, // reference
			ValOffset: valOffset,
			ValLen:    valLen,
		})

		node = node.Right
	}

	for i := range index {
		entry := &index[i]

		entryKeyOffset := offset
		n, _ := sstableWriter.Write(entry.Key)

		entry.KeyOffset = entryKeyOffset
		entry.KeyLen = int32(n)

		offset += int64(n)

	}

	index_block_offset := offset
	for i := range index {
		entry := &index[i]
		n, err := sstableWriter.Write(EncodeIndexBlock(entry))
		if err != nil {
			return nil, err
		}
		offset += int64(n)
	}

	// Write footer
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf[0:], uint64(index_block_offset))
	_, err = sstableWriter.Write(buf)

	return nil, nil
}

func EncodeIndexBlock(entry *IndexEntry) []byte {
	buf := make([]byte, IndexEntrySize)

	binary.LittleEndian.PutUint32(buf[0:], uint32(entry.KeyLen))
	binary.LittleEndian.PutUint64(buf[4:], uint64(entry.KeyOffset))
	binary.LittleEndian.PutUint32(buf[12:], uint32(entry.ValLen))
	binary.LittleEndian.PutUint64(buf[16:], uint64(entry.ValOffset))

	return buf
}

func (s *SSTable) Get(key []byte) (value []byte, err error) {
	f, err := os.Open(s.FileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}
	defer f.Close()

	indexBuff, indexBlocksOffset, err := getIndexes(f)
	if err != nil {
		return nil, fmt.Errorf("get indexes: %w", err)
	}
	keysBlockOffset := int64(binary.LittleEndian.Uint64(indexBuff[4:12])) // Offset of first key

	// Extract keys block
	keysBlockLen := indexBlocksOffset - int64(keysBlockOffset)
	keysBlock, err := readAtChecked(f, keysBlockOffset, keysBlockLen)
	if err != nil {
		return nil, fmt.Errorf("keys blocks read: %w", err)
	}

	// Search
	valLen, valOffset, found, err := searchKeysBlock(keysBlock, indexBuff, keysBlockOffset, key)

	// Extract val if found
	if found != true {
		return nil, nil
	}
	value, err = readAtChecked(f, valOffset, valLen)
	if err != nil {
		return nil, fmt.Errorf("value read: %w", err)
	}

	return value, nil
}

func getIndexes(f *os.File) (indexesBuf []byte, indexBlocksOffset int64, err error) {
	footerOffset, err := f.Seek(-8, io.SeekEnd)
	if err != nil {
		return nil, 0, fmt.Errorf("seek footer: %w", err)
	}

	footerBuf := make([]byte, 8)
	_, err = f.Read(footerBuf)
	if err != nil {
		return nil, 0, fmt.Errorf("read footer: %w", err)
	}

	indexBlocksOffset = int64(binary.LittleEndian.Uint64(footerBuf))
	indexesLength := footerOffset - indexBlocksOffset

	indexesBuf, err = readAtChecked(f, indexBlocksOffset, indexesLength)
	if err != nil {
		return nil, 0, fmt.Errorf("read index blocks: %w", err)
	}

	return
}

// Function accepts block of keys and block of indexes. If key is found return offset of value and its length, else nil.
func searchKeysBlock(keysBlock []byte, indexesBlock []byte, keysBlockOffset int64, key []byte) (valLen int64, valOffset int64, found bool, err error) {

	indexesLength := len(indexesBlock)
	if indexesLength%IndexEntrySize != 0 {
		return 0, 0, false, fmt.Errorf("Lenght of block of index is wrong (indexesLength: %d not multiple of IndexEntrySize: %d )", indexesLength, IndexEntrySize)
	}
	amountOfEntries := indexesLength / IndexEntrySize

	left, right := 0, amountOfEntries-1
	for left <= right {
		mid := (right + left) / 2

		offset := mid * IndexEntrySize
		index := indexesBlock[offset : offset+IndexEntrySize]

		keyLen := int32(binary.LittleEndian.Uint32(index[0:4]))                       // TODO: make those const values
		keyOffset := int64(binary.LittleEndian.Uint64(index[4:12])) - keysBlockOffset // keyOffsef is in regard to start of file, no start of keysBlock
		valLen := int64(binary.LittleEndian.Uint32(index[12:16]))
		valOffset := int64(binary.LittleEndian.Uint64(index[16:24]))

		keyCheck := keysBlock[keyOffset : keyOffset+int64(keyLen)]
		switch bytes.Compare(key, keyCheck) {
		case 0:
			return valLen, valOffset, true, nil
		case -1: // key < keyCheck
			right = mid - 1
		case 1:
			left = mid + 1
		}

	}
	return 0, 0, false, nil
}

func readAtChecked(f *os.File, offset int64, length int64) ([]byte, error) {
	if offset < 0 || length < 0 {
		return nil, fmt.Errorf("invalid read: negative offset/length (offset=%d, len=%d)", offset, length)
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	if offset+length > stat.Size() {
		return nil, fmt.Errorf(
			"invalid read: out of bounds (offset=%d len=%d fileSize=%d)",
			offset, length, stat.Size(),
		)
	}

	buf := make([]byte, length)
	if _, err := f.ReadAt(buf, offset); err != nil {
		return nil, fmt.Errorf("read at offset %d: %w", offset, err)
	}

	return buf, nil
}
