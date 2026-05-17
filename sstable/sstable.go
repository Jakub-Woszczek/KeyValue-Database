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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	// "path/filepath"

	datastructures "github.com/Jakub-Woszczek/kvdb/dataStructures"
	"github.com/Jakub-Woszczek/kvdb/memtable"
)

type SSTable struct {
	FileName string
	FilePath string
}

const IndexEntrySize = 24
const FooterSize = 8

type IndexEntry struct {
	Key       []byte
	KeyLen    int32
	KeyOffset int64
	ValLen    int32
	ValOffset int64
}

func (s *SSTable) BuildSSTable(mTable *memtable.Memtable, folderPath string) error {
	f, err := os.Create(s.FilePath)
	if err != nil {
		return fmt.Errorf("failed to create SSTable file: %w", err)
	}
	defer f.Close()

	fileSize := mTable.ValuesSize + mTable.KeysSize + int64(mTable.Size*IndexEntrySize) + FooterSize
	if err = f.Truncate(fileSize); err != nil {
		return fmt.Errorf("Failed to truncate (sstable file): %w", err)
	}

	// offsets
	valOffset := int64(0)
	keyOffset := mTable.ValuesSize
	indexOffset := keyOffset + mTable.KeysSize
	// buffers
	valPosBuffer := datastructures.NewSeekableBuffer(f, valOffset, int(mTable.ValuesSize)) // TODO: change size
	keyPosBuffer := datastructures.NewSeekableBuffer(f, keyOffset, int(mTable.KeysSize))
	indexPosBuffer := datastructures.NewSeekableBuffer(f, indexOffset, int(mTable.Size*IndexEntrySize))

	root := mTable.Root
	iter := memtable.NewMemtableIterator(root)

	for iter.HasNext() {
		node, valid := iter.Next()
		if !valid {
			break
		}
		vOffset := valPosBuffer.CurrentOffset()
		kOffset := keyPosBuffer.CurrentOffset()

		vLen := int32(len(node.Value))
		kLen := int32(len(node.Key))

		if err := valPosBuffer.Put(node.Value); err != nil {
			return fmt.Errorf("failed writing value payload: %w", err)
		}
		if err := keyPosBuffer.Put(node.Key); err != nil {
			return fmt.Errorf("failed writing key payload: %w", err)
		}

		encodedIndex := EncodeIndexBlock(kLen, kOffset, vLen, vOffset)
		if err := indexPosBuffer.Put(encodedIndex); err != nil {
			return fmt.Errorf("failed writing index entry: %w", err)
		}
	}

	// flush remaining at the end
	if err := valPosBuffer.Flush(); err != nil {
		return err
	}
	if err := keyPosBuffer.Flush(); err != nil {
		return err
	}
	if err := indexPosBuffer.Flush(); err != nil {
		return err
	}

	// footer
	footerBuf := make([]byte, FooterSize)
	binary.LittleEndian.PutUint64(footerBuf[0:], uint64(indexOffset))

	footerWriteOffset := indexOffset + int64(mTable.Size*IndexEntrySize)
	_, err = f.WriteAt(footerBuf, footerWriteOffset)
	if err != nil {
		return fmt.Errorf("failed writing footer block: %w", err)
	}
	return nil
}

func EncodeIndexBlock(keyLen int32, keyOffset int64, valLen int32, valOffset int64) []byte {
	buf := make([]byte, IndexEntrySize)

	binary.LittleEndian.PutUint32(buf[0:4], uint32(keyLen))
	binary.LittleEndian.PutUint64(buf[4:12], uint64(keyOffset))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(valLen))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(valOffset))

	return buf
}

func (s *SSTable) Get(key []byte) (value []byte, found bool, err error) {
	f, err := os.Open(s.FilePath)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open SSTable file: %w", err)
	}
	defer f.Close()

	if string(key) == "k9829" {
		fmt.Println("SS get() for k9829")
	}

	indexBuff, indexBlocksOffset, err := getIndexes(f)
	if err != nil {
		return nil, false, fmt.Errorf("get indexes: %w", err)
	}
	keysBlockOffset := int64(binary.LittleEndian.Uint64(indexBuff[4:12])) // Offset of first key

	// Extract keys block
	keysBlockLen := indexBlocksOffset - int64(keysBlockOffset)
	keysBlock, err := readAtChecked(f, keysBlockOffset, keysBlockLen)
	if err != nil {
		return nil, false, fmt.Errorf("keys blocks read: %w", err)
	}

	// Search
	valLen, valOffset, found, err := searchKeysBlock(keysBlock, indexBuff, keysBlockOffset, key)

	// Extract val if found
	if found != true {
		return nil, false, nil
	}
	value, err = readAtChecked(f, valOffset, valLen)
	if err != nil {
		return nil, false, fmt.Errorf("value read: %w", err)
	}

	return value, true, nil
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
