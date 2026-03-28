package serializer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const headerSize = 8

// Encode serializes a key-value pair into bytes.
// Format: [keyLen uint32][valueLen uint32][key bytes][val bytes]
func Encode(key []byte, value []byte) []byte {

	keyLen := len(key)
	valueLen := len(value)

	totalSize := headerSize + keyLen + valueLen
	saveBuf := make([]byte, totalSize)

	binary.LittleEndian.PutUint32(saveBuf[0:], uint32(keyLen))
	binary.LittleEndian.PutUint32(saveBuf[4:], uint32(valueLen))

	copy(saveBuf[8:], key)
	copy(saveBuf[8+len(key):], value)

	return saveBuf
}

func Decode(buff []byte) (key, val []byte, err error) {
	r := bytes.NewReader(buff)

	header := make([]byte, headerSize)

	_, err = io.ReadFull(r, header)
	if err != nil {
		return nil, nil, err
	}

	keyLen := binary.LittleEndian.Uint32(header[0:4])
	valLen := binary.LittleEndian.Uint32(header[4:8])

	if keyLen > 1<<20 || valLen > 1<<26 {
		// key max 1MB, val max 64MB
		return nil, nil, fmt.Errorf("record too large: key=%d val=%d", keyLen, valLen)
	}

	key = make([]byte, keyLen)
	_, err = io.ReadFull(r, key)
	if err != nil {
		return nil, nil, err
	}

	val = make([]byte, valLen)
	_, err = io.ReadFull(r, val)
	if err != nil {
		return nil, nil, err
	}

	return
}
