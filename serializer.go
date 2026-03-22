package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

const headerSize = 8;

// Encode serializes a key-value pair into bytes.
// Format: [key_len uint32][val_len uint32][key bytes][val bytes]
func Encode(key []byte, value []byte) []byte {

	key_len := len(key)
	value_len := len(value)

	total_size := headerSize + key_len + value_len
	save_buf := make([]byte,total_size)

	binary.LittleEndian.PutUint32(save_buf[0:], uint32(key_len))
	binary.LittleEndian.PutUint32(save_buf[4:], uint32(value_len))

    copy(save_buf[8:], key)
    copy(save_buf[8+len(key):], value)
	
    return save_buf
}

func Decode(r io.Reader) (key,val []byte, err error) {
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

	key = make([]byte,keyLen)
	_, err = io.ReadFull(r, key)
	if err != nil {
		return nil, nil, err
	}
	
	val = make([]byte,valLen)
	_, err = io.ReadFull(r, val)
	if err != nil {
		return nil, nil, err
	}

	return
}

