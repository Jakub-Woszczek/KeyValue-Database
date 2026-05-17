package datastructures

import (
	"fmt"
	"os"
)

type SeekableBuffer struct {
	file   *os.File
	buffer []byte
	pos    int   // Current position in the buffer
	offset int64 // The starting physical offset in the file for this buffer
}

func NewSeekableBuffer(f *os.File, startOffset int64, size int) *SeekableBuffer {
	return &SeekableBuffer{
		file:   f,
		buffer: make([]byte, size),
		offset: startOffset,
		pos:    0,
	}
}

func (sb *SeekableBuffer) Put(data []byte) error {
	dataLen := len(data)

	// data doesn't fit
	if sb.pos+dataLen > len(sb.buffer) {
		if err := sb.Flush(); err != nil {
			return err
		}
	}

	// data is larger than the entire buffer
	if dataLen > len(sb.buffer) {
		_, err := sb.file.WriteAt(data, sb.offset)
		sb.offset += int64(dataLen)
		return err
	}

	copy(sb.buffer[sb.pos:], data)
	sb.pos += dataLen
	return nil
}

func (sb *SeekableBuffer) Flush() error {
	if sb.pos == 0 {
		return nil
	}

	_, err := sb.file.WriteAt(sb.buffer[:sb.pos], sb.offset) // TODO: add gorutine for it
	if err != nil {
		return fmt.Errorf("flush failed at offset %d: %w", sb.offset, err)
	}

	sb.offset += int64(sb.pos)
	sb.pos = 0
	return nil
}

func (sb *SeekableBuffer) CurrentOffset() int64 {
	return sb.offset + int64(sb.pos)
}
