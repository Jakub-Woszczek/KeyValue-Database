package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

const walHeaderSize = 12 // 4 bytes for keyLen, 4 bytes for valLen, 4 bytes for crc

type WAL struct {
	walFile *os.File
	walPath string
	offset  int64
}

func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	// establishing current offset if file already exists
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat WAL file: %w", err)
	}

	return &WAL{walFile: f, walPath: path, offset: info.Size()}, nil
}

// Format: [crc uint32][keyLen uint32][valLen uint32][key bytes][val bytes]
// crc is calculated over concatenated key and value bytes
// return offset as LSN (log sequence number)
type LSN int64

func (w *WAL) AppendRecord(key, value []byte) (LSN, error) {
	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	// computing log size and preparing log buffer
	logLen := walHeaderSize + keyLen + valLen
	log := make([]byte, logLen)

	// witing lengths and data to log buffer
	binary.LittleEndian.PutUint32(log[4:], keyLen)
	binary.LittleEndian.PutUint32(log[8:], valLen)
	copy(log[walHeaderSize:], key)
	copy(log[walHeaderSize+int(keyLen):], value)

	// calculating crc and writing to log header
	checksumInput := log[4:]
	crc := crc32.ChecksumIEEE(checksumInput)
	binary.LittleEndian.PutUint32(log[0:], crc)

	lsn := w.offset
	w.offset += int64(logLen)

	if _, err := w.walFile.Write(log); err != nil {
		return 0, fmt.Errorf("failed to write to WAL: %w", err)
	}

	if err := w.walFile.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync WAL: %w", err)
	}

	return LSN(lsn), nil
}

func (w *WAL) Close(rmWalFile bool) error {
	if rmWalFile {
		if err := os.Remove(w.walPath); err != nil {
			return fmt.Errorf("failed to remove WAL file: %w", err)
		}
	}
	return w.walFile.Close()
}
