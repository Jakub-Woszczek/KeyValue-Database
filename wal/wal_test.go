package wal

import (
    "testing"
	"os"
	// "encoding/hex"
	// "github.com/stretchr/testify/assert"
)


func TestAppend_EmptyFile_LSNStartsAtZero(t *testing.T) {
	// GIVEN
	tmpFile, err := os.CreateTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := tmpFile.Name()
	tmpFile.Close()
	defer os.Remove(path)

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.walFile.Close()

	key := []byte("key")
	value := []byte("value")

	// WHEN
	lsn, err := wal.AppendRecord(key, value)

	// THEN
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if int64(lsn) != 0 {
		t.Fatalf("expected LSN = 0, got %d", lsn)
	}
}

func TestAppend_NonEmptyFile_LSNEqualsInitialSize(t *testing.T) {
	// GIVEN
	tmpFile, err := os.CreateTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := tmpFile.Name()
	defer os.Remove(path)

	// not empty file
	initialData := []byte("some-random-bytes-to-simulate-existing-wal-data")
	if _, err := tmpFile.Write(initialData); err != nil {
		t.Fatalf("failed to prefill file: %v", err)
	}
	tmpFile.Close()

	initialSize := int64(len(initialData))

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.walFile.Close()

	key := []byte("a")
	value := []byte("b")

	// WHEN
	lsn, err := wal.AppendRecord(key, value)

	// THEN
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if int64(lsn) != initialSize {
		t.Fatalf("expected LSN = %d, got %d", initialSize, lsn)
	}
}

func TestAppend_NonEmptyFile_OffsetMovesCorrectly(t *testing.T) {
	// GIVEN
	tmpFile, err := os.CreateTemp("", "wal-test-*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	path := tmpFile.Name()
	defer os.Remove(path)

	// prefill with arbitrary bytes
	initialData := []byte("garbage-data")
	if _, err := tmpFile.Write(initialData); err != nil {
		t.Fatalf("failed to prefill file: %v", err)
	}
	tmpFile.Close()

	initialSize := int64(len(initialData))

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer wal.walFile.Close()

	key := []byte("foo")
	value := []byte("bar")

	expectedDelta := int64(walHeaderSize + len(key) + len(value))

	// WHEN
	_, err = wal.AppendRecord(key, value)

	// THEN
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedOffset := initialSize + expectedDelta

	if wal.offset != expectedOffset {
		t.Fatalf("expected offset %d, got %d", expectedOffset, wal.offset)
	}
}