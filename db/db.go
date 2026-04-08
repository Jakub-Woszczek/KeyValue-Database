package db

import (
	"fmt"

	"github.com/Jakub-Woszczek/kvdb/memtable"
	"github.com/Jakub-Woszczek/kvdb/wal"
)

type DB struct {
	Memtable *memtable.MEMTABLE
	Wal      *wal.WAL
}

func NewDB() (*DB, error) {
	Wal, err := wal.OpenWAL("wal.log")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	d := &DB{
		Memtable: memtable.NewMemtable(),
		Wal:      Wal,
	}
	return d, nil
}

func (db *DB) Put(key, value []byte) error {
	_, err := db.Wal.AppendRecord(key, value)
	if err != nil {
		return fmt.Errorf("failed to append record to WAL: %w", err)
	}

	db.Memtable.Insert(key, value)
	return nil
}

func (db *DB) Get(key []byte) []byte {
	return db.Memtable.Get(key)
}

func (db *DB) Close(rmWalFile bool) error {
	return db.Wal.Close(rmWalFile)
}
