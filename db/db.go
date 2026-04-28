package db

import (
	"fmt"

	"github.com/Jakub-Woszczek/kvdb/memtable"
	"github.com/Jakub-Woszczek/kvdb/sstable"
	"github.com/Jakub-Woszczek/kvdb/wal"
)

type DB struct {
	Memtable         *memtable.Memtable
	SstMenager       *sstable.SSTableMenager
	Wal              *wal.WAL
	memtableTreshold int
}

func NewDB(mTreshold int) (*DB, error) {
	Wal, err := wal.OpenWAL("wal.log")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	db := &DB{
		Memtable:         memtable.NewMemtable(),
		Wal:              Wal,
		memtableTreshold: mTreshold,
	}
	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	_, err := db.Wal.AppendRecord(key, value)
	if err != nil {
		return fmt.Errorf("failed to append record to WAL: %w", err)
	}

	db.Memtable.Insert(key, value)
	return nil
}

func (db *DB) Get(key []byte) (value []byte, found bool, err error) {
	value, found = db.Memtable.Get(key)
	if found {
		return
	}

	value, found, err = db.SstMenager.Get(key)
	if found {
		return
	}

	return nil, false, nil
}

func (db *DB) Close(rmWalFile bool) error {
	return db.Wal.Close(rmWalFile)
}

func (db *DB) FlushIfOverflow() error {
	if db.memtableTreshold < db.Memtable.Size {
		return nil
	}
	err := db.SstMenager.Flush(db.Memtable)
	return err
}
