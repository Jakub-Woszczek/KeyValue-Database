package db

import (
	"fmt"

	"github.com/Jakub-Woszczek/kvdb/memtable"
	"github.com/Jakub-Woszczek/kvdb/sstable"
	"github.com/Jakub-Woszczek/kvdb/wal"
)

type DB struct {
	Memtable          *memtable.Memtable
	SstManager        *sstable.SSTableManager
	Wal               *wal.WAL
	memtableThreshold int
}

func NewDB(mThreshold int, ssTableDir string) (*DB, error) {
	Wal, err := wal.OpenWAL("wal.log")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %w", err)
	}

	ssm, err := sstable.NewSSTableManager(ssTableDir)
	if err != nil {
		return nil, fmt.Errorf("Failed to init sstable manager: %w", err)
	}

	db := &DB{
		Memtable:          memtable.NewMemtable(),
		SstManager:        ssm,
		Wal:               Wal,
		memtableThreshold: mThreshold,
	}

	return db, nil
}

func (db *DB) Put(key, value []byte) error {
	_, err := db.Wal.AppendRecord(key, value)
	if err != nil {
		return fmt.Errorf("failed to append record to WAL: %w", err)
	}

	db.Memtable.Insert(key, value)
	db.FlushIfOverflow()
	return nil
}

func (db *DB) Get(key []byte) (value []byte, found bool, err error) {
	value, found = db.Memtable.Get(key)
	if found {
		return
	}

	value, found, err = db.SstManager.Get(key)
	if found {
		return
	}

	return nil, false, nil
}

func (db *DB) Close(rmWalFile bool) error {
	return db.Wal.Close(rmWalFile)
}

func (db *DB) FlushIfOverflow() error {
	if db.memtableThreshold > db.Memtable.Size {
		return nil
	}
	err := db.SstManager.Flush(db.Memtable)

	db.Memtable = memtable.NewMemtable()
	return err
}
