/*
SSTable menager scheme:

Files are named: <level>_<fileID>.sst
Where:
	0 <= level < 10
	0 <= fileID < 1_000_000 // i use global increase

*/

package sstable

import (
	"errors"
	"fmt"
	"github.com/Jakub-Woszczek/kvdb/memtable"
	"path/filepath"
)

type SSTableMenager struct {
	Dir    string
	levels [][]*SSTable
	// memtable *memtable.Memtable
	sstCounter int32
}

func NewSSTableMenager(dir string) *SSTableMenager {
	sm := &SSTableMenager{
		Dir: dir,
	}
	return sm
}

func (sm *SSTableMenager) Get(key []byte) (value []byte, found bool, err error) {
	var errs error

	// Sstables search
	for _, level := range sm.levels {
		for _, sstable := range level {
			path := filepath.Join(sm.Dir, sstable.FileName)
			sstable.FileName = path // not sure if i should update like that

			value, found, err = sstable.Get(key)

			if err != nil {
				errs = errors.Join(errs, err)
				continue
			}

			if found {
				return
			}

		}
	}
	return nil, false, errs
}

func (sm *SSTableMenager) Flush(m *memtable.Memtable) error {
	// stLvlAmount := len(sm.levels[0])
	fileName := fmt.Sprintf("L%d_%06d.sst", 0, sm.sstCounter)

	s := &SSTable{
		FileName: fileName,
	}

	err := s.BuildSSTable(m)
	if err != nil {
		return fmt.Errorf("sstable build: %w", err)
	}

	// append to Lvl 0
	sm.levels[0] = append(sm.levels[0], s)
	sm.sstCounter++ // increase if succedes

	return nil
}
