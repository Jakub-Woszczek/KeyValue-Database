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
	"os"
	"path/filepath"

	"github.com/Jakub-Woszczek/kvdb/memtable"
)

const LevelsAmount = 7

type SSTableManager struct {
	Dir    string
	levels [][]*SSTable
	// memtable *memtable.Memtable
	sstCounter int32
}

func NewSSTableManager(sstFolderPath string) (*SSTableManager, error) {
	err := os.MkdirAll(sstFolderPath, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to init sstable folder: %w", err)
	}

	sm := &SSTableManager{
		Dir:        sstFolderPath,
		levels:     make([][]*SSTable, LevelsAmount),
		sstCounter: 0,
	}
	return sm, nil
}

func (sm *SSTableManager) Get(key []byte) (value []byte, found bool, err error) {
	var errs error

	// Sstables search
	for _, level := range sm.levels {
		for i := len(level) - 1; i >= 0; i-- {

			value, found, err = level[i].Get(key)
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

func (sm *SSTableManager) Flush(m *memtable.Memtable) error {
	// stLvlAmount := len(sm.levels[0])
	fileName := fmt.Sprintf("L%d_%06d.sst", 0, sm.sstCounter)

	s := &SSTable{
		FilePath: filepath.Join(sm.Dir, fileName),
	}

	err := s.BuildSSTable(m, sm.Dir)
	if err != nil {
		return fmt.Errorf("sstable build: %w", err)
	}

	// append to Lvl 0
	sm.levels[0] = append(sm.levels[0], s)
	sm.sstCounter++ // increase if succeeds

	return nil
}
