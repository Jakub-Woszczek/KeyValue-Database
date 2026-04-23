package visualizer

import (
	"encoding/binary"
	"fmt"
	"math/rand"

	// "strconv"
	"time"

	"github.com/Jakub-Woszczek/kvdb/memtable"
	// "github.com/Jakub-Woszczek/kvdb/visualizer"
)

const charset = "abcdefghijklmnopqrstuvwxyz"
const keySize = 3

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func GenerateRandomTree(N int, showBuild bool) (m *memtable.MEMTABLE, keys []string) {
	m = memtable.NewMemtable()

	rand.Seed(time.Now().UnixNano())

	keys = make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = randomString(keySize)
		k := []byte(keys[i])
		v := []byte(fmt.Sprintf("VAL-%s", keys[i]))
		m.Insert(k, v)
		PrintTree(m)
		fmt.Println("-----------------------------")
	}
	return m, keys
}

func IntToBytes(i int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}
