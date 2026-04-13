package memtable

import (
	"encoding/binary"
	"fmt"
	"math/rand"

	// "strconv"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz"
const keySize = 5

func randomString(length int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func GenerateRandomTree(N int) (m *MEMTABLE, keys []string) {
	m = NewMemtable()

	rand.Seed(time.Now().UnixNano())

	keys = make([]string, N)
	for i := 0; i < N; i++ {
		keys[i] = randomString(keySize)
		k := []byte(keys[i])
		v := []byte(fmt.Sprintf("VAL-%s", keys[i]))
		m.Insert(k, v)
	}
	return m, keys
}

func IntToBytes(i int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}
