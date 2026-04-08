package visualizer

import (
	"fmt"
	"github.com/Jakub-Woszczek/kvdb/memtable"
	"math/rand"
	"strconv"
	// "testing"
	"time"
)

func Visualize() {
	N := 15 // liczba losowych wartości
	m := memtable.NewMemtable()

	rand.Seed(time.Now().UnixNano())

	keys := make([]int, N)
	for i := 0; i < N; i++ {
		keys[i] = rand.Intn(100) // losowa liczba 0..99
		k := []byte(strconv.Itoa(keys[i]))
		v := []byte(fmt.Sprintf("val-%d", keys[i]))
		m.Insert(k, v)
	}

	fmt.Println("Wygenerowane klucze:", keys)
	fmt.Println("\nDrzewo RB:")
	PrintTree(m)
}
