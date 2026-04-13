package visualizer

import (
	"fmt"

	"github.com/Jakub-Woszczek/kvdb/memtable"
)

func Visualize() {
	N := 4 // Number of random keys to insert
	m, keys := memtable.GenerateRandomTree(N)

	fmt.Println("Wygenerowane klucze:", keys)
	fmt.Println("\nDrzewo RB:")
	PrintTree(m)
}
