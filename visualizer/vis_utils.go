package visualizer

import (
	// "bytes"
	"fmt"

	"github.com/Jakub-Woszczek/kvdb/memtable"
)

// Publiczna metoda do wywołania
func PrintTree(m *memtable.Memtable) {
	if m.Root == nil {
		fmt.Println("(empty tree)")
		return
	}
	printNode(m.Root, "", true)
}

// Rekurencyjny printer
func printNode(n *memtable.Node, prefix string, isTail bool) {
	if n == nil {
		return
	}

	// Najpierw prawa strona (żeby drzewo było "obrócone")
	if n.Right != nil {
		newPrefix := prefix
		if isTail {
			newPrefix += "│   "
		} else {
			newPrefix += "    "
		}
		printNode(n.Right, newPrefix, false)
	}

	// Print aktualnego noda
	fmt.Print(prefix)
	if isTail {
		fmt.Print("└── ")
	} else {
		fmt.Print("┌── ")
	}

	fmt.Printf("%s\n", formatKey(n.Key))

	// Lewa strona
	if n.Left != nil {
		newPrefix := prefix
		if isTail {
			newPrefix += "    "
		} else {
			newPrefix += "│   "
		}
		printNode(n.Left, newPrefix, true)
	}
}

// Helper: ładne wyświetlanie []byte
func formatKey(key []byte) string {
	// jeśli to tekst → pokaż jako string
	if isPrintable(key) {
		return string(key)
	}
	// fallback → hex
	return fmt.Sprintf("%x", key)
}

// sprawdza czy byte slice wygląda jak tekst
func isPrintable(b []byte) bool {
	for _, c := range b {
		if c < 32 || c > 126 {
			return false
		}
	}
	return true
}
