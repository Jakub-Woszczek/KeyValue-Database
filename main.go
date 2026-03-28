package main

import (
	"fmt"
	// "github.com/Jakub-Woszczek/kvdb/serializer"
	"github.com/Jakub-Woszczek/kvdb/memtable"
)

func main() {
	key := []byte("myKey")
	value := []byte("myValue")

	node := memtable.Node{}
	fmt.Printf("Original key: %s, value: %s\n", key, value)

	fmt.Println(node)
	node.Key = key
	fmt.Println(node)
}
