package main

import (
	// "fmt"
	// "github.com/Jakub-Woszczek/kvdb/serializer"
	// "github.com/Jakub-Woszczek/kvdb/memtable"
	"flag"
	"fmt"
	// "github.com/Jakub-Woszczek/kvdb/visualizer"
)

var flagvar int
var verbose bool

func main() {
	// visualizer.Visualize()
	var flagN = flag.Int("n", 15, "number of random keys to insert")
	flag.IntVar(&flagvar, "flagname", 1234, "help message for flagname")
	verbose := flag.Bool("v", false, "enable verbose mode")

	flag.Parse()

	fmt.Printf("Inserting %d random keys into the memtable...\n", *flagN)
	fmt.Println("flagvar has value ", flagvar)
	if *verbose {
		fmt.Println("Verbose mode enabled")
	}

}
