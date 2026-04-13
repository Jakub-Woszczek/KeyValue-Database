package main

import (
	// "fmt"
	// "github.com/Jakub-Woszczek/kvdb/serializer"
	// "github.com/Jakub-Woszczek/kvdb/memtable"
	"flag"
	"fmt"
	"os"

	"github.com/Jakub-Woszczek/kvdb/db"
	"github.com/Jakub-Woszczek/kvdb/memtable"
	"github.com/Jakub-Woszczek/kvdb/sandbox"
	"github.com/Jakub-Woszczek/kvdb/server"
	"github.com/Jakub-Woszczek/kvdb/sstable"
	"github.com/Jakub-Woszczek/kvdb/visualizer"
	"github.com/joho/godotenv"
)

var runVis bool
var runServer bool
var runSandbox bool
var runSSTable bool

func main() {
	flag.BoolVar(&runVis, "v", false, "run the visualizer")
	flag.BoolVar(&runServer, "s", false, "run the server")
	flag.BoolVar(&runSandbox, "sdbx", false, "run the sandbox code")
	flag.BoolVar(&runSSTable, "st", false, "run sstable")

	flag.Parse()

	if runServer {
		godotenv.Load()
		fmt.Println("Starting server...")
		d, err := db.NewDB()
		if err != nil {
			fmt.Println("Failed to initialize database:", err)
			return
		}
		defer d.Close(true)

		serverPort := os.Getenv("SERVER_PORT")
		if serverPort == "" {
			fmt.Println("SERVER_PORT environment variable not set, defaulting to 7777")
			serverPort = "7777"
		}

		srv, err := server.New(d, ":"+serverPort)
		if err != nil {
			fmt.Println("Failed to start server:", err)
		}
		fmt.Println("kvdb listening on :" + serverPort)
		srv.Serve()
		return
	}
	if runVis {
		fmt.Println("Running visualizer...")
		visualizer.Visualize()
		return
	}
	if runSandbox {
		fmt.Println("Running sandbox...")
		sandbox.EasyCompareXD()
	}
	if runSSTable {
		fmt.Println("Running SSTable builder...")
		s := sstable.SSTable{SSTableFilePath: "sstable.dat"}
		m, _ := memtable.GenerateRandomTree(3)

		visualizer.PrintTree(m)
		s.BuildSSTable(m)
		return
	}

}
