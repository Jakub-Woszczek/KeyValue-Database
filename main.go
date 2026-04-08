package main

import (
	// "fmt"
	// "github.com/Jakub-Woszczek/kvdb/serializer"
	// "github.com/Jakub-Woszczek/kvdb/memtable"
	"flag"
	"fmt"
	"os"

	// "github.com/Jakub-Woszczek/kvdb/visualizer"
	"github.com/Jakub-Woszczek/kvdb/db"
	"github.com/Jakub-Woszczek/kvdb/server"
	"github.com/joho/godotenv"
)

var flagvar int

// var verbose bool
var runServer bool

func main() {
	// var flagN = flag.Int("n", 15, "number of random keys to insert")
	// flag.IntVar(&flagvar, "flagname", 1234, "help message for flagname")
	// verbose := flag.Bool("v", false, "enable verbose mode")
	flag.BoolVar(&runServer, "s", false, "run the server")

	flag.Parse()

	// fmt.Printf("Inserting %d random keys into the memtable...\n", *flagN)
	// fmt.Println("flagvar has value ", flagvar)
	// if *verbose {
	// fmt.Println("Verbose mode enabled")
	// }
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
	}

}
