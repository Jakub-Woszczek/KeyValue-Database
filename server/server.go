package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/Jakub-Woszczek/kvdb/db"
)

type Server struct {
	db       *db.DB
	listener net.Listener
	quit     chan struct{}
}

func New(d *db.DB, addr string) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	return &Server{
		db:       d,
		listener: ln,
		quit:     make(chan struct{}),
	}, nil
}

func (s *Server) Serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				return // expected shutdown
			default:
				continue // unexpected error
			}
		}
		go s.handle(conn)
	}
}

// Protocol (one command per line):
//
//	PUT <key> <value>\n  →  OK\n  or  ERR <msg>\n
//	GET <key>\n          →  OK <value>\n  or  NOT_FOUND\n  or  ERR <msg>\n
func (s *Server) handle(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(parts[0])

		var resp string
		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				resp = "ERR usage: PUT <key> <value>"
				break
			}
			err := s.db.Put([]byte(parts[1]), []byte(parts[2]))
			if err != nil {
				resp = "ERR " + err.Error()
			} else {
				resp = "OK"
			}

		case "GET":
			if len(parts) < 2 {
				resp = "ERR usage: GET <key>"
				break
			}
			val := s.db.Get([]byte(parts[1])) // returns nil if not found
			if val == nil {
				resp = "NOT_FOUND"
			} else {
				resp = "OK " + string(val)
			}
		case "QUIT":
			resp = "OK shutting down"
			fmt.Fprintln(conn, resp)

			go func() {
				s.Close()
			}()
			return

		default:
			resp = "ERR unknown command: " + cmd
		}

		fmt.Fprintln(conn, resp)
	}
}

func (s *Server) Close() {
	close(s.quit)      // signal shutdown
	s.listener.Close() // unblock Accept()
}
