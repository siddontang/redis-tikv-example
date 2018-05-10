package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/siddontang/goredis"
)

var (
	pdAddr = flag.String("pd", "127.0.0.1:2379", "PD endpoints")
	addr   = flag.String("addr", "127.0.0.1:6380", "Listening address")
)

func perror(err error) {
	if err == nil {
		return
	}

	println(err.Error())
	os.Exit(1)
}

func main() {
	flag.Parse()

	ln, err := net.Listen("tcp", *addr)
	perror(err)

	tikv.MaxConnectionCount = 128
	driver := tikv.Driver{}
	db, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", *pdAddr))
	perror(err)
	defer db.Close()

	for {
		conn, err := ln.Accept()
		perror(err)

		go handleConn(db, conn)
	}
}

func handleConn(db kv.Storage, conn net.Conn) {
	defer conn.Close()

	br := bufio.NewReaderSize(conn, 4096)
	bw := bufio.NewWriterSize(conn, 4096)
	r := goredis.NewRespReader(br)
	w := goredis.NewRespWriter(bw)

	for {
		reqs, err := r.ParseRequest()
		if len(reqs) == 0 {
			continue
		}

		if err != nil {
			fmt.Printf("invalid request %v, close conn", err)
			return
		}
		resp, err := handleRequest(db, string(reqs[0]), reqs[1:])

		if err != nil {
			w.FlushError(err)
			continue
		}

		switch v := resp.(type) {
		case []interface{}:
			w.FlushArray(v)
		case []byte:
			w.FlushBulk(v)
		case string:
			w.FlushString(v)
		case nil:
			w.FlushBulk(nil)
		case int64:
			w.FlushInteger(v)
		default:
			perror(fmt.Errorf("invalid type %T", v))
		}
	}
}

func handleRequest(db kv.Storage, cmd string, args [][]byte) (interface{}, error) {
	switch strings.ToLower(cmd) {
	case "set":
		return HandleSet(db, args[0], args[1])
	case "get":
		return HandleGet(db, args[0])
	case "del":
		return HandleDelete(db, args[0])
	case "hset":
		return HandleHashSet(db, args[0], args[1], args[2])
	case "hget":
		return HandleHashGet(db, args[0], args[1])
	default:
		return nil, fmt.Errorf("%s is not supported", cmd)
	}
	return nil, nil
}
