// Copyright 2026 The tcp-pep-go Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proxy

import (
	"fmt"
	"log"
	"net"
	"path"
	"strconv"
	"strings"

	"github.com/tidwall/redcon"
)

// RedisServer implements the built-in, lightweight Redis-protocol compatible server
// to expose live PEP statistics on-demand.
type RedisServer struct {
	addr   string
	server *redcon.Server
	peps   []PEPInstance
	ln     net.Listener
}

// PEPInstance defines the interface required by the Redis server to query live session metrics.
type PEPInstance interface {
	GetSessions() map[uint16]*Session
}

// NewRedisServer instantiates a new RedisServer monitoring daemon.
func NewRedisServer(addr string, peps ...PEPInstance) *RedisServer {
	return &RedisServer{
		addr: addr,
		peps: peps,
	}
}

// Start listens on the configured address and runs the embedded RESP protocol handler loop.
func (s *RedisServer) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.addr = ln.Addr().String()

	server := redcon.NewServer("", s.handleCommand, nil, nil)
	s.server = server
	go func() {
		log.Printf("[RedisServer] Starting embedded Redis server on %s", s.addr)
		if err := server.Serve(ln); err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				log.Printf("[RedisServer] Server stopped with error: %v", err)
			}
		}
	}()
	return nil
}

// Stop shuts down the embedded Redis server and closes its TCP listener socket.
func (s *RedisServer) Stop() {
	if s.server != nil {
		_ = s.server.Close()
	}
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

// Addr returns the resolved local address (host:port) the embedded server is listening on.
func (s *RedisServer) Addr() string {
	return s.addr
}

func matchPattern(pattern, key string) bool {
	matched, err := path.Match(pattern, key)
	return err == nil && matched
}

func (s *RedisServer) handleCommand(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) == 0 {
		return
	}
	switch strings.ToLower(string(cmd.Args[0])) {
	default:
		conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
	case "ping":
		conn.WriteString("PONG")
	case "quit":
		conn.WriteString("OK")
		_ = conn.Close()
	case "keys":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for 'keys' command")
			return
		}
		pattern := string(cmd.Args[1])
		keys := []string{}

		activeStreamsKey := "tcp-pep:active_streams"
		if matchPattern(pattern, activeStreamsKey) {
			keys = append(keys, activeStreamsKey)
		}

		for _, pep := range s.peps {
			for id := range pep.GetSessions() {
				sessKey := fmt.Sprintf("tcp-pep:session:%d", id)
				if matchPattern(pattern, sessKey) {
					keys = append(keys, sessKey)
				}
			}
		}

		conn.WriteArray(len(keys))
		for _, k := range keys {
			conn.WriteBulkString(k)
		}

	case "smembers":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for 'smembers' command")
			return
		}
		key := string(cmd.Args[1])
		if key != "tcp-pep:active_streams" {
			conn.WriteArray(0)
			return
		}

		streams := []string{}
		for _, pep := range s.peps {
			for id := range pep.GetSessions() {
				streams = append(streams, strconv.Itoa(int(id)))
			}
		}

		conn.WriteArray(len(streams))
		for _, st := range streams {
			conn.WriteBulkString(st)
		}

	case "hgetall":
		if len(cmd.Args) < 2 {
			conn.WriteError("ERR wrong number of arguments for 'hgetall' command")
			return
		}
		key := string(cmd.Args[1])
		if !strings.HasPrefix(key, "tcp-pep:session:") {
			conn.WriteArray(0)
			return
		}

		idStr := strings.TrimPrefix(key, "tcp-pep:session:")
		streamIDVal, err := strconv.Atoi(idStr)
		if err != nil {
			conn.WriteError("ERR invalid stream ID in key")
			return
		}
		if streamIDVal < 0 || streamIDVal > 65535 {
			conn.WriteError("ERR invalid stream ID range")
			return
		}
		streamID := uint16(streamIDVal)

		var foundSess *Session
		for _, pep := range s.peps {
			sessions := pep.GetSessions()
			if sess, ok := sessions[streamID]; ok {
				foundSess = sess
				break
			}
		}

		if foundSess == nil {
			conn.WriteArray(0)
			return
		}

		stats := foundSess.GetMonitorStats()

		conn.WriteArray(28) // 14 pairs

		conn.WriteBulkString("stream_id")
		conn.WriteBulkString(strconv.Itoa(int(stats.StreamID)))

		conn.WriteBulkString("mode")
		conn.WriteBulkString(stats.Mode)

		conn.WriteBulkString("target_addr")
		conn.WriteBulkString(stats.TargetAddr)

		conn.WriteBulkString("cur_m")
		conn.WriteBulkString(strconv.Itoa(stats.CurM))

		conn.WriteBulkString("fec_k")
		conn.WriteBulkString(strconv.Itoa(stats.FEC_K))

		conn.WriteBulkString("fec_m")
		conn.WriteBulkString(strconv.Itoa(stats.FEC_M))

		conn.WriteBulkString("tx_bytes")
		conn.WriteBulkString(strconv.FormatUint(stats.TxBytes, 10))

		conn.WriteBulkString("rx_bytes")
		conn.WriteBulkString(strconv.FormatUint(stats.RxBytes, 10))

		conn.WriteBulkString("tx_packets")
		conn.WriteBulkString(strconv.FormatUint(stats.TxPackets, 10))

		conn.WriteBulkString("rx_packets")
		conn.WriteBulkString(strconv.FormatUint(stats.RxPackets, 10))

		conn.WriteBulkString("losses")
		conn.WriteBulkString(strconv.FormatUint(uint64(stats.Losses), 10))

		conn.WriteBulkString("tx_retransmissions")
		conn.WriteBulkString(strconv.FormatUint(stats.TxRetransmissions, 10))

		conn.WriteBulkString("consecutive_ok")
		conn.WriteBulkString(strconv.Itoa(stats.ConsecutiveOk))

		conn.WriteBulkString("last_active")
		conn.WriteBulkString(stats.LastActive)
	}
}
