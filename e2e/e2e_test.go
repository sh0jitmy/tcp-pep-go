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

package e2e

import (
	"context"
	"crypto/rand"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"tcp-pep-go/config"
	"tcp-pep-go/proxy"

	"github.com/redis/go-redis/v9"
)

func TestE2EPepTransmission(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start Echo TCP Server (representing the target server)
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to start echo server: %v", err)
	}
	defer func() { _ = echoListener.Close() }()
	echoAddr := echoListener.Addr().String()

	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer func() { _ = c.Close() }()
				_, _ = io.Copy(c, c) // Echo data back
			}(conn)
		}
	}()

	// Configure proxy fallback destination for Darwin/Testing
	proxy.SetFallbackDst(echoAddr)

	// 2. Start Server-PEP (UDP listener)
	serverPEP := proxy.NewServerPEP(ctx, "127.0.0.1:0", 1200, 1000000, 5, 2, 5*time.Second) // 1 Mbps, K=5, M=2
	if err := serverPEP.Start(); err != nil {
		t.Fatalf("Failed to start Server-PEP: %v", err)
	}
	defer serverPEP.Stop()

	serverUDPAddr := serverPEP.LocalAddr().String()

	// 3. Create Routing Config
	// We map the target echo server destination to the Server-PEP UDP port.
	routesData := []byte("routes:\n  - original_dst: \"" + echoAddr + "\"\n    server_pep: \"" + serverUDPAddr + "\"\n")
	tmpFile, err := os.CreateTemp("", "routes-*.yaml")
	if err != nil {
		t.Fatalf("Failed to create temp routes file: %v", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	_, _ = tmpFile.Write(routesData)
	_ = tmpFile.Close()

	router, err := config.LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load temp routes config: %v", err)
	}

	// 4. Start Client-PEP (TCP transparent proxy listener)
	clientPEP := proxy.NewClientPEP(ctx, "127.0.0.1:0", router, 1200, 1000000, 5, 2, 5*time.Second)
	if err := clientPEP.Start(); err != nil {
		t.Fatalf("Failed to start Client-PEP: %v", err)
	}
	defer clientPEP.Stop()

	// Start embedded Redis server for monitoring
	redisServer := proxy.NewRedisServer("127.0.0.1:0", clientPEP)
	if err := redisServer.Start(); err != nil {
		t.Fatalf("Failed to start embedded Redis server: %v", err)
	}
	defer redisServer.Stop()
	redisAddr := redisServer.Addr()

	clientTCPAddr := clientPEP.LocalAddr().String()

	// 5. Connect client to Client-PEP
	conn, err := net.Dial("tcp", clientTCPAddr)
	if err != nil {
		t.Fatalf("Failed to connect to Client-PEP: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Generate 100KB of random data to trigger adaptive parity reduction (curM 2 -> 0)
	dataSize := 100 * 1024
	sendData := make([]byte, dataSize)
	if _, err := rand.Read(sendData); err != nil {
		t.Fatalf("Failed to generate random data: %v", err)
	}

	// 6. Write data in a goroutine
	errChan := make(chan error, 2)
	go func() {
		_, err := conn.Write(sendData)
		if err != nil {
			errChan <- err
			return
		}
		// Half-close write side to signal EOF to echo server through PEP
		tcpConn := conn.(*net.TCPConn)
		_ = tcpConn.CloseWrite()
		errChan <- nil
	}()

	// 7. Read echoed data
	recvData := make([]byte, dataSize)
	go func() {
		_, err := io.ReadFull(conn, recvData)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			errChan <- err
			return
		}
		errChan <- nil
	}()

	// Wait for transmission completion
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("Transmission error: %v", err)
		}
	}

	// 8. Validate data integrity
	for i := range sendData {
		if sendData[i] != recvData[i] {
			t.Fatalf("Data mismatch at byte %d! Send: %d, Recv: %d", i, sendData[i], recvData[i])
		}
	}
	// 8.5 Validate Redis Monitor Stats
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer func() { _ = rdb.Close() }()

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("Redis PING failed: %v", err)
	}
	if pong != "PONG" {
		t.Errorf("Unexpected Redis PING response: %s", pong)
	}

	activeStreams, err := rdb.SMembers(ctx, "tcp-pep:active_streams").Result()
	if err != nil {
		t.Fatalf("Redis SMEMBERS failed: %v", err)
	}
	if len(activeStreams) != 1 || activeStreams[0] != "1" {
		t.Errorf("Unexpected active streams: %v", activeStreams)
	}

	stats, err := rdb.HGetAll(ctx, "tcp-pep:session:1").Result()
	if err != nil {
		t.Fatalf("Redis HGETALL failed: %v", err)
	}
	if stats["stream_id"] != "1" {
		t.Errorf("Unexpected stream_id: %s", stats["stream_id"])
	}
	if stats["mode"] != "client" {
		t.Errorf("Unexpected mode: %s", stats["mode"])
	}
	txBytes, _ := strconv.ParseUint(stats["tx_bytes"], 10, 64)
	if txBytes == 0 {
		t.Errorf("tx_bytes should be greater than 0")
	}

	t.Logf("E2E transmission of %d bytes completed successfully with 100%% integrity", dataSize)

	// Wait briefly to allow pending LQR reports to arrive and trigger adaptation logging
	time.Sleep(1000 * time.Millisecond)
}
