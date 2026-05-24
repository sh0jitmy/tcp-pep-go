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
	"encoding/json"
	"fmt"
	"io"
	"log"
	mathrand "math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
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
	_ = tmpFile.Sync()
	_ = tmpFile.Close()

	router, err := config.LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load temp routes config: %v", err)
	}

	// Verify that the route was successfully loaded
	if pep, lookupErr := router.Lookup(echoAddr); lookupErr != nil || pep != serverUDPAddr {
		t.Fatalf("Loaded router does not contain target route! lookupErr=%v, pep=%s, expected=%s", lookupErr, pep, serverUDPAddr)
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

// LossyUDPLink simulates packet drops on UDP link between client and server.
type LossyUDPLink struct {
	addr1    *net.UDPAddr
	addr2    *net.UDPAddr
	conn1    *net.UDPConn
	conn2    *net.UDPConn
	lossRate float64
	lossType string // "random" or "burst"
	burstLen float64
	mu       sync.Mutex
	isBad1   bool
	isBad2   bool
	rng      *mathrand.Rand
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewLossyUDPLink instantiates a new LossyUDPLink simulator.
func NewLossyUDPLink(ctx context.Context, serverAddr *net.UDPAddr, lossRate float64, lossType string, burstLen float64) *LossyUDPLink {
	subCtx, cancel := context.WithCancel(ctx)
	// seed rand with constant value for deterministic (pseudo-random) behavior
	source := mathrand.NewSource(42)
	return &LossyUDPLink{
		addr2:    serverAddr,
		lossRate: lossRate,
		lossType: lossType,
		burstLen: burstLen,
		rng:      mathrand.New(source), //nolint:gosec
		ctx:      subCtx,
		cancel:   cancel,
	}
}

// Start registers UDP listener ports and relays packet streams concurrently.
func (l *LossyUDPLink) Start() error {
	var err error
	l.conn1, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		return err
	}
	l.conn2, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		_ = l.conn1.Close()
		return err
	}

	if l.lossType == "burst" {
		go l.burstStateLoop()
	}

	go l.relayLoop(l.conn1, l.conn2, l.addr2, true)
	go l.relayLoop(l.conn2, l.conn1, nil, false)
	return nil
}

func (l *LossyUDPLink) burstStateLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	p := 1.0 / l.burstLen
	q := l.lossRate / (l.burstLen * (1.0 - l.lossRate))

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			l.mu.Lock()
			if l.isBad1 {
				if l.rng.Float64() < p {
					l.isBad1 = false
				}
			} else {
				if l.rng.Float64() < q {
					l.isBad1 = true
				}
			}
			if l.isBad2 {
				if l.rng.Float64() < p {
					l.isBad2 = false
				}
			} else {
				if l.rng.Float64() < q {
					l.isBad2 = true
				}
			}
			l.mu.Unlock()
		}
	}
}

func (l *LossyUDPLink) shouldDrop(isDirection1 bool) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.lossRate == 0 {
		return false
	}
	if l.lossRate >= 1.0 {
		return true
	}

	if l.lossType == "random" {
		return l.rng.Float64() < l.lossRate
	}

	if isDirection1 {
		return l.isBad1
	}
	return l.isBad2
}

func (l *LossyUDPLink) relayLoop(srcConn, dstConn *net.UDPConn, targetAddr *net.UDPAddr, isDirection1 bool) {
	buf := make([]byte, 2048)
	for {
		select {
		case <-l.ctx.Done():
			return
		default:
		}

		_ = srcConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		n, rAddr, err := srcConn.ReadFromUDP(buf)
		if err != nil {
			continue
		}

		if isDirection1 {
			l.mu.Lock()
			l.addr1 = rAddr
			l.mu.Unlock()
		}

		if l.shouldDrop(isDirection1) {
			continue // Drop packet
		}

		var dstAddr *net.UDPAddr
		if isDirection1 {
			dstAddr = targetAddr
		} else {
			l.mu.Lock()
			dstAddr = l.addr1
			l.mu.Unlock()
		}

		if dstAddr == nil {
			continue
		}

		_, _ = dstConn.WriteToUDP(buf[:n], dstAddr)
	}
}

// LocalAddr returns the bound UDP listener address of the first peer side.
func (l *LossyUDPLink) LocalAddr() net.Addr {
	return l.conn1.LocalAddr()
}

// Close releases all socket resources and cancels goroutines.
func (l *LossyUDPLink) Close() {
	l.cancel()
	if l.conn1 != nil {
		_ = l.conn1.Close()
	}
	if l.conn2 != nil {
		_ = l.conn2.Close()
	}
}

// TestE2EPepLossyTransmission evaluates transmission performance under various packet loss conditions.
func TestE2EPepLossyTransmission(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	testCases := []struct {
		name       string
		lossRate   float64
		lossType   string
		burstLen   float64
		dataSize   int
		maxDur     time.Duration
		maxRetrans uint64
		fecK       int
		fecM       int
	}{
		{"Loss-30-Random", 0.30, "random", 1, 512, 3 * time.Second, 5, 5, 2},
		{"Loss-60-Random", 0.60, "random", 1, 512, 5 * time.Second, 5, 3, 4},
		{"Loss-90-Random", 0.90, "random", 1, 512, 10 * time.Second, 5, 1, 9},
		{"Loss-30-Burst", 0.30, "burst", 3, 512, 3 * time.Second, 5, 5, 2},
		{"Loss-60-Burst", 0.60, "burst", 3, 512, 5 * time.Second, 5, 3, 4},
		{"Loss-90-Burst", 0.90, "burst", 3, 512, 10 * time.Second, 5, 1, 9},
	}

	isCI := os.Getenv("CI") != ""

	fmt.Printf("\n=========================================================================\n")
	fmt.Printf("%-18s | %-8s | %-6s | %-12s | %-15s | %-8s\n", "Test Case", "LossRate", "Type", "Duration", "Retransmissions", "Result")
	fmt.Printf("-------------------------------------------------------------------------\n")

	for _, tc := range testCases {
		if isCI && tc.lossRate > 0.35 {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// 1. Start Echo TCP Server
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
						_, _ = io.Copy(c, c)
					}(conn)
				}
			}()

			// Configure proxy fallback destination
			proxy.SetFallbackDst(echoAddr)

			// 2. Start Server-PEP (UDP listener)
			serverPEP := proxy.NewServerPEP(ctx, "127.0.0.1:0", 1200, 0, tc.fecK, tc.fecM, 5*time.Second) // Disable shaper in lossy test
			if err := serverPEP.Start(); err != nil {
				t.Fatalf("Failed to start Server-PEP: %v", err)
			}
			defer serverPEP.Stop()

			serverUDPAddr := serverPEP.LocalAddr().(*net.UDPAddr)

			// 3. Start Lossy UDP Link
			lossyLink := NewLossyUDPLink(ctx, serverUDPAddr, tc.lossRate, tc.lossType, tc.burstLen)
			if err := lossyLink.Start(); err != nil {
				t.Fatalf("Failed to start LossyUDPLink: %v", err)
			}
			defer lossyLink.Close()

			relayUDPAddr := lossyLink.LocalAddr().String()

			// 4. Create Routing Config (routing client traffic to the lossy relay link)
			routesData := []byte("routes:\n  - original_dst: \"" + echoAddr + "\"\n    server_pep: \"" + relayUDPAddr + "\"\n")
			tmpFile, err := os.CreateTemp("", "routes-*.yaml")
			if err != nil {
				t.Fatalf("Failed to create temp routes file: %v", err)
			}
			defer func() { _ = os.Remove(tmpFile.Name()) }()
			_, _ = tmpFile.Write(routesData)
			_ = tmpFile.Sync()
			_ = tmpFile.Close()

			router, err := config.LoadConfig(tmpFile.Name())
			if err != nil {
				t.Fatalf("Failed to load temp routes config: %v", err)
			}

			// 5. Start Client-PEP
			clientPEP := proxy.NewClientPEP(ctx, "127.0.0.1:0", router, 1200, 0, tc.fecK, tc.fecM, 5*time.Second) // Disable shaper in lossy test
			if err := clientPEP.Start(); err != nil {
				t.Fatalf("Failed to start Client-PEP: %v", err)
			}
			defer clientPEP.Stop()

			// Start HTTP monitoring server
			httpServer := proxy.NewHTTPServer("127.0.0.1:0", "", "", clientPEP)
			if err := httpServer.Start(); err != nil {
				t.Fatalf("Failed to start HTTP server: %v", err)
			}
			defer httpServer.Stop()
			httpAddr := httpServer.Addr()

			clientTCPAddr := clientPEP.LocalAddr().String()

			// 6. Connect Client
			conn, err := net.Dial("tcp", clientTCPAddr)
			if err != nil {
				t.Fatalf("Failed to connect to Client-PEP: %v", err)
			}
			defer func() { _ = conn.Close() }()

			sendData := make([]byte, tc.dataSize)
			if _, err := rand.Read(sendData); err != nil {
				t.Fatalf("Failed to generate random data: %v", err)
			}

			startTime := time.Now()

			// 7. Write data
			errChan := make(chan error, 2)
			go func() {
				_, err := conn.Write(sendData)
				if err != nil {
					errChan <- err
					return
				}
				tcpConn := conn.(*net.TCPConn)
				_ = tcpConn.CloseWrite()
				errChan <- nil
			}()

			// 8. Read echoed data
			recvData := make([]byte, tc.dataSize)
			go func() {
				_, err := io.ReadFull(conn, recvData)
				if err != nil {
					errChan <- err
					return
				}
				errChan <- nil
			}()

			// Wait for transmission completion (with timeout)
			timeout := time.After(tc.maxDur)
			completed := false
			doneCount := 0

			for doneCount < 2 {
				select {
				case <-timeout:
					t.Errorf("Transmission timed out after %v", tc.maxDur)
					doneCount = 2 // exit loop
				case err := <-errChan:
					if err != nil {
						t.Errorf("Transmission failed: %v", err)
						doneCount = 2 // exit loop
					} else {
						doneCount++
						if doneCount == 2 {
							completed = true
						}
					}
				}
			}

			dur := time.Since(startTime)

			// 9. Verify data integrity
			if completed {
				for i := range sendData {
					if sendData[i] != recvData[i] {
						t.Fatalf("Data mismatch at byte %d", i)
					}
				}
			}

			// 10. Fetch HTTP Stats JSON & Validate
			resp, err := http.Get("http://" + httpAddr + "/stats")
			if err != nil {
				t.Fatalf("HTTP Get stats failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("Unexpected HTTP status: %d", resp.StatusCode)
			}

			var stats map[uint16]proxy.SessionStats
			if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
				t.Fatalf("Failed to decode JSON stats: %v", err)
			}

			var retransCount uint64
			for _, sStats := range stats {
				if sStats.StreamID == 1 {
					retransCount = sStats.TxRetransmissions
				}
			}

			// 11. Assert quality metrics thresholds (ensure we don't have catastrophic retransmissions)
			if retransCount > tc.maxRetrans {
				t.Errorf("Retransmissions count %d exceeded threshold %d", retransCount, tc.maxRetrans)
			}

			// Print metrics
			rateKbps := float64(tc.dataSize*8) / (dur.Seconds() * 1000)
			resultStr := "PASS"
			if t.Failed() {
				resultStr = "FAIL"
			}
			fmt.Printf("%-18s | %-8.2f | %-6s | %-12s | %-15d | %-8s (%.2f kbps)\n",
				tc.name, tc.lossRate, tc.lossType, dur.Truncate(time.Millisecond), retransCount, resultStr, rateKbps)
		})
	}
	fmt.Printf("=========================================================================\n\n")
}
