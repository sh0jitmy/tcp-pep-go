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
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"tcp-pep-go/config"
	"tcp-pep-go/protocol"
)

// ClientPEP implements the client-side transparent TCP proxy that intercepts outbound
// TCP connections, maps them to remote Server-PEP nodes, and translates TCP traffic into paced UDP packets.
type ClientPEP struct {
	listenAddr  string
	router      *config.Router
	mtu         int
	bandwidth   int
	fecK        int
	fecM        int
	idleTimeout time.Duration

	tcpListener *net.TCPListener
	udpConn     *net.UDPConn

	mu       sync.RWMutex
	sessions map[uint16]*Session
	lastID   uint16
	stopped  bool
	ctx      context.Context
	cancel   context.CancelFunc

	muConnect       sync.Mutex
	pendingConnects map[uint16]chan error
}

// NewClientPEP instantiates a new ClientPEP daemon configuration.
func NewClientPEP(ctx context.Context, listenAddr string, router *config.Router, mtu, bandwidth, k, m int, idleTimeout time.Duration) *ClientPEP {
	subCtx, cancel := context.WithCancel(ctx)
	return &ClientPEP{
		listenAddr:      listenAddr,
		router:          router,
		mtu:             mtu,
		bandwidth:       bandwidth,
		fecK:            k,
		fecM:            m,
		idleTimeout:     idleTimeout,
		sessions:        make(map[uint16]*Session),
		pendingConnects: make(map[uint16]chan error),
		ctx:             subCtx,
		cancel:          cancel,
	}
}

// Start launches the transparent proxy TCP listener, UDP packet socket,
// and the concurrent demux and connection clean-up routines.
func (c *ClientPEP) Start() error {
	// Listen on TCP transparent proxy port
	lAddr, err := net.ResolveTCPAddr("tcp", c.listenAddr)
	if err != nil {
		return err
	}
	l, err := net.ListenTCP("tcp", lAddr)
	if err != nil {
		return err
	}
	c.tcpListener = l

	// Listen on a random UDP port for sending/receiving encapsulated UDP traffic
	uAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		_ = l.Close()
		return err
	}
	u, err := net.ListenUDP("udp", uAddr)
	if err != nil {
		_ = l.Close()
		return err
	}
	c.udpConn = u
	_ = u.SetReadBuffer(2 * 1024 * 1024)
	_ = u.SetWriteBuffer(2 * 1024 * 1024)

	log.Printf("[Client-PEP] TCP listening on %s, UDP bound to %s", c.listenAddr, u.LocalAddr().String())

	// Start UDP receive demultiplexer
	go c.udpDemuxLoop()

	// Start connection cleanup loop
	go c.cleanupLoop()

	// Start accepting TCP connections
	go c.acceptLoop()

	return nil
}

// Stop gracefully shuts down the transparent TCP listener, UDP socket,
// and releases all active stream sessions.
func (c *ClientPEP) Stop() {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true
	c.mu.Unlock()

	c.cancel()
	if c.tcpListener != nil {
		_ = c.tcpListener.Close()
	}
	if c.udpConn != nil {
		_ = c.udpConn.Close()
	}

	c.mu.Lock()
	for _, sess := range c.sessions {
		sess.Close()
	}
	c.sessions = make(map[uint16]*Session)
	c.mu.Unlock()
}

func (c *ClientPEP) acceptLoop() {
	for {
		conn, err := c.tcpListener.AcceptTCP()
		if err != nil {
			c.mu.RLock()
			stopped := c.stopped
			c.mu.RUnlock()
			if stopped {
				return
			}
			log.Printf("[Client-PEP] Accept error: %v", err)
			continue
		}

		go c.handleTCPConn(conn)
	}
}

func (c *ClientPEP) getNextStreamID() (uint16, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 12-bit limit is 4095. StreamID 0 is reserved.
	start := c.lastID
	for {
		c.lastID = (c.lastID % 4095) + 1
		if _, ok := c.sessions[c.lastID]; !ok {
			return c.lastID, nil
		}
		if c.lastID == start {
			return 0, errors.New("out of stream IDs")
		}
	}
}

func (c *ClientPEP) handleTCPConn(conn *net.TCPConn) {
	defer func() {
		// If setup fails, ensure the connection is closed
	}()

	// 1. Get original destination address
	origDst, err := GetOriginalDST(conn)
	if err != nil {
		log.Printf("[Client-PEP] Failed to get original destination: %v", err)
		_ = conn.Close()
		return
	}

	// 2. Resolve Server-PEP address using router
	c.mu.RLock()
	router := c.router
	c.mu.RUnlock()

	serverPEPStr, err := router.Lookup(origDst)
	if err != nil {
		log.Printf("[Client-PEP] Routing failed for original destination %s: %v", origDst, err)
		_ = conn.Close()
		return
	}

	serverPEPAddr, err := net.ResolveUDPAddr("udp", serverPEPStr)
	if err != nil {
		log.Printf("[Client-PEP] Failed to resolve Server-PEP address %s: %v", serverPEPStr, err)
		_ = conn.Close()
		return
	}

	// 3. Allocate Stream ID
	streamID, err := c.getNextStreamID()
	if err != nil {
		log.Printf("[Client-PEP] Failed to allocate stream ID: %v", err)
		_ = conn.Close()
		return
	}

	log.Printf("[Client-PEP] Intercepted connection to %s. Routing via Server-PEP %s (StreamID=%d)", origDst, serverPEPStr, streamID)

	// 4. Register connect channel
	ch := make(chan error, 1)
	c.muConnect.Lock()
	c.pendingConnects[streamID] = ch
	c.muConnect.Unlock()

	// 5. Send CONNECT packet to Server-PEP
	connectPkt := &protocol.Packet{
		Type:     protocol.TypeConnect,
		StreamID: streamID,
		Addr:     origDst,
	}
	buf, err := protocol.Marshal(connectPkt)
	if err != nil {
		log.Printf("[Client-PEP] Marshal connect error: %v", err)
		_ = conn.Close()
		c.muConnect.Lock()
		delete(c.pendingConnects, streamID)
		c.muConnect.Unlock()
		return
	}

	_, err = c.udpConn.WriteTo(buf, serverPEPAddr)
	if err != nil {
		log.Printf("[Client-PEP] Send connect failed: %v", err)
		_ = conn.Close()
		c.muConnect.Lock()
		delete(c.pendingConnects, streamID)
		c.muConnect.Unlock()
		return
	}

	// 6. Wait for CONN_ACK or timeout (3 seconds)
	select {
	case connErr := <-ch:
		if connErr != nil {
			log.Printf("[Client-PEP] Connection error from Server-PEP for StreamID=%d: %v", streamID, connErr)
			_ = conn.Close()
			return
		}
	case <-time.After(3 * time.Second):
		log.Printf("[Client-PEP] Handshake timeout from Server-PEP for StreamID=%d", streamID)
		_ = conn.Close()
		c.muConnect.Lock()
		delete(c.pendingConnects, streamID)
		c.muConnect.Unlock()
		return
	}

	// 7. Handshake succeeded, instantiate session
	sess := NewSession(c.ctx, streamID, conn, serverPEPAddr, c.udpConn, false, c.mtu, c.bandwidth, c.fecK, c.fecM)

	c.mu.Lock()
	c.sessions[streamID] = sess
	c.mu.Unlock()

	sess.Start()
}

func (c *ClientPEP) udpDemuxLoop() {
	buf := make([]byte, 2048)
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		n, _, err := c.udpConn.ReadFrom(buf)
		if err != nil {
			c.mu.RLock()
			stopped := c.stopped
			c.mu.RUnlock()
			if stopped {
				return
			}
			log.Printf("[Client-PEP] UDP read error: %v", err)
			continue
		}

		pkt, err := protocol.Unmarshal(buf[:n])
		if err != nil {
			log.Printf("[Client-PEP] Failed to unmarshal UDP frame: %v", err)
			continue
		}

		// Handle handshake response directly
		if pkt.Type == protocol.TypeConnAck || pkt.Type == protocol.TypeConnErr {
			c.muConnect.Lock()
			ch, ok := c.pendingConnects[pkt.StreamID]
			if ok {
				delete(c.pendingConnects, pkt.StreamID)
				if pkt.Type == protocol.TypeConnAck {
					ch <- nil
				} else {
					ch <- fmt.Errorf("remote connection failed: %s", pkt.Addr)
				}
			}
			c.muConnect.Unlock()
			continue
		}

		// Dispatch to session
		c.mu.RLock()
		sess, ok := c.sessions[pkt.StreamID]
		c.mu.RUnlock()

		if !ok {
			// Skip or send reset for unknown streams
			continue
		}

		//nolint:gosec
		sess.RecordRx(uint64(n))

		switch pkt.Type {
		case protocol.TypeData:
			sess.HandleDataPacket(pkt)
		case protocol.TypeNak:
			sess.HandleNak(pkt)
		case protocol.TypeLqr:
			sess.HandleLQR(pkt)
		case protocol.TypeClose:
			sess.HandleClose()
		case protocol.TypeReset:
			log.Printf("[Client-PEP] Received RESET for StreamID=%d, closing session", pkt.StreamID)
			sess.Close()
			c.mu.Lock()
			delete(c.sessions, pkt.StreamID)
			c.mu.Unlock()
		}
	}
}

func (c *ClientPEP) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now()
			for id, sess := range c.sessions {
				if sess.IsClosed() || now.Sub(sess.GetLastActive()) > c.idleTimeout {
					log.Printf("[Client-PEP] Session StreamID=%d finished or idle timeout. Cleaning up.", id)
					sess.Close()
					delete(c.sessions, id)
				}
			}
			c.mu.Unlock()
		}
	}
}

// UpdateRouter thread-safely updates the routing configuration (e.g., after SIGHUP).
func (c *ClientPEP) UpdateRouter(newRouter *config.Router) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.router = newRouter
	log.Println("[Client-PEP] Routing table updated successfully.")
}

// LocalAddr returns the TCP transparent proxy port listener address.
func (c *ClientPEP) LocalAddr() net.Addr {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.tcpListener == nil {
		return nil
	}
	return c.tcpListener.Addr()
}

// GetSessions returns a snapshot copy of all active client-side proxy sessions.
func (c *ClientPEP) GetSessions() map[uint16]*Session {
	c.mu.RLock()
	defer c.mu.RUnlock()
	res := make(map[uint16]*Session)
	for k, v := range c.sessions {
		res[k] = v
	}
	return res
}
