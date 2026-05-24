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
	"log"
	"net"
	"sync"
	"time"

	"tcp-pep-go/protocol"
)

// ServerPEP implements the server-side proxy daemon that listens on a UDP port,
// accepts incoming encapsulated streams, dials original target destinations, and relays TCP traffic.
type ServerPEP struct {
	listenAddr  string
	mtu         int
	bandwidth   int
	fecK        int
	fecM        int
	idleTimeout time.Duration

	udpConn *net.UDPConn

	mu       sync.RWMutex
	sessions map[uint16]*Session
	stopped  bool
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewServerPEP instantiates a new ServerPEP daemon configuration.
func NewServerPEP(ctx context.Context, listenAddr string, mtu, bandwidth, k, m int, idleTimeout time.Duration) *ServerPEP {
	subCtx, cancel := context.WithCancel(ctx)
	return &ServerPEP{
		listenAddr:  listenAddr,
		mtu:         mtu,
		bandwidth:   bandwidth,
		fecK:        k,
		fecM:        m,
		idleTimeout: idleTimeout,
		sessions:    make(map[uint16]*Session),
		ctx:         subCtx,
		cancel:      cancel,
	}
}

// Start launches the UDP listener socket and concurrent session handling loops.
func (s *ServerPEP) Start() error {
	uAddr, err := net.ResolveUDPAddr("udp", s.listenAddr)
	if err != nil {
		return err
	}
	u, err := net.ListenUDP("udp", uAddr)
	if err != nil {
		return err
	}
	s.udpConn = u
	_ = u.SetReadBuffer(2 * 1024 * 1024)
	_ = u.SetWriteBuffer(2 * 1024 * 1024)

	log.Printf("[Server-PEP] Listening on UDP %s", s.listenAddr)

	// Start UDP receive loop
	go s.udpDemuxLoop()

	// Start session cleanup loop
	go s.cleanupLoop()

	return nil
}

// Stop gracefully shuts down the UDP listener socket and cleans up all active relay sessions.
func (s *ServerPEP) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()

	s.cancel()
	if s.udpConn != nil {
		_ = s.udpConn.Close()
	}

	s.mu.Lock()
	for _, sess := range s.sessions {
		sess.Close()
	}
	s.sessions = make(map[uint16]*Session)
	s.mu.Unlock()
}

func (s *ServerPEP) udpDemuxLoop() {
	buf := make([]byte, 2048)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		n, rAddr, err := s.udpConn.ReadFromUDP(buf)
		if err != nil {
			s.mu.RLock()
			stopped := s.stopped
			s.mu.RUnlock()
			if stopped {
				return
			}
			log.Printf("[Server-PEP] UDP read error: %v", err)
			continue
		}

		pkt, err := protocol.Unmarshal(buf[:n])
		if err != nil {
			log.Printf("[Server-PEP] Failed to unmarshal UDP frame: %v", err)
			continue
		}

		s.mu.RLock()
		sess, ok := s.sessions[pkt.StreamID]
		s.mu.RUnlock()

		if !ok {
			// If it's a CONNECT packet, initialize a new connection
			if pkt.Type == protocol.TypeConnect {
				go s.handleConnect(pkt, rAddr)
			}
			continue
		}

		if pkt.Type == protocol.TypeConnect {
			// Already connected but client retransmitted CONNECT.
			// Re-send CONN_ACK.
			ackPkt := &protocol.Packet{
				Type:     protocol.TypeConnAck,
				StreamID: pkt.StreamID,
			}
			ackBuf, err := protocol.Marshal(ackPkt)
			if err == nil {
				_, _ = s.udpConn.WriteToUDP(ackBuf, rAddr)
			}
			continue
		}

		//nolint:gosec
		sess.RecordRx(uint64(n))

		// Dispatch to session
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
			log.Printf("[Server-PEP] Received RESET for StreamID=%d, closing session", pkt.StreamID)
			sess.Close()
			s.mu.Lock()
			delete(s.sessions, pkt.StreamID)
			s.mu.Unlock()
		}
	}
}

func (s *ServerPEP) handleConnect(p *protocol.Packet, clientAddr *net.UDPAddr) {
	log.Printf("[Server-PEP] Received CONNECT for StreamID=%d, target=%s", p.StreamID, p.Addr)

	tcpAddr, err := net.ResolveTCPAddr("tcp", p.Addr)
	if err != nil {
		s.sendError(p.StreamID, err.Error(), clientAddr)
		return
	}

	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Printf("[Server-PEP] Dial TCP target %s failed: %v", p.Addr, err)
		s.sendError(p.StreamID, err.Error(), clientAddr)
		return
	}

	log.Printf("[Server-PEP] Connected to target %s for StreamID=%d", p.Addr, p.StreamID)

	// Send CONN_ACK to client
	ackPkt := &protocol.Packet{
		Type:     protocol.TypeConnAck,
		StreamID: p.StreamID,
	}
	ackBuf, err := protocol.Marshal(ackPkt)
	if err == nil {
		_, _ = s.udpConn.WriteToUDP(ackBuf, clientAddr)
	}

	// Create and start session
	sess := NewSession(s.ctx, p.StreamID, conn, clientAddr, s.udpConn, true, s.mtu, s.bandwidth, s.fecK, s.fecM)

	s.mu.Lock()
	// Clean up any stale session with the same StreamID if exists
	if old, ok := s.sessions[p.StreamID]; ok {
		old.Close()
	}
	s.sessions[p.StreamID] = sess
	s.mu.Unlock()

	sess.Start()
}

func (s *ServerPEP) sendError(streamID uint16, msg string, clientAddr *net.UDPAddr) {
	pkt := &protocol.Packet{
		Type:     protocol.TypeConnErr,
		StreamID: streamID,
		Addr:     msg,
	}
	buf, err := protocol.Marshal(pkt)
	if err == nil {
		_, _ = s.udpConn.WriteToUDP(buf, clientAddr)
	}
}

func (s *ServerPEP) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			for id, sess := range s.sessions {
				if sess.IsClosed() || now.Sub(sess.GetLastActive()) > s.idleTimeout {
					log.Printf("[Server-PEP] Session StreamID=%d finished or idle timeout. Cleaning up.", id)
					sess.Close()
					delete(s.sessions, id)
				}
			}
			s.mu.Unlock()
		}
	}
}

// LocalAddr returns the bound UDP listener network address.
func (s *ServerPEP) LocalAddr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.udpConn == nil {
		return nil
	}
	return s.udpConn.LocalAddr()
}

// GetSessions returns a snapshot copy of all active server-side stream sessions.
func (s *ServerPEP) GetSessions() map[uint16]*Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[uint16]*Session)
	for k, v := range s.sessions {
		res[k] = v
	}
	return res
}
