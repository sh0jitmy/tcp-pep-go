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
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"tcp-pep-go/fec"
	"tcp-pep-go/protocol"
	"tcp-pep-go/shaper"
)

const (
	BlockFlushTimeout = 50 * time.Millisecond
	MaxSeqNum         = 255
)

// TxBlock tracks metadata for a transmitted block of data shards.
type TxBlock struct {
	GroupID   uint16
	M         int
	LQRRecved bool
}

// RxBlock aggregates incoming packet shards and manages FEC decoding state for a block.
type RxBlock struct {
	GroupID     uint16
	K           int
	M           int
	Packets     [][]byte // Size K+M. Index 0..K-1: data, K..K+M-1: parity
	SeqNums     []byte   // Size K. SeqNum for each data index
	ReceivedCnt int
	LQRTimer    *time.Timer
	LQRReported bool
	Decoded     bool
}

// Session handles the lifecycle, packet encapsulation, adaptive FEC,
// HARQ NAK, and shaping for an individual active Stream ID.
type Session struct {
	StreamID uint16
	Conn     *net.TCPConn
	PEPAddr  *net.UDPAddr
	UDPConn  *net.UDPConn
	IsServer bool

	// Configuration Parameters
	LinkMTU       int
	Bandwidth     int
	FEC_K         int
	FEC_M         int
	LQRTimeoutDur time.Duration
	NAKDelayDur   time.Duration

	Shaper *shaper.Shaper
	FEC    *fec.FEC

	// Sender State (TCP -> UDP)
	muSend        sync.Mutex
	curM          int
	nextGroupID   uint16
	nextSeqNum    byte
	pendingData   [][]byte
	blockTimer    *time.Timer
	txBlocks      map[uint16]*TxBlock
	harqCache     map[byte]*protocol.Packet
	consecutiveOk int
	sendClosed    bool
	closeSent     bool
	closeRecv     bool

	// Receiver State (UDP -> TCP)
	muRecv       sync.Mutex
	nextWriteGrp uint16
	rxBlocks     map[uint16]*RxBlock
	writeCond    *sync.Cond
	recvClosed   bool
	maxRecvGrp   uint16
	hasRecv      bool

	// Lifecycle
	ctx        context.Context
	cancel     context.CancelFunc
	lastActive int64

	// Statistics
	txBytes           uint64
	rxBytes           uint64
	txPackets         uint64
	rxPackets         uint64
	losses            uint32
	txRetransmissions uint64
}

// NewSession instantiates a new Session for tracking and pacing data transfer on a stream.
func NewSession(ctx context.Context, streamID uint16, conn *net.TCPConn, pepAddr *net.UDPAddr, udpConn *net.UDPConn, isServer bool, mtu, bandwidth, k, m int) *Session {
	subCtx, cancel := context.WithCancel(ctx)
	s := &Session{
		StreamID:      streamID,
		Conn:          conn,
		PEPAddr:       pepAddr,
		UDPConn:       udpConn,
		IsServer:      isServer,
		LinkMTU:       mtu,
		Bandwidth:     bandwidth,
		FEC_K:         k,
		FEC_M:         m,
		curM:          m,                     // Start with max parity
		LQRTimeoutDur: 50 * time.Millisecond, // Will be updated dynamically based on bandwidth
		NAKDelayDur:   50 * time.Millisecond,
		FEC:           fec.NewFEC(),
		txBlocks:      make(map[uint16]*TxBlock),
		harqCache:     make(map[byte]*protocol.Packet),
		rxBlocks:      make(map[uint16]*RxBlock),
		lastActive:    time.Now().UnixNano(),
		ctx:           subCtx,
		cancel:        cancel,
	}

	// Dynamic calculation of LQRTimeout based on Bandwidth if available.
	// 1 packet size ~ LinkMTU bytes.
	// blockDuration = (K + M) * LinkMTU * 8 / Bandwidth seconds.
	if bandwidth > 0 {
		pktBits := float64(mtu * 8)
		blockSec := (float64(k+m) * pktBits) / float64(bandwidth)
		s.LQRTimeoutDur = time.Duration(blockSec*1000)*time.Millisecond + 50*time.Millisecond
		if s.LQRTimeoutDur < 50*time.Millisecond {
			s.LQRTimeoutDur = 50 * time.Millisecond
		}
	}

	burstSize := k + m
	// Burst interval corresponding to sending K+M packets
	var burstInterval time.Duration
	if bandwidth > 0 {
		burstInterval = time.Duration(float64(k+m)*float64(mtu)*8/float64(bandwidth)*1000) * time.Millisecond
	} else {
		burstInterval = 0
	}

	s.Shaper = shaper.NewShaper(burstSize, burstInterval)
	s.writeCond = sync.NewCond(&s.muRecv)

	return s
}

// Start kicks off concurrent goroutines for traffic shaping pacing,
// TCP packet reading, and TCP packet writing.
func (s *Session) Start() {
	// Start Shaper run loop
	go s.Shaper.Run(s.ctx, s.sendRawUDP)

	// Start TCP Read Loop (TCP -> UDP)
	go s.readTCPRun()

	// Start TCP Write Loop (UDP -> TCP)
	go s.writeTCPRun()

	// Initialize the first receive block (GroupID 0) to recover from early packet loss
	s.muRecv.Lock()
	if len(s.rxBlocks) == 0 {
		mSize := s.FEC_M
		rxBlock := &RxBlock{
			GroupID: 0,
			K:       s.FEC_K,
			M:       mSize,
			Packets: make([][]byte, s.FEC_K+mSize),
			SeqNums: make([]byte, s.FEC_K),
		}
		// Initialize SeqNums for Group 0
		for i := 0; i < s.FEC_K; i++ {
			rxBlock.SeqNums[i] = byte(i % 256)
		}
		s.rxBlocks[0] = rxBlock

		// Set up LQR timeout
		rxBlock.LQRTimer = time.AfterFunc(s.LQRTimeoutDur, func() {
			s.handleLQRTimeout(0)
		})
	}
	s.muRecv.Unlock()
}

// Close terminates all concurrent worker routines, closes the TCP connection,
// and releases all session timers.
func (s *Session) Close() {
	s.cancel()
	s.Shaper.Stop()
	_ = s.Conn.Close()

	s.muSend.Lock()
	if s.blockTimer != nil {
		s.blockTimer.Stop()
	}
	s.muSend.Unlock()

	s.muRecv.Lock()
	s.writeCond.Broadcast()
	s.muRecv.Unlock()
}

func (s *Session) updateLastActive() {
	atomic.StoreInt64(&s.lastActive, time.Now().UnixNano())
}

// GetLastActive returns the time of the most recent network activity on this session.
func (s *Session) GetLastActive() time.Time {
	nanos := atomic.LoadInt64(&s.lastActive)
	return time.Unix(0, nanos)
}

// sendRawUDP actually transmits a marshaled packet over UDP
func (s *Session) sendRawUDP(data []byte) error {
	s.updateLastActive()
	atomic.AddUint64(&s.txPackets, 1)
	atomic.AddUint64(&s.txBytes, uint64(len(data)))
	_, err := s.UDPConn.WriteTo(data, s.PEPAddr)
	return err
}

func (s *Session) sendPacketDirect(p *protocol.Packet) {
	buf, err := protocol.Marshal(p)
	if err != nil {
		log.Printf("[Stream %d] Marshal error: %v", s.StreamID, err)
		return
	}
	_ = s.sendRawUDP(buf)
}

func (s *Session) readTCPRun() {
	defer func() {
		s.muSend.Lock()
		s.sendClosed = true
		s.muSend.Unlock()

		select {
		case <-s.ctx.Done():
			// Context canceled, do not send CLOSE
			return
		default:
		}

		// Flush remaining pending data
		s.muSend.Lock()
		if len(s.pendingData) > 0 {
			s.flushPendingBlockLocked()
		}
		s.muSend.Unlock()

		// Wait for shaper queue to be empty
		for s.Shaper.QueueLen() > 0 {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}

		// Wait for all sent blocks to be acknowledged by LQR from the receiver
		// Max timeout 3 seconds to avoid blocking indefinitely if LQR is lost
		lqrWaitStart := time.Now()
		for time.Since(lqrWaitStart) < 3*time.Second {
			s.muSend.Lock()
			allAcked := true
			for _, block := range s.txBlocks {
				if !block.LQRRecved {
					allAcked = false
					break
				}
			}
			s.muSend.Unlock()

			if allAcked {
				break
			}
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}

		// Start CLOSE retransmission goroutine
		go func() {
			ticker := time.NewTicker(300 * time.Millisecond)
			defer ticker.Stop()

			// Send first CLOSE
			s.sendPacketDirect(&protocol.Packet{
				Type:     protocol.TypeClose,
				StreamID: s.StreamID,
			})

			s.muSend.Lock()
			s.closeSent = true
			bothClosed := s.closeSent && s.closeRecv
			s.muSend.Unlock()

			if bothClosed {
				s.Close()
				return
			}

			for {
				select {
				case <-s.ctx.Done():
					return
				case <-ticker.C:
					s.muSend.Lock()
					if s.closeRecv {
						s.muSend.Unlock()
						return
					}
					s.muSend.Unlock()

					// Retransmit CLOSE
					s.sendPacketDirect(&protocol.Packet{
						Type:     protocol.TypeClose,
						StreamID: s.StreamID,
					})
				}
			}
		}()
	}()

	// Max payload size calculation
	// DATA Packet header: 6B, IP: 20B, UDP: 8B -> Overhead = 34B
	// We also reserve 2 bytes for payload length inside FEC payload
	maxPayload := s.LinkMTU - 36
	if maxPayload <= 0 {
		maxPayload = 512 // Fallback
	}

	buf := make([]byte, maxPayload)
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// Apply backpressure to prevent shaper queue bloat and spurious NAKs
		for s.Shaper.QueueLen() > 10 {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
		}

		n, err := s.Conn.Read(buf)
		if n > 0 {
			s.updateLastActive()
			// Prepare packet data with 2-byte length header
			pktPayload := make([]byte, 2+n)
			//nolint:gosec
			binary.BigEndian.PutUint16(pktPayload[0:2], uint16(n))
			copy(pktPayload[2:], buf[:n])

			s.muSend.Lock()
			s.pendingData = append(s.pendingData, pktPayload)
			if len(s.pendingData) == 1 {
				// Start timer to flush block
				s.blockTimer = time.AfterFunc(BlockFlushTimeout, s.flushPendingBlock)
			}
			if len(s.pendingData) >= s.FEC_K {
				if s.blockTimer != nil {
					s.blockTimer.Stop()
					s.blockTimer = nil
				}
				s.flushPendingBlockLocked()
			}
			s.muSend.Unlock()
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("[Stream %d] TCP Read error: %v", s.StreamID, err)
				// Send RESET packet multiple times for redundancy
				for i := 0; i < 5; i++ {
					s.sendPacketDirect(&protocol.Packet{
						Type:     protocol.TypeReset,
						StreamID: s.StreamID,
					})
				}
				s.Close()
			}
			return
		}
	}
}

// flushPendingBlockLocked must be called under s.muSend lock
func (s *Session) flushPendingBlockLocked() {
	if len(s.pendingData) == 0 {
		return
	}

	// Pad block if we have fewer than K data shards
	for len(s.pendingData) < s.FEC_K {
		// Padding shard has length 2 (representing actual data length = 0)
		s.pendingData = append(s.pendingData, []byte{0, 0})
	}

	curM := s.curM
	grpID := s.nextGroupID
	s.nextGroupID++

	// Perform Reed-Solomon encoding
	parityShards, err := s.FEC.Encode(s.FEC_K, curM, s.pendingData)
	if err != nil {
		log.Printf("[Stream %d] FEC Encode failed: %v", s.StreamID, err)
		s.pendingData = nil
		return
	}

	packetsToSend := make([][]byte, 0, s.FEC_K+curM)

	// 1. Data packets
	for i := 0; i < s.FEC_K; i++ {
		seqNum := s.nextSeqNum
		s.nextSeqNum++

		pkt := &protocol.Packet{
			Type:     protocol.TypeData,
			StreamID: s.StreamID,
			SeqNum:   seqNum,
			GroupID:  grpID,
			Idx:      byte(i),
			IsParity: false,
			Payload:  s.pendingData[i],
		}

		// Cache data packet for HARQ retransmission
		s.harqCache[seqNum] = pkt

		buf, err := protocol.Marshal(pkt)
		if err == nil {
			packetsToSend = append(packetsToSend, buf)
		}
	}

	// 2. Parity packets
	for j := 0; j < curM; j++ {
		pkt := &protocol.Packet{
			Type:     protocol.TypeData,
			StreamID: s.StreamID,
			GroupID:  grpID,
			//nolint:gosec
			Idx:      byte(s.FEC_K + j),
			IsParity: true,
			Payload:  parityShards[j],
		}

		buf, err := protocol.Marshal(pkt)
		if err == nil {
			packetsToSend = append(packetsToSend, buf)
		}
	}

	// Clean up very old txBlocks to prevent leaks from lost LQRs
	for id := range s.txBlocks {
		if diff := grpID - id; diff > 100 && diff < 60000 {
			delete(s.txBlocks, id)
		}
	}

	// Record TX block state
	s.txBlocks[grpID] = &TxBlock{
		GroupID: grpID,
		M:       curM,
	}

	// Enqueue packets into the shaper
	s.Shaper.EnqueueMany(packetsToSend)

	// Clear pending data buffer
	s.pendingData = nil
}

func (s *Session) flushPendingBlock() {
	s.muSend.Lock()
	defer s.muSend.Unlock()
	s.flushPendingBlockLocked()
}

// HandleDataPacket processes an incoming data or parity packet, places it
// in the corresponding receive block, and attempts FEC recovery if enough shards are present.
func (s *Session) HandleDataPacket(p *protocol.Packet) {
	s.updateLastActive()
	s.muRecv.Lock()
	defer s.muRecv.Unlock()

	if s.recvClosed {
		return
	}

	grpID := p.GroupID
	if !s.hasRecv {
		s.maxRecvGrp = grpID
		s.hasRecv = true
	} else if diff := grpID - s.maxRecvGrp; diff > 0 && diff < 32768 {
		s.maxRecvGrp = grpID
	}

	if diff := s.nextWriteGrp - grpID; diff > 0 && diff < 32768 {
		return
	}

	rxBlock, ok := s.rxBlocks[grpID]
	if !ok {
		// New block received
		mSize := s.FEC_M // Allocate max potential slot size
		rxBlock = &RxBlock{
			GroupID: grpID,
			K:       s.FEC_K,
			M:       mSize,
			Packets: make([][]byte, s.FEC_K+mSize),
			SeqNums: make([]byte, s.FEC_K),
		}
		// Static initialization of SeqNums
		firstSeq := (int(grpID) * s.FEC_K) % 256
		for i := 0; i < s.FEC_K; i++ {
			//nolint:gosec
			rxBlock.SeqNums[i] = byte((firstSeq + i) % 256)
		}
		s.rxBlocks[grpID] = rxBlock

		// Set up LQR timeout
		rxBlock.LQRTimer = time.AfterFunc(s.LQRTimeoutDur, func() {
			s.handleLQRTimeout(grpID)
		})
	}

	idx := int(p.Idx)
	if idx >= len(rxBlock.Packets) {
		// Group size grew dynamically, adjust packet slice
		newPackets := make([][]byte, idx+1)
		copy(newPackets, rxBlock.Packets)
		rxBlock.Packets = newPackets
		rxBlock.M = idx + 1 - rxBlock.K
	}

	if rxBlock.Packets[idx] == nil {
		rxBlock.Packets[idx] = p.Payload
		rxBlock.ReceivedCnt++
	}

	// Trigger reconstruction if we have enough shards (>= K) and haven't decoded yet
	if rxBlock.ReceivedCnt >= rxBlock.K && !rxBlock.Decoded {
		s.tryDecodeBlock(rxBlock)
	}
}

func (s *Session) tryDecodeBlock(rxBlock *RxBlock) {
	// Count how many data shards are present
	dataOk := 0
	for i := 0; i < rxBlock.K; i++ {
		if rxBlock.Packets[i] != nil {
			dataOk++
		}
	}

	if dataOk == rxBlock.K {
		// All data shards already received, no need to reconstruct parity
		rxBlock.Decoded = true
		s.writeCond.Broadcast()
		return
	}

	// Needs reconstruction using Reed-Solomon
	// Find the maximum shard size among all received shards in the block
	maxLen := 0
	for _, p := range rxBlock.Packets {
		if p != nil && len(p) > maxLen {
			maxLen = len(p)
		}
	}
	if maxLen == 0 {
		return
	}

	m := len(rxBlock.Packets) - rxBlock.K
	shards := make([][]byte, rxBlock.K+m)
	for i := 0; i < len(shards); i++ {
		p := rxBlock.Packets[i]
		if p != nil {
			if len(p) < maxLen {
				padded := make([]byte, maxLen)
				copy(padded, p)
				shards[i] = padded
			} else {
				shards[i] = p
			}
		}
	}

	err := s.FEC.Reconstruct(rxBlock.K, m, shards)
	if err != nil {
		// Not enough shards to reconstruct yet
		return
	}

	// Copy reconstructed data back to rxBlock
	for i := 0; i < rxBlock.K; i++ {
		if rxBlock.Packets[i] == nil {
			rxBlock.Packets[i] = shards[i]
		}
	}
	rxBlock.Decoded = true
	s.writeCond.Broadcast()
}

func (s *Session) handleLQRTimeout(grpID uint16) {
	s.muRecv.Lock()
	rxBlock, ok := s.rxBlocks[grpID]
	if !ok || rxBlock.LQRReported {
		s.muRecv.Unlock()
		return
	}

	rxBlock.LQRReported = true

	// Find the maximum index of the packets received in this block
	maxIdx := -1
	for i, pktPayload := range rxBlock.Packets {
		if pktPayload != nil {
			maxIdx = i
		}
	}

	expectedTotal := rxBlock.K
	if maxIdx >= rxBlock.K {
		expectedTotal = maxIdx + 1
	}

	losses := expectedTotal - rxBlock.ReceivedCnt
	if losses < 0 {
		losses = 0
	}
	if losses > 255 {
		losses = 255
	}

	s.muRecv.Unlock()

	// Send LQR packet back
	s.sendPacketDirect(&protocol.Packet{
		Type:     protocol.TypeLqr,
		StreamID: s.StreamID,
		GroupID:  grpID,
		Losses:   byte(losses),
	})

	// Start periodic NAK loop
	var sendNakPeriodic func()
	sendNakPeriodic = func() {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		s.muRecv.Lock()
		rxBlock, ok := s.rxBlocks[grpID]
		if !ok || rxBlock.Decoded {
			s.muRecv.Unlock()
			return
		}

		if rxBlock.ReceivedCnt < rxBlock.K {
			// Send NAKs for missing data shards
			for i := 0; i < rxBlock.K; i++ {
				if rxBlock.Packets[i] == nil {
					seqNum := rxBlock.SeqNums[i]
					s.muRecv.Unlock()
					s.sendPacketDirect(&protocol.Packet{
						Type:     protocol.TypeNak,
						StreamID: s.StreamID,
						SeqNum:   seqNum,
					})
					s.muRecv.Lock()
				}
			}
		}
		s.muRecv.Unlock()

		// Schedule next periodic NAK check
		time.AfterFunc(s.NAKDelayDur, sendNakPeriodic)
	}

	// Schedule the first NAK check after NAKDelayDur
	time.AfterFunc(s.NAKDelayDur, sendNakPeriodic)
}

func (s *Session) writeTCPRun() {
	defer func() {
		s.muRecv.Lock()
		s.recvClosed = true
		s.muRecv.Unlock()
		_ = s.Conn.CloseWrite()
	}()

	for {
		s.muRecv.Lock()
		for {
			select {
			case <-s.ctx.Done():
				s.muRecv.Unlock()
				return
			default:
			}

			// Check if the next group to write is ready
			block, ok := s.rxBlocks[s.nextWriteGrp]
			if ok && block.Decoded {
				break
			}

			// If we received CLOSE, and we have written everything up to maxRecvGrp,
			// we can safely terminate the loop.
			s.muSend.Lock()
			closeRecv := s.closeRecv
			s.muSend.Unlock()

			if closeRecv {
				if !s.hasRecv {
					s.muRecv.Unlock()
					return
				}
				if diff := s.nextWriteGrp - s.maxRecvGrp; diff > 0 && diff < 32768 {
					s.muRecv.Unlock()
					return
				}
			}

			s.writeCond.Wait()
		}

		block := s.rxBlocks[s.nextWriteGrp]
		s.muRecv.Unlock()

		// Write data to TCP
		for i := 0; i < block.K; i++ {
			payload := block.Packets[i]
			if len(payload) < 2 {
				continue
			}

			// Extract actual data length
			dataLen := int(binary.BigEndian.Uint16(payload[0:2]))
			if dataLen > 0 && 2+dataLen <= len(payload) {
				_, err := s.Conn.Write(payload[2 : 2+dataLen])
				if err != nil {
					log.Printf("[Stream %d] TCP Write error: %v", s.StreamID, err)
					return
				}
			}
		}

		s.updateLastActive()

		s.muRecv.Lock()
		// Clean up processed block
		if block.LQRTimer != nil {
			block.LQRTimer.Stop()
		}
		if !block.LQRReported {
			s.muRecv.Unlock()
			s.handleLQRTimeout(s.nextWriteGrp)
			s.muRecv.Lock()
		}
		delete(s.rxBlocks, s.nextWriteGrp)
		s.nextWriteGrp++
		s.muRecv.Unlock()
	}
}

// HandleLQR processes an incoming Link Quality Report, adjusting the transmitter's
// parity size (M) dynamically using an AIMD-like algorithm based on the packet loss count.
func (s *Session) HandleLQR(p *protocol.Packet) {
	s.updateLastActive()
	s.muSend.Lock()
	defer s.muSend.Unlock()

	block, ok := s.txBlocks[p.GroupID]
	if !ok || block.LQRRecved {
		return
	}
	block.LQRRecved = true

	losses := int(p.Losses)
	atomic.StoreUint32(&s.losses, uint32(p.Losses))
	if losses == 0 {
		s.consecutiveOk++
		if s.consecutiveOk >= 5 {
			if s.curM > 0 {
				s.curM--
				s.updateShaperParams()
				log.Printf("[Stream %d] Link is clean. Decreasing parity M to %d", s.StreamID, s.curM)
			}
			s.consecutiveOk = 0
		}
	} else {
		s.consecutiveOk = 0
		newM := losses + 1
		if newM > s.FEC_M {
			newM = s.FEC_M
		}
		if s.curM < newM {
			s.curM = newM
			s.updateShaperParams()
			log.Printf("[Stream %d] Loss detected (%d packets). Increasing parity M to %d", s.StreamID, losses, s.curM)
		}
	}

	// Clean up txBlock metadata
	delete(s.txBlocks, p.GroupID)
}

func (s *Session) updateShaperParams() {
	// K + curM packets per burst
	burstSize := s.FEC_K + s.curM
	var burstInterval time.Duration
	if s.Bandwidth > 0 {
		burstInterval = time.Duration(float64(s.FEC_K+s.curM)*float64(s.LinkMTU)*8/float64(s.Bandwidth)*1000) * time.Millisecond
	} else {
		burstInterval = 0
	}
	s.Shaper.UpdateParams(burstSize, burstInterval)
}

// HandleNak processes an incoming Negative Acknowledgment (NAK) packet,
// retrieving the requested packet from cache and re-transmitting it.
func (s *Session) HandleNak(p *protocol.Packet) {
	s.updateLastActive()
	s.muSend.Lock()
	cachedPkt, ok := s.harqCache[p.SeqNum]
	s.muSend.Unlock()

	if ok {
		buf, err := protocol.Marshal(cachedPkt)
		if err == nil {
			// Re-enqueue packet for transmission (bypass normal FEC group formation)
			s.Shaper.Enqueue(buf)
			atomic.AddUint64(&s.txRetransmissions, 1)
			log.Printf("[Stream %d] HARQ retransmitting SeqNum %d", s.StreamID, p.SeqNum)
		}
	} else {
		log.Printf("[Stream %d] NAK for SeqNum %d missed (not in cache)", s.StreamID, p.SeqNum)
	}
}

// HandleClose processes an incoming Close packet, marking the receive side
// as closed and initiating session cleanup when both sides are closed.
func (s *Session) HandleClose() {
	s.muSend.Lock()
	s.closeRecv = true
	bothClosed := s.closeSent && s.closeRecv
	s.muSend.Unlock()

	s.muRecv.Lock()
	s.writeCond.Broadcast() // Wake up write loop to check closeRecv
	s.muRecv.Unlock()

	if bothClosed {
		s.Close()
	}
}

// IsClosed returns true if the session context has been canceled.
func (s *Session) IsClosed() bool {
	select {
	case <-s.ctx.Done():
		return true
	default:
		return false
	}
}

// SessionStats holds a snapshot of metrics and parameters for a stream session.
type SessionStats struct {
	StreamID          uint16 `json:"stream_id"`
	Mode              string `json:"mode"`
	TargetAddr        string `json:"target_addr"`
	CurM              int    `json:"cur_m"`
	FEC_K             int    `json:"fec_k"`
	FEC_M             int    `json:"fec_m"`
	TxBytes           uint64 `json:"tx_bytes"`
	RxBytes           uint64 `json:"rx_bytes"`
	TxPackets         uint64 `json:"tx_packets"`
	RxPackets         uint64 `json:"rx_packets"`
	Losses            uint32 `json:"losses"`
	TxRetransmissions uint64 `json:"tx_retransmissions"`
	ConsecutiveOk     int    `json:"consecutive_ok"`
	LastActive        string `json:"last_active"`
}

// GetMonitorStats returns a thread-safe snapshot of the current session metrics.
func (s *Session) GetMonitorStats() SessionStats {
	s.muSend.Lock()
	curM := s.curM
	consecutiveOk := s.consecutiveOk
	s.muSend.Unlock()

	mode := "client"
	if s.IsServer {
		mode = "server"
	}

	var target string
	if s.Conn != nil && s.Conn.RemoteAddr() != nil {
		target = s.Conn.RemoteAddr().String()
	}

	return SessionStats{
		StreamID:          s.StreamID,
		Mode:              mode,
		TargetAddr:        target,
		CurM:              curM,
		FEC_K:             s.FEC_K,
		FEC_M:             s.FEC_M,
		TxBytes:           atomic.LoadUint64(&s.txBytes),
		RxBytes:           atomic.LoadUint64(&s.rxBytes),
		TxPackets:         atomic.LoadUint64(&s.txPackets),
		RxPackets:         atomic.LoadUint64(&s.rxPackets),
		Losses:            atomic.LoadUint32(&s.losses),
		TxRetransmissions: atomic.LoadUint64(&s.txRetransmissions),
		ConsecutiveOk:     consecutiveOk,
		LastActive:        s.GetLastActive().Format(time.RFC3339),
	}
}

// RecordRx increments the received packet count and accumulates the received bytes.
func (s *Session) RecordRx(n uint64) {
	atomic.AddUint64(&s.rxPackets, 1)
	atomic.AddUint64(&s.rxBytes, n)
}
