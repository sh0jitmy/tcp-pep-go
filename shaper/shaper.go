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

package shaper

import (
	"context"
	"sync"
	"time"
)

// Shaper implements a Token Bucket packet shaper designed to smooth traffic bursts.
// It can be dynamically adapted to different MAC layer behaviors (CSMA, TDMA, WTRP).
type Shaper struct {
	mu            sync.Mutex
	burstSize     int
	burstInterval time.Duration

	queue   [][]byte
	cond    *sync.Cond
	stopped bool
}

// NewShaper creates a new Shaper instance.
func NewShaper(burstSize int, burstInterval time.Duration) *Shaper {
	s := &Shaper{
		burstSize:     burstSize,
		burstInterval: burstInterval,
		queue:         make([][]byte, 0),
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// UpdateParams dynamically updates the burst size and interval parameters.
func (s *Shaper) UpdateParams(burstSize int, burstInterval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.burstSize = burstSize
	s.burstInterval = burstInterval
}

// Enqueue adds a packet to the transmission queue.
func (s *Shaper) Enqueue(packet []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.queue = append(s.queue, packet)
	s.cond.Signal()
}

// EnqueueMany adds multiple packets to the transmission queue in order.
func (s *Shaper) EnqueueMany(packets [][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.queue = append(s.queue, packets...)
	s.cond.Signal()
}

// Stop stops the shaper loop and wakes up any waiting goroutines.
func (s *Shaper) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

// QueueLen returns the current number of packets in the shaper queue.
func (s *Shaper) QueueLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// Run starts the token bucket shaper loop.
// It retrieves packets from the queue and sends them using sendFn,
// adhering to the configured BurstSize and BurstInterval.
func (s *Shaper) Run(ctx context.Context, sendFn func([]byte) error) {
	var nextReset time.Time

	for {
		s.mu.Lock()
		for len(s.queue) == 0 && !s.stopped && ctx.Err() == nil {
			s.cond.Wait()
		}

		if s.stopped || ctx.Err() != nil {
			s.mu.Unlock()
			return
		}

		// Read current parameters
		burstSize := s.burstSize
		burstInterval := s.burstInterval
		s.mu.Unlock()

		// If burstInterval is <= 0, no shaping is applied: send all pending packets immediately.
		if burstInterval <= 0 {
			s.mu.Lock()
			pkts := s.queue
			s.queue = nil
			s.mu.Unlock()

			for _, pkt := range pkts {
				if ctx.Err() != nil {
					return
				}
				_ = sendFn(pkt)
			}
			continue
		}

		// Rate limiting: check if we need to wait before the next burst opportunity
		now := time.Now()
		if now.Before(nextReset) {
			sleepDur := nextReset.Sub(now)
			select {
			case <-ctx.Done():
				return
			case <-time.After(sleepDur):
			}
		}

		// Fetch up to burstSize packets from the queue
		s.mu.Lock()
		n := len(s.queue)
		if n > burstSize {
			n = burstSize
		}
		if n == 0 {
			s.mu.Unlock()
			continue
		}
		pkts := make([][]byte, n)
		copy(pkts, s.queue[:n])
		s.queue = s.queue[n:]
		s.mu.Unlock()

		// Schedule next burst opportunity
		nextReset = time.Now().Add(burstInterval)

		for _, pkt := range pkts {
			if ctx.Err() != nil {
				return
			}
			_ = sendFn(pkt)
		}
	}
}
