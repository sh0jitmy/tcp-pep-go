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

package fec

import (
	"fmt"
	"sync"

	"github.com/klauspost/reedsolomon"
)

// FEC wraps Klauspost's Reed-Solomon encoder/decoder and maintains a cache
// of encoder instances optimized for varying parity and data shard combinations.
type FEC struct {
	mu       sync.Mutex
	encoders map[string]reedsolomon.Encoder
}

// NewFEC initializes and returns a new FEC instance.
func NewFEC() *FEC {
	return &FEC{
		encoders: make(map[string]reedsolomon.Encoder),
	}
}

func (f *FEC) getEncoder(k, m int) (reedsolomon.Encoder, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := fmt.Sprintf("%d,%d", k, m)
	if enc, ok := f.encoders[key]; ok {
		return enc, nil
	}

	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, err
	}
	f.encoders[key] = enc
	return enc, nil
}

// Encode generates m parity shards from k data shards.
// Each data shard can have different lengths; they are padded to the maximum length.
func (f *FEC) Encode(k, m int, data [][]byte) ([][]byte, error) {
	if m <= 0 {
		return nil, nil
	}

	enc, err := f.getEncoder(k, m)
	if err != nil {
		return nil, err
	}

	maxLen := 0
	for _, d := range data {
		if len(d) > maxLen {
			maxLen = len(d)
		}
	}
	if maxLen == 0 {
		maxLen = 1
	}

	shards := make([][]byte, k+m)
	for i := 0; i < k; i++ {
		shards[i] = make([]byte, maxLen)
		if i < len(data) && data[i] != nil {
			copy(shards[i], data[i])
		}
	}
	for i := 0; i < m; i++ {
		shards[k+i] = make([]byte, maxLen)
	}

	if err := enc.Encode(shards); err != nil {
		return nil, err
	}

	return shards[k : k+m], nil
}

// Reconstruct reconstructs the missing shards in shards slice (which must be of size k+m).
// Missing shards should be nil.
func (f *FEC) Reconstruct(k, m int, shards [][]byte) error {
	if m <= 0 {
		return fmt.Errorf("cannot reconstruct with m=%d", m)
	}

	enc, err := f.getEncoder(k, m)
	if err != nil {
		return err
	}

	return enc.Reconstruct(shards)
}
