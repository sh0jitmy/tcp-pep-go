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

//go:build !linux
// +build !linux

package proxy

import (
	"net"
	"sync"
)

var (
	fallbackDst   = "127.0.0.1:8080"
	fallbackDstMu sync.RWMutex
)

// SetFallbackDst sets the fallback destination address used when SO_ORIGINAL_DST is not available (e.g., on macOS/Windows).
func SetFallbackDst(addr string) {
	fallbackDstMu.Lock()
	defer fallbackDstMu.Unlock()
	fallbackDst = addr
}

// GetOriginalDST returns a fallback destination address for testing and cross-platform compatibility.
func GetOriginalDST(conn *net.TCPConn) (string, error) {
	fallbackDstMu.RLock()
	defer fallbackDstMu.RUnlock()
	return fallbackDst, nil
}
