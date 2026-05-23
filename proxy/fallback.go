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

import "sync"

var (
	fallbackDst   string
	fallbackDstMu sync.RWMutex
)

// SetFallbackDst sets the fallback destination address used when SO_ORIGINAL_DST is not available (e.g., on macOS/Windows, or in tests).
func SetFallbackDst(addr string) {
	fallbackDstMu.Lock()
	defer fallbackDstMu.Unlock()
	fallbackDst = addr
}

func getFallbackDst() string {
	fallbackDstMu.RLock()
	defer fallbackDstMu.RUnlock()
	return fallbackDst
}
