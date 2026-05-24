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
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strings"
)

// HTTPServer implements the built-in HTTP/HTTPS server to expose live PEP statistics as JSON.
type HTTPServer struct {
	addr     string
	peps     []PEPInstance
	certFile string
	keyFile  string
	server   *http.Server
	ln       net.Listener
}

// NewHTTPServer instantiates a new HTTPServer monitoring daemon.
func NewHTTPServer(addr string, certFile, keyFile string, peps ...PEPInstance) *HTTPServer {
	return &HTTPServer{
		addr:     addr,
		certFile: certFile,
		keyFile:  keyFile,
		peps:     peps,
	}
}

// Start listens on the configured address and runs the HTTP request multiplexer.
func (s *HTTPServer) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = ln
	s.addr = ln.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleStats)
	mux.HandleFunc("/stats", s.handleStats)

	s.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 0, // Keep default, but can be configured if needed
	}

	go func() {
		log.Printf("[HTTPServer] Starting HTTP monitoring server on %s (TLS=%t)", s.addr, s.certFile != "" && s.keyFile != "")
		var serveErr error
		if s.certFile != "" && s.keyFile != "" {
			serveErr = s.server.ServeTLS(ln, s.certFile, s.keyFile)
		} else {
			serveErr = s.server.Serve(ln)
		}
		if serveErr != nil && !strings.Contains(serveErr.Error(), "use of closed network connection") {
			log.Printf("[HTTPServer] Server stopped with error: %v", serveErr)
		}
	}()

	return nil
}

// Stop shut down the embedded HTTP server and closes its TCP listener socket.
func (s *HTTPServer) Stop() {
	if s.server != nil {
		_ = s.server.Close()
	}
	if s.ln != nil {
		_ = s.ln.Close()
	}
}

// Addr returns the resolved local address (host:port) the embedded server is listening on.
func (s *HTTPServer) Addr() string {
	return s.addr
}

func (s *HTTPServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	allStats := make(map[uint16]SessionStats)
	for _, pep := range s.peps {
		for id, sess := range pep.GetSessions() {
			allStats[id] = sess.GetMonitorStats()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(allStats); err != nil {
		log.Printf("[HTTPServer] Failed to encode JSON: %v", err)
	}
}
