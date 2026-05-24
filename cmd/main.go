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

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"tcp-pep-go/config"
	"tcp-pep-go/proxy"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("[PEP Main] %v", err)
	}
}

func run() error {
	mode := flag.String("mode", "client", "PEP mode: client or server")
	listenAddr := flag.String("listen", ":10080", "TCP transparent proxy listen address (client) or UDP listen address (server)")
	routesPath := flag.String("routes", "routes.yaml", "Path to routing table YAML config (client mode only)")
	mtu := flag.Int("mtu", 1200, "Link MTU size (100-1500)")
	bandwidth := flag.Int("bandwidth", 128000, "Link bandwidth in bps (e.g. 128000 for 128kbps, 0 to disable shaper)")
	fecK := flag.Int("fec-k", 10, "FEC Data Shards K")
	fecM := flag.Int("fec-m", 3, "FEC Parity Shards M")
	idleTimeoutSec := flag.Int("idle-timeout", 300, "Idle session timeout in seconds")
	redisAddr := flag.String("redis-addr", ":6379", "Embedded Redis monitoring server address (empty to disable)")
	httpAddr := flag.String("http-addr", ":8080", "Embedded HTTP monitoring server address (empty to disable)")
	httpCert := flag.String("http-cert", "", "Path to SSL certificate file for HTTPS monitoring")
	httpKey := flag.String("http-key", "", "Path to SSL private key file for HTTPS monitoring")

	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	idleTimeout := time.Duration(*idleTimeoutSec) * time.Second

	log.Printf("[PEP Main] Starting in %s mode (MTU=%d, Bandwidth=%d bps, FEC K=%d, M=%d)", *mode, *mtu, *bandwidth, *fecK, *fecM)

	var clientPEP *proxy.ClientPEP
	var serverPEP *proxy.ServerPEP

	switch *mode {
	case "client":
		// Load routing config
		router, err := config.LoadConfig(*routesPath)
		if err != nil {
			return fmt.Errorf("failed to load routes from %s: %w", *routesPath, err)
		}
		log.Printf("[PEP Main] Loaded routing config from %s", *routesPath)

		clientPEP = proxy.NewClientPEP(ctx, *listenAddr, router, *mtu, *bandwidth, *fecK, *fecM, idleTimeout)
		if err := clientPEP.Start(); err != nil {
			return fmt.Errorf("client-PEP start failed: %w", err)
		}
		defer clientPEP.Stop()

	case "server":
		serverPEP = proxy.NewServerPEP(ctx, *listenAddr, *mtu, *bandwidth, *fecK, *fecM, idleTimeout)
		if err := serverPEP.Start(); err != nil {
			return fmt.Errorf("server-PEP start failed: %w", err)
		}
		defer serverPEP.Stop()

	default:
		return fmt.Errorf("invalid mode: %s. Must be 'client' or 'server'", *mode)
	}

	if *redisAddr != "" {
		var pepInst proxy.PEPInstance
		if *mode == "client" {
			pepInst = clientPEP
		} else {
			pepInst = serverPEP
		}
		redisServer := proxy.NewRedisServer(*redisAddr, pepInst)
		if err := redisServer.Start(); err != nil {
			log.Printf("[PEP Main] Embedded Redis server failed to start: %v", err)
		} else {
			defer redisServer.Stop()
		}
	}

	if *httpAddr != "" {
		var pepInst proxy.PEPInstance
		if *mode == "client" {
			pepInst = clientPEP
		} else {
			pepInst = serverPEP
		}
		httpServer := proxy.NewHTTPServer(*httpAddr, *httpCert, *httpKey, pepInst)
		if err := httpServer.Start(); err != nil {
			log.Printf("[PEP Main] Embedded HTTP server failed to start: %v", err)
		} else {
			defer httpServer.Stop()
		}
	}

	// Wait for OS signals
	for {
		sig := <-sigChan
		if sig == syscall.SIGHUP {
			if *mode == "client" && clientPEP != nil {
				log.Printf("[PEP Main] SIGHUP received. Reloading routing config from %s...", *routesPath)
				newRouter, err := config.LoadConfig(*routesPath)
				if err != nil {
					log.Printf("[PEP Main] Reload failed: %v. Keeping old routing table.", err)
				} else {
					clientPEP.UpdateRouter(newRouter)
				}
			}
		} else {
			log.Printf("[PEP Main] Received signal %v, shutting down...", sig)
			break
		}
	}
	return nil
}
