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

package config

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// RouteConfig represents a routing rule mapping an original destination address
// (either a host IP, IP:port, or CIDR block) to a corresponding Server-PEP UDP address.
type RouteConfig struct {
	OriginalDst string `yaml:"original_dst"`
	ServerPEP   string `yaml:"server_pep"`
}

// Config represents the top-level YAML configuration structure containing route definitions.
type Config struct {
	Routes []RouteConfig `yaml:"routes"`
}

type subnetRoute struct {
	ipNet     *net.IPNet
	serverPEP string
}

// Router maintains the matching tables (exact matches and subnet matches) for routing decisions.
type Router struct {
	mu           sync.RWMutex
	exactMatches map[string]string
	subnets      []subnetRoute
}

// LoadConfig reads the YAML configuration file from path, parses the routing table,
// and returns an initialized Router instance.
func LoadConfig(path string) (*Router, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	router := &Router{
		exactMatches: make(map[string]string),
	}

	for _, r := range cfg.Routes {
		if strings.Contains(r.OriginalDst, "/") {
			_, ipNet, err := net.ParseCIDR(r.OriginalDst)
			if err != nil {
				return nil, fmt.Errorf("invalid CIDR route %s: %w", r.OriginalDst, err)
			}
			router.subnets = append(router.subnets, subnetRoute{
				ipNet:     ipNet,
				serverPEP: r.ServerPEP,
			})
		} else {
			router.exactMatches[r.OriginalDst] = r.ServerPEP
		}
	}

	return router, nil
}

// Lookup queries the routing tables for the given destination address (host:port or host)
// and returns the target Server-PEP UDP address. Returns an error if no match is found.
func (r *Router) Lookup(destAddr string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 1. Exact match (host:port or host)
	if pep, ok := r.exactMatches[destAddr]; ok {
		return pep, nil
	}

	// Try splitting host and port
	host, _, err := net.SplitHostPort(destAddr)
	if err == nil {
		if pep, ok := r.exactMatches[host]; ok {
			return pep, nil
		}
	} else {
		host = destAddr
	}

	// 2. Subnet match
	ip := net.ParseIP(host)
	if ip != nil {
		for _, subnet := range r.subnets {
			if subnet.ipNet.Contains(ip) {
				return subnet.serverPEP, nil
			}
		}
	}

	return "", fmt.Errorf("no route found for destination %s", destAddr)
}
