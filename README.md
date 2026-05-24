# TCP-PEP-GO: Adaptive FEC & Hybrid ARQ Accelerator for Ad-hoc Narrowband Networks

[![Go Reference](https://pkg.go.dev/badge/github.com/sh0jitmy/tcp-pep-go.svg)](https://pkg.go.dev/github.com/sh0jitmy/tcp-pep-go)
[![Go Test Status](https://github.com/sh0jitmy/tcp-pep-go/actions/workflows/test.yml/badge.svg)](https://github.com/sh0jitmy/tcp-pep-go/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/sh0jitmy/tcp-pep-go)](https://goreportcard.com/report/github.com/sh0jitmy/tcp-pep-go)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

TCP-PEP-GO is a high-performance TCP Performance Enhancing Proxy (PEP) daemon written in Go, specifically optimized for high-packet-loss, high-latency, and narrowband wireless ad-hoc networks.

It features transparent TCP interception, Reed-Solomon Forward Error Correction (RS-FEC), Hybrid ARQ (NAK-based retransmission), dynamic adaptive parity sizing via Link Quality Reports (LQR), and a token-bucket traffic shaper customizable for different MAC layer architectures (CSMA, TDMA, WTRP).

## Key Features

1. **Sessionless UDP Encapsulation**: Intercepts TCP streams transparently and wraps them into structured UDP packets with low overhead.
2. **Adaptive Forward Error Correction (Adaptive FEC)**:
   - Encodes data blocks using Reed-Solomon erasure coding ($K$ data shards to $M$ parity shards).
   - Dynamically scales parity shards $M$ down to $0$ on clean links to completely eliminate overhead.
   - Automatically increases parity $M$ up to the maximum configuration limit when packet losses are detected.
3. **Hybrid ARQ (HARQ)**:
   - Instantly triggers target packet NAK retransmissions for missing sequence numbers before waiting for TCP timeouts.
4. **Token-Bucket Traffic Shaper**: Paces packet bursts to prevent congestion and queuing delays at the bottleneck radio link.
5. **Dynamic Routing Config & SIGHUP Reload**: Manages Client-to-Server PEP mappings via CIDR/subnets in a YAML file, with hot-reloading support upon SIGHUP.
6. **Built-in Redis Monitoring Server**: Embeds a Redis-protocol-compatible RESP server inside the daemon. You can query real-time stream status, parity size, and traffic stats using any standard Redis client (like `redis-cli`) without deploying an external database.

---

## Architecture Overview

```mermaid
graph LR
    App[TCP Client] <-->|TCP Interception| CPEP[Client-PEP]
    CPEP <-->|Paced RS-FEC + HARQ over UDP| SPEP[Server-PEP]
    SPEP <-->|TCP Relay| Srv[Target TCP Server]
    
    Monitor[redis-cli / Grafana] <-->|RESP protocol| CPEP
```

---

## Installation & Build

### Prerequisites
- Go 1.25 or later

### Building the Daemon
Use the provided `Makefile` to build and test:

```bash
# Build the binary
make build

# Run unit and E2E integration tests
make test
```

### Static Analysis & Vulnerability Scan
```bash
# Run golangci-lint
make lintcheck

# Run govulncheck dependency vulnerability scans
make vulncheck
```

---

## Configuration

In client mode, the mapping of target TCP destinations to their corresponding Server-PEPs is defined in `routes.yaml`:

```yaml
routes:
  # Route specific host
  - original_dst: "192.168.1.100:80"
    server_pep: "10.0.0.2:20000"
  
  # Route subnet
  - original_dst: "172.16.0.0/16"
    server_pep: "10.0.0.3:20000"
```

To reload this routing table dynamically without stopping active proxy connections:
```bash
kill -HUP <PID_OF_TCP_PEP_DAEMON>
```

---

## Command Line Options

The PEP daemon supports the following command-line flags:

| Option | Type | Default | Description |
| :--- | :---: | :---: | :--- |
| `-mode` | string | `client` | PEP operation mode. Either `client` (TCP transparent proxy intercepting) or `server` (UDP listening and TCP termination). |
| `-listen` | string | `:10080` | Listen address. For `client` mode, it is the TCP transparent proxy listen address. For `server` mode, it is the UDP encapsulation listening address. |
| `-routes` | string | `routes.yaml` | Path to the routing table YAML file mapping original TCP destinations to Server-PEP UDP addresses (client mode only). |
| `-mtu` | int | `1200` | Link MTU size (maximum payload size for UDP packets). Allowed range: 100 to 1500. |
| `-bandwidth` | int | `128000` | Link bandwidth limit in bps (e.g., `128000` for 128 kbps). Set to `0` to disable the token-bucket shaper. |
| `-fec-k` | int | `10` | FEC data shards $K$ (number of data packets in one coding block). |
| `-fec-m` | int | `3` | FEC maximum parity shards $M$ (maximum number of parity packets in one block, defining the upper bound for adaptive scaling). |
| `-idle-timeout` | int | `300` | Session idle timeout in seconds before resources are automatically cleaned up. |
| `-redis-addr` | string | `:6379` | Address of the embedded Redis (RESP) monitoring server. Set to an empty string to disable. |
| `-http-addr` | string | `:8080` | Address of the embedded HTTP/HTTPS monitoring server. Set to an empty string to disable. |
| `-http-cert` | string | `""` | Path to the SSL certificate file (.crt) for HTTPS monitoring. If set, HTTPS is enabled. |
| `-http-key` | string | `""` | Path to the SSL private key file (.key) for HTTPS monitoring. Must be provided alongside `-http-cert`. |

---

## Usage

### 1. Server-PEP Mode
Start the PEP daemon on the server side (near the target destination):
```bash
./tcp-pep-daemon \
  -mode server \
  -listen :20000 \
  -mtu 1200 \
  -bandwidth 128000 \
  -fec-k 5 \
  -fec-m 2
```

### 2. Client-PEP Mode
Start the PEP daemon on the client side (intercepting transparently redirected TCP traffic):
```bash
./tcp-pep-daemon \
  -mode client \
  -listen :10080 \
  -routes routes.yaml \
  -mtu 1200 \
  -bandwidth 128000 \
  -fec-k 5 \
  -fec-m 2 \
  -redis-addr :6379
```

---

## Real-time Monitoring via Built-in HTTP/HTTPS Interface

The Client-PEP and Server-PEP daemons spin up a lightweight embedded HTTP/HTTPS server (on port `:8080` by default or as configured via `-http-addr`).

You can fetch live session statistics in JSON format by sending a GET request to `/` or `/stats`.

### 1. Querying over HTTP
```bash
$ curl http://127.0.0.1:8080/stats
```
or
```bash
$ curl http://127.0.0.1:8080/
```

### 2. JSON Response Example
```json
{
  "1": {
    "stream_id": 1,
    "mode": "client",
    "target_addr": "127.0.0.1:8080",
    "cur_m": 0,
    "fec_k": 5,
    "fec_m": 2,
    "tx_bytes": 109840,
    "rx_bytes": 109840,
    "tx_packets": 110,
    "rx_packets": 110,
    "tx_retransmissions": 0,
    "losses": 0,
    "consecutive_ok": 12,
    "last_active": "2026-05-23T22:31:49Z"
  }
}
```

### 3. Querying over HTTPS
When certificate and key paths are specified:
```bash
$ ./tcp-pep-daemon -http-addr :8443 -http-cert server.crt -http-key server.key
```
In this case, query the endpoint using HTTPS (use `-k` or `--insecure` if you are using self-signed certificates):
```bash
$ curl -k https://127.0.0.1:8443/stats
```

---

## Real-time Monitoring via Built-in Redis Interface

The Client-PEP daemon spins up a lightweight embedded Redis-protocol compatible server (on port `:6379` by default or as configured via `-redis-addr`). 

You can query live session statistics, adaptive parity statuses, and network traffic volume directly using `redis-cli`:

### Querying active stream IDs
```bash
$ redis-cli SMEMBERS tcp-pep:active_streams
1) "1"
2) "2"
```

### Querying individual session statistics
```bash
$ redis-cli HGETALL tcp-pep:session:1
 1) "stream_id"
 2) "1"
 3) "mode"
 4) "client"
 5) "target_addr"
 6) "127.0.0.1:8080"
 7) "cur_m"               # Current adaptive parity size (M)
 8) "0"
 9) "fec_k"
10) "5"
11) "fec_m"
12) "2"
13) "tx_bytes"            # Accumulated transmitted bytes (UDP)
14) "109840"
15) "rx_bytes"            # Accumulated received bytes (UDP)
16) "109840"
17) "tx_packets"
18) "110"
19) "rx_packets"
20) "110"
21) "losses"              # Last reported packet loss count from LQR
22) "0"
23) "consecutive_ok"      # Consecutive error-free blocks
24) "12"
25) "last_active"
26) "2026-05-23T22:31:49Z"
```

### Checking server keys
```bash
$ redis-cli KEYS "*"
1) "tcp-pep:active_streams"
2) "tcp-pep:session:1"
```

---

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](file:///Users/shjtmy/gravity/tcp-pep/LICENSE) for the full license text.
