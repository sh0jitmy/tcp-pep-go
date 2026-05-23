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

//go:build linux
// +build linux

package proxy

import (
	"fmt"
	"net"
	"syscall"
	"unsafe"
)

const (
	SO_ORIGINAL_DST      = 80
	IP6T_SO_ORIGINAL_DST = 80
)

// GetOriginalDST returns the original destination address of a transparently proxied TCP connection in Linux.
func GetOriginalDST(conn *net.TCPConn) (string, error) {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return "", err
	}

	var originalAddr string
	var errGetSockOpt error

	err = rawConn.Control(func(fd uintptr) {
		// Try IPv4 first
		val := syscall.RawSockaddrInet4{}
		sz := uint32(unsafe.Sizeof(val))
		_, _, errno := syscall.Syscall6(
			syscall.SYS_GETSOCKOPT,
			fd,
			syscall.SOL_IP,
			SO_ORIGINAL_DST,
			uintptr(unsafe.Pointer(&val)),
			uintptr(unsafe.Pointer(&sz)),
			0,
		)
		if errno == 0 {
			ip := net.IP(val.Addr[:])
			port := int(val.Port[0])<<8 | int(val.Port[1])
			originalAddr = fmt.Sprintf("%s:%d", ip.String(), port)
			return
		}

		// Try IPv6
		val6 := syscall.RawSockaddrInet6{}
		sz6 := uint32(unsafe.Sizeof(val6))
		_, _, errno = syscall.Syscall6(
			syscall.SYS_GETSOCKOPT,
			fd,
			syscall.SOL_IPV6,
			IP6T_SO_ORIGINAL_DST,
			uintptr(unsafe.Pointer(&val6)),
			uintptr(unsafe.Pointer(&sz6)),
			0,
		)
		if errno == 0 {
			ip := net.IP(val6.Addr[:])
			port := int(val6.Port[0])<<8 | int(val6.Port[1])
			originalAddr = net.JoinHostPort(ip.String(), fmt.Sprint(port))
			return
		}

		errGetSockOpt = errno
	})

	if err != nil {
		return "", err
	}
	if errGetSockOpt != nil {
		return "", fmt.Errorf("getsockopt SO_ORIGINAL_DST failed: %w", errGetSockOpt)
	}
	return originalAddr, nil
}
