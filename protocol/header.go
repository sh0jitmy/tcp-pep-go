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

package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// PacketType represents the type identifier of a PEP protocol packet.
type PacketType byte

const (
	// TypeConnect represents a TCP transparent proxy connection request.
	TypeConnect PacketType = 0x0
	// TypeConnAck represents a connection acknowledgment response.
	TypeConnAck PacketType = 0x1
	// TypeConnErr represents a connection failure response.
	TypeConnErr PacketType = 0x2
	// TypeData represents a segmented payload or parity packet.
	TypeData PacketType = 0x3
	// TypeClose represents a graceful connection close message.
	TypeClose PacketType = 0x4
	// TypeReset represents an abrupt connection termination message.
	TypeReset PacketType = 0x5
	// TypeNak represents a Negative Acknowledgment request for missing sequence numbers.
	TypeNak PacketType = 0x6
	// TypeLqr represents a Link Quality Report carrying block loss counts.
	TypeLqr PacketType = 0x7
)

// String returns the string representation of the PacketType.
func (t PacketType) String() string {
	switch t {
	case TypeConnect:
		return "CONNECT"
	case TypeConnAck:
		return "CONN_ACK"
	case TypeConnErr:
		return "CONN_ERR"
	case TypeData:
		return "DATA"
	case TypeClose:
		return "CLOSE"
	case TypeReset:
		return "RESET"
	case TypeNak:
		return "NAK"
	case TypeLqr:
		return "LQR"
	default:
		return fmt.Sprintf("UNKNOWN(0x%x)", byte(t))
	}
}

// Packet represents a unified structure for all PEP protocol packet variants.
type Packet struct {
	Type     PacketType
	StreamID uint16

	// CONNECT / CONN_ERR
	Addr string

	// DATA
	SeqNum   byte
	GroupID  uint16
	Idx      byte // 5 bits (0-31)
	IsParity bool // T (3 bits): 1=DATA, 2=PARITY
	Payload  []byte

	// LQR
	Losses byte
}

// Marshal serializes a Packet structure into a binary byte slice.
func Marshal(p *Packet) ([]byte, error) {
	if p.StreamID > 0x0FFF {
		return nil, fmt.Errorf("stream ID %d exceeds 12-bit limit", p.StreamID)
	}

	base0 := (byte(p.Type) << 4) | byte(p.StreamID>>8)
	base1 := byte(p.StreamID & 0xFF)

	switch p.Type {
	case TypeConnect, TypeConnErr:
		addrBytes := []byte(p.Addr)
		if len(addrBytes) > 255 {
			return nil, errors.New("address string too long (max 255 bytes)")
		}
		buf := make([]byte, 3+len(addrBytes))
		buf[0] = base0
		buf[1] = base1
		//nolint:gosec
		buf[2] = byte(len(addrBytes))
		copy(buf[3:], addrBytes)
		return buf, nil

	case TypeConnAck, TypeClose, TypeReset:
		buf := make([]byte, 2)
		buf[0] = base0
		buf[1] = base1
		return buf, nil

	case TypeData:
		buf := make([]byte, 6+len(p.Payload))
		buf[0] = base0
		buf[1] = base1
		buf[2] = p.SeqNum
		binary.BigEndian.PutUint16(buf[3:5], p.GroupID)
		tVal := byte(1)
		if p.IsParity {
			tVal = byte(2)
		}
		buf[5] = (p.Idx << 3) | (tVal & 0x07)
		copy(buf[6:], p.Payload)
		return buf, nil

	case TypeNak:
		buf := make([]byte, 3)
		buf[0] = base0
		buf[1] = base1
		buf[2] = p.SeqNum
		return buf, nil

	case TypeLqr:
		buf := make([]byte, 6)
		buf[0] = base0
		buf[1] = base1
		binary.BigEndian.PutUint16(buf[2:4], p.GroupID)
		buf[4] = p.Losses
		buf[5] = 0 // Reserved
		return buf, nil

	default:
		return nil, fmt.Errorf("unknown packet type %v", p.Type)
	}
}

// Unmarshal parses a binary byte slice into a Packet structure.
func Unmarshal(buf []byte) (*Packet, error) {
	if len(buf) < 2 {
		return nil, errors.New("packet too short for base header")
	}

	pType := PacketType(buf[0] >> 4)
	streamID := (uint16(buf[0]&0x0F) << 8) | uint16(buf[1])

	p := &Packet{
		Type:     pType,
		StreamID: streamID,
	}

	switch pType {
	case TypeConnect, TypeConnErr:
		if len(buf) < 3 {
			return nil, fmt.Errorf("%s packet too short for length field", pType)
		}
		length := int(buf[2])
		if len(buf) < 3+length {
			return nil, fmt.Errorf("%s packet too short for address payload", pType)
		}
		p.Addr = string(buf[3 : 3+length])

	case TypeConnAck, TypeClose, TypeReset:
		// Base header is sufficient

	case TypeData:
		if len(buf) < 6 {
			return nil, errors.New("DATA packet too short for header")
		}
		p.SeqNum = buf[2]
		p.GroupID = binary.BigEndian.Uint16(buf[3:5])
		p.Idx = buf[5] >> 3
		tVal := buf[5] & 0x07
		p.IsParity = tVal == 2
		p.Payload = make([]byte, len(buf)-6)
		copy(p.Payload, buf[6:])

	case TypeNak:
		if len(buf) < 3 {
			return nil, errors.New("NAK packet too short")
		}
		p.SeqNum = buf[2]

	case TypeLqr:
		if len(buf) < 6 {
			return nil, errors.New("LQR packet too short")
		}
		p.GroupID = binary.BigEndian.Uint16(buf[2:4])
		p.Losses = buf[4]

	default:
		return nil, fmt.Errorf("unknown packet type %v", pType)
	}

	return p, nil
}
