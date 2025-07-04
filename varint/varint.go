// Package varint encodes and decodes SQLite4 variable length integers
// which is an encoding of 64-bit unsigned integers into 1-9 bytes
// (varuint).
// See: http://www.sqlite.org/src4/doc/trunk/www/varint.wiki
package varint

// Uvarint decodes the received varint encoded byte slice; returning
// the value and the amount of bytes used.
//
// From: http://www.sqlite.org/src4/doc/trunk/www/varint.wiki
//
// Decode:
//
//    If A0 is between 0 and 240 inclusive, then the result is the value of A0.
//    If A0 is between 241 and 248 inclusive, then the result is 240+256*(A0-241)+A1.
//    If A0 is 249 then the result is 2288+256*A1+A2.
//    If A0 is 250 then the result is A1..A3 as a 3-byte big-endian integer.
//    If A0 is 251 then the result is A1..A4 as a 4-byte big-endian integer.
//    If A0 is 252 then the result is A1..A5 as a 5-byte big-endian integer.
//    If A0 is 253 then the result is A1..A6 as a 6-byte big-endian integer.
//    If A0 is 254 then the result is A1..A7 as a 7-byte big-endian integer.
//    If A0 is 255 then the result is A1..A8 as a 8-byte big-endian integer.

// Size returns the number of bytes that would be needed to encode x.
// This function follows the same encoding rules as PutUvarint but only returns
// the size without performing the actual encoding.
func Size(x uint64) int {
	if x < 241 {
		return 1
	}
	if x < 2288 {
		return 2
	}
	if x < 67824 {
		return 3
	}
	if x < 1<<24 {
		return 4
	}
	if x < 1<<32 {
		return 5
	}
	if x < 1<<40 {
		return 6
	}
	if x < 1<<48 {
		return 7
	}
	if x < 1<<56 {
		return 8
	}
	return 9
}

func Read(buf []byte) (uint64, int) {
	// check the first byte
	if buf[0] <= 0xF0 {
		return uint64(buf[0]), 1
	}
	if buf[0] <= 0xF8 {
		if len(buf) < 2 {
			return 0, 0
		}
		return 240 + 256*(uint64(buf[0])-241) + uint64(buf[1]), 2
	}
	if buf[0] == 0xF9 {
		if len(buf) < 3 {
			return 0, 0
		}
		return 2288 + 256*uint64(buf[1]) + uint64(buf[2]), 3
	}
	if buf[0] == 0xFA {
		if len(buf) < 4 {
			return 0, 0
		}
		return uint64(buf[1])<<16 | uint64(buf[2])<<8 | uint64(buf[3]), 4
	}
	if buf[0] == 0xFB {
		if len(buf) < 5 {
			return 0, 0
		}
		return uint64(buf[1])<<24 | uint64(buf[2])<<16 | uint64(buf[3])<<8 | uint64(buf[4]), 5
	}
	if buf[0] == 0xFC {
		if len(buf) < 6 {
			return 0, 0
		}
		return uint64(buf[1])<<32 | uint64(buf[2])<<24 | uint64(buf[3])<<16 | uint64(buf[4])<<8 | uint64(buf[5]), 6
	}
	if buf[0] == 0xFD {
		if len(buf) < 7 {
			return 0, 0
		}
		return uint64(buf[1])<<40 | uint64(buf[2])<<32 | uint64(buf[3])<<24 | uint64(buf[4])<<16 | uint64(buf[5])<<8 | uint64(buf[6]), 7
	}
	if buf[0] == 0xFE {
		if len(buf) < 8 {
			return 0, 0
		}
		return uint64(buf[1])<<48 | uint64(buf[2])<<40 | uint64(buf[3])<<32 | uint64(buf[4])<<24 | uint64(buf[5])<<16 | uint64(buf[6])<<8 | uint64(buf[7]), 8
	}

	if len(buf) < 9 {
		return 0, 0
	}
	return uint64(buf[1])<<56 | uint64(buf[2])<<48 | uint64(buf[3])<<40 | uint64(buf[4])<<32 | uint64(buf[5])<<24 | uint64(buf[6])<<16 | uint64(buf[7])<<8 | uint64(buf[8]), 9
}

// PutUvarint encodes the received uint64 into varint using the minimum
// necessary bytes.  The number of bytes written is returned.
//
// From: http://www.sqlite.org/src4/doc/trunk/www/varint.wiki
//
// Encode:
//
//    Let the input value be V.
//
//    If V<=240 then output a single by A0 equal to V.
//    If V<=2287 then output A0 as (V-240)/256 + 241 and A1 as (V-240)%256.
//    If V<=67823 then output A0 as 249, A1 as (V-2288)/256, and A2 as (V-2288)%256.
//    If V<=16777215 then output A0 as 250 and A1 through A3 as a big-endian 3-byte integer.
//    If V<=4294967295 then output A0 as 251 and A1..A4 as a big-endian 4-byte integer.
//    If V<=1099511627775 then output A0 as 252 and A1..A5 as a big-endian 5-byte integer.
//    If V<=281474976710655 then output A0 as 253 and A1..A6 as a big-endian 6-byte integer.
//    If V<=72057594037927935 then output A0 as 254 and A1..A7 as a big-endian 7-byte integer.
//    Otherwise then output A0 as 255 and A1..A8 as a big-endian 8-byte integer.
func Write(buf []byte, x uint64) int {
	if x < 241 {
		buf[0] = byte(x)
		return 1
	}
	if x < 2288 {
		buf[0] = byte((x-240)/256 + 241)
		buf[1] = byte((x - 240) % 256)
		return 2
	}
	if x < 67824 {
		buf[0] = 0xF9
		buf[1] = byte((x - 2288) / 256)
		buf[2] = byte((x - 2288) % 256)
		return 3
	}
	if x < 1<<24 {
		buf[0] = 0xFA
		buf[1] = byte(x >> 16)
		buf[2] = byte(x >> 8)
		buf[3] = byte(x)
		return 4
	}
	if x < 1<<32 {
		buf[0] = 0xFB
		buf[1] = byte(x >> 24)
		buf[2] = byte(x >> 16)
		buf[3] = byte(x >> 8)
		buf[4] = byte(x)
		return 5
	}
	if x < 1<<40 {
		buf[0] = 0xFC
		buf[1] = byte(x >> 32)
		buf[2] = byte(x >> 24)
		buf[3] = byte(x >> 16)
		buf[4] = byte(x >> 8)
		buf[5] = byte(x)
		return 6
	}
	if x < 1<<48 {
		buf[0] = 0xFD
		buf[1] = byte(x >> 40)
		buf[2] = byte(x >> 32)
		buf[3] = byte(x >> 24)
		buf[4] = byte(x >> 16)
		buf[5] = byte(x >> 8)
		buf[6] = byte(x)
		return 7
	}
	if x < 1<<56 {
		buf[0] = 0xFE
		buf[1] = byte(x >> 48)
		buf[2] = byte(x >> 40)
		buf[3] = byte(x >> 32)
		buf[4] = byte(x >> 24)
		buf[5] = byte(x >> 16)
		buf[6] = byte(x >> 8)
		buf[7] = byte(x)
		return 8
	}
	buf[0] = 0xFF
	buf[1] = byte(x >> 56)
	buf[2] = byte(x >> 48)
	buf[3] = byte(x >> 40)
	buf[4] = byte(x >> 32)
	buf[5] = byte(x >> 24)
	buf[6] = byte(x >> 16)
	buf[7] = byte(x >> 8)
	buf[8] = byte(x)
	return 9
}

// Wrapper to Write()
func Encode(buf []byte, x uint64) int {
	return Write(buf, x)
}

// Wrapper to Read()
func Decode(buf []byte) (uint64, int) {
	return Read(buf)
}
