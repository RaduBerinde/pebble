// Copyright 2013 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

// Package bloom implements Bloom filters.
package bloom

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/internal/base"
)

const (
	cacheLineSize = 64
	cacheLineBits = cacheLineSize * 8
)

type tableFilter []byte

func (f tableFilter) MayContain(key []byte) bool {
	if len(f) <= 5 {
		return false
	}
	n := len(f) - 5
	nProbes := f[n]
	nLines := binary.LittleEndian.Uint32(f[n+1:])
	cacheLineBits := 8 * (uint32(n) / nLines)

	h := hash(key)
	delta := h>>17 | h<<15
	b := (h % nLines) * cacheLineBits

	for j := uint8(0); j < nProbes; j++ {
		bitPos := b + (h % cacheLineBits)
		if f[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func calculateProbes(bitsPerKey int) uint32 {
	// We intentionally round down to reduce probing cost a little bit
	n := uint32(float64(bitsPerKey) * 0.69) // 0.69 =~ ln(2)
	if n < 1 {
		n = 1
	}
	if n > 30 {
		n = 30
	}
	return n
}

// hash implements a hashing algorithm similar to the Murmur hash.
func hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(uint64(uint32(len(b))*m))
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}

	// The code below first casts each byte to a signed 8-bit integer. This is
	// necessary to match RocksDB's behavior. Note that the `byte` type in Go is
	// unsigned. What is the difference between casting a signed 8-bit value vs
	// unsigned 8-bit value into an unsigned 32-bit value?
	// Sign-extension. Consider the value 250 which has the bit pattern 11111010:
	//
	//   uint32(250)        = 00000000000000000000000011111010
	//   uint32(int8(250))  = 11111111111111111111111111111010
	//
	// Note that the original LevelDB code did not explicitly cast to a signed
	// 8-bit value which left the behavior dependent on whether C characters were
	// signed or unsigned which is a compiler flag for gcc (-funsigned-char).
	switch len(b) {
	case 3:
		h += uint32(int8(b[2])) << 16
		fallthrough
	case 2:
		h += uint32(int8(b[1])) << 8
		fallthrough
	case 1:
		h += uint32(int8(b[0]))
		h *= m
		h ^= h >> 24
	}
	return h
}

const hashBlockLen = 16384

type hashBlock [hashBlockLen]uint32

var hashBlockPool = sync.Pool{
	New: func() interface{} {
		return &hashBlock{}
	},
}

type tableFilterWriter struct {
	bitsPerKey int

	numHashes int
	// We store the hashes in blocks.
	blocks   []*hashBlock
	lastHash uint32

	// Initial "in-line" storage for the blocks slice (to avoid some small
	// allocations).
	blocksBuf [16]*hashBlock
}

func newTableFilterWriter(bitsPerKey int) *tableFilterWriter {
	w := &tableFilterWriter{
		bitsPerKey: bitsPerKey,
	}
	w.blocks = w.blocksBuf[:0]
	return w
}

// AddKey implements the base.FilterWriter interface.
func (w *tableFilterWriter) AddKey(key []byte) {
	h := hash(key)
	if w.numHashes != 0 && h == w.lastHash {
		return
	}
	ofs := w.numHashes % hashBlockLen
	if ofs == 0 {
		// Time for a new block.
		w.blocks = append(w.blocks, hashBlockPool.Get().(*hashBlock))
	}
	w.blocks[len(w.blocks)-1][ofs] = h
	w.numHashes++
	w.lastHash = h
}

// Finish implements the base.TableFilterWriter interface.
func (w *tableFilterWriter) Finish() ([]byte, base.TableFilterFamily) {
	if w.numHashes == 0 {
		return nil, Family
	}
	// The table filter format matches the RocksDB full-file filter format.
	nLines := (w.numHashes*w.bitsPerKey + cacheLineBits - 1) / (cacheLineBits)
	// Make nLines an odd number to make sure more bits are involved when
	// determining which block.
	nLines |= 1

	nBytes := nLines * cacheLineSize
	// Format:
	//   - nBytes: filter bits
	//   - 1 byte: number of probes
	//   - 4 bytes: number of lines
	filter := make([]byte, nBytes+5)

	nProbes := calculateProbes(w.bitsPerKey)
	for bIdx, b := range w.blocks {
		length := hashBlockLen
		if bIdx == len(w.blocks)-1 && w.numHashes%hashBlockLen != 0 {
			length = w.numHashes % hashBlockLen
		}
		for _, h := range b[:length] {
			delta := h>>17 | h<<15 // rotate right 17 bits
			b := (h % uint32(nLines)) * (cacheLineBits)
			for i := uint32(0); i < nProbes; i++ {
				bitPos := b + (h % cacheLineBits)
				filter[bitPos/8] |= (1 << (bitPos % 8))
				h += delta
			}
		}
	}
	filter[nBytes] = byte(nProbes)
	binary.LittleEndian.PutUint32(filter[nBytes+1:], uint32(nLines))

	// Release the hash blocks.
	for i, b := range w.blocks {
		hashBlockPool.Put(b)
		w.blocks[i] = nil
	}
	w.blocks = w.blocks[:0]
	w.numHashes = 0
	return filter, Family
}

// Family name for bloom filters. This string looks arbitrary, but its value is
// written to LevelDB .sst files, and should be this exact value to be
// compatible with those files and with the C++ LevelDB code.
const Family base.TableFilterFamily = "rocksdb.BuiltinBloomFilter"

// Decoder implements base.TableFilterDecoder for Bloom filters.
var Decoder base.TableFilterDecoder = decoderImpl{}

type decoderImpl struct{}

func (d decoderImpl) Family() base.TableFilterFamily {
	return Family
}

func (d decoderImpl) MayContain(filter, key []byte) bool {
	return tableFilter(filter).MayContain(key)
}

// TableFilterPolicy implements base.TableFilterPolicy for Bloom filters
type TableFilterPolicy struct {
	BitsPerKey int
}

var _ base.TableFilterPolicy = TableFilterPolicy{}

// FilterPolicy returns a table filter policy that creates bloom filters
// with the given number of bits per key (approximately). A good value is 10,
// which yields a filter with ~ 1% false positive rate.
func FilterPolicy(bitsPerKey int) base.TableFilterPolicy {
	return TableFilterPolicy{BitsPerKey: bitsPerKey}
}

// Name is part of the base.TableFilterPolicy interface.
func (p TableFilterPolicy) Name() string {
	if p.BitsPerKey == 10 {
		// We return rocksdb.BuiltinBloomFilter for backward compatibility.
		return string(Family)
	}
	return fmt.Sprintf("bloom(%d)", p.BitsPerKey)
}

// NewWriter is part of the base.TableFilterPolicy interface.
func (p TableFilterPolicy) NewWriter() base.TableFilterWriter {
	return newTableFilterWriter(p.BitsPerKey)
}

// PolicyFromName returns the TableFilterPolicy corresponding to the given
// name (i.e. for which TableFilterPolicy.Name() == name), or false if the
// string is not recognized as a bloom filter policy.
func PolicyFromName(name string) (_ base.TableFilterPolicy, ok bool) {
	if name == string(Family) {
		return FilterPolicy(10), true
	}
	var bitsPerKey int
	n, err := fmt.Sscanf(name, "bloom(%d)", &bitsPerKey)
	if err != nil || n != 1 || bitsPerKey < 1 {
		return nil, false
	}
	return FilterPolicy(bitsPerKey), true
}
