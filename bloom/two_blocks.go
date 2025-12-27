// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/internal/base"
)

// buildTwoBlocksFilter builds a two-block bloom filter with the given number of
// lines and probes per block.
func buildTwoBlocksFilter(nLines, nProbes uint32, hc *hashCollector) []byte {
	nBytes := nLines * cacheLineSize
	// Format:
	//   - nBytes: filter bits
	//   - 1 byte: number of probes
	//   - 4 bytes: number of lines
	filter := make([]byte, nBytes+5)
	bits := aliasFilterBits(filter, nLines)
	for b := range hc.Blocks() {
		for _, h := range b {
			bits.set(nProbes, h)
			bits.set(nProbes, remix32(h))
		}
	}
	filter[nBytes] = byte(nProbes)
	binary.LittleEndian.PutUint32(filter[nBytes+1:], nLines)
	return filter
}

// twoBlocksMayContain reports whether the given key may be contained in the
// two-block bloom filter, created by buildFilter.
func twoBlocksMayContain(filter []byte, h uint32) bool {
	if len(filter) <= 5 {
		return false
	}
	n := len(filter) - 5
	nProbes := filter[n]
	nLines := binary.LittleEndian.Uint32(filter[n+1:])
	if 8*(uint32(n)/nLines) != cacheLineBits {
		panic("bloom filter: unexpected cache line size")
	}
	bits := aliasFilterBits(filter, nLines)
	if !bits.probe(nProbes, h) {
		return false
	}
	// Only a very small fraction of negatives will this far.
	return bits.probe(nProbes, remix32(h))
}

// remix32 applies a mixing function to a 32-bit hash value.
func remix32(x uint32) uint32 {
	// MurmurHash3 32-bit finalizer.
	x ^= x >> 16
	x *= 0x85ebca6b
	x ^= x >> 13
	x *= 0xc2b2ae35
	x ^= x >> 16
	return x
}

// This table contains the optimal number of probes for each bitsPerKey. For
// bits per key over 20, probes[20] should be used.
//
// The values are derived from simulations (see simulation.md).
var twoBlocksProbes = [21]uint32{
	1:  1,
	2:  1,
	3:  1,
	4:  1,
	5:  2,
	6:  2,
	7:  2,
	8:  3,
	9:  3,
	10: 3,
	11: 4,
	12: 4,
	13: 4,
	14: 4,
	15: 5,
	16: 5,
	17: 5,
	18: 5,
	19: 6,
	20: 6,
}

func calculateTwoBlocksProbes(bitsPerKey uint32) uint32 {
	return twoBlocksProbes[min(bitsPerKey, uint32(len(twoBlocksProbes)-1))]
}

// tableFilterWriter implements base.TableFilterWriter for Bloom filters.
type twoBlocksTableFilterWriter struct {
	bitsPerKey uint32
	numProbes  uint32

	hc hashCollector
}

func newTwoBlocksTableFilterWriter(bitsPerKey uint32) *twoBlocksTableFilterWriter {
	w := &twoBlocksTableFilterWriter{}
	w.init(bitsPerKey)
	return w
}

func (w *twoBlocksTableFilterWriter) init(bitsPerKey uint32) {
	w.bitsPerKey = bitsPerKey
	w.numProbes = calculateTwoBlocksProbes(bitsPerKey)
	w.hc.Init()
}

// AddKey implements the base.FilterWriter interface.
func (w *twoBlocksTableFilterWriter) AddKey(key []byte) {
	w.hc.Add(hash(key))
}

// Finish implements the base.twoBlocksTableFilterWriter interface.
func (w *twoBlocksTableFilterWriter) Finish() (_ []byte, _ base.TableFilterFamily, ok bool) {
	numHashes := w.hc.NumHashes()
	if numHashes == 0 {
		return nil, "", false
	}
	// The table filter format matches the RocksDB full-file filter format.
	nLines := calculateNumLines(numHashes, w.bitsPerKey)
	filter := buildTwoBlocksFilter(nLines, w.numProbes, &w.hc)
	w.hc.Reset()
	return filter, FamilyTwoBlocks, true
}

// FamilyTwoBlocks is the family name for bloom filters that use two cache lines
// for each key. This yields a much better false-positive ratio for higher (10+)
// bits-per-key, but is not supported by older versions.
const FamilyTwoBlocks base.TableFilterFamily = "bloom2"

type twoBlocksFilterPolicyImpl struct {
	BitsPerKey uint32
}

// TwoBlocksFilterPolicy is a base.TableFilterPolicy that creates two-block
// bloom filters with the given number of bits per key (approximately). For
// bits-per-key values under 10, the single-block FilterPolicy is preferable.
//
// The table below contains false positive rates for various bits-per-key values
// (obtained from simulations). Note that these rates don't take into account
// the additional chance of 32-bit hash collision, which is
// <num-hashes-in-block> / 2^32.
//
//	Bits/key | Probes |        FPR
//	---------+--------+-------------------
//	       1 |   1x2  | 72.4% (1 in 1.38)
//	       2 |   1x2  | 38.8% (1 in 2.58)
//	       3 |   1x2  | 23.1% (1 in 4.33)
//	       4 |   1x2  | 15.2% (1 in 6.59)
//	       5 |   2x2  | 8.98% (1 in 11.1)
//	       6 |   2x2  | 5.50% (1 in 18.2)
//	       7 |   2x2  | 3.54% (1 in 28.3)
//	       8 |   3x2  | 2.15% (1 in 46.5)
//	       9 |   3x2  | 1.34% (1 in 74.9)
//	      10 |   3x2  | 0.856% (1 in 117)
//	      11 |   4x2  | 0.537% (1 in 186)
//	      12 |   4x2  | 0.337% (1 in 296)
//	      13 |   4x2  | 0.218% (1 in 458)
//	      14 |   4x2  | 0.144% (1 in 693)
//	      15 |   5x2  | 0.093% (1 in 1080)
//	      16 |   5x2  | 0.061% (1 in 1638)
//	      17 |   5x2  | 0.041% (1 in 2443)
//	      18 |   5x2  | 0.028% (1 in 3555)
//	      19 |   6x2  | 0.019% (1 in 5177)
//	      20 |   6x2  | 0.013% (1 in 7519)
func TwoBlocksFilterPolicy(bitsPerKey uint32) base.TableFilterPolicy {
	if bitsPerKey < 1 {
		panic(fmt.Sprintf("invalid bitsPerKey %d", bitsPerKey))
	}
	return twoBlocksFilterPolicyImpl{BitsPerKey: bitsPerKey}
}

var _ base.TableFilterPolicy = twoBlocksFilterPolicyImpl{}

// Name is part of the base.TableFilterPolicy interface.
func (p twoBlocksFilterPolicyImpl) Name() string {
	return fmt.Sprintf("bloom2(%d)", p.BitsPerKey)
}

// NewWriter is part of the base.TableFilterPolicy interface.
func (p twoBlocksFilterPolicyImpl) NewWriter() base.TableFilterWriter {
	return newTwoBlocksTableFilterWriter(p.BitsPerKey)
}

// TwoBlocksDecoder implements base.TableFilterDecoder for 2-block Bloom
// filters.
var TwoBlocksDecoder base.TableFilterDecoder = twoBlocksDecoderImpl{}

type twoBlocksDecoderImpl struct{}

func (d twoBlocksDecoderImpl) Family() base.TableFilterFamily {
	return FamilyTwoBlocks
}

func (d twoBlocksDecoderImpl) MayContain(filter, key []byte) bool {
	if len(filter) <= 5 {
		return false
	}
	n := len(filter) - 5
	nProbes := filter[n]
	nLines := binary.LittleEndian.Uint32(filter[n+1:])
	if 8*(uint32(n)/nLines) != cacheLineBits {
		panic("bloom filter: unexpected cache line size")
	}
	h := hash(key)
	bits := aliasFilterBits(filter, nLines)
	if bits.probe(nProbes, h) {
		return true
	}
	// Note: the vast majority of negatives will be excluded by the time we get
	// here.
	h = remix32(h)
	return bits.probe(nProbes, h)
}
