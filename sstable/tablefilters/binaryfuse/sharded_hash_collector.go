// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"iter"

	"github.com/cockroachdb/pebble/internal/invariants"
)

const (
	maxShards           = 16
	maxDepth            = 4 // log2(maxShards)
	shardSplitThreshold = 1 << 18
)

// shardedHashCollector uses extendible hashing to shard hash collection across
// up to maxShards. We use this because building a single large binary fuse
// filter takes a lot of memory; splitting into shards yields the same
// false-positive rate with lower peak memory usage.
//
// See https://en.wikipedia.org/wiki/Extendible_hashing
//
// We use a simple function to map each hash to maxDepth bits; these bits are
// used to index into the hashCollector directory which is effectively a trie.
// The number of bits used is the globalDepth, resulting in 2^globalDepth
// directory entries. Adjacent entries in the directory are allowed to point to
// the same collector which enables resizing to be done incrementally.
//
// Each directory entry has a localDepth which is less than or equal to the
// globalDepth. If the localDepth for a shard equals the globalDepth then only a
// single directory entry points to that hash collector. Otherwise, more than
// one directory entry points to the hash collector (in which case all entries
// have the same localDepth).
//
// The diagram below shows one possible scenario for the directory and
// hash collectors. With a globalDepth of 2 the directory contains 4 entries. The
// first 2 entries have localDepth=1 and point to the same hash collector, while
// the last 2 entries point to different hash collectors.
//
//	 dir(globalDepth=2)
//	+----+
//	| 00 | --\
//	+----+    +--> hash collector (localDepth=1)
//	| 01 | --/
//	+----+
//	| 10 | ------> hash collector (localDepth=2)
//	+----+
//	| 11 | ------> hash collector (localDepth=2)
//	+----+
//
// The index into the directory is "bits(h) >> (maxDepth - globalDepth)".
//
// When a hash collector gets larger than shardSplitThreshold it is split; this
// increments the localDepth of the shards that use that hash collector. If the
// localDepth is less than or equal to its globalDepth then the newly split hash
// collectors can be installed in the directory. If the new localDepth exceeds
// the globalDepth then the globalDepth is incremented and the directory is
// reallocated at twice its current size. In the diagram above, consider what
// happens if the hash collector at dir[3] is split:
//
//	 dir(globalDepth=3)
//	+-----+
//	| 000 | --\
//	+-----+    \
//	| 001 | ----\
//	+-----+      +--> hash collector (localDepth=1)
//	| 010 | ----/
//	+-----+    /
//	| 011 | --/
//	+-----+
//	| 100 | --\
//	+-----+    +----> hash collector (localDepth=2)
//	| 101 | --/
//	+-----+
//	| 110 | --------> hash collector (localDepth=3)
//	+-----+
//	| 111 | --------> hash collector (localDepth=3)
//	+-----+
//
// Note that the diagram above is very unlikely with a good hash function as
// the buckets will tend to fill at a similar rate.
type shardedHashCollector struct {
	globalDepth uint8
	globalMask  uint8 // (1 << globalDepth) - 1
	globalShift uint8 // maxDepth - globalDepth

	// Fast path: embedded collector for single-shard case (globalDepth == 0).
	singleShard hashCollector

	// Multi-shard case: separate arrays to keep hot path fast
	// (localDepths not needed in Add hot path).
	shards      [maxShards]*hashCollector
	localDepths [maxShards]uint8
}

func (shc *shardedHashCollector) Init() {
	shc.globalDepth = 0
	shc.globalMask = 0
	shc.globalShift = maxDepth
	shc.singleShard.Init()
	shc.shards[0] = &shc.singleShard
	shc.localDepths[0] = 0
}

func (shc *shardedHashCollector) Add(h uint64) {
	if shc.globalDepth == 0 && shc.singleShard.NumHashes() < shardSplitThreshold {
		// Fast path: single shard.
		shc.singleShard.Add(h)
		return
	}

	idx := shc.shardIndex(h)
	for shc.shards[idx].NumHashes() >= shardSplitThreshold && shc.localDepths[idx] < maxDepth {
		shc.split(idx)
		idx = shc.shardIndex(h)
	}
	shc.shards[idx].Add(h) //gcassert:bce
}

//gcassert:inline
func (shc *shardedHashCollector) shardIndex(h uint64) uint8 {
	// A typical choice would be to use the high bits of the hash value. However,
	// this would be a very bad choice if the binary fuse filter implementation
	// also chooses to similarly use the high bits internally. Instead, we use a
	// mix of high and mid bits to avoid correlation.
	bits := uint8((h>>21)^(h>>45)) & (maxShards - 1)

	// Masking globalShift is a no-op but allows the compiler to elide the special
	// code path for large shift values.
	return bits >> (shc.globalShift & 7)
}

func (shc *shardedHashCollector) split(idx uint8) {
	if shc.localDepths[idx] == shc.globalDepth {
		shc.increaseGlobalDepth()
		idx <<= 1
	}

	localDepth := shc.localDepths[idx]
	old := shc.shards[idx]

	// Create two new hashCollectors.
	// split[0] will hold entries with bit (globalDepth - newLocalDepth) == 0
	// split[1] will hold entries with bit (globalDepth - newLocalDepth) == 1
	split := &[2]hashCollector{}
	split[0].Init()
	split[1].Init()

	// This collector is used by entries [startIdx, startIdx+n). We reassign these
	// to use the two new buckets.
	n := uint8(1) << (shc.globalDepth - localDepth)
	startIdx := idx &^ (n - 1)
	if invariants.Enabled {
		// Verify that the shards that use this collector are exactly those we
		// expect.
		for i := uint8(0); i <= shc.globalMask; i++ {
			if (shc.shards[i] == old) != (i >= startIdx && i < startIdx+n) {
				panic("collector mismatch")
			}
		}
	}
	for i := startIdx; i < startIdx+n/2; i++ {
		shc.shards[i] = &split[0]
		shc.localDepths[i] = localDepth + 1
	}
	for i := startIdx + n/2; i < startIdx+n; i++ {
		shc.shards[i] = &split[1]
		shc.localDepths[i] = localDepth + 1
	}

	// Redistribute hashes. Hash blocks are returned to the pool as iterated, so
	// new collectors can reuse them.
	for block := range old.BlocksAndReset() {
		for _, h := range block {
			newIdx := shc.shardIndex(h)
			shc.shards[newIdx].Add(h) //gcassert:bce
		}
	}
}

func (shc *shardedHashCollector) increaseGlobalDepth() {
	if shc.globalDepth == maxDepth {
		panic("global depth limit exceeded")
	}
	shc.globalDepth++
	shc.globalMask = (1 << shc.globalDepth) - 1
	shc.globalShift = maxDepth - shc.globalDepth

	// Double the table by duplicating pointers.
	// Process in reverse order to avoid overwriting entries before copying.
	for i := int(shc.globalMask); i >= 0; i-- {
		shc.shards[i] = shc.shards[i/2]
		shc.localDepths[i] = shc.localDepths[i/2]
	}
}

func (shc *shardedHashCollector) NumShards() int {
	return 1 << shc.globalDepth
}

// Collectors returns an iterator over the hash collectors. For each collector,
// the number of consecutive shards that use the same collector is also
// returned. The sum of these integers is NumShards().
//
// The shardedHashCollector must not be modified while the iterator is in use.
func (shc *shardedHashCollector) Collectors() iter.Seq2[*hashCollector, int] {
	return func(yield func(*hashCollector, int) bool) {
		for i := 0; i <= int(shc.globalMask); {
			n := 1 << (shc.globalDepth - shc.localDepths[i])
			if !yield(shc.shards[i], n) {
				return
			}
			i += n
		}
	}
}

// Reset releases all hash collectors.
func (shc *shardedHashCollector) Reset() {
	// Reset shards. Note that shc.singleShard either is shc.shards[0] or it has
	// already been reset.
	for hc := range shc.Collectors() {
		hc.Reset()
	}
	shc.Init()
}
