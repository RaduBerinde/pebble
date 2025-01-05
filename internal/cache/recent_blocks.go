// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"github.com/cockroachdb/crlib/fifo"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/invariants"
	"github.com/cockroachdb/pebble/internal/manual"
)

// recentBlocks stores a FIFO queue of recently written blocks corresponding to
// a shard. It assumes that the shard mutex is held when calling its methods.
type recentBlocks struct {
	limit int64
	// total is the sum of len(value.buf) for all entries in the queue.
	total int64
	queue fifo.Queue[recentBlock]
	byKey map[key]manual.Buf
}

type recentBlock struct {
	key key
	// data is a buffer allocated with manual.New(manual.BlockCacheRecentlyWritten, ..).
	data manual.Buf
}

var recentBlocksBackingPool = fifo.MakeQueueBackingPool[recentBlock]()

func (rb *recentBlocks) Init(limit int64) {
	rb.limit = limit
	rb.total = 0
	rb.queue = fifo.MakeQueue[recentBlock](&recentBlocksBackingPool)
	rb.byKey = make(map[key]manual.Buf)
}

// Add a block to the recent blocks cache. Older blocks are evicted as
// necessary. The recent blocks cache takes ownership of the buffers and will
// free them when each block falls out of the cache (possibly even during
// this function call).
func (rb *recentBlocks) Add(blocks ...recentBlock) {
	for i := range blocks {
		rb.total += int64(blocks[i].data.Len())
	}
	for rb.total > rb.limit {
		if rb.queue.Len() == 0 {
			// We removed everything from the queue, but we are still over the limit.
			// Drop one of the new blocks.
			rb.total -= int64(blocks[0].data.Len())
			manual.Free(manual.BlockCacheRecentlyWritten, blocks[0].data)
			blocks[0] = recentBlock{}
			blocks = blocks[1:]
			continue
		}

		oldest := rb.queue.PeekFront()
		manual.Free(manual.BlockCacheRecentlyWritten, oldest.data)
		rb.total -= int64(oldest.data.Len())
		delete(rb.byKey, oldest.key)

		rb.queue.PopFront()
	}
	for i := range blocks {
		rb.queue.PushBack(blocks[i])
		rb.byKey[blocks[i].key] = blocks[i].data
	}
}

func (rb *recentBlocks) Lookup(k key) manual.Buf {
	return rb.byKey[k]
}

func (rb *recentBlocks) Free() {
	for rb.queue.Len() > 0 {
		manual.Free(manual.BlockCacheRecentlyWritten, rb.queue.PeekFront().data)
		rb.queue.PopFront()
	}
	*rb = recentBlocks{}
}

// RecentBlocksCollector is used to populate the shards' recent blocks with
// newly written blocks.
type RecentBlocksCollector struct {
	shards []shard
	blocks [][]recentBlock
}

const recentBlocksCollectorChunkSize = 16

// NewRecentBlocksCollector creates a RecentBlocksCollector.
//
// Add is used to add recently written blocks to the collector. Finish must be
// called at the end.
func NewRecentBlocksCollector(c *Cache) *RecentBlocksCollector {
	rbc := &RecentBlocksCollector{
		shards: c.shards,
		blocks: make([][]recentBlock, len(c.shards)),
	}
	blocksBuf := make([]recentBlock, recentBlocksCollectorChunkSize*len(c.shards))
	for i := range rbc.blocks {
		rbc.blocks[i] = blocksBuf[:0:recentBlocksCollectorChunkSize]
		blocksBuf = blocksBuf[recentBlocksCollectorChunkSize:]
	}
	if invariants.UseFinalizers {
		invariants.SetFinalizer(rbc, func(rbc *RecentBlocksCollector) {
			for i := range rbc.blocks {
				if len(rbc.blocks[i]) != 0 {
					panic("RecentBlocksCollector not finished")
				}
			}
		})
	}
	return rbc
}

// Add a recently written block. The data buffer will be freed when the block
// falls out of the recent blocks cache (possibly even during this function
// call).
func (rbc *RecentBlocksCollector) Add(
	id ID, fileNum base.DiskFileNum, offset uint64, data manual.Buf,
) {
	k := makeKey(id, fileNum, offset)
	b := recentBlock{key: k, data: data}
	shardIdx := k.shardIdx(len(rbc.shards))
	rbc.blocks[shardIdx] = append(rbc.blocks[shardIdx], b)
	if len(rbc.blocks[shardIdx]) == recentBlocksCollectorChunkSize {
		rbc.flush(shardIdx)
	}
}

// Finish must be called after all blocks have been added. It needs to be called
// even if the operation failed and we don't need the blocks (otherwise, any
// accumulated blocks that were not flushed will be leaked).
func (rbc *RecentBlocksCollector) Finish() {
	for i := range rbc.blocks {
		if len(rbc.blocks[i]) > 0 {
			rbc.flush(i)
		}
	}
}

func (rbc *RecentBlocksCollector) flush(shardIdx int) {
	shard := &rbc.shards[shardIdx]
	shard.mu.Lock()
	shard.recentBlocks.Add(rbc.blocks[shardIdx]...)
	shard.mu.Unlock()
	rbc.blocks[shardIdx] = rbc.blocks[shardIdx][:0]
}
