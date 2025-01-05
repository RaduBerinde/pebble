// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package cache

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manual"
)

func TestRecentBlocks(t *testing.T) {
	var rb recentBlocks

	datadriven.RunTest(t, "testdata/recent_blocks", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			var limit int64
			td.ScanArgs(t, "limit", &limit)
			rb.Init(limit)

		case "add":
			var blocks []recentBlock
			for _, l := range crstrings.Lines(td.Input) {
				var id, fileNum, offset, size int
				_, err := fmt.Sscanf(l, "%d:%d:%d %d", &id, &fileNum, &offset, &size)
				if err != nil {
					td.Fatalf(t, "expected an integer size: %q", l)
				}
				k := makeKey(ID(id), base.DiskFileNum(fileNum), uint64(offset))
				data := manual.New(manual.BlockCacheRecentlyWritten, uintptr(size))
				blocks = append(blocks, recentBlock{key: k, data: data})
			}
			rb.Add(blocks...)
			return recentBlocksStr(&rb)
		default:
			td.Fatalf(t, "invalid command: %s", td.Cmd)
		}
		return ""
	})
}

func recentBlocksStr(rb *recentBlocks) string {
	var queue []recentBlock
	for rb.queue.Len() > 0 {
		queue = append(queue, *rb.queue.PeekFront())
		rb.queue.PopFront()
	}
	// Restore the queue.
	for _, b := range queue {
		rb.queue.PushBack(b)
	}
	var buf strings.Builder
	fmt.Fprintf(&buf, "total: %d/%d\n", rb.total, rb.limit)
	for _, b := range queue {
		fmt.Fprintf(&buf, "%d:%d:%d size=%d\n", b.key.id, b.key.fileNum, b.key.offset, b.data.Len())
	}
	return buf.String()
}

func TestRecentBlocksCollector(t *testing.T) {
	counter := 0
	c := &Cache{}
	var rbc *RecentBlocksCollector

	datadriven.RunTest(t, "testdata/recent_blocks_collector", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			var limit int64
			var shards int
			td.ScanArgs(t, "limit", &limit)
			td.ScanArgs(t, "shards", &shards)
			c.shards = make([]shard, shards)
			for i := range c.shards {
				c.shards[i].recentBlocks.Init(limit)
			}
			rbc = NewRecentBlocksCollector(c)

		case "add":
			for _, l := range crstrings.Lines(td.Input) {
				var size, num int
				_, err := fmt.Sscanf(l, "size=%d num=%d", &size, &num)
				if err != nil {
					td.Fatalf(t, "invalid line: %q", l)
				}
				for i := 0; i < num; i++ {
					counter++
					data := manual.New(manual.BlockCacheRecentlyWritten, uintptr(size))
					rbc.Add(ID(1+counter/100), base.DiskFileNum(1+counter%100/10), uint64(counter%10), data)
				}
			}

		case "finish":
			rbc.Finish()

		default:
			td.Fatalf(t, "invalid command: %s", td.Cmd)
		}

		var bld strings.Builder
		for i := range c.shards {
			fmt.Fprintf(&bld, "shard %d:\n", i)
			for _, l := range crstrings.Lines(recentBlocksStr(&c.shards[i].recentBlocks)) {
				fmt.Fprintf(&bld, "  %s\n", l)
			}
		}
		return bld.String()
	})
}
