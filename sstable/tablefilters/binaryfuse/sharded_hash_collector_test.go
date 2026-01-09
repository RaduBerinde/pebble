// Copyright 2025 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package bloom

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
)

func TestShardedHashCollector(t *testing.T) {
	var shc shardedHashCollector
	datadriven.RunTest(t, "testdata/sharded_hash_collector", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init":
			shc = shardedHashCollector{}
			shc.Init()
			return formatShardedState(&shc)

		case "add":
			for line := range crstrings.LinesSeq(d.Input) {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				hashes := parseHashes(t, line)
				for _, h := range hashes {
					shc.Add(h)
				}
			}
			return formatShardedState(&shc)

		case "reset":
			shc.Reset()
			return formatShardedState(&shc)

		default:
			d.Fatalf(t, "unknown command: %s", d.Cmd)
		}
		return ""
	})
}

// parseHashes parses a hash specification which can be:
//   - Single: "x/y" -> hash value x<<45 | y
//   - Range: "x/y1-y2" -> hashes x/y1, x/y1+1, ..., x/y2
func parseHashes(t *testing.T, s string) []uint64 {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 {
		t.Fatalf("invalid hash format %q, expected x/y or x/y1-y2", s)
	}
	x, err := strconv.ParseUint(parts[0], 10, 8)
	if err != nil || x > 15 {
		t.Fatalf("invalid shard bits %q in %q", parts[0], s)
	}

	// Check for range.
	if rangeParts := strings.SplitN(parts[1], "-", 2); len(rangeParts) == 2 {
		y1, err1 := strconv.ParseUint(rangeParts[0], 10, 64)
		y2, err2 := strconv.ParseUint(rangeParts[1], 10, 64)
		if err1 != nil || err2 != nil || y1 > y2 {
			t.Fatalf("invalid range %q in %q", parts[1], s)
		}
		result := make([]uint64, 0, y2-y1+1)
		for y := y1; y <= y2; y++ {
			result = append(result, x<<45|y)
		}
		return result
	}

	// Single hash.
	y, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		t.Fatalf("invalid y value %q in %q", parts[1], s)
	}
	return []uint64{x<<45 | y}
}

// formatHash converts a hash back to x/y format.
func formatHash(h uint64) string {
	x := ((h >> 21) ^ (h >> 45)) & 15
	y := h & ((1 << 45) - 1)
	return fmt.Sprintf("%d/%d", x, y)
}

// formatHashRange formats a slice of hashes, collapsing consecutive y values
// with the same x into ranges.
func formatHashRange(hashes []uint64) string {
	if len(hashes) == 0 {
		return "(empty)"
	}

	// Group hashes by their x value and sort y values.
	type hashGroup struct {
		x   uint64
		ys  []uint64
	}
	groups := make(map[uint64]*hashGroup)
	var groupOrder []uint64

	for _, h := range hashes {
		x := ((h >> 21) ^ (h >> 45)) & 15
		y := h & ((1 << 45) - 1)
		if g, ok := groups[x]; ok {
			g.ys = append(g.ys, y)
		} else {
			groups[x] = &hashGroup{x: x, ys: []uint64{y}}
			groupOrder = append(groupOrder, x)
		}
	}

	// Sort groups by x and sort ys within each group.
	slices.Sort(groupOrder)
	for _, g := range groups {
		slices.Sort(g.ys)
	}

	var parts []string
	for _, x := range groupOrder {
		g := groups[x]
		// Collapse consecutive y values into ranges.
		ys := g.ys
		for len(ys) > 0 {
			start := ys[0]
			end := start
			i := 1
			for i < len(ys) && ys[i] == end+1 {
				end = ys[i]
				i++
			}
			if start == end {
				parts = append(parts, fmt.Sprintf("%d/%d", x, start))
			} else {
				parts = append(parts, fmt.Sprintf("%d/%d-%d", x, start, end))
			}
			ys = ys[i:]
		}
	}
	return strings.Join(parts, " ")
}

// formatShardedState returns a string representation of the sharded collector state.
func formatShardedState(shc *shardedHashCollector) string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "globalDepth=%d\n", shc.globalDepth)

	// Iterate through collectors.
	idx := 0
	for hc, n := range shc.Collectors() {
		// Build the directory entry range.
		endIdx := idx + n - 1
		var dirStr string
		if idx == endIdx {
			dirStr = fmt.Sprintf("[%d]", idx)
		} else {
			dirStr = fmt.Sprintf("[%d-%d]", idx, endIdx)
		}

		// Collect all hashes from this collector.
		var hashes []uint64
		if hc.NumHashes() > 0 {
			for block := range hc.Blocks() {
				hashes = append(hashes, block...)
			}
		}

		fmt.Fprintf(&buf, "  %-8s localDepth=%d hashes: %s\n",
			dirStr, shc.localDepths[idx], formatHashRange(hashes))

		idx += n
	}
	return buf.String()
}
